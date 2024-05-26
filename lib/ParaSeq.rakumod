# Intended to be as fast as possible and to someday become part of the
# Rakudo core
use v6.*;
use nqp;

my uint $default-batch  = 16;
my uint $default-degree = Kernel.cpu-cores-but-one;
my uint $target-nsecs   = 500_000;  # .0005 second

# Helper sub to determine granularity of batches depending on the signature
# of a callable
my sub granularity(&callable) {
    my $count := &callable.count;
    $count == Inf ?? 1 !! $count
}

# Shortcut for long names, just for esthetics
my constant IB = IterationBuffer;
my constant IE = IterationEnd;

# An empty IterationBuffer, for various places where it is needed as a source
my constant emptyIB = nqp::create(IB);

#- ParaQueue -------------------------------------------------------------------
# A blocking concurrent queue to which one can nqp::push and from which one
# can nqp::shift
my class ParaQueue is repr('ConcBlockingQueue') { }

#- BufferIterator --------------------------------------------------------------
# An iterator that takes a buffer and an iterator, and first produces all
# of the values from the buffer, and then from the iterator

my class BufferIterator does Iterator {
    has $!parent;    # ParaSeq parent object
    has $!buffer;    # first buffer to produce from
    has $!iterator;  # iterator to produce from onwards

    method new(\parent, \buffer, \iterator) {
        my $self := nqp::create(self);
        nqp::bindattr($self,BufferIterator,'$!parent',  parent  );
        nqp::bindattr($self,BufferIterator,'$!buffer',  buffer  );
        nqp::bindattr($self,BufferIterator,'$!iterator',iterator);
        $self
    }

    method pull-one() {
        nqp::elems($!buffer)
          ?? nqp::shift($!buffer)
          !! $!iterator.pull-one
    }

    method skip-at-least(uint $skipping) {

        # Need to skip more than in initial buffer
        my uint $elems  = nqp::elems($!buffer);
        if $skipping > $elems {
            nqp::setelems($!buffer,0);
            $!iterator.skip-at-least($skipping - $elems)
        }

        # Less than in inital buffer, remove so many entries from it
        else {
            nqp::splice($!buffer,emptyIB,0,$skipping);
            1
        }
    }

    method push-all(\target) {
        nqp::istype(target,IB)
          ?? nqp::splice(target,$!buffer,nqp::elems(target),0)
          !! $!buffer.iterator.push-all(target);
        nqp::setelems($!buffer,0);

        $!iterator.push-all(target)
    }

    method stats(--> emptyIB) { }
}

#- ParaIterator ----------------------------------------------------------------
# An iterator that takes a number of ParaQueues and produces all values
# from them until the last ParaQueue has been exhausted.  Note that the
# queue of queues is also a ParaQueue so that queues can also be added
# and removed in a thread-safe manner

my class ParaIterator does Iterator {
    has           $!parent;     # ParaSeq parent object
    has           $!current;    # current producer
    has ParaQueue $!queues;     # other queues to produce from
    has ParaQueue $!pressure;   # backpressure provider
    has ParaQueue $.waiting;    # queue with unprocessed ParaStats objects
    has IB        $!stats;      # list with processed ParaStats objects
    has uint      $!batch;      # initial / current batch size
    has uint      $!auto;       # 1 if batch size automatically adjust on load
    has uint      $!processed;  # number of items processed so far
    has uint      $!produced;   # number of items produced so far
    has uint      $!nsecs;      # nano seconds wallclock used so far
    has uint      $!todo;       # number of items left to deliver
    has atomicint $!stop;       # stop all processing if 1

    method new(
           \parent,
           \pressure,
      uint $batch,
      uint $auto,
      uint $todo,
    ) {
        my $self := nqp::create(self);

        nqp::bindattr($self,ParaIterator,'$!parent',  parent                );
        nqp::bindattr($self,ParaIterator,'$!current', emptyIB               );
        nqp::bindattr($self,ParaIterator,'$!queues',  nqp::create(ParaQueue));
        nqp::bindattr($self,ParaIterator,'$!pressure',pressure              );
        nqp::bindattr($self,ParaIterator,'$!waiting', nqp::create(ParaQueue));
        nqp::bindattr($self,ParaIterator,'$!stats',   nqp::create(IB)       );

        nqp::bindattr_i($self,ParaIterator,'$!batch', $batch );
        nqp::bindattr_i($self,ParaIterator,'$!auto',  $auto  );
        nqp::bindattr_i($self,ParaIterator,'$!todo',  $todo  );
        $self
    }

    # Thread-safely handle next batch
    method !next-batch(\next) {

        # Next buffer to be processed
        my $buffer := nqp::shift(next);

        # Complete shutdown requested
        if nqp::atomicload_i($!stop) {
            return emptyIB;
        }

        # Need to monitor number of results to deliver
        elsif $!todo {
            my int $todo = nqp::sub_i($!todo,nqp::elems($buffer));
            if $todo <= 0 {
                # Stop all processing, we have enough
                self.stop;

                # Get rid of all values we don't need
                return nqp::splice(
                  $buffer,
                  emptyIB,
                  nqp::add_i(nqp::elems($buffer),$todo),
                  nqp::neg_i($todo)
                );
            }
            else {
                $!todo = $todo;
            }
        }

        # Nothing else ready to be delivered
        if nqp::isnull(nqp::atpos($!waiting,0)) {

            # Initiate more work using last batch value calculated
            nqp::push($!pressure,$!batch);
        }

        # At least one further batch ready to be delivered
        else {

            # Fetch statistics of batches that have been produced until now
            my uint $processed = $!processed;
            my uint $produced  = $!produced;
            my uint $nsecs     = $!nsecs;
            nqp::until(
              nqp::isnull(nqp::atpos($!waiting,0)),
              nqp::stmts(
                nqp::push($!stats,my $stats := nqp::shift($!waiting)),
                ($processed = nqp::add_i($processed,$stats.processed)),
                ($produced  = nqp::add_i($produced, $stats.produced )),
                ($nsecs     = nqp::add_i($nsecs,    $stats.nsecs    ))
              )
            );

            # Update total statistics
            $!processed = $processed;
            $!produced  = $produced;
            $!nsecs     = $nsecs;

            # Update batch size
            if $!auto {
                $!batch =
                  nqp::div_i(nqp::mul_i($processed,$target-nsecs),$nsecs)
                  if $nsecs;
            }

            # Initiate more work with updated batch size
            nqp::push($!pressure,$!batch);
        }

        $buffer
    }

    method pull-one() {
        nqp::if(
          nqp::elems($!current),
          (my $pulled := nqp::shift($!current)),
          nqp::if(
            nqp::eqaddr((my $next := nqp::shift($!queues)),IE),
            (return IE),
            nqp::stmts(
              ($!current := self!next-batch($next)),
              ($pulled   := self.pull-one)
            )
          )
        );

        $pulled
    }

    method skip-at-least(uint $skipping) {
        my      $current  := $!current;
        my      $queues   := $!queues;
        my uint $toskip    = $skipping;

        nqp::while(
          1,
          nqp::stmts(
            (my uint $elems = nqp::elems($current)),
            nqp::if(
              $elems > $toskip,  # can't ignore whole buffer, only part
              nqp::stmts(
                ($!current := nqp::splice($current, nqp::list, 0, $toskip)),
                (return 1)
              )
            ),
            nqp::if(             # ignored whole buffer, fetch next
              nqp::eqaddr((my $next := nqp::shift($queues)),IE),
              nqp::stmts(
                nqp::setelems($!current,0),  # invalidate current contents
                (return 0)                   # really done
              )
            ),
            ($toskip = nqp::sub_i($toskip,$elems)),
            ($current := $!current := self!next-batch($next))
          )
        );
    }

    proto method push-all(|) {*}

    # Optimized version appending to an IterationBuffer
    multi method push-all(IterationBuffer:D \target) {
        my $current  := $!current;
        my $queues   := $!queues;

        nqp::while(
          1,
          nqp::stmts(
            nqp::splice(target,$current,nqp::elems(target),0),
            nqp::if(
              nqp::eqaddr((my $next := nqp::shift($queues)),IE),
              (return IE),
              ($current := self!next-batch($next))
            )
          )
        );
    }

    # Slower generic version that needs to coerce each buffer to a List
    # to ensure the correct semantics with .append
    multi method push-all(\target) {
        my $current  := $!current;
        my $queues   := $!queues;

        nqp::while(
          1,
          nqp::stmts(
            target.append($current.List),
            nqp::if(
              nqp::eqaddr((my $next := nqp::shift($queues)),IE),
              (return IE),
              ($current := self!next-batch($next))
            )
          )
        );
    }

    method close-queues(ParaIterator:D:) is implementation-detail {
        nqp::push($!queues,IE)
    }

    method stats(ParaIterator:D:) is implementation-detail { $!stats }

    method stop(ParaIterator:D:) is implementation-detail {
        nqp::atomicstore_i($!stop,1);  # stop producing
        nqp::unshift($!queues,IE);     # stop reading from queues
        $!parent.stop;                 # stop all threads
    }

    # Add a semaphore to the queue.  This is a ParaQueue to which a fully
    # processed IterationBuffer will be pushed when it is ready.  This
    # guarantees that the order of the results will be the order in which
    # this method will be called.
    method semaphore() is implementation-detail {
        nqp::push($!queues,nqp::create(ParaQueue))
    }
}

#- ParaStats -------------------------------------------------------------------
# A class for keeping stats about the production of a single result queue
# from a batch of values

class ParaStats {
    has uint $.threadid;
    has uint $.ordinal;
    has uint $.processed;
    has uint $.produced;
    has uint $.nsecs;

    method new(uint $ordinal, uint $processed, uint $produced, uint $nsecs) {
        my $self := nqp::create(self);
        nqp::bindattr_i($self, ParaStats, '$!threadid',
          nqp::threadid(nqp::currentthread)
        );

        nqp::bindattr_i($self, ParaStats, '$!ordinal',   $ordinal  );
        nqp::bindattr_i($self, ParaStats, '$!processed', $processed);
        nqp::bindattr_i($self, ParaStats, '$!produced',  $produced );
        nqp::bindattr_i($self, ParaStats, '$!nsecs',     $nsecs    );
        $self
    }

    method average-nsecs(ParaStats:D:) { $!nsecs div $!processed }

    multi method gist(ParaStats:D:) {
        "#$!threadid - $!ordinal: $!processed / $!produced ($!nsecs nsecs)"
    }
}

#- ParaSeq ---------------------------------------------------------------------
# The class containing all of the logic for parallel sequences
class ParaSeq does Sequence {
    has           $!buffer;      # first buffer
    has           $!source;      # iterator producing source values
    has           $!result;      # iterator producing result values
    has           $!SCHEDULER;   # $*SCHEDULER to be used
    has uint      $.batch;       # initial batch size, must be > 0
    has uint      $!auto;        # 1 if batch size automatically adjusts
    has uint      $.degree;      # number of CPUs, must be > 1
    has uint      $!stop-after;  # stop after these number of values
    has atomicint $!stop;        # stop all processing if 1
    has atomicint $.discarded;   # produced values discarded because of stop
 
#- private helper methods ------------------------------------------------------

    # Do error checking and set up object if all ok
    method !setup(
       str $method,
      uint $batch,
      uint $auto,
      uint $degree,
      uint $stop-after,
           $buffer,
           $source
    ) is hidden-from-backtrace {
        X::Invalid::Value.new(:$method, :name<batch>,  :value($batch)).throw
          if $batch <= 0;
        X::Invalid::Value.new(:$method, :name<degree>, :value($degree)).throw
          if $degree <= 1;

        # Set it up!
        my $self := nqp::create(self);
        nqp::bindattr_i($self, ParaSeq, '$!batch',      $batch     );
        nqp::bindattr_i($self, ParaSeq, '$!auto',       $auto      );
        nqp::bindattr_i($self, ParaSeq, '$!degree',     $degree    );
        nqp::bindattr_i($self, ParaSeq, '$!stop-after', $stop-after);

        nqp::bindattr($self, ParaSeq, '$!SCHEDULER', $*SCHEDULER         );
        nqp::bindattr($self, ParaSeq, '$!buffer',    nqp::decont($buffer));
        nqp::bindattr($self, ParaSeq, '$!source',    $source             );
        $self
    }

    # Start the async process with the given processor logic and the
    # required granularity for producing values
    method !start(&processor, uint $granularity = 1) {

        # Logic for making sure batch size has correct granularity
        my &granulize := $granularity == 1
          ?? -> uint $batch { $batch }
          !! -> uint $batch {
                nqp::mod_i($batch,$granularity)
                  ?? nqp::mul_i(nqp::div_i($batch,$granularity),$granularity)
                       || $granularity
                  !! $batch
             }

        # Local copy of first buffer, make sure it is granulized correctly
        my $first := $!buffer;
        $!buffer  := Mu;  # release buffer in object
        nqp::until(
          nqp::elems($first) %% $granularity
            || nqp::eqaddr((my $pulled := $!source.pull-one),IE),
          nqp::push($first,$pulled)
        );

        # Batch size allowed to vary, so set up back pressure queue
        # with some simple variation
        my $pressure  := nqp::create(ParaQueue);
        my uint $batch = $!batch;
        if $!auto {
            my uint $half = nqp::div_i($batch,2) || $batch;
            $batch = 0;

            nqp::push($pressure,granulize($batch = $batch + $half))
              for ^$!degree;
        }

        # Fixed batch size, so set up back pressure queue with fixed values
        else {
            $batch = granulize($batch);
            nqp::push($pressure,$batch) for ^$!degree;
        }

        # Queue the first buffer we already filled, and set up the
        # result iterator
        processor(
          0,
          $first,
          ($!result := ParaIterator.new(
            self, $pressure, $!batch, $!auto, $!stop-after
          )).semaphore
        );

        # Make sure the scheduling of further batches actually happen in a
        # separate thread
        $!SCHEDULER.cue: {
            my uint $ordinal;    # ordinal number of batch
            my uint $exhausted;  # flag: 1 if exhausted

            # some shortcuts
            my $source := $!source;
            my $result := $!result;

            # Until we're halted or have a buffer that's not full
            nqp::until(
              nqp::atomicload_i($!stop)  # complete shutdown requested
                || $exhausted,           # nothing left to batch
              nqp::stmts(
                ($exhausted = nqp::eqaddr(
                  $source.push-exactly(
                    (my $buffer := nqp::create(IB)),
                    granulize(nqp::shift($pressure))  # wait for ok to proceed
                  ),
                  IE
                )),
                nqp::if(         # add if something to add
                  nqp::elems($buffer) && nqp::not_i(nqp::atomicload_i($!stop)),
                  processor(++$ordinal, $buffer, $result.semaphore);
                )
              )
            );

            # No more result queues will come
            $result.close-queues
        }

        # All scheduled now, so let the show begin!
        self
    }

    # Entry point in chain, from an Iterator
    method !pass-the-chain(\source) {
        my $self := nqp::create(self);
        nqp::bindattr_i($self, ParaSeq, '$!degree',    $!degree   );
        nqp::bindattr_i($self, ParaSeq, '$!batch',     $!batch    );
        nqp::bindattr(  $self, ParaSeq, '$!SCHEDULER', $!SCHEDULER);
        nqp::bindattr(  $self, ParaSeq, '$!source',    source     );
        $self
    }

    # Mark the given queue as done
    method !batch-done(
      uint $ordinal, uint $then, $input, $semaphore, $output
    ) {

        # No longer running, mark as discarded
        if nqp::atomicload_i($!stop) {
            nqp::atomicadd_i($!discarded,nqp::elems(nqp::decont($output)));
        }

        # Still running
        else {

            # Push the statistics into the waiting queue of the iterator
            # so that it has up-to-date information to determine future
            # batch sizes
            nqp::push(
              $!result.waiting,
              ParaStats.new(
                $ordinal,
                nqp::elems(nqp::decont($input)),
                nqp::elems(nqp::decont($output)),
                nqp::time() - $then
              )
            );

            # Make the produced values available to the result iterator
            nqp::push(nqp::decont($semaphore),nqp::decont($output));
        }
    }

    # Just count the number of produced values
    method !count() {
        my $iterator := self.iterator;
        my uint $elems;
        nqp::until(
          nqp::eqaddr($iterator.pull-one, IE),
          ++$elems
        );
        $elems
    }

#- entry points ----------------------------------------------------------------

    # Entry point from the subs: made as small as possible so that the
    # fast path for iterators that don't produce enough values to warrant
    # paralellization, will quickly continue as if nothing happenend
    method parent(
           $source,
      uint $initial,
      uint $auto,
      uint $degree,
      uint $stop-after,
      str  $method
    ) is implementation-detail {
        my $iterator := $source.iterator;
        my $buffer   := nqp::create(IB);

        nqp::eqaddr($iterator.push-exactly($buffer, $initial),IE)
          # First batch already exhausted iterator, so work with buffer
          ?? $buffer.Seq
          # Need to actually parallelize, set up ParaSeq object
          !! self!setup(
               $method, $initial, $auto, $degree, $stop-after,
               $buffer, $iterator
             )
    }

#- where all the magic happens under the hood ----------------------------------

    # Acts as a normal iterator, producing values from queues that are
    # filled asynchronously.  If there is no result iterator, then the
    # source iterator be recreated using the initial buffer if there is
    # one.  Otherwise
    method iterator(ParaSeq:D:) {
        $!result //= nqp::istype($!buffer,IB)
          ?? BufferIterator.new(self, $!buffer, $!source)
          !! $!source
    }

    method stop(ParaSeq:D:) {
        nqp::atomicstore_i($!stop,1);
        $!source.stop if nqp::istype($!source,ParaIterator);
    }

#- introspection ---------------------------------------------------------------

    method auto(ParaSeq:D:) { nqp::hllbool($!auto) }

    method batch-sizes(ParaSeq:D:) {
        self.stats.map(*.processed).minmax
    }

    method default-batch()  { $default-batch  }
    method default-degree() { $default-degree }

    multi method is-lazy(ParaSeq:D:) {
        nqp::hllbool($!source.is-lazy && nqp::not_i($!stop-after))
    }

    method stats( ParaSeq:D:) { $!result.stats.List }

    method stop-after(ParaSeq:D:) { $!stop-after || False }

    method stopped(ParaSeq:D:) { nqp::hllbool(nqp::atomicload_i($!stop)) }

    method threads(ParaSeq:D:) {
        self.stats.map(*.threadid).unique.List
    }

#- endpoints -------------------------------------------------------------------

    multi method elems(ParaSeq:D:) {
        self.is-lazy
          ?? self.fail-iterator-cannot-be-lazy('.elems',"")
          !! self!count
    }

    multi method end(ParaSeq:D:) {
        self.is-lazy
          ?? self.fail-iterator-cannot-be-lazy('.end',"")
          !! self!count - 1
    }

    multi method first(ParaSeq:D:) { self.head }

    proto method head(|) {*}
    multi method head(ParaSeq:D:) {
        my $value := self.iterator.pull-one;
        self.stop;
        nqp::eqaddr($value,IE) ?? Nil !! $value
    }

    multi method join(ParaSeq:D: Str:D $joiner = '') {
        self.fail-iterator-cannot-be-lazy('.join') if self.is-lazy;

        # Logic for joining a buffer
        my $SCHEDULER := $!SCHEDULER;
        sub processor(uint $ordinal, $input, $semaphore) {
            $SCHEDULER.cue: {
                my uint $then = nqp::time;

                nqp::push(
                  (my $output := nqp::create(IB)),
                  $input.List.join($joiner)
                );

                self!batch-done($ordinal, $then, $input, $semaphore, $output);
            }
        }

        # Finish off by joining the joined strings
        self!start(&processor).List.join($joiner)
    }

    proto method max(|) {*}
    multi method max(ParaSeq:D: &by = &[cmp]) { self.Seq.max(&by) }

    proto method min(|) {*}
    multi method min(ParaSeq:D: &by = &[cmp]) { self.Seq.min(&by) }

    proto method minmax(|) {*}
    multi method minmax(ParaSeq:D: &by = &[cmp]) { self.Seq.minmax(&by) }

    proto method pick(|) {*}
    multi method pick(ParaSeq:D:) { self.List.pick }

    multi method reduce(ParaSeq:D: Callable:D $reducer) {

        # Logic for reducing a buffer
        my $SCHEDULER := $!SCHEDULER;
        sub processor(uint $ordinal, $input, $semaphore) {
            $SCHEDULER.cue: {
                my uint $then = nqp::time;

                self!batch-done(
                  $ordinal, $then, $input, $semaphore, $input.reduce($reducer)
                );
            }
        }

        # Finish off by reducing the reductions
        self!start(&processor).Seq.reduce($reducer)
    }

    proto method roll(|) {*}
    multi method roll(ParaSeq:D:) { self.List.roll }

    multi method sum(ParaSeq:D:) {
        self.fail-iterator-cannot-be-lazy('.sum') if self.is-lazy;

        # Logic for summing a buffer
        my $SCHEDULER := $!SCHEDULER;
        sub processor(uint $ordinal, $input, $semaphore) {
            $SCHEDULER.cue: {
                my uint $then = nqp::time;

                nqp::push((my $output := nqp::create(IB)),$input.List.sum);

                self!batch-done($ordinal, $then, $input, $semaphore, $output);
            }
        }

        # Finish off by summing the sums
        self!start(&processor).List.sum
    }

    proto method tail(|) {*}
    multi method tail(ParaSeq:D:) {
        self.Seq.tail
    }

#- first -----------------------------------------------------------------------

    multi method first(ParaSeq:D: Callable:D $matcher) {
        self.Seq.first($matcher, |%_)
    }
    multi method first(ParaSeq:D: $matcher) {
        self.Seq.first($matcher, |%_)
    }
    multi method first(ParaSeq:D: $matcher, :$end!) {
        $end
          ?? self.List.first($matcher, :end, |%_)
          !! self.first($matcher, |%_)
    }

#- grep ------------------------------------------------------------------------

    multi method grep(ParaSeq:D: Callable:D $matcher, :$k, :$kv, :$p) {
        my $SCHEDULER := $!SCHEDULER;
        my uint $base;  # base offset for :k, :kv, :p

        # Logic for queuing a buffer for bare grep { }, producing values
        sub v(uint $ordinal, $input, $semaphore) {
            $SCHEDULER.cue: {
                my uint $then    = nqp::time;
                my      $output := nqp::create(IB);
                my $iterator := $input.Seq.grep($matcher).iterator;
                nqp::until(
                  nqp::eqaddr((my $pulled := $iterator.pull-one),IE)
                    || nqp::atomicload_i($!stop),
                  nqp::push($output,$pulled)
                );
                self!batch-done($ordinal, $then, $input, $semaphore, $output);
            }
        }

        # Logic for queuing a buffer for grep { } :k
        sub k(uint $ordinal, $input, $semaphore) {
            my uint $offset = $base;
            $base = $base + nqp::elems(nqp::decont($input));

            $SCHEDULER.cue: {
                my uint $then    = nqp::time;
                my      $output := nqp::create(IB);

                my $iterator := $input.Seq.grep($matcher, :k).iterator;
                nqp::until(
                  nqp::eqaddr((my $key := $iterator.pull-one),IE)
                    || nqp::atomicload_i($!stop),
                  nqp::push($output,$offset + $key)
                );
                self!batch-done($ordinal, $then, $input, $semaphore, $output);
            }
        }

        # Logic for queuing a buffer for grep { } :kv
        sub kv(uint $ordinal, $input, $semaphore) {
            my uint $offset = $base;
            $base = $base + nqp::elems(nqp::decont($input));

            $SCHEDULER.cue: {
                my uint $then    = nqp::time;
                my      $output := nqp::create(IB);

                my $iterator := $input.Seq.grep($matcher, :kv).iterator;
                nqp::until(
                  nqp::eqaddr((my $key := $iterator.pull-one),IE)
                    || nqp::atomicload_i($!stop),
                  nqp::stmts(
                    nqp::push($output,$offset + $key),     # key
                    nqp::push($output,$iterator.pull-one)  # value
                  )
                );
                self!batch-done($ordinal, $then, $input, $semaphore, $output);
            }
        }

        # Logic for queuing a buffer for grep { } :p
        sub p(uint $ordinal, $input, $semaphore) {
            my uint $offset = $base;
            $base = $base + nqp::elems(nqp::decont($input));

            $SCHEDULER.cue: {
                my uint $then    = nqp::time;
                my      $output := nqp::create(IB);

                my $iterator := $input.Seq.grep($matcher, :kv).iterator;
                nqp::until(
                  nqp::eqaddr((my $key := $iterator.pull-one),IE)
                    || nqp::atomicload_i($!stop),
                  nqp::push(
                    $output,
                    Pair.new($offset + $key, $iterator.pull-one)
                  )
                );
                self!batch-done($ordinal, $then, $input, $semaphore, $output);
            }
        }

        # Let's go!
        self!start(
          $k ?? &k !! $kv ?? &kv !! $p ?? &p !! &v,
          granularity($matcher)
        )
    }

    multi method grep(ParaSeq:D: $matcher, :$k, :$kv, :$p) {
        my $SCHEDULER := $!SCHEDULER;
        my uint $base;  # base offset for :k, :kv, :p

        # Logic for queuing a buffer for grep /.../
        sub v(uint $ordinal, $input, $semaphore) {
            $SCHEDULER.cue: {
                my uint $then    = nqp::time;
                my      $output := nqp::create(IB);

                my $iterator := $input.Seq.grep($matcher).iterator;
                nqp::until(
                  nqp::eqaddr((my $value := $iterator.pull-one),IE)
                    || nqp::atomicload_i($!stop),
                  nqp::push($output,$value)
                );
                self!batch-done($ordinal, $then, $input, $semaphore, $output);
            }
        }

        # Logic for queuing a buffer for grep /.../ :k
        sub k(uint $ordinal, $input, $semaphore) {
            my uint $offset = $base;
            $base = $base + nqp::elems(nqp::decont($input));

            $SCHEDULER.cue: {
                my uint $then    = nqp::time;
                my      $output := nqp::create(IB);

                my $iterator := $input.Seq.grep($matcher, :k).iterator;
                nqp::until(
                  nqp::eqaddr((my $key := $iterator.pull-one),IE)
                    || nqp::atomicload_i($!stop),
                  nqp::push($output,$offset + $key)
                );
                self!batch-done($ordinal, $then, $input, $semaphore, $output);
            }
        }

        # Logic for queuing a buffer for grep /.../ :kv
        sub kv(uint $ordinal, $input, $semaphore) {
            my uint $offset = $base;
            $base = $base + nqp::elems(nqp::decont($input));

            $SCHEDULER.cue: {
                my uint $then    = nqp::time;
                my      $output := nqp::create(IB);

                my $iterator := $input.Seq.grep($matcher, :kv).iterator;
                nqp::until(
                  nqp::eqaddr((my $key := $iterator.pull-one),IE)
                    || nqp::atomicload_i($!stop),
                  nqp::stmts(
                    nqp::push($output,$offset + $key),     # key
                    nqp::push($output,$iterator.pull-one)  # value
                  )
                );
                self!batch-done($ordinal, $then, $input, $semaphore, $output);
            }
        }

        # Logic for queuing a buffer for grep /.../ :p
        sub p(uint $ordinal, $input, $semaphore) {
            my uint $offset = $base;
            $base = $base + nqp::elems(nqp::decont($input));

            $SCHEDULER.cue: {
                my uint $then    = nqp::time;
                my      $output := nqp::create(IB);

                my $iterator := $input.Seq.grep($matcher, :kv).iterator;
                nqp::until(
                  nqp::eqaddr((my $key := $iterator.pull-one),IE)
                    || nqp::atomicload_i($!stop),
                  nqp::push(
                    $output,
                    Pair.new($offset + $key, $iterator.pull-one)
                  )
                );
                self!batch-done($ordinal, $then, $input, $semaphore, $output);
            }
        }

        # Let's go!
        self!start($k ?? &k !! $kv ?? &kv !! $p ?? &p !! &v)
    }

#- map -------------------------------------------------------------------------

    multi method map(ParaSeq:D: Callable:D $mapper) {

        # Logic for queuing a buffer for map
        my $SCHEDULER := $!SCHEDULER;
        sub processor(uint $ordinal, $input, $semaphore) {
            $SCHEDULER.cue: {
                my uint $then    = nqp::time;
                my      $output := nqp::create(IB);

                my $iterator := $input.Seq.map($mapper).iterator;
                nqp::until(
                  nqp::eqaddr((my $pulled := $iterator.pull-one),IE)
                    || nqp::atomicload_i($!stop),
                  nqp::push($output,$pulled)
                );
                self!batch-done($ordinal, $then, $input, $semaphore, $output);
            }
        }

        # Let's go!
        self!start(&processor, granularity($mapper))
    }

#- other standard Iterable interfaces ------------------------------------------

    multi method antipairs(ParaSeq:D:) {
        self!pass-the-chain: self.Seq.antipairs.iterator
    }

    proto method batch(|) {*}
    multi method batch(ParaSeq:D: |c) {
        self!pass-the-chain: self.List.batch(|c).iterator
    }

    proto method collate(|) {*}
    multi method collate(ParaSeq:D: |c) {
        self!pass-the-chain: self.List.collate(|c).iterator
    }

    multi method combinations(ParaSeq:D: $of) {
        self!pass-the-chain: self.List.combinations($of).iterator
    }

    multi method deepmap(ParaSeq:D: |c) {
        self!pass-the-chain: self.Seq.deepmap(|c).iterator
    }

    multi method duckmap(ParaSeq:D: |c) {
        self!pass-the-chain: self.Seq.duckmap(|c).iterator
    }

    multi method eager(ParaSeq:D:) {
        self!pass-the-chain: self.List.iterator
    }

    multi method flat(ParaSeq:D:) {
        self!pass-the-chain: self.Seq.flat.iterator
    }

    proto method flatmap(|) {*}
    multi method flatmap(ParaSeq:D: |c) {
        self!pass-the-chain: self.Seq.flatmap(|c).iterator
    }

    multi method head(ParaSeq:D: |c) {
        self!pass-the-chain: self.Seq.head(|c).iterator
    }

    multi method invert(ParaSeq:D:) {
        self!pass-the-chain: self.Seq.invert.iterator
    }

    multi method keys(ParaSeq:D:) {
        self!pass-the-chain: self.Seq.keys.iterator
    }

    multi method kv(ParaSeq:D:) {
        self!pass-the-chain: self.Seq.kv.iterator
    }

    multi method max(ParaSeq:D: &by = &[cmp], :$k!) {
        $k
          ?? self!pass-the-chain(self.Seq.max(&by, :k))
          !! self.max(&by, |%_)
    }
    multi method max(ParaSeq:D: &by = &[cmp], :$kv!) {
        $kv
          ?? self!pass-the-chain(self.Seq.max(&by, :kv))
          !! self.max(&by, |%_)
    }
    multi method max(ParaSeq:D: &by = &[cmp], :$p!) {
        $p
          ?? self!pass-the-chain(self.Seq.max(&by, :p))
          !! self.max(&by, |%_)
    }

    multi method min(ParaSeq:D: &by = &[cmp], :$k!) {
        $k
          ?? self!pass-the-chain(self.Seq.min(&by, :k))
          !! self.min(&by, |%_)
    }
    multi method min(ParaSeq:D: &by = &[cmp], :$kv!) {
        $kv
          ?? self!pass-the-chain(self.Seq.min(&by, :kv))
          !! self.min(&by, |%_)
    }
    multi method min(ParaSeq:D: &by = &[cmp], :$p!) {
        $p
          ?? self!pass-the-chain(self.Seq.min(&by, :p))
          !! self.min(&by, |%_)
    }

    multi method maxpairs(ParaSeq:D:) {
        self!pass-the-chain: self.Seq.maxpairs.iterator
    }

    multi method minpairs(ParaSeq:D:) {
        self!pass-the-chain: self.Seq.minpairs.iterator
    }

    multi method nodemap(ParaSeq:D: |c) {
        self!pass-the-chain: self.Seq.nodemap(|c).iterator
    }

    multi method pairs(ParaSeq:D:) {
        self!pass-the-chain: self.Seq.pairs.iterator
    }

    multi method pairup(ParaSeq:D:) {
        self!pass-the-chain: self.Seq.pairup.iterator
    }

    proto method permutations(|) {*}
    multi method permutations(ParaSeq:D: |c) {
        self!pass-the-chain: self.List.permutations(|c).iterator
    }

    multi method pick(ParaSeq:D: |c) {
        self!pass-the-chain: self.Seq.pick(|c).iterator
    }

    multi method produce(ParaSeq:D: Callable:D $producer) {
        self!pass-the-chain: self.Seq.produce($producer).iterator
    }

    proto method repeated(|) {*}
    multi method repeated(ParaSeq:D: |c) {
        self!pass-the-chain: self.Seq.repeated(|c).iterator
    }

    multi method reverse(ParaSeq:D:) {
        self!pass-the-chain:
          Rakudo::Iterator.ReifiedReverse:
            self.IterationBuffer, Mu
    }

    multi method rotate(ParaSeq:D: $rotate) {
        self!pass-the-chain: self.List.rotate($rotate).iterator
    }

    proto method rotor(|) {*}
    multi method rotor(ParaSeq:D: |c) {
        self!pass-the-chain: self.List.rotor(|c).iterator
    }

    multi method serial(ParaSeq:D:) { self.Seq }

    proto method skip(|) {*}
    multi method skip(ParaSeq:D: |c) {
        self!pass-the-chain: self.Seq.skip(|c).iterator
    }

    proto method slice(|) {*}
    multi method slice(ParaSeq:D: |c) {
        self!pass-the-chain: self.List.slice(|c).iterator
    }

    proto method snip(|) {*}
    multi method snip(ParaSeq:D: |c) {
        self!pass-the-chain: self.Seq.snip(|c).iterator
    }

    proto method snitch(|) {*}
    multi method snitch(ParaSeq:D: &snitcher = &note) {
        self!pass-the-chain: self.Seq.snitch(&snitch).iterator
    }

    proto method sort(|) {*}
    multi method sort(ParaSeq:D: |c) {
        self!pass-the-chain: self.List.sort(|c).iterator
    }

    proto method squish(|) {*}
    multi method squish(ParaSeq:D: |c) {
        self!pass-the-chain: self.Seq.squish(|c).iterator
    }

    multi method tail(ParaSeq:D: |c) {
        self!pass-the-chain: self.Seq.tail(|c).iterator
    }

    proto method toggle(|) {*}
    multi method toggle(ParaSeq:D: |c) {
        self!pass-the-chain: self.Seq.toggle(|c).iterator
    }

    proto method unique(|) {*}
    multi method unique(ParaSeq:D: |c) {
        self!pass-the-chain: self.Seq.unique(|c).iterator
    }

    multi method values(ParaSeq:D:) { self }

#- coercers --------------------------------------------------------------------

    multi method Array(  ParaSeq:D:) { self.List.Array   }
    multi method Bag(    ParaSeq:D:) { self.List.Bag     }
    multi method BagHash(ParaSeq:D:) { self.List.BagHash }

    multi method Bool(ParaSeq:D:) {
        my $Bool := nqp::hllbool(
          nqp::not_i(nqp::eqaddr(self.iterator.pull-one,IterationEnd))
        );
        self.stop;
        $Bool
    }

    multi method Hash(ParaSeq:D:) { self.List.Hash }
    multi method Int( ParaSeq:D:) { self.elems     }

    multi method IterationBuffer(ParaSeq:D:) {
        self.iterator.push-all(my $buffer := nqp::create(IB));
        $buffer
    }

    multi method List(ParaSeq:D:) { self.IterationBuffer.List }

    multi method Map(    ParaSeq:D:) { self.List.Map     }
    multi method Mix(    ParaSeq:D:) { self.List.Mix     }
    multi method MixHash(ParaSeq:D:) { self.List.MixHash }
          method Numeric(ParaSeq:D:) { self.elems        }

    multi method Seq(ParaSeq:D:) { Seq.new: self.iterator }

    multi method Set(    ParaSeq:D:) { self.List.Set     }
    multi method SetHash(ParaSeq:D:) { self.List.SetHash }
    multi method Supply( ParaSeq:D:) { self.List.Supply  }

    multi method Slip(ParaSeq:D:) { self.IterationBuffer.Slip }
    multi method Str( ParaSeq:D:) { self.join(" ")            }
}

#- actual interface ------------------------------------------------------------

proto sub hyperize(|) is export {*}
multi sub hyperize(\iterable, $, 1, *%_) is raw { iterable }
multi sub hyperize(
         \iterable,
        $batch?,
        $degree?,
  Bool :$auto       = True,
       :$stop-after = Inf,
) {
    ParaSeq.parent(
      iterable,
      ($batch // $default-batch).Int,
      $auto,
      ($degree // $default-degree).Int,
      $stop-after == Inf ?? 0 !! $stop-after,
      'hyperize'
    )
}
multi sub hyperize(
  List:D $list,
         $size?,
         $degree?,
  Bool  :$auto       = True,
        :$stop-after = Inf,
) {
    my uint $batch = $size // $default-batch;
    $list.is-lazy || $list.elems > $batch
      ?? ParaSeq.parent(
           $list,
           ($batch // $default-batch).Int,
           $auto,
           ($degree // $default-degree).Int,
           $stop-after == Inf ?? 0 !! $stop-after,
           'hyperize'
         )
      !! $list
}

# There doesn't seem to be too much to be gained by supporting an alternate
# path where the order of the results is *not* preserved, as the difference
# in implementation is only one nqp::push/nqp::shift less per batch.  So
# just equate "racify" with "hyperize"
my constant &racify is export = &hyperize;

# vim: expandtab shiftwidth=4
