# Intended to be as fast as possible and to someday become part of the
# Rakudo core
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

# Shortcut for long IterationBuffer name, just for esthetics
my constant IB = IterationBuffer;

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

    method push-all(\target) {
        nqp::istype(target, IB)
          ?? nqp::splice(target, $!buffer, nqp::elems(target), 0)
          !! $!buffer.iterator.push-all(target);
        $!buffer := Mu;

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
    has IB        $.stats;      # list with processed ParaStats objects
    has uint      $.batch;      # initial / current batch size
    has uint      $.processed;  # number of items processed seen so far
    has uint      $.produced;   # number of items produced seen so far
    has uint      $.nsecs;      # nano seconds wallclock used so far
    has atomicint $!stop;       # stop all processing if 1

    method new(\parent, \pressure, uint $batch) {
        my $self := nqp::create(self);
        nqp::bindattr($self,ParaIterator,'$!parent',parent);
        nqp::bindattr($self,ParaIterator,'$!current',emptyIB);  # first fetch
        nqp::bindattr($self,ParaIterator,'$!queues',nqp::create(ParaQueue));
        nqp::bindattr($self,ParaIterator,'$!pressure',pressure);
        nqp::bindattr($self,ParaIterator,'$!waiting',nqp::create(ParaQueue));
        nqp::bindattr($self,ParaIterator,'$!stats',nqp::create(IB));

        nqp::bindattr_i($self,ParaIterator,'$!batch',$batch);
        $self
    }

    method close-queues(ParaIterator:D:) { nqp::push($!queues,IterationEnd) }

    method stop(ParaIterator:D:) {
        nqp::atomicstore_i($!stop,1);  # stop producing here
        $!parent.stop;                 # stop all threads
    }

    # Add a semaphore to the queue.  This is a ParaQueue to which a
    # fully processed IterationnBuffer will be pushed when it is ready.
    # Until then, it will block if it is not ready yet.  As such, the
    # associated nqp::shift acts both as a semaphore as well as the
    # IterationBuffer with values
    method semaphore() {
        nqp::push($!queues,nqp::create(ParaQueue))
    }

    # Thread-safely handle next batch
    method !next-batch(\next) {

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
            $!batch = nqp::div_i(nqp::mul_i($processed,$target-nsecs),$nsecs)
              if $nsecs;

            # Initiate more work with updated batch size
            nqp::push($!pressure,$!batch);
        }

        # Next buffer to be processed
        nqp::shift(next)
    }

    method pull-one() {
        nqp::if(
          nqp::elems($!current),
          (my $pulled := nqp::shift($!current)),
          nqp::if(
            nqp::eqaddr((my $next := nqp::shift($!queues)),IterationEnd),
            (return IterationEnd),
            nqp::stmts(
              ($!current := self!next-batch($next)),
              ($pulled   := self.pull-one)
            )
          )
        );

        nqp::atomicload_i($!stop) ?? IterationEnd !! $pulled
    }

    method skip-at-least(uint $skipping) {
        my      $current  := $!current;
        my      $queues   := $!queues;
        my      $pressure := $!pressure;
        my uint $toskip    = $skipping;

        nqp::until(
          nqp::atomicload_i($!stop),
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
              nqp::eqaddr((my $next := nqp::shift($queues)),IterationEnd),
              (return 0)         # really done
            ),
            ($toskip = nqp::sub_i($toskip,$elems)),
            ($current := $!current := self!next-batch($next))
          )
        );

        0  # really done, because stopped
    }

    method push-all(\target) {
        my $current  := $!current;
        my $queues   := $!queues;
        my $pressure := $!pressure;

        nqp::until(
          nqp::atomicload_i($!stop),
          nqp::stmts(
            target.append($current),
            nqp::if(
              nqp::eqaddr((my $next := nqp::shift($queues)),IterationEnd),
              (return IterationEnd),
              ($current := self!next-batch($next))
            )
          )
        );

        IterationEnd  # really done, because stopped
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
    has           $!buffer;     # first buffer
    has           $!source;     # iterator producing source values
    has           $!result;     # iterator producing result values
    has           $!SCHEDULER;  # $*SCHEDULER to be used
    has uint      $.degree;     # number of CPUs, must be > 1
    has uint      $.batch;      # initial batch size, must be > 0
    has atomicint $.discarded;  # produced values discarded because of stop
    has atomicint $!stop;       # stop all processing if 1

#- private helper methods ------------------------------------------------------

    # Do error checking and set up object if all ok
    method !setup(
      str $method, uint $batch, uint $degree, $buffer, $source
    ) is hidden-from-backtrace {
        X::Invalid::Value.new(:$method, :name<batch>,  :value($batch)).throw
          if $batch <= 0;
        X::Invalid::Value.new(:$method, :name<degree>, :value($degree)).throw
          if $degree <= 1;

        # Set it up!
        my $self := nqp::create(self);
        nqp::bindattr_i($self, ParaSeq, '$!batch',     $batch              );
        nqp::bindattr_i($self, ParaSeq, '$!degree',    $degree             );
        nqp::bindattr(  $self, ParaSeq, '$!SCHEDULER', $*SCHEDULER         );
        nqp::bindattr(  $self, ParaSeq, '$!buffer',    nqp::decont($buffer));
        nqp::bindattr(  $self, ParaSeq, '$!source',    $source             );
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
            || nqp::eqaddr((my $pulled := $!source.pull-one),IterationEnd),
          nqp::push($first,$pulled)
        );

        # Set up back pressure queue with some simple variation
        my $pressure   := nqp::create(ParaQueue);
        my uint $degree = $!degree;
        my uint $half   = nqp::div_i($!batch,2) || $!batch;
        my uint $batch;
        for ^$!degree {
            nqp::push($pressure,granulize($batch = $batch + $half));
        }

        # Queue the first buffer we already filled, and set up the
        # result iterator
        processor(
          0,
          $first,
          ($!result := ParaIterator.new(
            self, $pressure, $!batch)
          ).semaphore
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
                  IterationEnd
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
          nqp::eqaddr($iterator.pull-one, IterationEnd),
          ++$elems
        );
        $elems
    }

#- entry points ----------------------------------------------------------------

    # Entry point from the subs: made as small as possible so that the
    # fast path for iterators that don't produce enough values to warrant
    # paralellization, will quickly continue as if nothing happenend
    method parent($source, uint $initial, uint $degree, str $method) {
        my $iterator := $source.iterator;
        my $buffer   := nqp::create(IB);

        nqp::eqaddr($iterator.push-exactly($buffer, $initial),IterationEnd)
          # First batch already exhausted iterator, so work with buffer
          ?? $buffer.Seq
          # Need to actually parallelize, set up ParaSeq object
          !! self!setup($method, $initial, $degree, $buffer, $iterator)
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

    method default-degree() { $default-degree }
    method default-batch()  { $default-batch  }

    method degree(ParaSeq:D:) { $!degree            }
    method batch( ParaSeq:D:) { $!batch             }
    method stats( ParaSeq:D:) { $!result.stats.List }

    method stopped(ParaSeq:D:) { nqp::hllbool(nqp::atomicload_i($!stop)) }

    method threads(ParaSeq:D:) {
        self.stats.map(*.threadid).unique
    }
    method batch-sizes(ParaSeq:D:) {
        self.stats.map(*.processed).minmax
    }

#- map -------------------------------------------------------------------------

    proto method map(|) {*}
    multi method map(ParaSeq:D: Callable:D $mapper) {

        # Logic for queuing a buffer for map
        my $SCHEDULER := $!SCHEDULER;
        sub processor(uint $ordinal, $input, $semaphore) {
            $SCHEDULER.cue: {
                my uint $then    = nqp::time;
                my      $output := nqp::create(IB);

                my $iterator := $input.Seq.map($mapper).iterator;
                nqp::until(
                  nqp::eqaddr((my $pulled := $iterator.pull-one),IterationEnd)
                    || nqp::atomicload_i($!stop),
                  nqp::push($output,$pulled)
                );
                self!batch-done($ordinal, $then, $input, $semaphore, $output);
            }
        }

        # Let's go!
        self!start(&processor, granularity($mapper))
    }

#- grep ------------------------------------------------------------------------

    proto method grep(|) {*}
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
                  nqp::eqaddr((my $pulled := $iterator.pull-one),IterationEnd)
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
                  nqp::eqaddr((my $key := $iterator.pull-one),IterationEnd)
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
                  nqp::eqaddr((my $key := $iterator.pull-one),IterationEnd)
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
                  nqp::eqaddr((my $key := $iterator.pull-one),IterationEnd)
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
                  nqp::eqaddr((my $value := $iterator.pull-one),IterationEnd)
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
                  nqp::eqaddr((my $key := $iterator.pull-one),IterationEnd)
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
                  nqp::eqaddr((my $key := $iterator.pull-one),IterationEnd)
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
                  nqp::eqaddr((my $key := $iterator.pull-one),IterationEnd)
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

#- first -----------------------------------------------------------------------

    proto method first(|) {*}
    multi method first(ParaSeq:D:) {
        self.Seq.first
    }
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

#- other standard Iterable interfaces ------------------------------------------

    proto method invert(|) {*}
    multi method invert(ParaSeq:D:) {
        self!pass-the-chain: self.Seq.invert.iterator
    }

    proto method skip(|) {*}
    multi method skip(ParaSeq:D: |c) {
        self!pass-the-chain: self.Seq.skip(|c).iterator
    }

    proto method head(|) {*}
    multi method head(ParaSeq:D: |c) {
        self!pass-the-chain: self.Seq.head(|c).iterator
    }

    proto method tail(|) {*}
    multi method tail(ParaSeq:D: |c) {
        self!pass-the-chain: self.Seq.tail(|c).iterator
    }

    proto method reverse(|) {*}
    multi method reverse(ParaSeq:D:) {
        self!pass-the-chain:
          Rakudo::Iterator.ReifiedReverse:
            self.IterationBuffer, Mu
    }

#- endpoints -------------------------------------------------------------------

    multi method elems(ParaSeq:D:) {
        $!source.is-lazy
          ?? self.fail-iterator-cannot-be-lazy('.elems',"")
          !! self!count
    }

    multi method end(ParaSeq:D:) {
        $!source.is-lazy
          ?? self.fail-iterator-cannot-be-lazy('.end',"")
          !! self!count - 1
    }

    multi method head(ParaSeq:D:) {
        my $value := self.iterator.pull-one;
        self.stop;
        $value
    }
    multi method tail(   ParaSeq:D:) { self.Seq.tail  }
    multi method is-lazy(ParaSeq:D:) { $!source.is-lazy }

#- coercers --------------------------------------------------------------------

    multi method IterationBuffer(ParaSeq:D:) {
        self.iterator.push-all(my $buffer := nqp::create(IB));
        $buffer
    }

    multi method Array(ParaSeq:D:) { self.IterationBuffer.List.Array }
    multi method Hash( ParaSeq:D:) { self.IterationBuffer.List.Hash  }
    multi method List( ParaSeq:D:) { self.IterationBuffer.List       }
    multi method Map(  ParaSeq:D:) { self.IterationBuffer.List.Map   }
    multi method Seq(  ParaSeq:D:) { Seq.new: self.iterator          }
    multi method Slip( ParaSeq:D:) { self.IterationBuffer.Slip       }

    multi method serial(ParaSeq:D:) { self.Seq }
}

#- actual interface ------------------------------------------------------------

proto sub hyperize(|) is export {*}
multi sub hyperize(\iterable, $, 1) is raw { iterable }
multi sub hyperize(\iterable) {
   ParaSeq.parent(iterable, $default-batch, $default-degree, 'hyperize')
}
multi sub hyperize(\iterable, Int:D $batch) {
   ParaSeq.parent(iterable, $batch, $default-degree, 'hyperize')
}
multi sub hyperize(\iterable, Int:D $batch, Int:D $degree) {
   ParaSeq.parent(iterable, $batch, $degree, 'hyperize')
}

# For now, there doesn't seem to be too much to be gained by supporting
# an alternate path where the order of the results is *not* preserved,
# so just equate "racify" with "hyperize" for now
my constant &racify is export = &hyperize;

# vim: expandtab shiftwidth=4
