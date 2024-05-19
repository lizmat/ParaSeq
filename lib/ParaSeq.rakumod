# Intended to be as fast as possible and to someday become part of the
# Rakudo core
use nqp;

my uint $default-batch  = 10;
my uint $default-degree = Kernel.cpu-cores-but-one;

#- ParaQueue -------------------------------------------------------------------
# A blocking concurrent queue to which one can nqp::push and from which one
# can nqp::shift
my class ParaQueue is repr('ConcBlockingQueue') { }

#- ParaStats -------------------------------------------------------------------
# A class for keeping stats about the production of a single result queue
# from a batch of values

my class ParaStats {
    has uint $.ordinal;
    has uint $.elems;
    has uint $.nsecs;

    method new(uint $ordinal, uint $elems, uint $nsecs) {
        my $self := nqp::create(self);
        nqp::bindattr_i($self, ParaStats, '$!ordinal',$ordinal);
        nqp::bindattr_i($self, ParaStats, '$!elems',  $elems  );
        nqp::bindattr_i($self, ParaStats, '$!nsecs',  $nsecs  );
        $self
    }
}

#- BufferIterator --------------------------------------------------------------
# An iterator that takes a buffer and an iterator, and first produces all
# of the values from the buffer, and then from the iterator

my class BufferIterator does Iterator {
    has $!buffer;
    has $!iterator;

    method new($buffer, $iterator) {
        my $self := nqp::create(self);
        nqp::bindattr($self,BufferIterator,'$!buffer',  nqp::decont($buffer));
        nqp::bindattr($self,BufferIterator,'$!iterator',$iterator           );
        $self
    }

    method pull-one() {
        nqp::elems($!buffer)
          ?? nqp::shift($!buffer)
          !! $!iterator.pull-one
    }

    method push-all(\target) {
        nqp::istype(target, IterationBuffer)
          ?? nqp::splice(target, $!buffer, nqp::elems(target), 0)
          !! $!buffer.iterator.push-all(target);
        $!buffer := Mu;

        $!iterator.push-all(target)
    }
}

#- ParaIterator ----------------------------------------------------------------
# An iterator that takes a number of ParaQueues and produces all values
# from them until the last ParaQueue has been exhausted.  Note that the
# queue of queues is also a ParaQueue so that queues can also be added
# and removed in a thread-safe manner

my class ParaIterator does Iterator {
    has $!current;   # current producer
    has $!queues;    # other queues to produce from
    has $!pressure;  # backpressure provider

    method new(\current, \pressure) {
        my $self := nqp::create(self);
        nqp::bindattr($self,ParaIterator,'$!current',current);
        nqp::bindattr($self,ParaIterator,'$!pressure',pressure);
        nqp::bindattr($self,ParaIterator,'$!queues',nqp::create(ParaQueue));
        $self
    }

    method nr-queues()          { nqp::elems($!queues)      }
    method add-queue(Mu \queue) { nqp::push($!queues,queue) }

    method pull-one() {
        my $pulled := nqp::shift($!current);

        nqp::while(
          nqp::eqaddr($pulled,IterationEnd),               # queue exhausted
          nqp::if(
            nqp::eqaddr(($pulled := nqp::shift($!queues)),IterationEnd),
            (return IterationEnd),                         # no queue left, done
            nqp::stmts(                                    # one more queue
              nqp::push($!pressure,nqp::shift($!current)), # allow more work
              $pulled := nqp::shift($!current := $pulled)  # set next queue
            )
          )
        );

        $pulled
    }

    method push-all(\target) {
        my $current  := $!current;
        my $queues   := $!queues;
        my $pressure := $!pressure;

        nqp::while(
          1,
          nqp::stmts(
            (my $pulled := nqp::shift($current)),
            nqp::while(
              nqp::eqaddr($pulled,IterationEnd),              # queue exhausted
              nqp::if(
                nqp::eqaddr(($pulled := nqp::shift($queues)),IterationEnd),
                (return IterationEnd),                        # really done
                nqp::stmts(                                   # one more queue
                  nqp::push($pressure,nqp::shift($current)),  # allow more work
                  $pulled := nqp::shift($current := $pulled)  # set next queue
                )
              )
            ),
            nqp::push(target, $pulled)
          )
        );
    }
}

#- ParaSeq ---------------------------------------------------------------------
# The class containing all of the logic for parallel sequences
class ParaSeq {
    has      $!buffer;     # first buffer
    has      $!source;     # iterator producing source values
    has      $!result;     # iterator producing result values
    has      $!stats;      # list with ParaStats objects
    has      $!SCHEDULER;  # $*SCHEDULER to be used
    has uint $.degree;     # number of CPUs, must be > 1
    has uint $.batch;      # initial batch size, must be > 0
    has uint $!stop;       # stop all processing if 1

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

    # Start the async process with the first buffer and the given buffer
    # queuing logic and required granularity for producing values
    method !start(&queue-buffer, uint $granularity = 1) {

        # Logic for making sure batch size has correct granularity
        my sub granulize(uint $batch) {
            $batch %% $granularity
              ?? $batch
              !! $batch div $granularity * $granularity || $granularity
        }

        # Local copy of first buffer
        my $first := $!buffer;
        $!buffer  := Mu;  # release buffer in object

        # Set up initial back pressure queue allowing for one batch to
        # be created for each worker thread
        my $pressure := nqp::create(ParaQueue);
        nqp::push($pressure, Mu) for ^$!degree;

        # Queue the first buffer we already filled, and set up the
        # result iterator
        $!result := ParaIterator.new(queue-buffer(0, $first), $pressure);
        $!stats  := nqp::list;

        # Make sure the scheduling of further batches actually happen in a
        # separate thread
        $!SCHEDULER.cue: {
            my uint $ordinal;          # ordinal number of batch
            my uint $exhausted;        # flag: 1 if exhausted

            # some shortcuts
            my $source := $!source;
            my $result := $!result;
            my $stats  := $!stats;

            # Make sure batch size has the right granularity
            my uint $batch = granulize($!batch);

            # Until we're halted or have a buffer that's not full
            nqp::until(
              $!stop             # complete shutdown requested
                || $exhausted,   # nothing left to batch
              nqp::stmts(
                nqp::if(         # wait for ok to proceed
                  nqp::isconcrete(my $ok := nqp::shift($pressure)),
                  nqp::bindpos($stats, $ok.ordinal, $ok)
                ),
                ($exhausted = nqp::eqaddr(
                  $source.push-exactly(
                    (my $buffer := nqp::create(IterationBuffer)),
                    $batch
                  ),
                  IterationEnd
                )),
                nqp::if(                               # add if something to add
                  nqp::elems($buffer),
                  $result.add-queue(queue-buffer(++$ordinal, $buffer))
                )
              )
            );

            # No more result queues will come
            $result.add-queue(IterationEnd);
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
    method !queue-done(uint $ordinal, uint $then, $buffer, $queue) {
        my uint $delta = nqp::time() - $then;

        # Indicate this result queue is done
        nqp::push(nqp::decont($queue),IterationEnd);

        # Values produced after IterationEnd should be ignored.
        # Use this property to tell the result iterator how much
        # time it took to produce these, which in turn will allow
        # the result producer to tell the batcher it may have to
        # adapt the batch size
        nqp::push(
          nqp::decont($queue),
          ParaStats.new(
            $ordinal,
            nqp::elems(nqp::decont($buffer)),
            nqp::time() - $then
          )
        );
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
        my $buffer   := nqp::create(IterationBuffer);

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
    multi method iterator(ParaSeq:D:) {
        $!result //= nqp::istype($!buffer,IterationBuffer)
          ?? BufferIterator.new($!buffer, $!source)
          !! $!source
    }

#- introspection ---------------------------------------------------------------

    method default-degree() { $default-degree }
    method default-batch()  { $default-batch  }

    method degree(ParaSeq:D:) { $!degree }
    method batch( ParaSeq:D:) { $!batch  }

#- map -------------------------------------------------------------------------

    proto method map(|) {*}
    multi method map(ParaSeq:D: Callable:D $mapper) {

        # Logic for queuing a buffer for map
        my $SCHEDULER := $!SCHEDULER;
        sub queue-buffer(uint $ordinal, $buffer) {
            my $queue := nqp::create(ParaQueue);
            $SCHEDULER.cue: {
                my uint $then = nqp::time;

                my $iterator := $buffer.Seq.map($mapper).iterator;
                nqp::until(
                  nqp::eqaddr((my $pulled := $iterator.pull-one),IterationEnd),
                  nqp::push($queue,$pulled)
                );
                self!queue-done($ordinal, $then, $buffer, $queue);
            }
            $queue
        }

        # Let's go!
        my $count := $mapper.count;
        self!start(&queue-buffer, $count == Inf ?? 1 !! $count)
    }

#- grep ------------------------------------------------------------------------

    proto method grep(|) {*}
    multi method grep(ParaSeq:D: Callable:D $matcher, :$k, :$kv, :$p) {
        my $SCHEDULER := $!SCHEDULER;
        my uint $base;  # base offset for :k, :kv, :p

        # Logic for queuing a buffer for bare grep { }, producing values
        sub v(uint $ordinal, $buffer) {
            my $queue := nqp::create(ParaQueue);
            $SCHEDULER.cue: {
                my uint $then = nqp::time;

                my $iterator := $buffer.Seq.grep($matcher).iterator;
                nqp::until(
                  nqp::eqaddr((my $pulled := $iterator.pull-one),IterationEnd),
                  nqp::push($queue,$pulled)
                );
                self!queue-done($ordinal, $then, $buffer, $queue);
            }
            $queue
        }

        # Logic for queuing a buffer for grep { } :k
        sub k(uint $ordinal, $buffer) {
            my uint $offset = $base;
            $base = $base + nqp::elems(nqp::decont($buffer));

            my $queue := nqp::create(ParaQueue);
            $SCHEDULER.cue: {
                my uint $then = nqp::time;

                my $iterator := $buffer.Seq.grep($matcher, :k).iterator;
                nqp::until(
                  nqp::eqaddr((my $key := $iterator.pull-one),IterationEnd),
                  nqp::push($queue,$offset + $key)
                );
                self!queue-done($ordinal, $then, $buffer, $queue);
            }
            $queue
        }

        # Logic for queuing a buffer for grep { } :kv
        sub kv(uint $ordinal, $buffer) {
            my uint $offset = $base;
            $base = $base + nqp::elems(nqp::decont($buffer));

            my $queue := nqp::create(ParaQueue);
            $SCHEDULER.cue: {
                my uint $then = nqp::time;

                my $iterator := $buffer.Seq.grep($matcher, :kv).iterator;
                nqp::until(
                  nqp::eqaddr((my $key := $iterator.pull-one),IterationEnd),
                  nqp::stmts(
                    nqp::push($queue,$offset + $key),     # key
                    nqp::push($queue,$iterator.pull-one)  # value
                  )
                );
                self!queue-done($ordinal, $then, $buffer, $queue);
            }
            $queue
        }

        # Logic for queuing a buffer for grep { } :p
        sub p(uint $ordinal, $buffer) {
            my uint $offset = $base;
            $base = $base + nqp::elems(nqp::decont($buffer));

            my $queue := nqp::create(ParaQueue);
            $SCHEDULER.cue: {
                my uint $then = nqp::time;

                my $iterator := $buffer.Seq.grep($matcher, :kv).iterator;
                nqp::until(
                  nqp::eqaddr((my $key := $iterator.pull-one),IterationEnd),
                  nqp::push(
                    $queue,
                    Pair.new($offset + $key, $iterator.pull-one)
                  )
                );
                self!queue-done($ordinal, $then, $buffer, $queue);
            }
            $queue
        }

        # Let's go!
        my $count := $matcher.count;
        self!start(
          $k ?? &k !! $kv ?? &kv !! $p ?? &p !! &v,
          $count == Inf ?? 1 !! $count
        )
    }
    multi method grep(ParaSeq:D: $matcher, :$k, :$kv, :$p) {
        my $SCHEDULER := $!SCHEDULER;
        my uint $base;  # base offset for :k, :kv, :p

        # Logic for queuing a buffer for grep /.../
        sub v(uint $ordinal, $buffer) {
            my $queue  := nqp::create(ParaQueue);
            $SCHEDULER.cue: {
                my uint $then = nqp::time;

                my $iterator := $buffer.Seq.grep($matcher).iterator;
                nqp::until(
                  nqp::eqaddr((my $pulled := $iterator.pull-one),IterationEnd),
                  nqp::push($queue,$pulled)
                );
                self!queue-done($ordinal, $then, $buffer, $queue);
            }
            $queue
        }

        # Logic for queuing a buffer for grep /.../ :k
        sub k(uint $ordinal, $buffer) {
            my uint $offset = $base;
            $base = $base + nqp::elems(nqp::decont($buffer));

            my $queue := nqp::create(ParaQueue);
            $SCHEDULER.cue: {
                my uint $then = nqp::time;

                my $iterator := $buffer.Seq.grep($matcher, :k).iterator;
                nqp::until(
                  nqp::eqaddr((my $key := $iterator.pull-one),IterationEnd),
                  nqp::push($queue,$offset + $key)
                );
                self!queue-done($ordinal, $then, $buffer, $queue);
            }
            $queue
        }

        # Logic for queuing a buffer for grep /.../ :kv
        sub kv(uint $ordinal, $buffer) {
            my uint $offset = $base;
            $base = $base + nqp::elems(nqp::decont($buffer));

            my $queue := nqp::create(ParaQueue);
            $SCHEDULER.cue: {
                my uint $then = nqp::time;

                my $iterator := $buffer.Seq.grep($matcher, :kv).iterator;
                nqp::until(
                  nqp::eqaddr((my $key := $iterator.pull-one),IterationEnd),
                  nqp::stmts(
                    nqp::push($queue,$offset + $key),     # key
                    nqp::push($queue,$iterator.pull-one)  # value
                  )
                );
                self!queue-done($ordinal, $then, $buffer, $queue);
            }
            $queue
        }

        # Logic for queuing a buffer for grep /.../ :p
        sub p(uint $ordinal, $buffer) {
            my uint $offset = $base;
            $base = $base + nqp::elems(nqp::decont($buffer));

            my $queue := nqp::create(ParaQueue);
            $SCHEDULER.cue: {
                my uint $then = nqp::time;

                my $iterator := $buffer.Seq.grep($matcher, :kv).iterator;
                nqp::until(
                  nqp::eqaddr((my $key := $iterator.pull-one),IterationEnd),
                  nqp::push(
                    $queue,
                    Pair.new($offset + $key, $iterator.pull-one)
                  )
                );
                self!queue-done($ordinal, $then, $buffer, $queue);
            }
            $queue
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
          ?? self.IterationBuffer.List.first($matcher, :end, |%_)
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

    multi method head(   ParaSeq:D:) { self.Seq.head  }
    multi method tail(   ParaSeq:D:) { self.Seq.tail  }
    multi method is-lazy(ParaSeq:D:) { $!source.is-lazy }


#- coercers --------------------------------------------------------------------


    multi method IterationBuffer(ParaSeq:D:) {
        self.iterator.push-all(my $buffer := nqp::create(IterationBuffer));
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
