# Intended to be as fast as possible and to someday become part of the
# Rakudo core
use nqp;

my int $default-batch  = 10;
my int $default-degree = Kernel.cpu-cores-but-one;

#- ParaQueue -------------------------------------------------------------------
# A blocking concurrent queue to which one can nqp::push and from which one
# can nqp::shift
my class ParaQueue is repr('ConcBlockingQueue') { }

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
    has $!current;
    has $!queues;

    method new(\current) {
        my $self := nqp::create(self);
        nqp::bindattr($self,ParaIterator,'$!current',current);
        nqp::bindattr($self,ParaIterator,'$!queues',nqp::create(ParaQueue));
        $self
    }

    method nr-queues()          { nqp::elems($!queues)      }
    method add-queue(Mu \queue) { nqp::push($!queues,queue) }

    method pull-one() {
        my $pulled := nqp::shift($!current);

        nqp::while(
          nqp::eqaddr($pulled,IterationEnd),             # this queue exhausted
          nqp::if(
            nqp::eqaddr(($pulled := nqp::shift($!queues)),IterationEnd),
            (return IterationEnd),                       # no queue, really done
            $pulled := nqp::shift($!current := $pulled)  # set next queue
          )
        );

        $pulled
    }

    method push-all(\target) {
        my $queues  := $!queues;
        my $current := $!current;

        nqp::while(
          1,
          nqp::stmts(
            (my $pulled := nqp::shift($current)),
            nqp::while(
              nqp::eqaddr($pulled,IterationEnd),            # queue exhausted
              nqp::if(
                nqp::eqaddr(($pulled := nqp::shift($queues)),IterationEnd),
                (return IterationEnd),                      # really done
                $pulled := nqp::shift($current := $pulled)  # set next queue
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
    has     $!buffer;     # first buffer
    has     $!source;     # iterator producing source values
    has     $!result;     # iterator producing result values
    has     $!SCHEDULER;  # $*SCHEDULER to be used
    has int $.degree;     # number of CPUs, must be > 1
    has int $.initial;    # initial batch size, must be > 0
    has int $.batch;      # current batch size, must be > 0
    has int $!stop;       # stop all processing if 1

#- private helper methods ------------------------------------------------------

    # Do error checking and set up object if all ok
    method !setup(
      str $method, int $initial, int $degree, $buffer, $source
    ) is hidden-from-backtrace {
        X::Invalid::Value.new(:$method, :name<batch>,  :value($initial)).throw
          if $initial <= 0;
        X::Invalid::Value.new(:$method, :name<degree>, :value($degree)).throw
          if $degree <= 1;

        # Set it up!
        my $self := nqp::create(self);
        nqp::bindattr_i($self, ParaSeq, '$!degree',  $degree);
        nqp::bindattr_i($self, ParaSeq, '$!batch',
          nqp::bindattr_i($self, ParaSeq, '$!initial', $initial)
        );
        nqp::bindattr($self, ParaSeq, '$!SCHEDULER', $*SCHEDULER         );
        nqp::bindattr($self, ParaSeq, '$!buffer',    nqp::decont($buffer));
        nqp::bindattr($self, ParaSeq, '$!source',    $source             );
        $self
    }

    # Start the async process with the first buffer and the given buffer
    # queuing logic
    method !start(&queue-buffer, int $divisibility = 1) {

        # Local copy of first buffer
        my $first := $!buffer;
        $!buffer  := Mu;  # release buffer in object

        # Queue the first buffer we already filled, and set up the
        # result iterator
        $!result := my $result := ParaIterator.new(queue-buffer($first));

        # Make sure batch size has the right granularity
        unless $!batch %% $divisibility {
            $!batch =
              $!batch div $divisibility * $divisibility || $divisibility;
        }

        # Make sure the scheduling of further batches actually happen in a
        # separate thread
        my $source := $!source;
        $!SCHEDULER.cue: {

            # Until we're halted or have a buffer that's not full
            nqp::until(
              $!stop || nqp::eqaddr(
                $source.push-exactly(
                  (my $buffer := nqp::create(IterationBuffer)),
                  $!batch  # intentionally use the attribute so that any
                           # changes to it will be reflected then
                ),
                IterationEnd
              ),
              $result.add-queue(queue-buffer($buffer))
            );

            # Some leftovers to process?
            $result.add-queue(queue-buffer($buffer)) if nqp::elems($buffer);

            # No more result queues will come
            $result.add-queue(IterationEnd);
        }

        # All scheduled now, so let the show begin!
        self
    }

    # Entry point in chain, from an Iterator
    method !pass-the-chain(\source) {
        my $self := nqp::create(self);
        nqp::bindattr_i($self, ParaSeq, '$!degree', $!degree);
        nqp::bindattr_i($self, ParaSeq, '$!batch',
          nqp::bindattr_i($self, ParaSeq, '$!initial', $!initial)
        );
        nqp::bindattr($self, ParaSeq, '$!SCHEDULER', $!SCHEDULER);
        nqp::bindattr($self, ParaSeq, '$!source',    source     );
        $self
    }

    # Just count the number of produced values
    method !count() {
        my $iterator := self.iterator;
        my int $elems;
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
    method parent($source, int $initial, int $degree, str $method) {
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

    method default-batch()  { $default-batch  }
    method default-degree() { $default-degree }

    method degree(       ParaSeq:D:) { $!degree  }
    method initial-batch(ParaSeq:D:) { $!initial }
    method current-batch(ParaSeq:D:) { $!batch   }

#- map -------------------------------------------------------------------------

    proto method map(|) {*}
    multi method map(ParaSeq:D: Callable:D $mapper) {

        # Logic for queuing a buffer for map
        my $SCHEDULER := $!SCHEDULER;
        sub queue-buffer($buffer) {
            my $queue := nqp::create(ParaQueue);
            $SCHEDULER.cue: {
                my $iterator := $buffer.Seq.map($mapper).iterator;
                nqp::until(
                  nqp::eqaddr((my $pulled := $iterator.pull-one),IterationEnd),
                  nqp::push($queue,$pulled)
                );

                # Indicate this result queue is done
                nqp::push($queue,IterationEnd);
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
        my int $base;  # base offset for :k, :kv, :p

        # Logic for queuing a buffer for bare grep { }, producing values
        sub v($buffer) {
            my $queue := nqp::create(ParaQueue);
            $SCHEDULER.cue: {
                my $iterator := $buffer.Seq.grep($matcher).iterator;
                nqp::until(
                  nqp::eqaddr((my $pulled := $iterator.pull-one),IterationEnd),
                  nqp::push($queue,$pulled)
                );

                # Indicate this result queue is done
                nqp::push($queue,IterationEnd);
            }
            $queue
        }

        # Logic for queuing a buffer for grep { } :k
        sub k($buffer) {
            my int $offset = $base;
            $base = $base + nqp::elems(nqp::decont($buffer));

            my $queue := nqp::create(ParaQueue);
            $SCHEDULER.cue: {
                my $iterator := $buffer.Seq.grep($matcher, :k).iterator;
                nqp::until(
                  nqp::eqaddr((my $key := $iterator.pull-one),IterationEnd),
                  nqp::push($queue,$offset + $key)
                );

                # Indicate this result queue is done
                nqp::push($queue,IterationEnd);
            }
            $queue
        }

        # Logic for queuing a buffer for grep { } :kv
        sub kv($buffer) {
            my int $offset = $base;
            $base = $base + nqp::elems(nqp::decont($buffer));

            my $queue := nqp::create(ParaQueue);
            $SCHEDULER.cue: {
                my $iterator := $buffer.Seq.grep($matcher, :kv).iterator;
                nqp::until(
                  nqp::eqaddr((my $key := $iterator.pull-one),IterationEnd),
                  nqp::push($queue,$offset + $key),      # key
                  nqp::push($queue,$iterator.pull-one),  # value
                );

                # Indicate this result queue is done
                nqp::push($queue,IterationEnd);
            }
            $queue
        }

        # Logic for queuing a buffer for grep { } :p
        sub p($buffer) {
            my int $offset = $base;
            $base = $base + nqp::elems(nqp::decont($buffer));

            my $queue := nqp::create(ParaQueue);
            $SCHEDULER.cue: {
                my $iterator := $buffer.Seq.grep($matcher, :kv).iterator;
                nqp::until(
                  nqp::eqaddr((my $key := $iterator.pull-one),IterationEnd),
                  nqp::push(
                    $queue,
                    Pair.new($offset + $key, $iterator.pull-one)
                  )
                );

                # Indicate this result queue is done
                nqp::push($queue,IterationEnd);
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
        my int $base;  # base offset for :k, :kv, :p

        # Logic for queuing a buffer for grep /.../
        sub v($buffer) {
            my $queue  := nqp::create(ParaQueue);
            $SCHEDULER.cue: {
                my $iterator := $buffer.Seq.grep($matcher).iterator;
                nqp::until(
                  nqp::eqaddr((my $pulled := $iterator.pull-one),IterationEnd),
                  nqp::push($queue,$pulled)
                );

                # Indicate this result queue is done
                nqp::push($queue,IterationEnd);
            }
            $queue
        }

        # Logic for queuing a buffer for grep /.../ :k
        sub k($buffer) {
            my int $offset = $base;
            $base = $base + nqp::elems(nqp::decont($buffer));

            my $queue := nqp::create(ParaQueue);
            $SCHEDULER.cue: {
                my $iterator := $buffer.Seq.grep($matcher, :k).iterator;
                nqp::until(
                  nqp::eqaddr((my $key := $iterator.pull-one),IterationEnd),
                  nqp::push($queue,$offset + $key)
                );

                # Indicate this result queue is done
                nqp::push($queue,IterationEnd);
            }
            $queue
        }

        # Logic for queuing a buffer for grep /.../ :kv
        sub kv($buffer) {
            my int $offset = $base;
            $base = $base + nqp::elems(nqp::decont($buffer));

            my $queue := nqp::create(ParaQueue);
            $SCHEDULER.cue: {
                my $iterator := $buffer.Seq.grep($matcher, :kv).iterator;
                nqp::until(
                  nqp::eqaddr((my $key := $iterator.pull-one),IterationEnd),
                  nqp::push($queue,$offset + $key),      # key
                  nqp::push($queue,$iterator.pull-one),  # value
                );

                # Indicate this result queue is done
                nqp::push($queue,IterationEnd);
            }
            $queue
        }

        # Logic for queuing a buffer for grep /.../ :p
        sub p($buffer) {
            my int $offset = $base;
            $base = $base + nqp::elems(nqp::decont($buffer));

            my $queue := nqp::create(ParaQueue);
            $SCHEDULER.cue: {
                my $iterator := $buffer.Seq.grep($matcher, :kv).iterator;
                nqp::until(
                  nqp::eqaddr((my $key := $iterator.pull-one),IterationEnd),
                  nqp::push(
                    $queue,
                    Pair.new($offset + $key, $iterator.pull-one)
                  )
                );

                # Indicate this result queue is done
                nqp::push($queue,IterationEnd);
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
