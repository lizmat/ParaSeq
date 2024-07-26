# Intended to be as fast as possible and to someday become part of the
# Rakudo core
use v6.*;
use nqp;

#- ParaQueue -------------------------------------------------------------------
# A blocking concurrent queue to which one can nqp::push and from which one
# can nqp::shift
my class ParaQueue is repr('ConcBlockingQueue') { }

#- globals ---------------------------------------------------------------------

my uint $default-batch  = 16;
my uint $default-degree = Kernel.cpu-cores-but-one;
my uint $target-nsecs   = 1_000_000;  # .001 second
my uint $target-nsecs2  = nqp::mul_i($target-nsecs,2);

# Helper sub to determine granularity of batches depending on the signature
# of a Callable
my sub granularity(&callable) {
    my $count := &callable.count;
    $count == Inf ?? 1 !! $count
}

# Helper sub to return the positional argument as fast as possible
my sub identity(\value) is raw { value }

# Shortcut for long names, just for esthetics
my constant IB = IterationBuffer;
my constant IE = IterationEnd;

# An empty IterationBuffer, for various places where it is needed as a source
my constant emptyIB   = nqp::create(IB);
my constant emptyList = emptyIB.List;

# A separator line
my constant separator = "-" x 80;

#- exception handling ----------------------------------------------------------

my $exceptions := nqp::create(ParaQueue);

# Create a Bag of the exceptions so far
my sub bag-the-exceptions() {
    my $buffer := nqp::create(IB);
    nqp::while(
      nqp::elems($exceptions),
      nqp::push($buffer, nqp::shift($exceptions))
    );
    $buffer.List.map(*.gist.chomp).Bag
}

# Show the exceptions that weren't bagged before
END {
    if nqp::elems($exceptions) -> uint $total {
        my      $bagged := bag-the-exceptions;
        my uint $elems   = $bagged.elems;
        my str  $s       = $elems == 1 ?? "" !! "s";

        my $ERR := $*ERR;
        $ERR.say:
          "Caught $elems unique exception$s (out of $total) in hypered code:";
        for $bagged.sort(-*.value) {
            $ERR.say: separator;
            $ERR.print(.value ~ "x: ") if .value > 1;
            $ERR.say: .key;
        }
        $ERR.say: separator;
    }
}

#- BlockMapper -----------------------------------------------------------------

# A role to give a Block extra capabilities, needed to be able to run
# map / grep like parallel sequences
my role BlockMapper {
    has uint $!granularity;  # the granularity of the Block
    has      $!FIRSTs;       # any FIRST phasers
    has      $!LASTs;        # any LAST phasers

    # Set up the extra capabilities
    method setup(uint $granularity, :$no-first, :$no-last) {
        $!granularity = $granularity;
        my $phasers  := nqp::getattr(self,Block,'$!phasers');

        # If it is a single LEAVE phaser, we need to convert it to a
        # multi phaser hash first
        if nqp::isinvokable($phasers) {
            my $LEAVE := nqp::create(IB);
            nqp::push($LEAVE,$phasers);
            $phasers := nqp::hash(
              '!LEAVE-ORDER', $LEAVE, 'LEAVE', $LEAVE
            );
        }

        # Make sure we're going to do all stuff on a clone
        elsif $phasers {
            $phasers := nqp::clone($phasers);
        }

        # Otherwise set up a clean slate
        else {
            $phasers := nqp::hash;
        }

        # Save any FIRST phasers and remove them.  NOTE: this currently
        # only serves as a flag, as the FIRST phasers are actually
        # embedded into the code block itself.  But this may change
        # with RakuAST.
        nqp::deletekey($phasers,"FIRST")
          if $!FIRSTs := self.callable_for_phaser("FIRST");
        $!FIRSTs := Mu if $no-first;

        # Save any LAST phasers and remove them
        nqp::deletekey($phasers,"LAST")
          if $!LASTs := self.callable_for_phaser("LAST");
        $!LASTs := Mu if $no-last;

        # These are the phasers for now
        nqp::bindattr(self,Block,'$!phasers',$phasers);

        # Return invocant for convenience
        self
    }

    # The actual running logic, which makes sure that any FIRST / LAST phasers
    # are only run once, and any "last" in the block is also handled as
    # expected
    method run(
      uint $first,             # is this the first batch
      uint $last,              # is this the last batch
      str  $method,            # method to be executed
           $input     is raw,  # the IterationBuffer to work on
           $result    is raw,  # the result iterator
           $semaphore is raw,  # the semaphore to which result will be pushed
           $postprocess?       # any postprocessing to be done
    ) {
        # Make sure we have our own mapper / phasers
        my $this-mapper  := nqp::clone(self);
        my $this-phasers := nqp::clone(nqp::getattr(self,Block,'$!phasers'));
        nqp::bindattr($this-mapper,Block,'$!phasers',$this-phasers);

        # logic for setting a phaser for this run
        sub set-phaser(str $name, Callable:D $phaser) {
            nqp::push((my $buffer := nqp::create(IB)),$phaser);
            nqp::bindkey($this-phasers,$name,$buffer);
        }

        # Set the FIRST flag on the first batch only
        if $first && $!FIRSTs {

            # Actually make FIRST phaser getting ran (legacy).  Need to
            # create a clone of the code, as otherwise a second thread
            # *may* be running the block with the first flag still set
            # and thus cause the FIRST phaser to be executed (in the
            # legacy runtime)
            nqp::p6setfirstflag(
              nqp::bindattr(
                $this-mapper,Code,'$!do',
                nqp::clone(nqp::getattr($this-mapper,Code,'$!do'))
              )
            );
            set-phaser("FIRST", $!FIRSTs);
        }

        # This will become 1 if a "last" was executed
        my $*LAST;

        # Set LAST phaser, to actually execute if it's the last
        # batch, or a "last" got executed, as long as there was
        # no other thread having executed it
        set-phaser("LAST", {
            if ($last || $*LAST) && $!LASTs {
                $!LASTs();
                $!LASTs := Mu;  # make sure we only call LAST once
            }
        }) if $!LASTs;

        # Fetch the iterator and setup output buffer
        my $iterator := $input.List."$method"($this-mapper, |%_).iterator;
        my $output   := nqp::create(IB);

        # Run element by element if post-processing needed
        if $postprocess {
            nqp::until(
              nqp::eqaddr((my $pulled := $iterator.pull-one),IE),
              $postprocess($iterator, $output, $pulled)
            );
        }

        # Just get all the value
        else {
            $iterator.push-all($output);
        }

        # Mark this semaphore as the last to be handled if a
        # "last" was executed
        $result.set-lastsema($semaphore, $output) if $*LAST;

        # Return the result
        $output
    }

    # Fool the core's mapper into thinking that this block does not actually
    # have FIRST phaser, which stops it from setting the first flag (on
    # Rakudo 2024.06+)
    multi method has-phaser("FIRST" --> False) { }
    multi method has-phaser(Str:D) { nextsame }
}

#- BufferIterator --------------------------------------------------------------
# An iterator that takes a buffer and an iterator, and first produces all
# of the values from the buffer, and then from the iterator

my class BufferIterator does Iterator {
    has $!buffer;  # first buffer to produce from
    has $.source;  # iterator to produce from onwards

    method new(\buffer, \source) {
        my $self := nqp::create(self);
        nqp::bindattr($self,BufferIterator,'$!buffer',buffer);
        nqp::bindattr($self,BufferIterator,'$!source',source);
        $self
    }

    method pull-one(BufferIterator:D:) {
        nqp::elems($!buffer)
          ?? nqp::shift($!buffer)
          !! $!source.pull-one
    }

    method skip-at-least(BufferIterator:D: uint $skipping) {

        # Need to skip more than in initial buffer
        my uint $elems  = nqp::elems($!buffer);
        if $skipping > $elems {
            nqp::setelems($!buffer,0);
            $!source.skip-at-least($skipping - $elems)
        }

        # Less than in inital buffer, remove so many entries from it
        else {
            nqp::splice($!buffer,emptyIB,0,$skipping);
            1
        }
    }

    method push-all(BufferIterator:D: \target) {
        nqp::istype(target,IB)
          ?? nqp::splice(target,$!buffer,nqp::elems(target),0)
          !! $!buffer.iterator.push-all(target);
        nqp::setelems($!buffer,0);

        $!source.push-all(target)
    }

    method is-lazy(BufferIterator:D:) {
        $!source.is-lazy
    }
    method is-deterministic(BufferIterator:D:) {
        $!source.is-deterministic
    }
    method is-monotonically-increasing(BufferIterator:D:) {
        $!source.is-monotonically-increasing
    }

    # If there is a buffer available, return it and reset in object, else Nil
    method zap-buffer(BufferIterator:D:) {
        my $buffer := $!buffer;
        if nqp::elems($buffer) {
            $!buffer := nqp::create(IB);
            $buffer
        }
        else {
            Nil
        }
    }

    method stats(--> emptyList) { }
}

#- SquishIterator --------------------------------------------------------------
# An iterator that takes a source iterator that produces batches of values
# and which compares the last of a batch with the first of the next batch
# using .squish semantics

my class SquishIterator does Iterator {
    has $!source    # source iterator
      handles <is-lazy is-deterministic is-monotonically-increasing>;
    has $!as;       # any transformation prior to comparison
    has $!with;     # comparator to be used
    has $!current;  # the current buffer producing values
    has $!last;     # last value seen of the current buffer

    method new($source, &as, &with) {
        my $self := nqp::create(self);
        nqp::bindattr($self,SquishIterator,'$!source', $source         );
        nqp::bindattr($self,SquishIterator,'$!as',     &as             );
        nqp::bindattr($self,SquishIterator,'$!with',   &with           );
        nqp::bindattr($self,SquishIterator,'$!current',$source.pull-one);
        $self
    }

    method !next-batch() {
        my $current := $!source.pull-one;
        if nqp::eqaddr($current,IE) {
            IterationEnd
        }
        else {
            my $nextlast := nqp::atpos($current,-1);
            nqp::shift($current)
              if $!with($!as(nqp::atpos($current,0)),$!as($!last));

            nqp::elems($!current := $current)
              ?? nqp::shift($current)
              !! self!next-batch
        }
    }

    method pull-one(SquishIterator:D:) {
        nqp::elems(my $current := $!current) > 1
          ?? nqp::shift($current)
          !! nqp::elems($current)
            ?? ($!last = nqp::shift($current))
            !! self!next-batch
    }

    method push-all(SquishIterator:D: \target --> IterationEnd) {
        my $source  := $!source;
        my &as      := $!as;
        my &with    := $!with;
        my $current := $!current;

        my $last := nqp::elems($current) ?? nqp::atpos($current,-1) !! $!last;
        target.append($current);

        nqp::until(
          nqp::eqaddr(($current := $source.pull-one),IE),
          nqp::stmts(
            (my $nextlast := nqp::atpos($current,-1)),
            nqp::if(
              with(as(nqp::atpos($current,0)),as($last)),
              nqp::shift($current)
            ),
            ($last := $nextlast),
            target.append($current)
          )
        );
    }
}

#- WhichIterator ---------------------------------------------------------------
# An iterator that takes a buffer filled with values on even locations, and
# .WHICH strings on odd values, and produces just the values on the even
# positions

my class WhichIterator does Iterator {
    has $!buffer;    # buffer to produce from

    method new(\buffer) {
        my $self := nqp::create(self);
        nqp::bindattr($self,WhichIterator,'$!buffer',buffer);
        $self
    }

    method pull-one() {
        my $buffer := $!buffer;

        nqp::if(
          nqp::elems($buffer),
          nqp::stmts(
            (my $pulled := nqp::shift($buffer)),
            nqp::shift($buffer),
            $pulled
          ),
          IterationEnd
        )
    }

    method skip-at-least(uint $skipping is copy) {
        $skipping = $skipping + $skipping;

        nqp::if(
          $skipping > nqp::elems($!buffer),
          0,
          nqp::stmts(
            nqp::splice($!buffer,emptyIB,0,$skipping),
            1
          )
        )
    }

    method push-all(\target --> IterationEnd) {
        my $buffer := $!buffer;

        nqp::while(
          nqp::elems($buffer),
          nqp::stmts(
            nqp::push(target,nqp::shift($buffer)),
            nqp::shift($buffer)
          )
        );
    }

    method stats(--> emptyList) { }
}

#- UniqueIterator --------------------------------------------------------------
# A variation on the WhichIterator that also takes a hash with lookup status
# If a key exists in the seen hash, and the value is > 0, then it is the first
# time this value is produced.  In that case, produce the value and set the
# value in the hash to 0 (indicating any further occurrences should be ignored

my class UniqueIterator does Iterator {
    has $!seen;    # lookup hash: value > 1 means first, so pass, 0 means skip
    has $!buffer;  # buffer with value / key alternates

    method new(\seen, \buffer) {
        my $self := nqp::create(self);
        nqp::bindattr(
          $self,UniqueIterator,'$!seen',nqp::getattr(seen,Map,'$!storage')
        );
        nqp::bindattr($self,UniqueIterator,'$!buffer',buffer);
        $self
    }

    method pull-one() {
        my $seen   := $!seen;
        my $buffer := $!buffer;

        nqp::while(
          nqp::elems($buffer),
          nqp::stmts(
            (my $pulled := nqp::shift($buffer)),
            nqp::if(
              nqp::existskey($seen,(my str $key = nqp::shift($buffer))),
              nqp::if(
                nqp::atkey($seen,$key),
                nqp::stmts(
                  nqp::bindkey($seen,$key,0),
                  (return $pulled)
                )
              ),
              (return $pulled)
            )
          )
        );

        IterationEnd
    }
}

#- ParaIterator ----------------------------------------------------------------
# An iterator that takes a number of ParaQueues and produces all values
# from them until the last ParaQueue has been exhausted.  Note that the
# queue of queues is also a ParaQueue so that queues can also be added
# and removed in a thread-safe manner

my class ParaIterator does Iterator {
    has $!parent                # ParaSeq parent object
      handles <is-lazy is-deterministic is-monotonically-increasing>;

    has           $!current;    # current producer
    has ParaQueue $!queues;     # other queues to produce from
    has ParaQueue $!pressure;   # backpressure provider
    has ParaQueue $.waiting;    # queue with unprocessed ParaStats objects
    has IB        $!stats;      # list with processed ParaStats objects
    has uint      $!racing;     # 1 if racing, 0 if hypering
    has uint      $!batch;      # initial / current batch size
    has uint      $!auto;       # 1 if batch size automatically adjust on load
    has uint      $!processed;  # number of items processed so far
    has uint      $!produced;   # number of items produced so far
    has uint      $!nsecs;      # nano seconds wallclock used so far
    has           $!lastsema;   # the last semaphore that should be processed
    has atomicint $!unhandled;  # number of unhandled batches

    method new(
           \parent,
           \pressure,
      uint $racing,
      uint $batch,
      uint $auto,
    ) {
        my $self := nqp::create(self);

        nqp::bindattr($self,ParaIterator,'$!parent',  parent                );
        nqp::bindattr($self,ParaIterator,'$!current', emptyIB               );
        nqp::bindattr($self,ParaIterator,'$!queues',  nqp::create(ParaQueue));
        nqp::bindattr($self,ParaIterator,'$!pressure',pressure              );
        nqp::bindattr($self,ParaIterator,'$!waiting', nqp::create(ParaQueue));
        nqp::bindattr($self,ParaIterator,'$!stats',   nqp::create(IB)       );

        nqp::bindattr_i($self,ParaIterator,'$!racing', $racing);
        nqp::bindattr_i($self,ParaIterator,'$!batch',  $batch );
        nqp::bindattr_i($self,ParaIterator,'$!auto',   $auto  );

        # Set up initial barrier.  Once the final batch has been scheduled
        # for processing, .close-queues will decrement this final value and
        # thus indicate completion
        nqp::bindattr_i($self,ParaIterator,'$!unhandled',1);
        $self
    }

    # Mark another batch for processing
    method mark-batch-for-processing() {
        nqp::atomicinc_i($!unhandled);
    }

    # Handling when the current result buffer is exhausted
    method !next() {
        if nqp::atomicload_i($!unhandled) {
            nqp::atomicdec_i($!unhandled);
            nqp::shift($!queues);  # block until provided/IE on premature end
        }
        else {
            IE                     # delivered all batches
        }
    }

    # Thread-safely handle next batch
    method !next-batch($next is raw) {

        # Next buffer to be processed
        my $buffer := $!racing ?? $next !! nqp::shift($next);

        # Stop delivering after this buffer
        if nqp::eqaddr($next,$!lastsema) {
            nqp::unshift($!queues,IE);
        }

        # Need to handle stats to calculate optimum batch size
        elsif $!auto {
            self!update-stats unless nqp::isnull(nqp::atpos($!waiting,0));
            $!batch = nqp::div_i(nqp::mul_i($!processed,$target-nsecs),$!nsecs)
              if $!nsecs;

            # Initiate more work with possibly updated batch size
            nqp::push($!pressure,$!batch);
        }

        # No need to handle stats here and now
        else {
            # Initiate more work using default batch value calculated
            nqp::push($!pressure,$!batch);
        }

        $buffer
    }

    # Update statistics from the waiting queue
    method !update-stats() {
        # Fetch statistics of batches that have been produced until now
        my uint $processed = $!processed;
        my uint $produced  = $!produced;
        my uint $nsecs     = $!nsecs;
        nqp::until(
          nqp::isnull(nqp::atpos($!waiting,0)),
          nqp::stmts(
            nqp::push($!stats,my $stats := nqp::shift($!waiting)),
            ($processed = nqp::add_i($processed,$stats.processed     )),
            ($produced  = nqp::add_i($produced, $stats.produced      )),
            ($nsecs     = nqp::add_i($nsecs,    $stats.smoothed-nsecs))
          )
        );

        # Update total statistics
        $!processed = $processed;
        $!produced  = $produced;
        $!nsecs     = $nsecs;
    }

    method pull-one(ParaIterator:D:) {
        nqp::if(
          nqp::elems($!current),
          (my $pulled := nqp::shift($!current)),
          nqp::if(
            nqp::eqaddr((my $next := self!next),IE),
            (return IE),
            nqp::stmts(
              ($!current := self!next-batch($next)),
              ($pulled   := self.pull-one)
            )
          )
        );

        $pulled
    }

    method skip-at-least(ParaIterator:D: uint $skipping) {
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
              nqp::eqaddr((my $next := self!next),IE),
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
    multi method push-all(ParaIterator:D: IterationBuffer:D \target) {
        my $current  := $!current;

        nqp::while(
          1,
          nqp::stmts(
            nqp::splice(target,$current,nqp::elems(target),0),
            nqp::if(
              nqp::eqaddr((my $next := self!next),IE),
              (return IE),
              ($current := self!next-batch($next))
            )
          )
        );
    }

    # Slower generic version that needs to coerce each buffer to a List
    # to ensure the correct semantics with .append
    multi method push-all(ParaIterator:D: \target) {
        my $current  := $!current;

        nqp::while(
          1,
          nqp::stmts(
            target.append($current.List),
            nqp::if(
              nqp::eqaddr((my $next := self!next),IE),
              (return IE),
              ($current := self!next-batch($next))
            )
          )
        );
    }

    # Optimized version used for its side-effects
    method sink-all(ParaIterator:D:) {
        nqp::while(
          1,
          nqp::if(
            nqp::eqaddr((my $next := self!next),IE),
            (return IE),
            self!next-batch($next)
          )
        );
    }

    method close-queues(ParaIterator:D:) is implementation-detail {
        nqp::atomicdec_i($!unhandled)  # remove the barrier
    }

    method stats(ParaIterator:D:) is raw is implementation-detail {
        self!update-stats;
        $!stats
    }

    # Set the last batch to be produced, unless it was already set:
    # the first one set, wins!
    method set-lastsema($lastsema is raw, $output is raw) {
        $!lastsema := $!racing ?? $output !! $lastsema
          unless nqp::isconcrete($!lastsema);
    }

    # Add a semaphore to the queue.  This is a ParaQueue to which a fully
    # processed IterationBuffer will be pushed when it is ready.  This
    # guarantees that the order of the results will be the order in which
    # this method will be called.  When racing, just return the queues
    # Paraqueue to push to whenever results are ready to be pushed
    method semaphore() is implementation-detail {
        $!racing
          ?? $!queues
          !! nqp::push($!queues,nqp::create(ParaQueue))
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

    # If the time it took for the batch to be processed is more than twice
    # the target slice time, then most likely there has been a garbage
    # collection run, which halts the world for a tiny bit.  But enough to
    # mess up any calculations.  So we basically do a modulo on the time,
    # but make sure we have at least the target slice time in there as well.
    # This appears to produce the best timing calculations
    method smoothed-nsecs(ParaStats:D:) {
        $!nsecs > $target-nsecs2
          ?? nqp::add_i(nqp::mod_i($!nsecs,$target-nsecs),$target-nsecs)
          !! $!nsecs
    }

    multi method gist(ParaStats:D:) {
        "#$!threadid - $!ordinal: $!processed / $!produced ($!nsecs nsecs)"
    }
}

#- ParaSeq ---------------------------------------------------------------------
# The class containing all of the logic for parallel sequences
class ParaSeq is Seq {
    has           $!source;      # iterator producing source values
    has           $!SCHEDULER;   # $*SCHEDULER to be used
    has           $!snitcher;    # the snitcher to be called before/after
    has uint      $!racing;      # 1 = racing, 0 = hypering
    has uint      $!batch;       # initial batch size, must be > 0
    has uint      $!auto;        # 1 if batch size automatically adjusts
    has uint      $!catch;       # 1 if exceptions should be caught
    has uint      $!degree;      # number of CPUs, must be > 1

#- "last" handling -------------------------------------------------------------

    # A very hacky way to catch the excution of "last".  This requires the
    # the scope of the mapper to be within the scope where the ParaSeq module
    # is imported to
    proto sub last(|) is export {*}
    multi sub last() {
        my $LAST := $*LAST;
        $LAST = 1 unless nqp::istype($LAST,Failure);
        &CORE::last()
    }
    multi sub last(Mu $value) {
        my $LAST := $*LAST;
        $LAST = 1 unless nqp::istype($LAST,Failure);
        &CORE::last($value)
    }

#- private helper methods ------------------------------------------------------

    method !cue(&callable) {
        my $promise;

        # Already living inside an async environment, so use this Promise
        # with its dynamic context
        with $*PROMISE {
            $promise := $_;
        }

        # First time we do an async call: create a promise that can be
        # living inside the async code's scope, to allow access to the
        # dynamic variables living in *this* dynamic context
        else {
            $promise := Promise.new(:scheduler($!SCHEDULER));
            nqp::bindattr($promise,Promise,'$!dynamic_context',nqp::ctx);
        }

        # Set up code to be actually queued, which adds a $*PROMISE to its
        # dynamic scope, so that references to dynamic variables will refer
        # to dynamic variables of the original scope
        my &code := {
            my $*PROMISE := $promise;
            callable()
        }

        # Perform the actual cueing
        $!catch
          ?? $!SCHEDULER.cue: &code, :catch({
                 nqp::push($exceptions,$_);
                 .resume
             })
          !! $!SCHEDULER.cue: &code
    }

    # Set up object and return it
    method !setup(
           $source,
      uint $racing,
      uint $batch,
      uint $auto,
      uint $catch,
      uint $degree,
    ) is hidden-from-backtrace {
        my $self := nqp::create(self);
        nqp::bindattr($self, ParaSeq, '$!source',    $source    );
        nqp::bindattr($self, Seq,     '$!iter',      nqp::null  );
        nqp::bindattr($self, ParaSeq, '$!SCHEDULER', $*SCHEDULER);
        nqp::bindattr($self, ParaSeq, '$!snitcher',  nqp::null  );

        nqp::bindattr_i($self, ParaSeq, '$!racing', $racing);
        nqp::bindattr_i($self, ParaSeq, '$!batch',  $batch );
        nqp::bindattr_i($self, ParaSeq, '$!auto',   $auto  );
        nqp::bindattr_i($self, ParaSeq, '$!catch',  $catch );
        nqp::bindattr_i($self, ParaSeq, '$!degree', $degree);
        $self
    }

    # Start the async process with the given processor logic and the
    # required granularity for producing values
    method !start(&processor, uint $granularity = 1, uint :$slow) {

        # Logic for making sure batch size has correct granularity
        my &granulize := $granularity == 1
          ?? -> uint $batch { $batch }
          !! -> uint $batch {
                nqp::mod_i($batch,$granularity)
                  ?? nqp::mul_i(nqp::div_i($batch,$granularity),$granularity)
                       || $granularity
                  !! $batch
             }

        # Get copy of first buffer if possible
        my $first;
        if nqp::istype($!source,BufferIterator)
          && ($first := $!source.zap-buffer) {

            # We can get rid of the BufferIterator now
            $!source := $!source.source;
            nqp::until(
              nqp::elems($first) %% $granularity
                || nqp::eqaddr((my $pulled := $!source.pull-one),IE),
              nqp::push($first,$pulled)
            );
        }

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

        # Set up the result iterator
        nqp::bindattr(
          self, Seq, '$!iter',
          ParaIterator.new(self, $pressure, $!racing, $!batch, $!auto)
        );

        # Batcher logic for fast batching, where no special phaser handling
        # is required
        sub fast() {
            my $source   := $!source;
            my $result   := nqp::getattr(self,Seq,'$!iter');
            my $snitcher := $!snitcher;

            my uint $ordinal;    # ordinal number of batch
            my uint $exhausted;  # flag: 1 if exhausted


            # Queue the first buffer we already filled, and set up the
            # result iterator
            if nqp::isconcrete($first) {
                $snitcher($first) unless nqp::isnull($snitcher);
                $result.mark-batch-for-processing;
                processor($ordinal, $first, $result.semaphore);
            }

            # Until we're halted or have a buffer that's not full
            nqp::until(
              $exhausted,       # nothing left to batch
              nqp::stmts(
                ($exhausted = nqp::eqaddr(
                  $source.push-exactly(
                    (my $buffer := nqp::create(IB)),
                    granulize(nqp::shift($pressure))  # wait for ok to proceed
                  ),
                  IE
                )),
                nqp::if(         # add if something to add
                  nqp::elems($buffer),
                  nqp::stmts(
                    nqp::unless(
                      nqp::isnull($snitcher),
                      $snitcher($buffer.List)
                    ),
                    $result.mark-batch-for-processing;
                    processor(++$ordinal, $buffer, $result.semaphore);
                  )
                )
              )
            );

            # No more result queues will come
            $result.close-queues
        }

        # Batcher logic for slower batching, which tells each process unit
        # whether or not it is the last batch, so that it can make sure the
        # correct phasers are executed
        sub slow() {
            my $source   := $!source;
            my $result   := nqp::getattr(self,Seq,'$!iter');
            my $snitcher := $!snitcher;

            my uint $ordinal; # ordinal number of batch

            # flag: 1 if exhausted
            my uint $exhausted =
              nqp::eqaddr((my $initial := $source.pull-one),IE);

            # Queue the first buffer we already filled, and set up the
            # result iterator
            if nqp::isconcrete($first) {
                $snitcher($first) unless nqp::isnull($snitcher);
                $result.mark-batch-for-processing;
                processor($ordinal, $exhausted, $first, $result.semaphore);
            }

            # Until the source iterator is exhausted or we're halted
            until $exhausted {

                # Setup buffer with initial value
                nqp::push((my $buffer := nqp::create(IB)),$initial);

                # Wait for ok to proceed
                if granulize(nqp::shift($pressure)) -> uint $needed {

                    # Fetch any additional values needed and set flag
                    $exhausted = nqp::eqaddr(
                      $source.push-exactly($buffer,$needed - 1),IE
                    );
                }

                # If not already exhausted attempt to get one value more
                $exhausted = nqp::eqaddr(($initial := $source.pull-one),IE)
                  unless $exhausted;

                # Process if there's something to process
                if nqp::elems($buffer) {

                    # Snitch if so indicated
                    $snitcher($buffer.List) unless nqp::isnull($snitcher);

                    # Schedule the batch
                    $result.mark-batch-for-processing;
                    processor(
                      ++$ordinal, $exhausted, $buffer, $result.semaphore
                    );
                }
            }

            # No more result queues will come
            $result.close-queues
        }

        # Make sure the scheduling of further batches actually happen in a
        # separate thread, whether they'd be slow or fast
        self!cue: $slow ?? &slow !! &fast;

        # All scheduled now, so let the show begin!
        self
    }

    # If there was no result iterator yet, return invocant: can still use
    # this object.  If there was, clone the invocant and make the result
    # iterator the source for the clone.
    method !re-shape() {
        if nqp::isnull(nqp::getattr(self,Seq,'$!iter')) {
            self
        }
        else {
            my $self := nqp::clone(self);
            nqp::bindattr(
              $self,ParaSeq,'$!source',
              nqp::getattr(self,Seq,'$!iter')
            );
            nqp::bindattr($self,Seq,'$!iter',nqp::null);
            $self
        }
    }

    # Create a clone of the invocant, set the given iterator as source and
    # reset the result iterator
    method !re-source($source is raw) {
        my $self := nqp::clone(self);
        nqp::bindattr($self,ParaSeq,'$!source',$source);
        nqp::bindattr($self,Seq,'$!iter',nqp::null);
        $self
    }

    # Mark the given queue as done
    method !batch-done(
      uint $ordinal,
      uint $then,
           $input is raw,
           $semaphore is raw,
           $output is raw
    ) {
        # Push the statistics into the waiting queue of the iterator
        # so that it has up-to-date information to determine future
        # batch sizes
        nqp::push(
          nqp::getattr(self,Seq,'$!iter').waiting,
          ParaStats.new(
            $ordinal,
            nqp::elems($input),
            nqp::elems($output),
            nqp::time() - $then
          )
        ) if nqp::istype(nqp::getattr(self,Seq,'$!iter'),ParaIterator);

        # Make the produced values available to the result iterator
        nqp::push($semaphore,$output);
    }

#- debugging -------------------------------------------------------------------

    multi method snitch(ParaSeq:D: &snitcher = &note) {
        $!snitcher := &snitcher;
        self
    }

#- where all the magic happens under the hood ----------------------------------

    method iterator(ParaSeq:D:) {
        nqp::ifnull(
          nqp::getattr(self,Seq,'$!iter'),
          $!source
        )
    }

    method sink(ParaSeq:D: --> Nil) { self.iterator.sink-all }

#- introspection ---------------------------------------------------------------

    method auto(ParaSeq:D:) { nqp::hllbool($!auto) }

    method batch-sizes(ParaSeq:D:) {
        self.stats.map(*.processed).minmax
    }

    method catch(ParaSeq:D:) { nqp::hllbool($!catch) }

    method default-batch()  is raw { $default-batch  }
    method default-degree() is raw { $default-degree }

    method exceptions() { bag-the-exceptions }

    multi method is-lazy(ParaSeq:D:) { $!source.is-lazy }

    method is-deterministic(ParaSeq:D:) {
        nqp::hllbool($!source.is-deterministic)
    }
    method is-monotonically-increasing(ParaSeq:D:) {
        nqp::hllbool($!source.is-monotonically-increasing)
    }

    method racing(ParaSeq:D:) { nqp::hllbool($!racing) }

    method stats(ParaSeq:D:) {
        nqp::isnull(nqp::getattr(self,Seq,'$!iter'))
          ?? ()
          !! nqp::getattr(self,Seq,'$!iter').stats.List
    }

    method processed(ParaSeq:D:) { self.stats.map(*.processed).sum }
    method produced(ParaSeq:D:)  { self.stats.map(*.produced ).sum }

    method threads(ParaSeq:D:) {
        self.stats.map(*.threadid).unique.List
    }

#- endpoints -------------------------------------------------------------------

    proto method first(|) {*}
    multi method first(ParaSeq:D:) { self.head }
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

    proto method head(|) {*}
    multi method head(ParaSeq:D:) {
        my $value := self.iterator.pull-one;
        nqp::eqaddr($value,IE) ?? Nil !! $value
    }

    multi method join(ParaSeq:D: Str:D $joiner = '') {
        self.fail-iterator-cannot-be-lazy('.join') if self.is-lazy;

        # Logic for joining a buffer
        sub processor(uint $ordinal, $input is raw, $semaphore is raw) {
            self!cue: {
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

    proto method minmax(|) {*}
    multi method minmax(ParaSeq:D: &by = &[cmp]) {
        self.fail-iterator-cannot-be-lazy('.minmax') if self.is-lazy;

        # Logic for joining a buffer
        sub processor(uint $ordinal, $input is raw, $semaphore is raw) {
            self!cue: {
                my uint $then = nqp::time;

                my $min := my $max := nqp::atpos($input,0);

                my uint $elems = nqp::elems($input);
                my uint $i;
                nqp::while(
                  nqp::islt_i(++$i,$elems),  # intentionally skip first
                  nqp::stmts(
                    my $value := nqp::atpos($input,$i);
                    nqp::if(
                      nqp::islt_i(by($value,$min),0),
                      $min := $value
                    ),
                    nqp::if(
                      nqp::isgt_i(by($value,$max),0),
                      $max := $value
                    )
                  )
                );

                my $output := nqp::create(IB);
                nqp::push($output,$min);
                nqp::push($output,$max);

                self!batch-done($ordinal, $then, $input, $semaphore, $output);
            }
        }

        # Start processing and return result of batches
        my $batches := self!start(&processor).IterationBuffer;

        # Post process the result
        my $min := nqp::shift($batches);
        my $max := nqp::shift($batches);
        my $value;

        nqp::while(
          nqp::elems($batches),
          nqp::stmts(
            nqp::if(
              nqp::islt_i(by(($value := nqp::shift($batches)),$min),0),
              ($min := $value)
            ),
            nqp::if(
              nqp::isgt_i(by(($value := nqp::shift($batches)),$max),0),
              ($max := $value)
            )
          )
        );

        Range.new($min, $max)
    }

    proto method pick(|) {*}
    multi method pick(ParaSeq:D:) { self.List.pick }

    multi method reduce(ParaSeq:D: Callable:D $reducer) {

        # Logic for reducing a buffer
        sub processor(uint $ordinal, $input is raw, $semaphore is raw) {
            self!cue: {
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
        sub processor(uint $ordinal, $input is raw, $semaphore is raw) {
            self!cue: {
                my uint $then = nqp::time;

                nqp::push((my $output := nqp::create(IB)),$input.List.sum);

                self!batch-done($ordinal, $then, $input, $semaphore, $output);
            }
        }

        # Finish off by summing the sums
        self!start(&processor).List.sum
    }

    proto method tail(|) {*}
    multi method tail(ParaSeq:D:) { self.List.tail }

#-- antipairs ------------------------------------------------------------------

    multi method antipairs(ParaSeq:D:) {
        my uint $base;  # base offset

        # Logic for queuing a buffer for .antipairs
        sub processor(uint $ordinal, $input is raw, $semaphore is raw) {
            my uint $offset = $base;
            my uint $elems  = nqp::elems($input);
            $base = $base + $elems;

            self!cue: {
                my uint $then    = nqp::time;
                my      $output := nqp::create(IB);

                # Store the Pairs as fast as possible
                my uint $i;
                nqp::while(
                  $i < $elems,
                  nqp::push(
                    $output,
                    Pair.new(nqp::atpos($input,$i),$offset + $i++)
                  )
                );

                self!batch-done($ordinal, $then, $input, $semaphore, $output);
            }
        }

        # Let's go!
        self!re-shape!start(&processor)
    }

#- batch -----------------------------------------------------------------------

    method !batch(uint $size, uint $partial) {

        # Logic for queuing a buffer for batches
        sub processor(uint $ordinal, $input is raw, $semaphore is raw) {
            self!cue: {
                my uint $then    = nqp::time;
                my      $output := nqp::create(IB);

                # Ultra-fast batching
                my uint $elems = nqp::elems($input);
                my uint $start;
                my uint $end = nqp::sub_i($size,1);
                nqp::while(
                  $start < $elems,
                  nqp::stmts(
                    nqp::push($output,nqp::slice($input,$start,$end).List),
                    ($start = nqp::add_i($start,$size)),
                    ($end   = nqp::add_i($end,  $size)),
                    nqp::if(
                      $end >= $elems,
                      nqp::if(                          # incomplete final batch
                        $partial,
                        ($end = nqp::sub_i($elems,1)),  # take final partially
                        ($start = $elems)               # stop iterating
                      )
                    )
                  )
                );
                self!batch-done($ordinal, $then, $input, $semaphore, $output);
            }
        }

        # Let's go!
        self!re-shape!start(&processor, $size)
    }

    proto method batch(|) {*}
    multi method batch(ParaSeq:D: Int:D :$elems!) { self!batch($elems,1) }
    multi method batch(ParaSeq:D: Int:D  $elems ) { self!batch($elems,1) }

#- deepmap ---------------------------------------------------------------------

    multi method deepmap(ParaSeq:D: Block:D $mapper) {
        my str @phasers;
        nqp::push(@phasers,$_) if $mapper.has-phaser($_) for <FIRST NEXT LAST>;
        warn "@phasers.join(", ") phaser(s) will be ignored" if @phasers;

        my uint $granularity = granularity($mapper);
        $granularity == 1
          ?? self!mapBlock($mapper, 'deepmap', :no-first, :no-last)
          !! die "Can only handle blocks with one parameter, got $granularity"
    }
    multi method deepmap(ParaSeq:D: Callable:D $mapper) {
        self!mapCallable($mapper, 'deepmap')
    }

#- duckmap ---------------------------------------------------------------------

    multi method duckmap(ParaSeq:D: Block:D $mapper) {
        my str @phasers;
        nqp::push(@phasers,$_) if $mapper.has-phaser($_) for <FIRST NEXT LAST>;
        warn "@phasers.join(", ") phaser(s) will be ignored" if @phasers;

        my uint $granularity = granularity($mapper);
        $granularity == 1
          ?? self!mapBlock($mapper, 'duckmap', :no-first, :no-last)
          !! die "Can only handle blocks with one parameter, got $granularity"
    }
    multi method duckmap(ParaSeq:D: Callable:D $mapper) {
        self!mapCallable($mapper, 'duckmap')
    }

#- grep ------------------------------------------------------------------------

    # Handling Callables that can have phasers
    multi method grep(ParaSeq:D: Block:D $matcher, :$k, :$kv, :$p) {

        # Need to have postprocessing
        if $k || $kv || $p { 
            my uint $granularity = granularity($matcher);
            my uint $base;  # base offset to be added for each batch

            my %nameds := Map.new( $k ?? :k !! :kv );
            my $mapper := (nqp::clone($matcher) but BlockMapper)
              .setup($granularity, :no-first, :no-last);

            sub processor(
              uint $ordinal,
              uint $last,
                   $input     is raw,
                   $semaphore is raw,
            ) {
                my uint $offset = $base;
                $base = $base + nqp::elems($input);

                # Set up post-processor depend on named argument.  Needs
                # to be done here because of scoping of $offset
                my $postprocess := $k
                  ?? -> $, $output is raw, $key {
                         nqp::push($output,$offset + $key)
                     }
                  !! $kv
                    ?? -> $iterator, $output is raw, $key {
                           nqp::push($output,$offset + $key),     # key
                           nqp::push($output,$iterator.pull-one)  # value
                       }
                    !! -> $iterator, $output is raw, $key {
                           nqp::push(
                             $output,
                             Pair.new($offset + $key, $iterator.pull-one)
                           )
                       }

                # Cue the actual work
                self!cue: {
                    my uint $then = nqp::time;

                    self!batch-done(
                      $ordinal, $then, $input, $semaphore,
                      $mapper.run(
                        $ordinal == 0, $last,
                        'grep', $input, nqp::getattr(self,Seq,'$!iter'),
                        $semaphore, $postprocess, |%nameds
                      )
                    );
                }
            }

            # Let's go using the slower path
            self!re-shape!start(&processor, $granularity, :slow)
        }       

        # Use standard "map-like" handling
        else {
            self!mapBlock($matcher, 'grep', :no-first, :no-last)
        }
    }

    # Handling other Callables that cannot have phasers
    multi method grep(ParaSeq:D: Callable:D $matcher, :$k, :$kv, :$p) {
        my uint $base;  # base offset for :k, :kv, :p

        # Logic for queuing a buffer for bare grep { }, producing values
        sub v(uint $ordinal, uint $last, $input is raw, $semaphore is raw) {
            self!cue: {
                my uint $then    = nqp::time;
                my      $output := nqp::create(IB);

                $input.Seq.grep($matcher).iterator.push-all($output);

                self!batch-done($ordinal, $then, $input, $semaphore, $output);
            }
        }

        # Logic for queuing a buffer for grep { } :k
        sub k(uint $ordinal, uint $last, $input is raw, $semaphore is raw) {
            my uint $offset = $base;
            $base = $base + nqp::elems($input);

            self!cue: {
                my uint $then    = nqp::time;
                my      $output := nqp::create(IB);

                my $iterator := $input.Seq.grep($matcher, :k).iterator;
                nqp::until(
                  nqp::eqaddr((my $key := $iterator.pull-one),IE),
                  nqp::push($output,$offset + $key)
                );
                self!batch-done($ordinal, $then, $input, $semaphore, $output);
            }
        }

        # Logic for queuing a buffer for grep { } :kv
        sub kv(uint $ordinal, uint $last, $input is raw, $semaphore is raw) {
            my uint $offset = $base;
            $base = $base + nqp::elems($input);

            self!cue: {
                my uint $then    = nqp::time;
                my      $output := nqp::create(IB);

                my $iterator := $input.Seq.grep($matcher, :kv).iterator;
                nqp::until(
                  nqp::eqaddr((my $key := $iterator.pull-one),IE),
                  nqp::stmts(
                    nqp::push($output,$offset + $key),     # key
                    nqp::push($output,$iterator.pull-one)  # value
                  )
                );
                self!batch-done($ordinal, $then, $input, $semaphore, $output);
            }
        }

        # Logic for queuing a buffer for grep { } :p
        sub p(uint $ordinal, uint $last, $input is raw, $semaphore is raw) {
            my uint $offset = $base;
            $base = $base + nqp::elems($input);

            self!cue: {
                my uint $then    = nqp::time;
                my      $output := nqp::create(IB);

                my $iterator := $input.Seq.grep($matcher, :kv).iterator;
                nqp::until(
                  nqp::eqaddr((my $key := $iterator.pull-one),IE),
                  nqp::push(
                    $output,
                    Pair.new($offset + $key, $iterator.pull-one)
                  )
                );
                self!batch-done($ordinal, $then, $input, $semaphore, $output);
            }
        }

        # Let's go!
        self!re-shape!start(
          $k ?? &k !! $kv ?? &kv !! $p ?? &p !! &v,
          granularity($matcher),
          :slow
        )
    }

    # Handling smart-matching logic
    multi method grep(ParaSeq:D: $matcher, :$k, :$kv, :$p) {
        my uint $base;  # base offset for :k, :kv, :p

        # Logic for queuing a buffer for grep /.../
        sub v(uint $ordinal, $input is raw, $semaphore is raw) {
            self!cue: {
                my uint $then    = nqp::time;
                my      $output := nqp::create(IB);

                $input.Seq.grep($matcher).iterator.push-all($output);

                self!batch-done($ordinal, $then, $input, $semaphore, $output);
            }
        }

        # Logic for queuing a buffer for grep /.../ :k
        sub k(uint $ordinal, $input is raw, $semaphore is raw) {
            my uint $offset = $base;
            $base = $base + nqp::elems($input);

            self!cue: {
                my uint $then    = nqp::time;
                my      $output := nqp::create(IB);

                my $iterator := $input.Seq.grep($matcher, :k).iterator;
                nqp::until(
                  nqp::eqaddr((my $key := $iterator.pull-one),IE),
                  nqp::push($output,$offset + $key)
                );
                self!batch-done($ordinal, $then, $input, $semaphore, $output);
            }
        }

        # Logic for queuing a buffer for grep /.../ :kv
        sub kv(uint $ordinal, $input is raw, $semaphore is raw) {
            my uint $offset = $base;
            $base = $base + nqp::elems($input);

            self!cue: {
                my uint $then    = nqp::time;
                my      $output := nqp::create(IB);

                my $iterator := $input.Seq.grep($matcher, :kv).iterator;
                nqp::until(
                  nqp::eqaddr((my $key := $iterator.pull-one),IE),
                  nqp::stmts(
                    nqp::push($output,$offset + $key),     # key
                    nqp::push($output,$iterator.pull-one)  # value
                  )
                );
                self!batch-done($ordinal, $then, $input, $semaphore, $output);
            }
        }

        # Logic for queuing a buffer for grep /.../ :p
        sub p(uint $ordinal, $input is raw, $semaphore is raw) {
            my uint $offset = $base;
            $base = $base + nqp::elems($input);

            self!cue: {
                my uint $then    = nqp::time;
                my      $output := nqp::create(IB);

                my $iterator := $input.Seq.grep($matcher, :kv).iterator;
                nqp::until(
                  nqp::eqaddr((my $key := $iterator.pull-one),IE),
                  nqp::push(
                    $output,
                    Pair.new($offset + $key, $iterator.pull-one)
                  )
                );
                self!batch-done($ordinal, $then, $input, $semaphore, $output);
            }
        }

        # Let's go!
        self!re-shape!start($k ?? &k !! $kv ?? &kv !! $p ?? &p !! &v)
    }

#- kv --------------------------------------------------------------------------

    multi method kv(ParaSeq:D:) {
        my uint $base;  # base offset

        # Logic for queuing a buffer for .keys
        sub processor(uint $ordinal, $input is raw, $semaphore is raw) {
            my uint $offset = $base;
            my uint $elems  = nqp::elems($input);
            $base = $base + $elems;

            self!cue: {
                my uint $then    = nqp::time;
                my      $output := nqp::create(IB);

                # Store the key values as fast as possible
                my uint $i;
                nqp::while(
                  $i < $elems,
                  nqp::stmts(
                    nqp::push($output,$offset + $i),
                    nqp::push($output,nqp::atpos($input,$i++))
                  )
                );

                self!batch-done($ordinal, $then, $input, $semaphore, $output);
            }
        }

        # Let's go!
        self!re-shape!start(&processor)
    }

#- map -------------------------------------------------------------------------

    # Handling Callables that can have phasers
    method !mapBlock($Mapper, $method) {
        my uint $granularity = granularity($Mapper);

        my $mapper := (nqp::clone($Mapper) but BlockMapper)
          .setup($granularity, |%_);

        sub processor (
          uint $ordinal,
          uint $last,
               $input     is raw,
               $semaphore is raw,
        ) {
            self!cue: {
                my uint $then = nqp::time;

                self!batch-done(
                  $ordinal, $then, $input, $semaphore,
                  $mapper.run(
                    $ordinal == 0, $last,
                    $method, $input, nqp::getattr(self,Seq,'$!iter'),
                    $semaphore
                  )
                );
            }
        }

        # Let's go using the slower path
        self!re-shape!start(&processor, $granularity, :slow)
    }

    # Handling Callables that cannot have phasers
    method !mapCallable($mapper, str $method) {

        sub processor(uint $ordinal, $input is raw, $semaphore is raw) {
            self!cue: {
                 my uint $then    = nqp::time;

                $input.List."$method"($mapper).iterator.push-all(
                  my $output := nqp::create(IB)
                );

                self!batch-done($ordinal, $then, $input, $semaphore, $output);
             }
        }

        # Let's go!
        self!re-shape!start(&processor, granularity($mapper))
    }

    multi method map(ParaSeq:D: Block:D $mapper) {
        self!mapBlock($mapper, 'map')
    }
    multi method map(ParaSeq:D: Callable:D $mapper) {
        self!mapCallable($mapper, 'map')
    }

#- max -------------------------------------------------------------------------

    # Helper method for max / min / maxpairs / minpairs
    method !limiter(
       int $direction,  # direction for limiting: 1=max, -1=min
           &by,         # comparison op
      uint $p,          # 1 = want pairs
      uint $kv?,        # 1 = want keys and values
      uint $k?,         # 1 = want keys
      uint $v?          # 1 = want values
    ) {

        # Sorry, we're lazy!
        self.fail-iterator-cannot-be-lazy('.max') if self.is-lazy;

        # Keeper for key values at start of a batch
        my uint $base;

        # Always produce .kv internally for convenience
        sub processor(uint $ordinal, $input is raw, $semaphore is raw) {
            my uint $offset = $base;
            my uint $elems  = nqp::elems($input);
            $base = $base + $elems;

            self!cue: {
                my uint $then    = nqp::time;
                my      $output := nqp::create(IB);

                # Initial limited value
                nqp::push($output,nqp::clone($offset));
                nqp::push($output,my $limit := nqp::atpos($input,0));

                my uint $i;
                my  int $cmp;
                nqp::while(
                  nqp::islt_i(++$i,$elems),  # intentionally skip first
                  nqp::if(
                    ($cmp = by(nqp::atpos($input,$i), $limit)),
                    nqp::if(                          # not Same as limit
                      nqp::iseq_i($cmp,$direction),
                      nqp::stmts(                     # right direction
                        nqp::setelems($output,0),     # clean slate
                        nqp::push($output,++$offset),
                        nqp::push($output,$limit := nqp::atpos($input,$i))
                      ),
                      ++$offset                       # wrong direction
                    ),
                    nqp::stmts(                       # Same as limit
                      nqp::push($output,++$offset),
                      nqp::push($output,nqp::atpos($input,$i))
                    )
                  )
                );

                self!batch-done(
                  $ordinal, $then, $input, $semaphore, $output
                );
            }
        }

        # Get result batches as a single result
        my $batches := self!start(&processor).IterationBuffer;
        my $final   := nqp::create(IB);

        my $key   := nqp::shift($batches);
        my $value := nqp::shift($batches);

        # Set up result handling and store first result
        my &resulter = $p
          ?? { nqp::push($final,Pair.new($key,$value)) }
          !! $kv
            ?? { nqp::push($final,$key); nqp::push($final,$value) }
            !! $k
              ?? { nqp::push($final,$key)   }
              !! { nqp::push($final,$value) }
        resulter();

        # Create final result by running over batch results
        my $limit := $value;
        my int $cmp;
        nqp::while(
          nqp::elems($batches),
          nqp::stmts(
            ($key   := nqp::shift($batches)),
            ($value := nqp::shift($batches)),
            ($cmp    = by($value, $limit)),
            nqp::if(
              $cmp,
              nqp::if(                # not Same as limit
                nqp::iseq_i($cmp,$direction),
                nqp::stmts(           # right direction
                  nqp::setelems($final,0),
                  resulter()
                )
              ),
              resulter()              # Same as limit
            )
          )
        );

        self!re-source: $final.iterator
    }

    proto method max(|) {*}
    multi method max(ParaSeq:D: &by = &[cmp], :$p, :$kv, :$k, :$v) {
        $p || $kv || $k || $v
          # Not an endpoint, use our own logic
          ?? self!limiter(1, &by, $p.Bool, $kv.Bool, $k.Bool, $v.Bool)
          # Standard endpoint .max, nothing to optimize here
          !! self.Seq.max(&by)
    }

#- maxpairs --------------------------------------------------------------------

    proto method maxpairs(|) {*}
    multi method maxpairs(ParaSeq:D: &by = &[cmp]) { self!limiter(1, &by, 1) }

#- min -------------------------------------------------------------------------

    proto method min(|) {*}
    multi method min(ParaSeq:D: &by = &[cmp], :$p, :$kv, :$k, :$v) {
        $p || $kv || $k || $v
          # Not an endpoint, use our own logic
          ?? self!limiter(-1, &by, $p.Bool, $kv.Bool, $k.Bool, $v.Bool)
          # Standard endpoint .max, nothing to optimize here
          !! self.Seq.min(&by)
    }

#- minpairs --------------------------------------------------------------------

    proto method minpairs(|) {*}
    multi method minpairs(ParaSeq:D: &by = &[cmp]) { self!limiter(-1, &by, 1) }

#- nodemap ---------------------------------------------------------------------

    multi method nodemap(ParaSeq:D: Block:D $mapper) {
        my str @phasers;
        nqp::push(@phasers,$_) if $mapper.has-phaser($_) for <FIRST NEXT LAST>;
        warn "@phasers.join(", ") phaser(s) will be ignored" if @phasers;

        my uint $granularity = granularity($mapper);
        $granularity == 1
          ?? self!mapBlock($mapper, 'nodemap', :no-first, :no-last)
          !! die "Can only handle blocks with one parameter, got $granularity"
    }
    multi method nodemap(ParaSeq:D: Callable:D $mapper) {
        self!mapCallable($mapper, 'nodemap')
    }

#- pairs -----------------------------------------------------------------------

    multi method pairs(ParaSeq:D:) {
        my uint $base;  # base offset

        # Logic for queuing a buffer for .pairs
        sub processor(uint $ordinal, $input is raw, $semaphore is raw) {
            my uint $offset = $base;
            my uint $elems  = nqp::elems($input);
            $base = $base + $elems;

            self!cue: {
                my uint $then    = nqp::time;
                my      $output := nqp::create(IB);

                # Store the Pairs as fast as possible
                my uint $i;
                nqp::while(
                  $i < $elems,
                  nqp::push(
                    $output,
                    Pair.new($offset + $i,nqp::atpos($input,$i++))
                  )
                );

                self!batch-done($ordinal, $then, $input, $semaphore, $output);
            }
        }

        # Let's go!
        self!re-shape!start(&processor)
    }

#- squish ----------------------------------------------------------------------
# The squish method only uses a hypered approach if it is a non-standard one
# as the standard one is too optimized already to make the hypering overhead
# make sense

    proto method squish(|) {*}
    multi method squish(ParaSeq:D: :&as = &identity, :&with = &[===]) {

        # Set iterator to be used, default if nothing special
        my $iterator := do if nqp::eqaddr(&as,&identity)
          && nqp::eqaddr(&with,&[===]) {
            self.List.squish.iterator
        }

        # Either a non-standard "as" or "with", hypering then *does make sense
        else {

            sub processor(uint $ordinal, $input is raw, $semaphore is raw) {
                self!cue: {
                    my uint $then = nqp::time;

                    $input.Seq.squish(:&as, :&with).iterator.push-all(
                      my $batch := nqp::create(IB)
                    );

                    # Change each batch into a single element
                    nqp::push((my $output := nqp::create(IB)),$batch);

                    self!batch-done(
                      $ordinal, $then, $input, $semaphore, $output
                    );
                }
            }

            # Let's go!
            SquishIterator.new: self!start(&processor).iterator, &as, &with
        }

        self!re-source: $iterator
    }

#- unique ----------------------------------------------------------------------
# The unique method uses a 2 stage approach: the first stage is having all
# the threads produce "their" unique values, with a "seen" hash for each
# thread that was being used.  The second stage then post-processes the
# entire result, making sure that a value that was seen by more than one
# thread *still* would only be produced once, with the first value seen
# (when using a specific :as modifier)

    proto method unique(|) {*}
    multi method unique(ParaSeq:D: :with(&op)!, :&as = &identity) {
        return self.unique(:&as) if nqp::eqaddr(&op,&[===]);

        my $hashes    := nqp::create(IB);  # list of hashes, keyed on threadid
        my $lock      := Lock.new;

        # Logic for queuing a buffer for unique WITH an infix op
        sub processor(uint $ordinal, $input is raw, $semaphore is raw) {

            self!cue: {
                my uint $then = nqp::time;
                my      $output := nqp::create(IB);

                # Get the seen hash for this thread
                my int $i = nqp::threadid(nqp::currentthread);
                my $seen := $lock.protect: {
                    nqp::ifnull(
                      nqp::atpos($hashes,$i),
                      nqp::bindpos($hashes,$i,nqp::hash)
                    )
                }

                # Process all values
                my int $m = nqp::elems($input);
                $i = 0;
                nqp::until(
                  $i == $m,
                  nqp::unless(
                    nqp::existskey(
                      $seen,
                      (my str $key = as(
                        my $value := nqp::atpos($input,$i++)
                      ).WHICH)
                    ),
                    nqp::stmts(
                      nqp::bindkey($seen,$key,1),
                      (my int $j = 0),
                      (my int $n = nqp::elems($output)),
                      nqp::until(  # check all values for match using op(x,y)
                        $j == $n || op(nqp::atpos($output,$j), $value),
                        $j = nqp::add_i($j,2)
                      ),
                      nqp::if(
                        $j == $n,
                        nqp::stmts(  # not found with op(x,y) either, so add
                          nqp::push($output,$value),
                          nqp::push($output,nqp::clone($key))
                        )
                      )
                    )
                  )
                );

                self!batch-done($ordinal, $then, $input, $semaphore, $output);
            }
        }

        # Start the process and fill buffer with initial result
        my $result := self!start(&processor).IterationBuffer;

        # Get rid of the empty slots, caused by threadids of threads that did
        # *not* run any uniquing logic
        my $seens := nqp::create(IB);
        nqp::while(
          nqp::elems($hashes),
          nqp::unless(
            nqp::isnull(my $hash := nqp::shift($hashes)),
            nqp::push($seens,$hash)
          )
        );

        # If there was only one hash with seen information, there was only
        # one thread doing it, and so we've done all the checking that we
        # need already
        if nqp::elems($seens) == 1 {
            self!re-source: WhichIterator.new($result)
        }

        # Multiple threads processed values
        else {
            # Merge all the seen hashes into a single one, with value 0 for
            # the values that were seen in only a single thread, and 1 and
            # higher for values that were seen in multiple threads
            my $seen := nqp::hash;
            nqp::while(
              nqp::elems($seens),
              nqp::stmts(
                (my $iter := nqp::iterator(nqp::shift($seens))),
                nqp::while(
                  $iter,
                  nqp::stmts(
                    (my str $key = nqp::iterkey_s(nqp::shift($iter))),
                    nqp::bindkey(
                      $seen,
                      $key,
                      nqp::add_i(nqp::ifnull(nqp::atkey($seen,$key),-1),1)
                    )
                  )
                )
              )
            );

            # Now remove all of the values that only were seen in a single
            # thread: we're sure that these only occur once in the result
            $iter := nqp::iterator($seen);
            nqp::while(
              $iter,
              nqp::unless(
                nqp::iterval(nqp::shift($iter)),
                nqp::deletekey($seen,nqp::iterkey_s($iter))
              )
            );

            self!re-source: nqp::elems($seen)
              ?? UniqueIterator.new($seen, $result)  # need further checks
              !! WhichIterator.new($result)          # all ok
        }
    }
    multi method unique(ParaSeq:D: :&as = &identity) {
        self!re-source: self.List.unique(:&as).iterator
    }

#- interfaces that are just infectious -----------------------------------------

    proto method collate(|) {*}
    multi method collate(ParaSeq:D:) {
        self!re-source: self.List.collate.iterator
    }

    proto method combinations(|) {*}
    multi method combinations(ParaSeq:D:) {
        self!re-source: self.List.combinations.iterator
    }
    multi method combinations(ParaSeq:D: $of) {
        self!re-source: self.List.combinations($of).iterator
    }

    multi method flat(ParaSeq:D:) {
        self!re-source: self.List.flat.iterator
    }

    multi method head(ParaSeq:D: $what) {
        self!re-source: self.Seq.head($what).iterator
    }

    multi method invert(ParaSeq:D:) {
        self!re-source: self.Seq.invert.iterator
    }

    multi method keys(ParaSeq:D:) {
        self!re-source: self.Seq.keys.iterator
    }

    multi method pairup(ParaSeq:D:) {
        self!re-source: self.List.pairup.iterator
    }

    multi method permutations(ParaSeq:D:) {
        self!re-source: self.List.permutations.iterator
    }

    multi method pick(ParaSeq:D: $what) {
        self!re-source: self.List.pick($what).iterator
    }

    multi method produce(ParaSeq:D: Callable:D $producer) {
        self!re-source: self.Seq.produce($producer).iterator
    }

    proto method repeated(|) {*}
    multi method repeated(ParaSeq:D: |c) {
        self!re-source: self.Seq.repeated(|c).iterator
    }

    multi method reverse(ParaSeq:D:) {
        self!re-source:
          Rakudo::Iterator.ReifiedReverse(self.IterationBuffer, Mu)
    }

    multi method roll(ParaSeq:D: $what) {
        self!re-source: self.List.roll($what).iterator
    }

    multi method rotate(ParaSeq:D: Int(Cool) $rotate) {
        $rotate
          ?? self!re-source(
               Rakudo::Iterator.ReifiedRotate($rotate, self.IterationBuffer, Mu)
             )
          !! self
    }

    proto method rotor(|) {*}
    multi method rotor(ParaSeq:D: Int:D $size, :$partial) {
        self!batch($size, $partial.Bool)                   # can be faster
    }
    multi method rotor(ParaSeq:D: |c) {
        self!re-source: self.List.rotor(|c).iterator  # nah, too difficult
    }

    proto method skip(|) {*}
    multi method skip(ParaSeq:D:) {
        self.iterator.skip-one;
        self
    }
    multi method skip(ParaSeq:D: Int(Cool) $what) {
        self.iterator.skip-at-least($what);
        self
    }
    multi method skip(ParaSeq:D: Whatever) {
        self!re-source: Rakudo::Iterator.Empty
    }
    multi method skip(ParaSeq:D: |c) {
        self!re-source: self.Seq.skip(|c).iterator
    }

    proto method slice(|) {*}
    multi method slice(ParaSeq:D: |c) {
        self!re-source: self.List.slice(|c).iterator
    }

    proto method snip(|) {*}
    multi method snip(ParaSeq:D: |c) {
        self!re-source: self.Seq.snip(|c).iterator
    }

    proto method sort(|) {*}
    multi method sort(ParaSeq:D:) {
        self!re-source: self.List.sort.iterator
    }
    multi method sort(ParaSeq:D: Callable:D $how) {
        self!re-source: self.List.sort($how).iterator
    }

    multi method tail(ParaSeq:D: $what) {
        self!re-source: self.List.tail($what).iterator
    }

    proto method toggle(|) {*}
    multi method toggle(ParaSeq:D: |c) {
        self!re-source: self.Seq.toggle(|c).iterator
    }

    multi method values(ParaSeq:D:) { self }

#- coercers --------------------------------------------------------------------

    multi method Array(  ParaSeq:D:) { self.List.Array   }
    multi method Bag(    ParaSeq:D:) { self.List.Bag     }
    multi method BagHash(ParaSeq:D:) { self.List.BagHash }

    multi method Bool(ParaSeq:D:) {
        nqp::hllbool(
          nqp::not_i(nqp::eqaddr(self.iterator.pull-one,IterationEnd))
        )
    }

    multi method eager(ParaSeq:D:) { self.List      }
    multi method Hash(ParaSeq:D: ) { self.List.Hash }

    multi method IterationBuffer(ParaSeq:D:) {
        self.iterator.push-all(my $buffer := nqp::create(IB));
        $buffer
    }

    multi method list(ParaSeq:D:) { List.from-iterator(self.iterator) }
    multi method List(ParaSeq:D:) { self.IterationBuffer.List         }

    multi method Map(    ParaSeq:D:) { self.List.Map     }
    multi method Mix(    ParaSeq:D:) { self.List.Mix     }
    multi method MixHash(ParaSeq:D:) { self.List.MixHash }

    multi method Seq(    ParaSeq:D:) { Seq.new: self.iterator    }
    multi method serial( ParaSeq:D:) { self.Seq                  }
    multi method Set(    ParaSeq:D:) { self.List.Set             }
    multi method SetHash(ParaSeq:D:) { self.List.SetHash         }
    multi method Slip(   ParaSeq:D:) { self.IterationBuffer.Slip }
    multi method Str(    ParaSeq:D:) { self.join(" ")            }
    multi method Supply( ParaSeq:D:) { self.List.Supply          }

#- hyper -----------------------------------------------------------------------

    proto method hyper(|) {*}

    # If the degree is 1, then just return the iterable, nothing to parallelize
    multi method hyper(ParaSeq: \iterable,$,$,$,$, 1) is implementation-detail {
        iterable
    }

    # Entry point from the subs
    multi method hyper(ParaSeq:U:
      \iterable,
      $racing,
      $batch  is copy,
      $auto   is copy,
      $catch  is copy,
      $degree is copy,
    ) is implementation-detail {
        $batch := ($batch // $default-batch).Int;
        X::Invalid::Value.new(
          :method<hyper>, :name<batch>,  :value($batch)
        ).throw if $batch <= 0;

        $degree := ($degree // $default-degree).Int;
        X::Invalid::Value.new(
          :method<hyper>, :name<degree>, :value($degree)
        ).throw if $degree <= 1;

        my $iterator := iterable.iterator;
        my $buffer   := nqp::create(IB);

        # First batch already exhausted iterator, so work with buffer in
        # a non-parallelized way
        if nqp::eqaddr($iterator.push-exactly($buffer, $batch),IE) {
            $buffer.Seq
        }

        # Need to actually parallelize, set up ParaSeq object
        else {
            $auto  := ($auto  // True).Bool;
            $catch := ($catch // True).Bool;
            self!setup(
              BufferIterator.new($buffer, $iterator),
              $racing, $batch, $auto, $catch, $degree
            )
        }
    }

    # Change hypering settings along the way
    multi method hyper(ParaSeq:D:
      $batch?, $degree?, :$auto, :$catch, :$racing
    ) {
        $degree && $degree == 1
          ?? self.serial     # re-hypering with degree == 1 -> serialize
          !! ParaSeq.hyper(  # restart, taking over defaults
               self,
               $racing // $!racing,
               $batch  // $!batch,
               $auto   // $!auto,
               $catch  // $!catch,
               $degree // $!degree
             )
    }
}

#- actual interface ------------------------------------------------------------

proto sub hyperize(|) is export {*}
multi sub hyperize(\iterable, $batch?, $degree?, :$auto, :$catch) {
    ParaSeq.hyper(iterable, 0, $batch, $auto, $catch, $degree)
}
multi sub hyperize(List:D $list, $size?, $degree?, :$auto, :$catch) {
    my uint $batch = $size // $default-batch;
    $list.is-lazy || $list.elems > $batch
      ?? ParaSeq.hyper($list, 0, $batch, $auto, $catch, $degree)
      !! $list
}

proto sub racify(|) is export {*}
multi sub racify(\iterable, $batch?, $degree?, :$auto, :$catch) {
    ParaSeq.hyper(iterable, 1, $batch, $auto, $catch, $degree)
}
multi sub racify(List:D $list, $size?, $degree?, :$auto, :$catch) {
    my uint $batch = $size // $default-batch;
    $list.is-lazy || $list.elems > $batch
      ?? ParaSeq.hyper($list, 1, $batch, $auto, $catch, $degree)
      !! $list
}

# vim: expandtab shiftwidth=4
