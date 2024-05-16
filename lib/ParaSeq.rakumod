# Intended to be as fast as possible and to someday become part of the
# Rakudo core
use nqp;

my int $default-batch  = 10;
my int $default-degree = Kernel.cpu-cores-but-one;

#- ParaQueue -------------------------------------------------------------------
# A blocking concurrent queue to which one can nqp::push and from which one
# can nqp::shift
my class ParaQueue is repr('ConcBlockingQueue') { }

#- ParaIterator ----------------------------------------------------------------
# An iterator that takes a number of ParaQueues and produces all values
# from them until the last ParaQueue has been exhausted.  Note that the
# queue of queues is also a ParaQueue so that queues can also be added
# and removed in a thread-safe manner

my role ParaIterator does Iterator {
    has $!current;
    has $!queues;

    method new(\current) {
        my $self := nqp::create(self);
        nqp::bindattr($self,ParaIterator,'$!current',current);
        nqp::bindattr($self,ParaIterator,'$!queues',nqp::create(ParaQueue));
        $self
    }

    method nr-queues()       { nqp::elems($!queues)      }
    method add-queue(\queue) { nqp::push($!queues,queue) }

    method pull-one() {
        my $pulled := nqp::shift($!current);

        nqp::while(
          nqp::eqaddr($pulled,IterationEnd),
          nqp::if(
            nqp::elems($!queues),
            ($pulled := nqp::shift($!current := nqp::shift($!queues))),
            (return IterationEnd)
          )
        );

        $pulled
    }
}

#- ParaIteratorIndex -----------------------------------------------------------
# Same as ParaIterator, but produces index values of the produced values,
# instead of the actual values.

my class ParaIteratorIndex does ParaIterator {
    has int $!index;

    method pull-one() {
        my $pulled := nqp::shift($!current);

        nqp::while(
          nqp::eqaddr($pulled,IterationEnd),
          nqp::if(
            nqp::elems($!queues),
            ($pulled := nqp::shift($!current := nqp::shift($!queues))),
            (return IterationEnd)
          )
        );

        $!index++
    }
}

#- ParaIteratorPair ------------------------------------------------------------
# Same as ParaIterator, but produces a Pair of the index and the produced
# value

my class ParaIteratorPair does ParaIterator {
    has int $!index;

    method pull-one() {
        my $pulled := nqp::shift($!current);

        nqp::while(
          nqp::eqaddr($pulled,IterationEnd),
          nqp::if(
            nqp::elems($!queues),
            ($pulled := nqp::shift($!current := nqp::shift($!queues))),
            (return IterationEnd)
          )
        );

        Pair.new($!index++, $pulled)
    }
}

#- ParaIteratorIndexValue ------------------------------------------------------
# Same as ParaIterator, but produces an index and produced value alternately

my class ParaIteratorIndexValue does ParaIterator {
    has int $!index;
    has     $!pulled;

    my constant NEXT = nqp::create(Mu);

    method new(\current) {
        my $self := nqp::create(self);
        nqp::bindattr($self,ParaIterator,'$!current',current);
        nqp::bindattr($self,ParaIterator,'$!queues',nqp::create(ParaQueue));
        nqp::bindattr($self,ParaIteratorIndexValue,'$!pulled',NEXT);
        $self
    }

    method pull-one() {
        if nqp::eqaddr($!pulled,NEXT) {
            my $pulled := nqp::shift($!current);

            nqp::while(
              nqp::eqaddr($pulled,IterationEnd),
              nqp::if(
                nqp::elems($!queues),
                ($pulled := nqp::shift($!current := nqp::shift($!queues))),
                (return IterationEnd)
              )
            );

            nqp::bindattr(self,ParaIteratorIndexValue,'$!pulled',$pulled);
            $!index++;
        }
        else {
            my $pulled := $!pulled;
            nqp::bindattr(self,ParaIteratorIndexValue,'$!pulled',NEXT);
            $pulled
        }
    }
}

#- ParaSeq ---------------------------------------------------------------------
# The class containing all of the logic for parallel sequences
class ParaSeq {
    has      $!source;    # source iterator
    has      $!producer;  # iterator producing values
    has int  $.degree;    # number of CPUs, must be > 1
    has int  $.initial;   # initial batch size, must be > 0
    has int  $.batch;     # current batch size, must be > 0

#- private helper methods ------------------------------------------------------

    # Entry point in chain, from an Iterable
    method !from-iterable($source) {
        my $self := nqp::create(self);
        nqp::bindattr_i($self, ParaSeq, '$!initial',
          nqp::getattr_i(self, ParaSeq, '$!initial')
        );
        nqp::bindattr_i($self, ParaSeq, '$!degree',
          nqp::getattr_i(self, ParaSeq, '$!degree')
        );
        nqp::p6bindattrinvres($self, ParaSeq, '$!source', $source.iterator)
    }

    # Entry point in chain, from an Iterator
    method !from-iterator(\source) {
        my $self := nqp::create(self);
        nqp::bindattr_i($self, ParaSeq, '$!initial',
          nqp::getattr_i(self, ParaSeq, '$!initial')
        );
        nqp::bindattr_i($self, ParaSeq, '$!degree',
          nqp::getattr_i(self, ParaSeq, '$!degree')
        );
        nqp::p6bindattrinvres($self, ParaSeq, '$!source', source)
    }

    # Fill buffer with a batch of values, return 1 if exhausted, else 0
    method !batch($buffer) {
        nqp::eqaddr($!source.push-exactly($buffer, $!batch),IterationEnd)
    }

#- entry points ----------------------------------------------------------------

    # Entry point from the subs
    method parent($source, int $batch, int $degree, str $method) {

        # sanity check
        X::Invalid::Value.new(:$method, :name<batch>,  :value($!batch)).throw
          if $batch <= 0;
        X::Invalid::Value.new(:$method, :name<degree>, :value($!degree)).throw
          if $degree <= 1;

        my $self := nqp::create(self);
        nqp::bindattr_i($self, ParaSeq, '$!initial', $batch );
        nqp::bindattr_i($self, ParaSeq, '$!degree',  $degree);
        nqp::p6bindattrinvres($self, ParaSeq, '$!source', $source.iterator)
    }

#- where all the magic happens under the hood ----------------------------------

    # The producer acts as a normal iterator, producing values from
    # queues that are filled asynchronously.  If there is no producer,
    # then the source will act as the producer
    multi method iterator(ParaSeq:D:) { $!producer // $!source }

#- introspection ---------------------------------------------------------------

    method default-batch()  { $default-batch  }
    method default-degree() { $default-degree }

    method degree(       ParaSeq:D:) { $!degree  }
    method initial-batch(ParaSeq:D:) { $!initial }
    method current-batch(ParaSeq:D:) { $!batch   }

#- Iterable interfaces with special needs --------------------------------------

    proto method map(|) {*}
    multi method map(ParaSeq:D: Callable:D $mapper) {
        my $buffer := nqp::create(IterationBuffer);
        return self!from-iterable($buffer.List.map($mapper))
          if self!batch($buffer);

        self!from-iterable: self.Seq.map($mapper)
    }

    proto method grep(|) {*}
    multi method grep(ParaSeq:D: Callable:D $matcher) {
        self!from-iterable: self.Seq.grep($matcher, |%_)
    }
    multi method grep(ParaSeq:D: $matcher) {
        self!from-iterable: self.Seq.grep($matcher, |%_)
    }

    proto method first(|) {*}
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

#- standard Iterable interfaces ------------------------------------------------

    proto method invert(|) {*}
    multi method invert(ParaSeq:D:) {
        self!from-iterable: self.Seq.invert
    }

    proto method skip(|) {*}
    multi method skip(ParaSeq:D: |c) {
        self!from-iterable: self.Seq.skip(|c)
    }

    proto method head(|) {*}
    multi method head(ParaSeq:D: |c) {
        self!from-iterable: self.Seq.head(|c)
    }

    proto method tail(|) {*}
    multi method tail(ParaSeq:D: |c) {
        self!from-iterable: self.Seq.tail(|c)
    }

    proto method reverse(|) {*}
    multi method reverse(ParaSeq:D:) {
        self!from-iterator:
          Rakudo::Iterator.ReifiedReverse:
            self.IterationBuffer, Mu
    }

#- endpoints -------------------------------------------------------------------

    multi method first(ParaSeq:D:) { self.Seq.first }
    multi method head( ParaSeq:D:) { self.Seq.head  }
    multi method tail( ParaSeq:D:) { self.Seq.tail  }

    multi method elems(ParaSeq:D:) {
        $!source.is-lazy
          ?? self.fail-iterator-cannot-be-lazy('.elems',"")
          !! nqp::elems(self.IterationBuffer)
    }

    multi method end(ParaSeq:D:) {
        $!source.is-lazy
          ?? self.fail-iterator-cannot-be-lazy('.end',"")
          !! nqp::elems(self.IterationBuffer) - 1
    }

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

=begin pod

=head1 NAME

ParaSeq - Parallel execution of Iterables

=head1 SYNOPSIS

=begin code :lang<raku>

use ParaSeq;

=end code

=head1 DESCRIPTION

ParaSeq provides the functional equivalent of
L<C<hyper>|https://docs.raku.org/type/Iterable#method_hyper> and
L<C<race>|https://docs.raku.org/type/Iterable#method_race>, but
re-implemented from scratch with all of the experience from the
initial implementation of C<hyper> and <race> in 2014, and using
features that have since been added to the Raku Programming Language.

As such it exports two subroutines C<hyperize> and C<racify>, to
make them plug-in compatible with the
L<C<hyperize>|https://raku.land/zef:lizmat/hyperize> distribution.

=head1 IMPROVEMENTS

=head2 Automatic batch size adaptation

One of the main issues with the current implemementation of C<.hyper>
and C<.race> in the Rakudo core is that the batch size is fixed.  Worse,
there is no way to dynamically adapt the batch size depending on the
load.

Batch sizes that are too big, have a tendency to not use all of
the CPUs (because they have a tendency to eat all of the source items
too soon, thus removing the chance to start up more threads).

Batch sizes that are too small, have a tendency to have their resource
usage drowned out by the overhead of batching and dispatching to
threads.

This implementation aims to adapt batch sizes from the originally
(implicitely) specified one for better throughput and resource usage.

=head2 Unnecessary parallelization

If the C<degree> specified is B<1>, then there is no point in batching
or parallelization.  In that case, this implementation will take itself
completely out of the flow.

Alternately, if the initial batch size is large enough to exhaust the
source, it is clearly too large.  Which is interpreted as not making
any sense at parallelization either.  So it won't.

Note that the default initial batch size is B<10>, rather than B<64>
in the current implementation of C<.hyper> and C<.race>, making the
chance smaller that parallelization is abandoned too soon.

=head2 Infectiousness

The C<.serial> method or C<.Seq> coercer can be typically be used to
"unhyper" a hypered sequence.  However many other interface methods do
the same in the current implementation of C<.hyper> and C<.race>,
thereby giving the impression that the flow is still parallelized.
When in fact they aren't anymore.

Also, hyperized sequences in the current implementation are considered
to be non-lazy, even if the source B<is> lazy.

This implementation aims to make all interface methods pass on the
hypered nature and laziness of the sequence.

=head2 Loop control statements

Some loop control statements may affect the final result.  Specifically
the C<last> statement does.  In the current implementation of C<.hyper>
and C<.race>, this will only affect the batch in which it occurs.

This implementation aims to make C<last> stop any processing of current
and not create anymore batches.

=head2 Support more interface methods

Currently only the C<.map> and C<.grep> methods are completely supported
by the current implementation of C<.hyper> and C<.race>.  Other methods,
such as C<.first>, will also be supported.

=head2 Use of phasers

When an interface method takes a C<Callable>, then that C<Callable>
can contain phasers that may need to be called (or not called) depending
on the situation.  The current implementation of C<.hyper> and C<.race>
do not allow phasers at all.

This implementation aims to support phasers in a sensible manner:

=head3 ENTER

Called before each iteration.

=head3 FIRST

Called on the first iteration in the first batch.

=head3 NEXT

Called at the end of each iteration.

=head3 LAST

Called on the last iteration in the last batch.  Note that this can be
short-circuited with a C<last> control statement.

=head3 LEAVE

Called after each iteration.

=head1 AUTHOR

Elizabeth Mattijsen <liz@raku.rocks>

Source can be located at: https://github.com/lizmat/ParaSeq .
Comments and Pull Requests are welcome.

If you like this module, or what I’m doing more generally, committing to a
L<small sponsorship|https://github.com/sponsors/lizmat/>  would mean a great
deal to me!

=head1 COPYRIGHT AND LICENSE

Copyright 2024 Elizabeth Mattijsen

This library is free software; you can redistribute it and/or modify it under
the Artistic License 2.0.

=end pod

# vim: expandtab shiftwidth=4
