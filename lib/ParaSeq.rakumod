# Intended to be as fast as possible and to be someday become
# part of the Rakudo core
use nqp;

my int $default-batch  = 10;
my int $default-degree = Kernel.cpu-cores-but-one;

#- ParaSeq ---------------------------------------------------------------------

class ParaSeq {
    has      $!source;
    has int  $.batch;
    has int  $.degree;

#- private helper methods ------------------------------------------------------

    # Entry point in chain, from an Iterable
    method !from-iterable($source) {
        my $self := nqp::create(self);
        nqp::bindattr_i($self, ParaSeq, '$!batch',
          nqp::getattr_i(self, ParaSeq, '$!batch')
        );
        nqp::bindattr_i($self, ParaSeq, '$!degree',
          nqp::getattr_i(self, ParaSeq, '$!degree')
        );
        nqp::p6bindattrinvres($self, ParaSeq, '$!source', $source.iterator)
    }

    # Entry point in chain, from an Iterator
    method !from-iterator($source) {
        my $self := nqp::create(self);
        nqp::bindattr_i($self, ParaSeq, '$!batch',
          nqp::getattr_i(self, ParaSeq, '$!batch')
        );
        nqp::bindattr_i($self, ParaSeq, '$!degree',
          nqp::getattr_i(self, ParaSeq, '$!degree')
        );
        nqp::p6bindattrinvres($self, ParaSeq, '$!source', $source)
    }

#- entry points ----------------------------------------------------------------

    # Entry point from the subs
    method parent($source, int $batch, int $degree, str $method) {

        # sanity check
        X::Invalid::Value.new(:$method, :name<batch>,  :value($!batch)).throw
          if $batch <= 0;
        X::Invalid::Value.new(:$method, :name<degree>, :value($!degree)).throw
          if $degree <= 0;

        my $self := nqp::create(self);
        nqp::bindattr_i($self, ParaSeq, '$!batch',  $batch );
        nqp::bindattr_i($self, ParaSeq, '$!degree', $degree);
        nqp::p6bindattrinvres($self, ParaSeq, '$!source', $source.iterator)
    }

#- where all the magic happens -------------------------------------------------

    method iterator() { $!source }  # for now

#- introspection ---------------------------------------------------------------

    method default-batch()  { $default-batch  }
    method default-degree() { $default-degree }

#- Iterable interfaces with special needs --------------------------------------

    proto method map(|) {*}
    multi method map(ParaSeq:D: Callable:D $mapper) {
        self!from-iterable: self.Seq.map($mapper, |%_)
    }

    proto method grep(|) {*}
    multi method grep(ParaSeq:D: Callable:D $matcher) {
        self!from-iterable: self.Seq.grep($matcher, |%_)
    }
    multi method grep(ParaSeq:D: $matcher) {
        self!from-iterable: self.Seq.grep($matcher, |%_)
    }

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
          ?? self.IterationBuffer.List.first($matcher, :$end, |%_)
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
    multi method head(ParaSeq:D:) {
        self.Seq.head
    }
    multi method head(ParaSeq:D: |c) {
        self!from-iterable: self.Seq.head(|c)
    }

    proto method tail(|) {*}
    multi method tail(ParaSeq:D:) {
        self.Seq.tail
    }
    multi method tail(ParaSeq:D: |c) {
        self!from-iterable: self.Seq.tail(|c)
    }

    proto method reverse(|) {*}
    multi method reverse(ParaSeq:D:) {
        self!from-iterator:
          Rakudo::Iterator.ReifiedReverse:
            self.IterationBuffer, Mu
    }

    proto method elems(|) {*}
    multi method elems(ParaSeq:D:) {
        $!source.is-lazy
          ?? self.fail-iterator-cannot-be-lazy('.elems',"")
          !! nqp::elems(self.IterationBuffer)
    }

    proto method end(|) {*}
    multi method end(ParaSeq:D:) {
        $!source.is-lazy
          ?? self.fail-iterator-cannot-be-lazy('.end',"")
          !! nqp::elems(self.IterationBuffer) - 1
    }

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
