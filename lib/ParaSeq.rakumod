# Intended to be as fast as possible and to be someday become
# part of the Rakudo core
use nqp;

my int $default-batch  = 64;
my int $default-degree = Kernel.cpu-cores-but-one;

#- ParaSeq ---------------------------------------------------------------------

class ParaSeq {
    has      $!source;
    has int  $.batch;
    has int  $.degree;

#- private helper methods ------------------------------------------------------

    # Entry point in chain, from an Iterable
    method !from-iterable($source) {
        nqp::p6bindattrinvres(
          nqp::clone(self), ParaSeq, '$!source', $source.iterator
        )
    }

    # Entry point in chain, from an Iterator
    method !from-iterator($source) {
        nqp::p6bindattrinvres(nqp::clone(self), ParaSeq, '$!source', $source)
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

#- standard Iterable interfaces ------------------------------------------------

    multi method map(ParaSeq:D: &mapper) {
        self!from-iterable: self.Seq.map(&mapper, |%_)
    }

    multi method grep(ParaSeq:D: &mapper) {
        self!from-iterable: self.Seq.grep( &mapper, |%_)
    }

    multi method first(ParaSeq:D: &mapper) {
        self!from-iterable: self.Seq.first(&mapper, |%_)
    }
    multi method first(ParaSeq:D: &mapper, :$end!) {
        $end
          ?? self!from-iterable(
               self.IterationBuffer.List.first(&mapper, :$end, |%_)
             )
          !! self.first(&mapper, |%_)
    }

    multi method invert(ParaSeq:D:) {
        self!from-iterable: self.Seq.invert
    }

    multi method skip(ParaSeq:D: |c) {
        self!from-iterable: self.Seq.skip(|c)
    }

    multi method head(ParaSeq:D:) {
        self.Seq.head
    }
    multi method head(ParaSeq:D: |c) {
        self!from-iterable: self.Seq.head(|c)
    }

    multi method tail(ParaSeq:D:) {
        self.Seq.tail
    }
    multi method tail(ParaSeq:D: |c) {
        self!from-iterable: self.Seq.tail(|c)
    }

    multi method reverse(ParaSeq:D:) {
        self!from-iterator:
          Rakudo::Iterator.ReifiedReverse:
            self.IterationBuffer, Mu
    }

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
