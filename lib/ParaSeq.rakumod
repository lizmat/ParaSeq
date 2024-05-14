# Intended to be as fast as possible and to be someday become
# part of the Rakudo core
use nqp;

my int $default-batch  = 64;
my int $default-degree = Kernel.cpu-cores-but-one;

#- ParaSeq ---------------------------------------------------------------------

my class ParaSeq {
    has     $!source;
    has int $.batch;
    has int $.degree;

    method parent($source, int $batch, int $degree) {
        my $self := nqp::create(self);
        nqp::bindattr_i($self, ParaSeq, '$!batch',  $batch );
        nqp::bindattr_i($self, ParaSeq, '$!degree', $degree);
        nqp::p6bindattrinvres($self, ParaSeq, '$!source', $source.iterator)
    }

    method child($source) {
        nqp::p6bindattrinvres(
          nqp::clone(self), ParaSeq, '$!source', $source.iterator
        )
    }

    method iterator() { $!source }

    method map(ParaSeq:D: &mapper) {
        self.child: Seq.new($!source).map(&mapper, |%_)
    }
    method grep(ParaSeq:D: &mapper) {
        self.child: Seq.new($!source).grep( &mapper, |%_)
    }
    method first(ParaSeq:D: &mapper) {
        self.child: Seq.new($!source).first(&mapper, |%_)
    }
    method head(ParaSeq:D: |c) {
        self.child: Seq.new($!source).head(|c)
    }
    method tail(ParaSeq:D: |c) {
        self.child: Seq.new($!source).tail(|c)
    }
    method reverse(ParaSeq:D: |c) {
        self.child: Seq.new($!source).reverse(|c)
    }
}

#- actual interface ------------------------------------------------------------

proto sub hyperize(|) is export {*}
multi sub hyperize(\iterable, $, 1) is raw { iterable }
multi sub hyperize(\iterable) {
   ParaSeq.parent(iterable, $default-batch, $default-degree)
}
multi sub hyperize(\iterable, Int:D $batch) {
   ParaSeq.parent(iterable, $batch, $default-degree)
}
multi sub hyperize(\iterable, Int:D $batch, Int:D $degree) {
   ParaSeq.parent(iterable, $batch, $degree)
}

proto sub racify(|) is export {*}
multi sub racify(\iterable, $, 1) is raw { iterable }
multi sub racify(\iterable) {
   ParaSeq.parent(iterable, $default-batch, $default-degree)
}
multi sub racify(\iterable, Int:D $batch) {
   ParaSeq.parent(iterable, $batch, $default-degree)
}
multi sub racify(\iterable, Int:D $batch, Int:D $degree) {
   ParaSeq.parent(iterable, $batch, $degree)
}


#.say for (1,2,3,4).first(-> $a { say "a = $a" });
.say for (1,2,3,4).&hyperize.reverse;

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
