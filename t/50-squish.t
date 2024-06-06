use Test;
use ParaSeq;

plan 24;

my constant $elems    = 10000;
my constant $list     = (^$elems).pick(*).List;
my constant $batch    = 16;
my constant $squish   = $list.squish.List;
my constant $as       = * div 100;
my constant $hundreds = $list.squish(:$as).List;
my constant $same     = (1 xx $elems).List;
my constant $one      = $same.squish.List;

for 1, ParaSeq.default-degree {
    for &[===], &[==] -> $with {
        my $seq := $list.&hyperize($batch, $_).squish(:$with);
        isa-ok $seq, $_ == 1 ?? Seq !! ParaSeq;
        is-deeply $seq.List, $squish,
          "list.squish with degree = $_ with $with.name()";

        $seq := $list.&hyperize($batch, $_).squish(:$with, :$as);
        isa-ok $seq, $_ == 1 ?? Seq !! ParaSeq;
        is-deeply $seq.List, $hundreds,
          "list.squish(:as(* div 100)) with degree = $_ with $with.name()";

        $seq := $same.&hyperize($batch, $_).squish(:$with);
        isa-ok $seq, $_ == 1 ?? Seq !! ParaSeq;
        is-deeply $seq.List, $one,
          "same.squish with degree = $_ with $with.name()";
    }
}

# vim: expandtab shiftwidth=4
