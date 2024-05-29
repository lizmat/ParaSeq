use Test;
use ParaSeq;

plan 16;

my constant $elems = 200000;
my constant $list  = (^$elems).List;
my constant $five  = $list.batch(5).List;
my constant $six   = $list.batch(6).List;
my constant $batch = 16;

for 1, ParaSeq.default-degree {
    my $seq := $list.&hyperize($batch, $_).batch(5);
    isa-ok $seq, $_ == 1 ?? Seq !! ParaSeq;
    is-deeply $seq.List, $five, ".batch(5) with degree = $_";

    $seq := $list.&hyperize($batch, $_).batch(6);
    isa-ok $seq, $_ == 1 ?? Seq !! ParaSeq;
    is-deeply $seq.List, $six, ".batch(6) with degree = $_";

    $seq := $list.&hyperize($batch, $_).batch(:elems(5));
    isa-ok $seq, $_ == 1 ?? Seq !! ParaSeq;
    is-deeply $seq.List, $five, ".batch(elems => 5) with degree = $_";

    $seq := $list.&hyperize($batch, $_).batch(:elems(6));
    isa-ok $seq, $_ == 1 ?? Seq !! ParaSeq;
    is-deeply $seq.List, $six, ".batch(elems => 6) with degree = $_";
}

# vim: expandtab shiftwidth=4
