use Test;
use ParaSeq;

plan 10;

my constant $elems = 200000;
my constant $list  = (^$elems).List;
my constant $all   = $list.Set;
my constant $half  = $elems / 2;
my constant $batch = 16;

for 1, ParaSeq.default-degree {
    cmp-ok $list.&hyperize($batch, $_).roll, &[(elem)], $all,
      ".roll with degree = $_";

    my $seq := $list.&hyperize($batch, $_).roll($elems);
    isa-ok $seq, $_ == 1 ?? Seq !! ParaSeq;
    cmp-ok $seq.List, &[(<=)], $all, ".roll($elems) with degree = $_";

    $seq := $list.&hyperize($batch, $_).roll($elems / 2);
    isa-ok $seq, $_ == 1 ?? Seq !! ParaSeq;
    cmp-ok $seq.List, &[(<)], $all, ".roll($half) with degree = $_";
}

# vim: expandtab shiftwidth=4
