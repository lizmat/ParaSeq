use Test;
use ParaSeq;

plan 6;

my constant $elems = 200000;
my constant $list  = (^$elems).List;
my constant $all   = $list.Set;
my constant $half  = $elems / 2;
my constant $batch = 16;

for 1, ParaSeq.default-degree {
    cmp-ok $list.&hyperize($batch, $_).roll, &[(elem)], $all,
      ".roll with degree = $_";
    cmp-ok $list.&hyperize($batch, $_).roll($elems).List, &[(<=)], $all,
      ".roll($elems) with degree = $_";
    cmp-ok $list.&hyperize($batch, $_).roll($elems / 2).List, &[(<)], $all,
      ".roll($half) with degree = $_";
}

# vim: expandtab shiftwidth=4
