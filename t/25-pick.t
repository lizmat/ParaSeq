use Test;
use ParaSeq;

plan 6;

my constant $elems = 200000;
my constant $list  = (^$elems).List;
my constant $all   = $list.pick(*).Set;
my constant $half  = $elems / 2;
my constant $batch = 16;

for 1, ParaSeq.default-degree {
    cmp-ok $list.&hyperize($batch, $_).pick, &[(elem)], $all,
      ".pick with degree = $_";
    cmp-ok $list.&hyperize($batch, $_).pick(*).List, &[(==)], $all,
      ".pick(*) with degree = $_";
    cmp-ok $list.&hyperize($batch, $_).pick($elems / 2).List, &[(<)], $all,
      ".pick($half) with degree = $_";
}

# vim: expandtab shiftwidth=4
