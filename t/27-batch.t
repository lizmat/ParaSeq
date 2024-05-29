use Test;
use ParaSeq;

plan 8;

my constant $elems = 200000;
my constant $list  = (^$elems).List;
my constant $five  = $list.batch(5).List;
my constant $six   = $list.batch(6).List;
my constant $batch = 16;

for 1, ParaSeq.default-degree {
    is-deeply $list.&hyperize($batch, $_).batch(5).List, $five,
      ".batch(5) with degree = $_";
    is-deeply $list.&hyperize($batch, $_).batch(6).List, $six,
      ".batch(6) with degree = $_";
    is-deeply $list.&hyperize($batch, $_).batch(:elems(5)).List, $five,
      ".batch(elems => 5) with degree = $_";
    is-deeply $list.&hyperize($batch, $_).batch(:elems(6)).List, $six,
      ".batch(elems => 6) with degree = $_";
}

# vim: expandtab shiftwidth=4
