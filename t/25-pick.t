use Test;
use ParaSeq;

plan 10;

my constant $elems = 200000;
my constant $list  = (^$elems).List;
my constant $all   = $list.Set;
my constant $half  = $elems / 2;
my constant $batch = 16;

for 1, ParaSeq.default-degree {
    cmp-ok $list.&hyperize($batch, $_).pick, &[(elem)], $all,
      ".pick with degree = $_";

    my $seq := $list.&hyperize($batch, $_).pick(*);
    isa-ok $seq, $_ == 1 ?? Seq !! ParaSeq;
    cmp-ok $seq.List, &[(==)], $all, ".pick(*) with degree = $_";

    $seq := $list.&hyperize($batch, $_).pick($elems / 2);
    isa-ok $seq, $_ == 1 ?? Seq !! ParaSeq;
    cmp-ok $seq.List, &[(<)], $all, ".pick($half) with degree = $_";
}

# vim: expandtab shiftwidth=4
