use Test;
use ParaSeq;

plan 24;

my constant $elems  = 200000;
my constant $list   = (^$elems).List;
my constant $five   = $list.rotor(5).List;
my constant $six    = $list.rotor(6).List;
my constant $fivep  = $list.rotor(5, :partial).List;
my constant $sixp   = $list.rotor(6, :partial).List;
my constant $pair   = 6 => -1;
my constant $pairs  = $list.rotor($pair).List;
my constant $pairsp = $list.rotor($pair, :partial).List;
my constant $batch  = 16;

for 1, ParaSeq.default-degree {
    my $seq := $list.&hyperize($batch, $_).rotor(5);
    isa-ok $seq, $_ == 1 ?? Seq !! ParaSeq;
    is-deeply $seq.List, $five, ".rotor(5) with degree = $_";

    $seq := $list.&hyperize($batch, $_).rotor(5, :partial);
    isa-ok $seq, $_ == 1 ?? Seq !! ParaSeq;
    is-deeply $seq.List, $fivep, ".rotor(5, :partial) with degree = $_";

    $seq := $list.&hyperize($batch, $_).rotor(6);
    isa-ok $seq, $_ == 1 ?? Seq !! ParaSeq;
    is-deeply $seq.List, $six, ".rotor(6) with degree = $_";

    $seq := $list.&hyperize($batch, $_).rotor(6, :partial);
    isa-ok $seq, $_ == 1 ?? Seq !! ParaSeq;
    is-deeply $seq.List, $sixp, ".rotor(6, :partial) with degree = $_";

    $seq := $list.&hyperize($batch, $_).rotor($pair);
    isa-ok $seq, $_ == 1 ?? Seq !! ParaSeq;
    is-deeply $seq.List, $pairs, ".rotor($pair.raku()) with degree = $_";

    $seq := $list.&hyperize($batch, $_).rotor($pair, :partial);
    isa-ok $seq, $_ == 1 ?? Seq !! ParaSeq;
    is-deeply $seq.List, $pairsp,
      ".rotor($pair.raku(), :partial) with degree = $_";
}

# vim: expandtab shiftwidth=4
