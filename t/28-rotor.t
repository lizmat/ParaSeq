use Test;
use ParaSeq;

plan 12;

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
    is-deeply $list.&hyperize($batch, $_).rotor(5).List,
      $five,
      ".rotor(5) with degree = $_";
    is-deeply $list.&hyperize($batch, $_).rotor(5, :partial).List,
      $fivep,
      ".rotor(5, :partial) with degree = $_";

    is-deeply $list.&hyperize($batch, $_).rotor(6).List,
      $six,
      ".rotor(6) with degree = $_";
    is-deeply $list.&hyperize($batch, $_).rotor(6, :partial).List,
      $sixp,
      ".rotor(6, :partial) with degree = $_";

    is-deeply $list.&hyperize($batch, $_).rotor($pair).List,
      $pairs,
      ".rotor($pair.raku()) with degree = $_";
    is-deeply $list.&hyperize($batch, $_).rotor($pair, :partial).List,
      $pairsp,
      ".rotor($pair.raku(), :partial) with degree = $_";
}

# vim: expandtab shiftwidth=4
