use Test;
use ParaSeq;

plan 8;

my constant $elems    = 200000;
my constant @list     = (^$elems).List;
my constant $answer   = 42;
my constant @forward  = @list.rotate($answer).List;
my constant @backward = @list.rotate(-$answer).List;
my constant $batch    = 16;

for 1, ParaSeq.default-degree {
    my $rotated := @list.&hyperize($batch, $_).rotate($answer);
    isa-ok $rotated, $_ == 1 ?? Seq !! ParaSeq;
    is-deeply $rotated.List, @forward,
      ".rotate($answer) with degree = $_";

    $rotated := @list.&hyperize($batch, $_).rotate(-$answer);
    isa-ok $rotated, $_ == 1 ?? Seq !! ParaSeq;
    is-deeply $rotated.List, @backward,
      ".rotate(-$answer) with degree = $_";
}

# vim: expandtab shiftwidth=4
