[![Actions Status](https://github.com/lizmat/ParaSeq/actions/workflows/test.yml/badge.svg)](https://github.com/lizmat/ParaSeq/actions)

NAME
====

ParaSeq - Parallel execution of Iterables

SYNOPSIS
========

```raku
use ParaSeq;
```

DESCRIPTION
===========

ParaSeq provides the functional equivalent of [`hyper`](https://docs.raku.org/type/Iterable#method_hyper) and [`race`](https://docs.raku.org/type/Iterable#method_race), but re-implemented from scratch with all of the experience from the initial implementation of `hyper` and <race> in 2014, and using features that have since been added to the Raku Programming Language.

As such it exports two subroutines `hyperize` and `racify`, to make them plug-in compatible with the [`hyperize`](https://raku.land/zef:lizmat/hyperize) distribution.

AUTHOR
======

Elizabeth Mattijsen <liz@raku.rocks>

Source can be located at: https://github.com/lizmat/ParaSeq . Comments and Pull Requests are welcome.

If you like this module, or what I’m doing more generally, committing to a [small sponsorship](https://github.com/sponsors/lizmat/) would mean a great deal to me!

COPYRIGHT AND LICENSE
=====================

Copyright 2024 Elizabeth Mattijsen

This library is free software; you can redistribute it and/or modify it under the Artistic License 2.0.

