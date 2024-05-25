[![Actions Status](https://github.com/lizmat/ParaSeq/actions/workflows/test.yml/badge.svg)](https://github.com/lizmat/ParaSeq/actions)

NAME
====

ParaSeq - Parallel execution of Iterables

SYNOPSIS
========

```raku
use ParaSeq;

# The 1-millionth prime number: 15485863
say (^Inf).&hyperize.grep(*.is-prime)[999999];
say (^Inf).&hyperize.grep(*.is-prime).skip(999999).head;
say (^Inf).&hyperize(stop-after => 1_000_000).grep(*.is-prime).tail;

# Fetching lines of files, each element containing a List of lines
my @lines = @filenames.&hyperize(1, :!auto).map(*.IO.lines.List);
my %lines = @filenames.&hyperize(1, :!auto).map: { $_ => .IO.lines.List }
```

DESCRIPTION
===========

ParaSeq provides the functional equivalent of [`hyper`](https://docs.raku.org/type/Iterable#method_hyper) and [`race`](https://docs.raku.org/type/Iterable#method_race), but re-implemented from scratch with all of the experience from the initial implementation of `hyper` and `race` in 2014, and using features that have since been added to the Raku Programming Language.

As such it exports two subroutines `hyperize` and `racify`, to make them plug-in compatible with the [`hyperize`](https://raku.land/zef:lizmat/hyperize) distribution.

SUBROUTINES
===========

hyperize
--------

```raku
# The 1-millionth prime number using subroutine syntax: 15485863
say hyperize(^Inf).grep(*.is-prime)[999999];

# Start with 2000 element batches, and use max 10 workers
say (^Inf).&hyperize(2000, 10).grep(*.is-prime)[999999];

# Stop producing results after 1 million values
say (^Inf).&hyperize(stop-after => 1_000_000).grep(*.is-prime).tail;

# Always work with a batch-size of 1
my @lines = @filenames.&hyperize(1, :!auto).map(*.lines.List);
```

Change the given `Iterable` into a `ParaSeq` object if the following conditions have been met:

  * degree is more than 1

If the number of simultaneous workers is set to 1, there is no real parallelizetion possible. In which case `hyperize` will simply return its first argument (the invocant when used with the `.&hyperize` syntax).

  * source is not exhausted with first batch

If the first batch already exhausts the source iterator, then there is little point in parallelizing. A `Seq` that will reproduce the original source sequence will then be returned.

### ARGUMENTS

  * source

The first positional argument indicates the `Iterable` source of values to be processed. If the `.&hyperize` syntax is used, this is the invocant.

  * initial batch size

The second positional argument indicates the initial batch size to be used. It defaults to `16`. Will also assume the default if an undefined value is specified.

  * maximum number of simultaneous workers

The third positional argument indicates the maximum number of worker threads that can be activated to process the values of the source. It defaults to the number of available CPUs minus 1. Will also assume the defailt if an undefined value is specified.

  * :auto / :!auto

Flag. Defaults to `True`. If specified with a `False` value, then the batch size will **not** be altered from the size (implicitely) specified with the second positional argument.

  * :stop-after(N)

Integer value. Defaults to `Inf`, indicating there is **no** maximum number of values that should be delivered. If specified, should be a positive integer value: the produced sequence is then **guaranteed** not to produce more values than the value given.

racify
------

```raku
my %lines = @filenames.&racify(1, :!auto).map: { $_ => .IO.lines.List }
```

There is no functional difference between `hyperize` and `racify` as of version `0.0.4`. So it is only kept to remain compatible with the `hyperize` module.

The decision to remove specific the racing capability was made after it became clear that there was a race condition (no pun intended) in determining when the iteration was finished. To fix this deficiency would require quite a bit of additional logic, that would slow down execution in both the hypered and racing case.

As the difference in performance between hypering and racing was basically only one less `push` / `shift` per batch, it felt that this would only be a small price to pay. On top of that, removing the feature also means two fewer attribute checks per batch as well, so that could be counted as an overall win.

PUBLIC CLASSES
==============

ParaSeq
-------

The class of which an instance is returned by `hyperize` and `racify` if the requirements for parallelizing have been met.

Can be used in any situation where a `Seq` could also be used. The following additional methods can also be called for introspection and debugginng. In alphabetical order:

### auto

Bool. Returns whether batch sizes will be automatically optimized to provide the best throughput.

### batch-sizes

Range. The smallest and largest batch size as a `Range`.

### default-batch

Int. The default initial batch size: currently `16`.

### default-degree

Int. The default maximum number of worker threads to be used. Currently set to the number of available CPUs minus one.

### stats

A `List` of `ParaStats` objects that were produced, in the order that they were produced.

### stop-after

Int or False. Returns `False` if there is **no limit** on the number of values that can be delivered. Otherwise returns the maximum number of values that will be delivered.

### stopped

Bool. `True` if the processing of this `ParaSeq` has been halted. Otherwise `False`

### threads

Returns a `List` of the IDs of the CPU threads that have been used to produce the result.

ParaStats
---------

The `ParaStats` class is created for each batch that has been processed. It provides the following methods (in alphabetical order):

### nsecs

Int. The number of nano-seconds that it took to process all values in the associated batch.

### ordinal

Int. The ordinal number of the associated batch (starting at 0).

### processed

Int. The number of values processed in the associated batch (aka the batch size of that batch).

### produced

Int. The number of values that were produced in the associated batch. This can be `0`, or a higher value than `processed` when `.map` was called with a `Callable` that produced `Slip` values.

### threadid

Int. The numerical ID of low-level thread that processed the associated batch (aka its `$*THREAD.id`).

COMPATIBILITY NOTES
===================

reduce
------

The `reduce` method is supported, but will only produce the correct results if the order of the values in the source is **not** important to determine the final result.

IMPROVEMENTS
============

Automatic batch size adaptation
-------------------------------

One of the main issues with the current implemementation of `.hyper` and `.race` in the Rakudo core is that the batch size is fixed. Worse, there is no way to dynamically adapt the batch size depending on the load.

Batch sizes that are too big, have a tendency to not use all of the CPUs (because they have a tendency to eat all of the source items too soon, thus removing the chance to start up more threads).

Batch sizes that are too small, have a tendency to have their resource usage drowned out by the overhead of batching and dispatching to threads.

This implementation automatically adapts batch sizes from the originally (implicitely) specified batch size for better throughput and resource usage. If this feature is not desired, the `:!auto` argumennt can be to switch automatic batch size adjustment off.

Unnecessary parallelization
---------------------------

If the `degree` specified is **1**, then there is no point in batching or parallelization. In that case, this implementation will take itself completely out of the flow.

Alternately, if the initial batch size is large enough to exhaust the source, it is clearly too large. Which is interpreted as not making any sense at parallelization either. So it won't.

Note that the default initial batch size is **16**, rather than **64** in the current implementation of `.hyper` and `.race`, making the chance smaller that parallelization is abandoned too soon.

Infectiousness
--------------

The `.serial` method or `.Seq` coercer can be typically be used to "unhyper" a hypered sequence. However many other interface methods do the same in the current implementation of `.hyper` and `.race`, thereby giving the impression that the flow is still parallelized. When in fact they aren't anymore.

Also, hyperized sequences in the current implementation are considered to be non-lazy, even if the source **is** lazy.

This implementation aims to make all interface methods pass on the hypered nature and laziness of the sequence.

Loop control statements
-----------------------

Some loop control statements may affect the final result. Specifically the `last` statement does. In the current implementation of `.hyper` and `.race`, this will only affect the batch in which it occurs.

This implementation aims to make `last` stop any processing of current and not create anymore batches.

Support more interface methods
------------------------------

Currently only the `.map` and `.grep` methods are supported by the current implementation of `.hyper` and `.race` (and not even completely). Complete support for `.map` and `.grep` are provided, and other methods (such as `.first`) will also be supported.

Use of phasers
--------------

When an interface method takes a `Callable`, then that `Callable` can contain phasers that may need to be called (or not called) depending on the situation. The current implementation of `.hyper` and `.race` do not allow phasers at all.

This implementation aims to support phasers in a sensible manner:

### ENTER

Called before each iteration.

### FIRST

Called on the first iteration in the first batch.

### NEXT

Called at the end of each iteration.

### LAST

Called on the last iteration in the last batch. Note that this can be short-circuited with a `last` control statement.

### LEAVE

Called after each iteration.

AUTHOR
======

Elizabeth Mattijsen <liz@raku.rocks>

Source can be located at: https://github.com/lizmat/ParaSeq . Comments and Pull Requests are welcome.

If you like this module, or what I’m doing more generally, committing to a [small sponsorship](https://github.com/sponsors/lizmat/) would mean a great deal to me!

COPYRIGHT AND LICENSE
=====================

Copyright 2024 Elizabeth Mattijsen

This library is free software; you can redistribute it and/or modify it under the Artistic License 2.0.

