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

The decision to remove the specific racing capability was made after it became clear that there was a race condition (no pun intended) in determining when the iteration was finished. To fix this deficiency would require quite a bit of additional logic, that would slow down execution in both the hypered and racing case.

As the difference in performance between hypering and racing was basically only one less `push` / `shift` per batch, it felt that this would only be a small price to pay. On top of that, removing the feature also means two fewer attribute checks per batch as well, so that could be counted as an overall win.

ENDPOINT METHODS
================

These methods return a single value that could be considered a reduction of the hypered iteration. In alphabetical order:

are
---

The [`.are`](https://docs.raku.org/type/Any#method_are) method returns either the most restrictive type, or a `Bool` if a test for a specific type was requested.

**Status**: since this method is already **highly** optimized, adding specific hypering would only slow down execution.

categorize
----------

The [`.categorize`](https://docs.raku.org/type/List#routine_categorize) method returns a `Hash` with the expected categorization.

**Status**: since the `.categorize` method produces a Hash, putting together the `Hash` results of each batch in a correct manner, would be very complicated and potentially CPU intensive. Therefore, **no** specific hypering logic has been added for this method at this point.

classify
--------

The [`.classify`](https://docs.raku.org/type/List#routine_classify) method returns a `Hash` with the expected classification.

**Status**: since the `.classify` method produces a Hash, putting together the `Hash` results of each batch in a correct manner, would be very complicated and potentially CPU intensive. Therefore, **no** specific hypering logic has been added for this method at this point.

elems
-----

The [`.elems`](https://docs.raku.org/type/List#routine_elems) method returns The number of elements that the iteration produced. Is typically less than the number of elements in the source, but can also be more in the case of a `Callable` in a `.map`.

**Status**: an optimized version of the `.elems` method has been implemented.

end
---

The [`.end`](https://docs.raku.org/type/List#routine_end) method return The number of `.elems` minus one.

**Status**: an optimized version of the `.end` method has been implemented.

first
-----

Only if the [`.first`](https://docs.raku.org/type/List#routine_first) is called **without** any arguments, is it an endpoint and as such will return the **first** delivered value.

**Status**: an optimized version of the `.first` method without any arguments, has been implemented.

head
----

Only if the [`.head`](https://docs.raku.org/type/List#method_head) is called **without** any arguments, is it an endpoint and as such will return the **first** delivered value.

**Status**: an optimized version of the `.head` method without any arguments, has been implemented.

join
----

The [`.join`](https://docs.raku.org/type/List#routine_join) method joins all delivered values together with the optional separator string (which defaults to `""`).

**Status**: an optimized version of the `.join` method has been implemented.

max
---

The [`.max`](https://docs.raku.org/type/Any#routine_max) method, when called **without** any named arguments, provides the maximum value.

min
---

The [`.min`](https://docs.raku.org/type/Any#routine_min) method, when called **without** any named arguments, provides the minimum value.

minmax
------

The [`.minmax`](https://docs.raku.org/type/Any#routine_minmax) method, when called **without** any named arguments, provides a `Range` with the minimum value and the maximum value as endpoints.

reduce
------

The [`.reduce`](https://docs.raku.org/type/List#routine_reduce) method is supported, but will only produce the correct results if the order of the values in the source is **not** important to determine the final result.

sum
---

The [`.sum`](https://docs.raku.org/type/List#routine_sum) method sums all delivered values. It is an optimized version of `.reduce(* + *)`.

tail
----

Only if the [`.tail`](https://docs.raku.org/type/List#method_tail) is called **without** any arguments, is it an endpoint and as such will return the **last** delivered value.

INTERFACE METHODS
=================

These methods produce another `ParaSeq` object that will keep the parallelization information so that the possibility for parallelization is used whenever possible.

In alphabetical order:

antipairs
---------

[`.antipairs`](https://docs.raku.org/type/List#routine_antipairs)

batch
-----

[`.batch`](https://docs.raku.org/type/List#method_batch)

collate
-------

[`.collate`](https://docs.raku.org/type/Any#method_collate)

combinations
------------

[`.combinations`](https://docs.raku.org/type/List#routine_combinations)

deepmap
-------

[`.deepmap`](https://docs.raku.org/type/Any#method_deepmap)

duckmap
-------

[`.duckmap`](https://docs.raku.org/type/Any#method_duckmap)

eager
-----

[`.eager`](https://docs.raku.org/type/List#routine_eager)

first
-----

[`.first`](https://docs.raku.org/type/List#routine_first)

flat
----

[`.flat`](https://docs.raku.org/type/Any#method_flat)

flatmap
-------

[`.flatmap`](https://docs.raku.org/type/List#method_flatmap)

grep
----

[`.grep`](https://docs.raku.org/type/List#routine_grep)

head
----

[`.head`](https://docs.raku.org/type/List#method_head)

invert
------

[`.invert`](https://docs.raku.org/type/List#routine_invert)

keys
----

[`.keys`](https://docs.raku.org/type/List#routine_keys)

kv
--

[`.kv`](https://docs.raku.org/type/List#routine_kv)

map
---

[`.map`](https://docs.raku.org/type/List#routine_map)

max
---

[`.max`](https://docs.raku.org/type/Any#routine_max)

maxpairs
--------

[`.maxpairs`](https://docs.raku.org/type/Any#routine_maxpairs)

min
---

[`.min`](https://docs.raku.org/type/Any#routine_min)

minmax
------

[`.minmax`](https://docs.raku.org/type/Any#routine_minmax)

minpairs
--------

[`.minpairs`](https://docs.raku.org/type/Any#routine_minpairs)

nodemap
-------

[`.nodemap`](https://docs.raku.org/type/Any#method_nodemap)

pairs
-----

[`.pairs`](https://docs.raku.org/type/List#routine_pairs)

permutations
------------

[`.permutations`](https://docs.raku.org/type/List#routine_permutations)

pick
----

[`.pick`](https://docs.raku.org/type/List#routine_pick)

produce
-------

[`.produce`](https://docs.raku.org/type/Any#routine_produce)

repeated
--------

[`.repeated`](https://docs.raku.org/type/Any#method_repeated)

reverse
-------

[`.reverse`](https://docs.raku.org/type/List#routine_reverse)

roll
----

[`.roll`](https://docs.raku.org/type/List#routine_roll)

rotate
------

[`.rotate`](https://docs.raku.org/type/List#routine_rotate)

rotor
-----

[`.rotor`](https://docs.raku.org/type/List#routine_rotor)

slice
-----

[`.slice`](https://docs.raku.org/type/Seq#multi_method_slice)

snitch
------

[`.snitch`](https://docs.raku.org/type/Any#routine_snitch)

sort
----

[`.sort`](https://docs.raku.org/type/List#routine_sort)

squish
------

[`.squish`](https://docs.raku.org/type/Any#method_squish)

tail
----

[`.tail`](https://docs.raku.org/type/List#method_tail)

toggle
------

[`.toggle`](https://docs.raku.org/type/Any#method_toggle)

unique
------

[`.unique`](https://docs.raku.org/type/Any#method_unique)

values
------

[`.values`](https://docs.raku.org/type/List#routine_values)

COERCERS
========

The following coercers have been optimized as much as possible with the use of `&hyperize`. Note that these methods are optimized for speed, rather than memory usage. Should you want to optimize for memory usage, then put in a `.Seq` coercer first. Or remove `.&hyperize` completely. So:

```raku
my $speed  = (^1_000_000).&hyperize.Set;      # optimized for speed
my $memory = (^1_000_000).&hyperize.Seq.Set;  # optimized for memory
my $none   = (^1_000_000).Seq.Set;            # no parallelization
```

In alphabetical order:

Array
-----

**Status**: the [`Array`](https://docs.raku.org/type/Array) coercer is implemented: it removes all hypering information from its invocant.

Bag
---

**Status**: putting together the [`Bag`](https://docs.raku.org/type/Bag) of each batch in a correct manner, would be complicated and potentially CPU intensive. Therefore, **no** specific hypering logic has been added for this coercer at this point.

BagHash
-------

**Status**: putting together the [`BagHash`](https://docs.raku.org/type/BagHash) of each batch in a correct manner, would be complicated and potentially CPU intensive. Therefore, **no** specific hypering logic has been added for this coercer at this point.

Bool
----

**Status**: an optimized version of the [`Bool`](https://docs.raku.org/type/Bool) coercer has been implemented.

Hash
----

**Status**: putting together the [`Hash`](https://docs.raku.org/type/Hash) of each batch in a correct manner, would be complicated and potentially CPU intensive. Therefore, **no** specific hypering logic has been added for this coercer at this point.

Int
---

**Status**: an optimized version of the [`Int`](https://docs.raku.org/type/Int) coercer has been implemented.

IterationBuffer
---------------

**Status**: the [`IterationBuffer`](https://docs.raku.org/type/IterationBuffer) method collects the result of the invocant in an `IterationBuffer` and returns that.

List
----

**Status**: the [`List`](https://docs.raku.org/type/List) coercer is implemented: it removes all hypering information from its invocant.

Map
---

**Status**: putting together the [`Map`](https://docs.raku.org/type/Map) of each batch in a correct manner, would be complicated and potentially CPU intensive. Therefore, **no** specific hypering logic has been added for this coercer at this point.

Mix
---

**Status**: putting together the [`Mix`](https://docs.raku.org/type/Mix) of each batch in a correct manner, would be complicated and potentially CPU intensive. Therefore, **no** specific hypering logic has been added for this coercer at this point.

MixHash
-------

**Status**: putting together the [`MixHash`](https://docs.raku.org/type/MixHash) of each batch in a correct manner, would be complicated and potentially CPU intensive. Therefore, **no** specific hypering logic has been added for this coercer at this point.

Numeric
-------

**Status**: an optimized version of the [`Int`](https://docs.raku.org/type/Numeric) coercer has been implemented.

Seq
---

**Status**: the [`Seq`](https://docs.raku.org/type/Seq) coercer is implemented: it removes all hypering information from its invocant.

Set
---

**Status**: putting together the [`Set`](https://docs.raku.org/type/Set) of each batch in a correct manner, would be complicated and potentially CPU intensive. Therefore, **no** specific hypering logic has been added for this coercer at this point.

SetHash
-------

**Status**: putting together the [`SetHash`](https://docs.raku.org/type/SetHash) of each batch in a correct manner, would be complicated and potentially CPU intensive. Therefore, **no** specific hypering logic has been added for this coercer at this point.

Slip
----

**Status**: the [`Slip`](https://docs.raku.org/type/Slip) coercer is implemented: it removes all hypering information from its invocant.

Str
---

**Status**: an optimized version of the [`Str`](https://docs.raku.org/type/Str) coercer has been implemented.

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

### is-lazy

Bool. Returns whether the `ParaSeq` iterator should be considered lazy or not. It will be considered lazy if the source iterator is lazy and **no** value has been specified with `:stop-after`.

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

