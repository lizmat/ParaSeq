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

Because the [`.are`](https://docs.raku.org/type/Any#method_are) method is already **highly** optimized, adding specific hypering would only slow down execution. So no specific hypering support has been added.

categorize
----------

Because the [`.categorize`](https://docs.raku.org/type/List#routine_categorize) method produces a Hash, putting together the `Hash` results of each batch in a correct manner, would be very complicated and potentially CPU intensive. Therefore, **no** specific hypering logic has been added for this method at this point.

classify
--------

Because the [`.classify`](https://docs.raku.org/type/List#routine_classify) method produces a Hash, putting together the `Hash` results of each batch in a correct manner, would be very complicated and potentially CPU intensive. Therefore, **no** specific hypering logic has been added for this method at this point.

elems
-----

An optimized version of the [`.elems`](https://docs.raku.org/type/List#routine_elems) method has been implemented.

end
---

An optimized version of the [`.end`](https://docs.raku.org/type/List#routine_end) method has been implemented.

first
-----

The [`.first`](https://docs.raku.org/type/List#routine_first) method is supported without any specific hypering support added as it turned out to be impossible to make it faster..

head
----

An optimized version of the [`.head`](https://docs.raku.org/type/List#method_head) method without any arguments (as an endpoint), has been implemented.

join
----

An optimized version of the [`.join`](https://docs.raku.org/type/List#routine_join) method has been implemented.

max
---

The [`.max`](https://docs.raku.org/type/Any#routine_max) method, when called **without** any named arguments, provides the maximum value without any specific hypering.

min
---

The [`.min`](https://docs.raku.org/type/Any#routine_min) method, when called **without** any named arguments, provides the minimum value without any specific hypering.

minmax
------

An optimized version of the [`.minmax`](https://docs.raku.org/type/Any#routine_minmax) method has been implemented.

reduce
------

An optimized version of the [`.reduce`](https://docs.raku.org/type/List#routine_reduce) method has been implemented.

**Caveat**: will only produce the correct results if the order of the values is **not** important to determine the final result.

sum
---

An optimized version of the [`.sum`](https://docs.raku.org/type/List#routine_sum) method has been implemented.

tail
----

An optimized version of the [`.tail`](https://docs.raku.org/type/List#method_tail) method without any arguments (as an endpoint), has been implemented.

INTERFACE METHODS
=================

These methods produce another `ParaSeq` object that will keep the parallelization information so that the possibility for parallelization is used whenever possible.

In alphabetical order:

antipairs
---------

**Status**: an optimized version of the [`.antipairs`](https://docs.raku.org/type/List#routine_antipairs) method has been implemented.

batch
-----

**Status**: an optimized version of the [`.batch`](https://docs.raku.org/type/List#method_batch) method has been implemented.

collate
-------

**Status**: the nature of the [`.collate`](https://docs.raku.org/type/Any#method_collate) method basically makes it impossible to hyper. Therefore, **no** specific hypering logic has been added for this method.

combinations
------------

**Status**: the nature of the [`.combinations`](https://docs.raku.org/type/List#routine_combinations) method basically makes it impossible to hyper. Therefore, **no** specific hypering logic has been added for this method.

deepmap
-------

**Status**: an optimized version of the [`.deepmap`](https://docs.raku.org/type/Any#method_deepmap) method has been implemented.

**Caveat**: to catch any execution of `last`, the `ParaSeq` module exports its own `last` subroutine. This means that any mapper code will either have to live within the scope in which the `use ParaSeq` command is executed, **or** will need to call the fully qualified `ParaSeq::last` subroutine to ensure proper `last` handling in a hypered `.map`.

duckmap
-------

**Status**: an optimized version of the [`.duckmap`](https://docs.raku.org/type/Any#method_duckmap) method has been implemented.

**Caveat**: to catch any execution of `last`, the `ParaSeq` module exports its own `last` subroutine. This means that any mapper code will either have to live within the scope in which the `use ParaSeq` command is executed, **or** will need to call the fully qualified `ParaSeq::last` subroutine to ensure proper `last` handling in a hypered `.map`.

**Caveat**: due to the way `duckmap` was implemented before the `2024.06` release of Rakudo, the `LEAVE` phaser would get called for every value attempted. This has since been fixed, so that the `ENTER` and `LEAVE` phasers are only called if the `Callable` is actually invoked.

flat
----

**Status**: the [`.flat`](https://docs.raku.org/type/Any#method_flat) method is already very simple, and adding hypering overhead would do just that: add overhead. Therefore, **no** specific hypering logic has been added for this method.

flatmap
-------

[`.flatmap`](https://docs.raku.org/type/List#method_flatmap)

grep
----

[`.grep`](https://docs.raku.org/type/List#routine_grep)

head
----

**Status**: the [`.head`](https://docs.raku.org/type/List#method_head) method is already very simple, and adding hypering overhead would do just that: add overhead. Therefore, **no** specific hypering logic has been added for this method.

invert
------

**Status**: the nature of the [`.invert`](https://docs.raku.org/type/List#routine_invert) method basically makes it impossible to hyper. Therefore, **no** specific hypering logic has been added for this method.

keys
----

**Status**: the [`.keys`](https://docs.raku.org/type/List#routine_keys) method is already very simple, and adding hypering overhead would do just that: add overhead. Therefore, **no** specific hypering logic has been added for this method.

kv
--

**Status**: an optimized version of the [`.kv`](https://docs.raku.org/type/List#routine_kv) method has been implemented.

map
---

**Status**: an optimized version of the [`.map`](https://docs.raku.org/type/List#routine_map) method has been implemented.

**Caveat**: due to a bug in Rakudo until 2024.06 release, any `FIRST` phaser will be run at the start of each batch. This can be worked around by using the [**$**, the nameless state variable](https://docs.raku.org/language/variables#The_$_variable):

```raku
@am.&hyperize.map({ FIRST say "first" unless $++; 42 });
```

**Caveat**: if a `last` statement is executed, it will ensure that no further values will be delivered. However, this may not stop other threads immediately. So any other phasers, such as `ENTER`, `LEAVE` and `NEXT` may still get executed, even though the values that were produced, will not be delivered.

**Caveat**: to catch any execution of `last`, the `ParaSeq` module exports its own `last` subroutine. This means that any mapper code will either have to live within the scope in which the `use ParaSeq` command is executed, **or** will need to call the fully qualified `ParaSeq::last` subroutine to ensure proper `last` handling in a hypered `.map`.

max
---

**Status**: an optimized version of the [`.max`](https://docs.raku.org/type/Any#routine_max) method with named arguments, has been implemented.

maxpairs
--------

**Status**: an optimized version of the [`.maxpairs`](https://docs.raku.org/type/Any#routine_maxpairs) method has been implemented.

min
---

**Status**: an optimized version of the [`.min`](https://docs.raku.org/type/Any#routine_min) method with named arguments, has been implemented.

minpairs
--------

**Status**: an optimized version of the [`.minpairs`](https://docs.raku.org/type/Any#routine_minpairs) method has been implemented.

nodemap
-------

[`.nodemap`](https://docs.raku.org/type/Any#method_nodemap)

pairs
-----

**Status**: an optimized version of the [`.pairs`](https://docs.raku.org/type/List#routine_pairs) method has been implemented.

pairup
------

**Status**: the nature of the [`pairup`](https://docs.raku.org/type/Any#method_pairup) method basically makes it impossible to hyper. Therefore, **no** specific hypering logic has been added for this method.

permutations
------------

**Status**: the nature of the [`.permutations`](https://docs.raku.org/type/List#routine_permutations) method basically makes it impossible to hyper. Therefore, **no** specific hypering logic has been added for this method.

pick
----

**Status**: the nature of the [`.pick`](https://docs.raku.org/type/List#routine_pick) method basically makes it impossible to hyper. Therefore, **no** specific hypering logic has been added for this method.

produce
-------

**Status**: the nature of the [`.produce`](https://docs.raku.org/type/Any#routine_produce) method basically makes it impossible to hyper. Therefore, **no** specific hypering logic has been added for this method.

repeated
--------

[`.repeated`](https://docs.raku.org/type/Any#method_repeated)

reverse
-------

**Status**: the nature of the [`.reverse`](https://docs.raku.org/type/List#routine_reverse) method basically makes it impossible to hyper. Therefore, **no** specific hypering logic has been added for this method.

roll
----

**Status**: the nature of the [`.roll`](https://docs.raku.org/type/List#routine_roll) method basically makes it impossible to hyper. Therefore, **no** specific hypering logic has been added for this method.

rotate
------

**Status**: the nature of the [`.rotate`](https://docs.raku.org/type/List#routine_rotate) method basically makes it impossible to hyper. Therefore, **no** specific hypering logic has been added for this method.

rotor
-----

**Status**: an optimized version of the [`.rotor`](https://docs.raku.org/type/List#routine_rotor) method has been implemented for the single argument non-`Pair` case. All other cases are basically too complicated to hyper, and therefore have no specific hypering logic.

skip
----

**Status**: the simple cases of `.skip()` and `.skip(N)` are handled by skipping that amount on the result iterator and returning the invocant.

The case of `.skip(*)` is handled by stopping any production of values and returning an empty `Seq`.

The nature of the other types of arguments on the [`.skip`](https://docs.raku.org/type/Seq#method_skip) method basically makes it impossible to hyper. Therefore, **no** specific hypering logic has been added for these cases.

slice
-----

**Status**: the nature of the [`.slice`](https://docs.raku.org/type/Seq#multi_method_slice) method basically makes it impossible to hyper. Therefore, **no** specific hypering logic has been added for this method.

snip
----

**Status**: the nature of the [`.snip`](https://docs.raku.org/type/Any#routine_snip) method basically makes it impossible to hyper. Therefore, **no** specific hypering logic has been added for this method.

snitch
------

**Status**: since it doesn't make much sense for the [`.snitch`](https://docs.raku.org/type/Any#routine_snitch) method to be called on the `ParaSeq` object as a whole, calling the `.snitch` method instead acivates snitcher logic on the `ParaSeq` object. If set, the snitcher code will be called for each input buffer before being scheduled (in a threadsafe manner). The snitcher code will receive this buffer as a `List`.

sort
----

**Status**: the nature of the [`.sort`](https://docs.raku.org/type/List#routine_sort) method basically makes it impossible to hyper. Therefore, **no** specific hypering logic has been added for this method.

squish
------

[`.squish`](https://docs.raku.org/type/Any#method_squish)

tail
----

**Status**: the nature of the [`.tail`](https://docs.raku.org/type/List#method_tail) method basically makes it impossible to hyper. Therefore, **no** specific hypering logic has been added for this method.

toggle
------

**Status**: the nature of the [`.toggle`](https://docs.raku.org/type/Any#method_toggle) method basically makes it impossible to hyper. Therefore, **no** specific hypering logic has been added for this method.

unique
------

**Status**: an optimized version of the [`.unique`](https://docs.raku.org/type/Any#method_unique) method has been implemented.

**Caveat**: using the `:with` option with an operator that checks for **inequality**, may produce erroneous results.

values
------

**status**: the [`.values`](https://docs.raku.org/type/List#routine_values) is basically a no-op, so it returns the invocant for simplicity.

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

The [`Array`](https://docs.raku.org/type/Array) coercer has been implemented: it removes all hypering information from its invocant.

Bag
---

Putting together the [`Bag`](https://docs.raku.org/type/Bag) of each batch in a correct manner, would be complicated and potentially CPU intensive. Therefore, **no** specific hypering logic has been added for this coercer at this point.

BagHash
-------

Putting together the [`BagHash`](https://docs.raku.org/type/BagHash) of each batch in a correct manner, would be complicated and potentially CPU intensive. Therefore, **no** specific hypering logic has been added for this coercer at this point.

Bool
----

An optimized version of the [`Bool`](https://docs.raku.org/type/Bool) coercer has been implemented.

eager
-----

The [`.eager`](https://docs.raku.org/type/List#routine_eager) method is functionally the same as the `.List`, and has been implemented as such.

Hash
----

Putting together the [`Hash`](https://docs.raku.org/type/Hash) of each batch in a correct manner, would be complicated and potentially CPU intensive. Therefore, **no** specific hypering logic has been added for this coercer at this point.

Int
---

An optimized version of the [`Int`](https://docs.raku.org/type/Int) coercer has been implemented.

IterationBuffer
---------------

The [`IterationBuffer`](https://docs.raku.org/type/IterationBuffer) coercer collects the result of the invocant in an `IterationBuffer` and returns that.

list
----

The [`list`](https://docs.raku.org/routine/list) coercer is implemented: it creates a `List` object with the object's iterator as its "todo" (so no elements are reified until they are actually demanded). And thus removes all hypering information.

List
----

The [`List`](https://docs.raku.org/type/List) coercer has been implemented: it creates a fully reified `List` and thus removes all hypering information.

Map
---

Putting together the [`Map`](https://docs.raku.org/type/Map) of each batch in a correct manner, would be complicated and potentially CPU intensive. Therefore, **no** specific hypering logic has been added for this coercer at this point.

Mix
---

Putting together the [`Mix`](https://docs.raku.org/type/Mix) of each batch in a correct manner, would be complicated and potentially CPU intensive. Therefore, **no** specific hypering logic has been added for this coercer at this point.

MixHash
-------

Putting together the [`MixHash`](https://docs.raku.org/type/MixHash) of each batch in a correct manner, would be complicated and potentially CPU intensive. Therefore, **no** specific hypering logic has been added for this coercer at this point.

Numeric
-------

An optimized version of the [`Int`](https://docs.raku.org/type/Numeric) coercer has been implemented.

Seq
---

The [`Seq`](https://docs.raku.org/type/Seq) coercer has been implemented: it removes all hypering information from its invocant.

serial
------

The [`.serial`](https://docs.raku.org/type/HyperSeq#method_serial) method is functionally the same as the `.Seq`, and has been implemented as such.

Set
---

Putting together the [`Set`](https://docs.raku.org/type/Set) of each batch in a correct manner, would be complicated and potentially CPU intensive. Therefore, **no** specific hypering logic has been added for this coercer at this point.

SetHash
-------

Putting together the [`SetHash`](https://docs.raku.org/type/SetHash) of each batch in a correct manner, would be complicated and potentially CPU intensive. Therefore, **no** specific hypering logic has been added for this coercer at this point.

Slip
----

The [`Slip`](https://docs.raku.org/type/Slip) coercer has been implemented: it removes all hypering information from its invocant.

Str
---

An optimized version of the [`Str`](https://docs.raku.org/type/Str) coercer has been implemented.

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

This implementation automatically adapts batch sizes from the originally (implicitely) specified batch size for better throughput and resource usage. If this feature is not desired, the `:!auto` argument can be used to switch automatic batch size adjustment off.

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

THEORY OF OPERATION
===================

A description of the program logic when you call `.&hyperize` on an object.

Step 1: is degree > 1
---------------------

If the degree is 1, then there's nothing to parallelize. The `&hyperize` sub will return its first argument. Since `.&hyperize` will most likely mostly be called as a method, the first argument will be described as the "invocant".

Step 2: fetch the initial batch
-------------------------------

Obtain an iterator from the invocant and read as many items into an `IterationBuffer` as the initial batch size indicates. If this exhausted the source iterator, then return a `Seq` on that `IterationBuffer`. This also effectively inhibits any parallelization.

Step 3: create a ParaSeq
------------------------

If step 1 and 2 didn't result in a premature return, then a `ParaSeq` object is created and returned. Apart from the initial `IterationBuffer` and source iterator, it also contains:

  * the initial batch size

  * the degree (max number of worker threads)

  * the "auto" flag

  * any "stop after" value

  * the $*SCHEDULER value

Step 4: an interface method is called
-------------------------------------

Each interface method decides whether they can support an optimized hypered operation. If does **not** support hypered operation, a special `BufferIterator` is created. This iterator takes the buffer and the source iterator from step 2, that will first deliver the values from the buffer, and then the values from the source iterator (until that is exhausted).

The following step then depends on whether the method is an endpoint (such as `max`) or a coercer (such as `Str`). If it is, then the `BufferIterator` will be wrapped in a standard `Seq` object and the core equivalent of that method will be called on it and its result returned.

If it is an other interface method, then a clone of the original invocant is created with the `BufferIterator` object as its source iterator. And that is then returned.

If the method **can** be parallized, then we continue.

Step 5: check granularity of initial batch
------------------------------------------

Some methods (such as `map` and `grep`) **may** require more than 1 value per iteration (the granularity). Batches need to have a size with the correct granularity, otherwise processing a batch may fail at the last iteration for lack of positional arguments.

So read additional values from the source iterator until the granularity is correct.

Step 6: keep producers in check
-------------------------------

One of the issues with hypering, is that multiple threads can easily produce so many values that the deliverer can be overwhelmed in the worst case. Or at least, it could be wasteful in resources because it would produce way more values than would actually need to be delivered. For example:

```raku
say (^Inf).&hyperize.map(*.is-prime)[999999];  # the millionth prime number
```

One would not want the parallelization to calculate 1.2 million prime numbers while only needing 1 million. Yet, this will happen quite easily if there is not enough ["back pressure"](https://medium.com/@jayphelps/backpressure-explained-the-flow-of-data-through-software-2350b3e77ce7).

So the thread delivering values, needs to be able to tell the batcher when it is ok to schedule another batch for processing. This is done through a `ParaQueue` object (a low-level blocking concurrent queue to which the delivering thread will push whenever it has delivered the results of a result batch. Since "back pressure queue" is a bit of a mouthful, this is being referred to as the "pressure queue".

The deliverer is also responsible for telling the batcher what it thinks the best size for the next batch is. See Step 6.

To get things started, a pressure queue is set up with various batch sizes (one for each degree) so that maximally that many threads can be started. By using variable sizes, the delivery iterator can more quickly decide what the optimum size is for the given workload because the different batch sizes produce different average time needed per processed value. Which is used to calculate the optimum batch size.

Step 7: set up the result queue
-------------------------------

The result queue is where all of the worker threads post the result of processing their batch. The result queue is a `ParaQueue` of `ParaQueue`s. Whenever the batcher is told that it may queue up the next batch, the batcher also pushes a new `ParaQueue` object to the result queue **and** passes that `ParaQueue` object to the worker thread. So that when the worker thread is done processing, it can push its result to **that** `ParaQueue`.

The `ParaQueue` that is pushed to the result queue by the batcher ensures that the order in which batches of work are handed out, will also be the order in which the results will be delivered. The fact that the result of processing is pushed to this `ParaQueue` itself, makes the deliverer block until the worker thread has pushed its result (even if other threads may have been ready earlier).

Step 8: set up the waiting queue
--------------------------------

To communicate back performance statistics from a worker thread finishing their work, another `ParaQueue` is created. Its sole purpose is to collect all `ParaStats` objects that each worker thread has completed **as soon as they are done**. It basically also represents the number of result batches that are waiting to be delivered: hence the name "waiting queue".

This queue is inspected by the delivery thread whenever it has completed delivery of a batch. It's possible that this queue is empty. It can also contain the statistics of more than one worker thread.

Step 9: start the batcher thread
--------------------------------

The batcher thread keeps running until the source iterator is exhausted. When it is exhausted, it pushes an `IterationEnd` to the result queue, indicating that no more batches will be coming. It's the only time a non-`ParaQueue` object is pushed to the result queue.

The batcher is basically a loop that blocks on the messages from the deliverer. Whenever a message is received (with the proposed batch size, calculated from any available performance statistics), it adjusts for correct granularity (as the deliverer is unaware of any granularity restrictions). The batcher then fetches that many values from the source iterator and schedules another job for a worker thread with the values obtained (if any).

Step 10: start delivering
-------------------------

At this point a `ParaIterator` object is created and installed as the (result) `.iterator` of the invocant. The paralellizing logic in the hypered variant of the method is started, which then in turn starts feeding the `ParaIterator`.

It is then up to the code that iterators over the (result) iterator to consume the values delivered. If no values are being delivered, production of values will halt after all inital "degree" batches have been produced.

When the deliverer is done with a result batch, it removes all available statistics information and adds these to the `stats` of the `ParaSeq` object. It also uses this information to calculate an optimum batch size for any other batches to be processed (unless this is inhibited by the `:!auto` named argument to `.&hyperize`).

The algorithm for batch size calculation is pretty simple at the moment. To allow for optimum responsiveness of the program, as well as any other processes running on the computer, a time slice of 500_000 nanoseconds has been chosen.

If a batch took almost exactly that amount of time to produce values, then the size of that batch is assumed to be the optimum size. If the batch took longer, then the batch size should be reduced. If the batch took less than that to complete, then the batch size should be increased.

Decision tree
-------------

      source
       \- hyperize
           |- invocant (degree == 1)
           |- Seq      (source exhausted with initial batch)
           |- ParaSeq
               \- method call
                   |- endpoint
                   |- coercer
                   |- hypering

Hypering control flow
---------------------

        /---------------------------------------------------------\
        |                |                                        |
        |               ⬇                                         |
        |         --------------                 ------------     |
        |         | statistics |         /-----⮕ | producer | ⮕ -|
        |         --------------        /        ------------     |
        ⬇               ⬇             /                          |
    -----------    ------------    -----------   ------------     |
    | results | ⮕ | deliverer | ⮕ | batcher | ⮕ | producer | ⮕ -|
    -----------    ------------    -----------   ------------     |
                        |              \                          |
                        |               \        ------------     |
                        |                \-----⮕ | producer | ⮕ -/
                        ⬇                        ------------
                     values
              consumed by program

AUTHOR
======

Elizabeth Mattijsen <liz@raku.rocks>

Source can be located at: https://github.com/lizmat/ParaSeq . Comments and Pull Requests are welcome.

If you like this module, or what I’m doing more generally, committing to a [small sponsorship](https://github.com/sponsors/lizmat/) would mean a great deal to me!

COPYRIGHT AND LICENSE
=====================

Copyright 2024 Elizabeth Mattijsen

This library is free software; you can redistribute it and/or modify it under the Artistic License 2.0.

