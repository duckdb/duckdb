# Window Function APIs

Starting with V2.0, window functions will no longer be special cased via enums
but will be stored in the catalog as a new function type.
They are in the same namepace as macros and as scalar and aggregate functions.

Among other things, this means that extensions will be able to register _new_ window functions.
New window functions should be functions that _cannot_ be implemented as windowed aggregates.
The internal `FILL` function is an example of such a function:
it is more like `LEAD`/`LAG` than an aggregate.

Note that the builtin window functions all use this API - there are no secret internal APIs!
The one minor exception to this precept is that for backward compatibility,
the builtin functions are serialized using the legacy mechanisms.

There are four sets of APIs for window functions:

* Binding
* Blocking implementation
* Streaming implementation
* Serialization

Note that not all APIs are required.

## Binding APIs

The window function binding interface has two pieces:

* flags, which indicate whether functionality is supported at all;
* functions, which bind and verify the arguments.

In addition, a window function has a legacy `enum` value for serialization.
For the builtin functions, these are the old `WINDOW_RANK` enums.
Newer functions should set use the new enum `WINDOW_FUNCTION`.
This will tell the serialization code to read and write any state information that cannot be inferred.

### Binding Flags

The binding flags are used by the bind to check whether the function can support various windowing modifiers:

| Flag | Default | Description |
| :--- | :--- | :--- |
| `can_distinct` | `false` | Does the function support restricting its arguments to `DISTINCT` values? |
| `can_filter` | `false` | Does the function support filtering of its arguments? |
| `can_order_by` | `true` | Does the function support using `ORDER BY` with its arguments? |
| `can_exclude` | `false` | Does the function support frame exclusion? |
| `can_ignore_nulls` | `true` | Can the function optionally `IGNORE` or `RESPECT NULLS`? |

Most of the builtin functions use the default values of the flags.
Two notable exceptions are:

* `can_ignore_nulls` is not supported by `FILL` (interpolating `NULL`s is what it does)
* `can_order_by` is not supported by `DENSE_RANK`.

### Binding Functions

The binder internals are currently only set up to validate function arguments,
but window functions also have ordering arguments that need validation.
For this reason there are two API for binding and validating specific window function calls:

* `window_bind_function_t` - This performs the usual binding operations of argument validation and override.
* `window_validate_function_t` - This performs additional checks of the ordering arguments after binding.

Among the builtin functions, these are only defined by the "value" functions (`XXX_VALUE`, `FILL`, `LEAD`/`LAG`).
Note that the binding APIs can return an optional `FunctionData` subclass,
but none of the builtin window functions need such an object.

## Blocking APIs

The main blocking window operator (`PhysicalWindow`) has been heavily optimized
for multithreading and expression sharing.

### Framing Bounds

There are up to 8 different frame locations that may be needed by a window function,
but computing some of them can be expensive (e.g., peer boundaries).
To improve performance, window functions request which of the frame locations they need.
The set of frame locations are:

| Name | Description |
| :--- | :--- |
| `PARTITION_BEGIN` | The start of the row's _partition_ (not the hash group) |
| `PARTITION_END` | The end of the row's partition |
| `PEER_BEGIN` | The start of the row's peer group (the rows that have the same `ORDER BY` values). |
| `PEER_END` | The end of the row's peer group. |
| `VALID_BEGIN` | The start of the `ORDER BY` values that are not `NULL`. |
| `VALID_END` | The end of the `ORDER BY` values that are not `NULL`. |
| `FRAME_BEGIN` | The start of the frame (e.g., `ROWS BETWEEN`). |
| `FRAME_END` | The end of the frame. |

The `window_bounds_function_t` function fills in a set of the bounds it requires.
This may depend on details of the window expression.
Note that some of the locations require others to be computed,
but these dependencies are handled by the hosting operator.

During evaluation, these bounds are passed to the `GetData` APIs as the `bounds` argument.

### Expression Sharing

When multiple window functions are being computed for a shared partitioning and ordering,
there are often expressions that are shared between these functions.
These expressions may need to be materialized for entire partitions
(e.g., the leaves of aggregate segment trees or large lags.)
To reduce the memory footprint, the window operator requires window functions to
_register_ their expressions using the `window_sharing_function_t` API so the computed results can be shared:

* `WindowExecutor &executor` - The window function execution manager;
* `WindowSharedExpressions &sharing` - The shared expression registry.

To register an expression, the window function passes the shared expression to the `WindowSharedExpressions` object
and receives an index into the corresponding `DataChunk` or `ColumnDataCollection`.
The function then uses this index to access the expression column.
The `WindowExecutor` class provides data structures for storing these indices for use during evaluation.

There are three types of expression that can be registered:

* **Sink** expressions are passed to the `Sink` APIs as the `sink_chunk` argument.
* **Collection** expressions are passed to the `Sink` APIs as the `coll_chunk` or the `Finalize` APIs as the collection argument.
* **Evaluate** expressions are passed to the `GetData` APIs as the `eval_chunk` argument.

### State Construction

Window functions sometimes require construction of large data structures for evaluation.
The most common example in the builtin window functions are merge sort trees used for argument ordering,
but all functions share some basic information.

The window function multithreading operations use a single global (`window_global_function_t`)
and multiple thread-local (`window_local_function_t`) states per hash-group.
In the short term, these need to be derived from `WindowExecutorGlobalState` and `WindowExecutorLocalState`
respectively, although this may be refactored in the future.
There is also a potential need for an _operator_ global state per function,
but at the moment that is modelled by the `WindowExecutor` class.

After initialization, these data structures can be updated by calls to the `window_sink_function_t` API:

* `ExecutionContext &context` - The thread's execution context;
* `DataChunk &sink_chunk` - The evaluated expressions being sunk;
* `DataChunk &coll_chunk` - The evaluated expressions for the shared collection;
* `idx_t input_idx` - The row index of the chunks inside the hash group;
* `OperatorSinkInput &sink` - The sink data containing any data structures computed on the first pass.

These calls can be made on multiple threads, but the local states will as usual be confined to a single thread.

When the partition has been fully scanned and sunk to all window functions,
the `window_finalize_function_t` will be called for _each thread_:

* `ExecutionContext &context` - The thread's execution context;
* `optional_ptr<WindowCollection> collection` - A wrapper around the `ColumnDataCollection` containing all hash group sized expressions;
* `OperatorSinkInput &sink` - The sink data containing any data structures being computed.

There is currently no provision for finalizing the global state except through locks shared by the local states.

### Evaluation

With all this machinery in place, it is now possible to evaluate the function on a single chunk.
The `window_evaluate_function_t` callback is similar to scalar function evaluation,
but takes additional arguments for the frame bounds and the row index inside the hash group:

* `ExecutionContext &context` - The thread's execution context;
* `DataChunk &eval_chunk` - The function arguments;
* `DataChunk &bounds` - The 8 column bounds chunk;
* `Vector &result` - The function results;
* `idx_t row_idx` - The row index inside the hash group;
* `OperatorSinkInput &sink` - The sink data containing any data structures computed on the first pass.

## Streaming APIs

## Serialization APIs

The serialization APIs are used to serialize and deserialize the `FunctionData` objects created during binding.
At present, these are not needed by any of the builtin window functions,
but they are available for new functions that may require them.
