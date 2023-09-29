# Aggregate Functions

Aggregate functions combine a set of values into a single value.
In DuckDB, they appear in several contexts:

* As part of the `SELECT` list of a query with a `GROUP BY` clause (ordinary aggregation)
* As the only elements of the `SELECT` list of a query _without_ a `GROUP BY` clause (simple aggregation)
* Modified by an `OVER` clause (windowed aggregation)
* As an argument to the `list_aggregate` function (list aggregation)

## Aggregation Operations

In order to define an aggregate function, you need to define some operations.
These operations accumulate data into a `State` object that is specific to the aggregate.
Each `State` represents the accumulated values for a single result,
so if (say) there are multiple groups in a `GROUP BY`, 
each result value would need its own `State` object.
Unlike simple scalar functions, there are several of these:

| Operation | Description | Required | 
| :-------- | :---------- | :------- | 
| `size`  | Returns the fixed size of the `State` | X | 
| `initialize` | Constructs the `State` in raw memory | X |
| `destructor` | Destructs the `State` back to raw memory |  |
| `update` | Accumulate the arguments into the corresponding `State` | X |
| `simple_update` | Accumulate the arguments into a single `State`. |  |
| `combine` | Merge one `State` into another |  |
| `finalize` | Convert a `State` into a final value. | X |
| `window` | Compute a windowed aggregate value from the inputs and frame bounds |  |
| `bind` | Modify the binding of the aggregate |  |
| `statistics` | Derive statistics of the result from the statistics of the arguments |  |
| `serialize` | Write a `State` to a relocatable binary blob |  |
| `deserialize` | Read a `State` from a binary blob |  |

In addition to these high level functions,
there is also a template `AggregateExecutor` that can be used to generate these functions
from row-oriented static methods in a class.

There are also a number of helper objects that contain various bits of context for the aggregate,
such as binding data and extracted validity masks.
By combining them into these helper objects, we reduce the number of arguments to various functions.
The helpers can vary by the number of arguments, and we will refer to them simply as `info` below.
Consult the code for details on what is available.

### Size

```cpp
size()
```

`State`s are allocated in memory blocks by the various operators
so each aggregate has to tell the operator how much memory it will require.
Note that this is just the memory that the aggregate needs to get started -
it is perfectly legal to allocate variable amounts of memory
and storing pointers to it in the `State`.

### Initialize

```cpp
initialize(State *)
```

Construct a _single_ empty `State` from uninitialized memory.

### Destructor

```cpp
destructor(Vector &state, AggregateInputData &info, idx_t count)
```

Destruct a `Vector` of state pointers.
If you are using a template, the method has the signature

```cpp
Destroy(State &state, AggregateInputData &info)
```

### Update and Simple Update

```cpp
update(Vector inputs[], AggregateInputData &info, idx_t ninputs, Vector &states, idx_t count)
```

Accumulate the input values for each row into the `State` object for that row.
The `states` argument contains pointers to the states, 
which allows different rows to be accumulated into the same row if they are in the same group.
This type of operations is called "scattering", which is why
the template generator methods for `update` operations are called `ScatterUpdate`s.

```cpp
simple_update(Vector inputs[], AggregateInputData &info, idx_t ninputs, State *state, idx_t count)
```

Accumulate the input arguments for each row into a single `State`.
Simple updates are used when there is only one `State` being updated, 
usually for `SELECT` queries with no `GROUP BY` clause.
They are defined when an update can be performed more efficiently in a single tight loop.
There are some other places where this operations will be used if available
when the caller has only one state to update.
The template generator methods for simple updates are just called `Update`s.

The template generators use two methods for single rows:

```cpp
ConstantOperation(State& state, const Arg1Type &arg1, ..., AggregateInputInfo &info, idx_t count)
```

Called when there is a single value that can be accumulated `count` times.

```cpp
Operation(State& state, const Arg1Type &arg1, ..., AggregateInputInfo &info)
```

Called for each tuple of argument values with the `State` to update.

### Combine

```cpp
combine(Vector &sources, Vector &targets, AggregateInputData &info, idx_t count)
```

Merges the source states into the corresponding target states.
If you are using template generators, 
the generator is `StateCombine` and the method it wraps is:

```cpp
Combine(const State& source, State &target, AggregateInputData &info)
```

Note that the `sources` should _not_ be modified for efficiency because the caller may be using them
for multiple operations(e.g., window segment trees).
If you wish to combine destructively, you _must_ define a `window` function.

The `combine` operation is optional, but it is needed for multi-threaded aggregation.
If it is not provided, then _all_ aggregate functions in the grouping must be computed on a single thread. 

### Finalize

```cpp
finalize(Vector &state, AggregateInputData &info, Vector &result, idx_t count, idx_t offset)
```

Converts states into result values.
If you are using template generators, the generator is `StateFinalize`
and the method you define is:

```cpp
Finalize(const State &state, ResultType &result, AggregateFinalizeData &info)
```

### Window

```cpp
window(Vector inputs[], const ValidityMask &filter,
       AggregateInputData &info, idx_t ninputs, State *state,
       const FrameBounds &frame, const FrameBounds &prev, Vector &result, idx_t rid,
       idx_t bias)
```

The Window operator usually works with the basic aggregation operations `update`, `combine` and `finalize`
to compute moving aggregates via segment trees or simply computing the aggregate over a range of inputs.

In some situations, this is either overkill (`COUNT(*)`) or too slow (`MODE`) 
and an optional window function can be defined.
This function will be passed the values in the window frame, 
along with the current frame, the previous frame 
the result `Vector` and the result row number being computed.

The previous frame is provided so the function can use 
the delta from the previous frame to update the `State`.
This could be kept in the `State` itself.

The `bias` argument was used for handling large input partitions,
and contains the partition offset where the `inputs` rows start.
Currently, it is always zero, but this could change in the future 
to handle constrained memory situations.

The template generator method for windowing is:

```cpp
Window(const ArgType *arg, ValidityMask &filter, ValidityMask &valid, 
       AggregateInputData &info, State *state, 
       const FrameBounds &frame, const FrameBounds &prev, 
       ResultType &result, idx_t rid, idx_tbias)
```

Defining `window` is also useful if the aggregate wishes to use a destructive `combine` operation.
This may be tricky to implement efficiently. 

### Bind

```cpp
bind(ClientContext &context, AggregateFunction &function,vector<unique_ptr<Expression>> &arguments)
```
 
Like scalar functions, aggregates can sometimes have complex binding rules 
or need to cache data (such as constant arguments to quantiles).
The `bind` function is how the aggregate hooks into the binding system.

### Statistics

```cpp
statistics(ClientContext &context, BoundAggregateExpression &expr, AggregateStatisticsInput &input)
```

Also like scalar functions, aggregates can sometime be able to produce result statistics
based on their arguments.
The `statistics` function is how the aggregate hooks into the planner.

### Serialization

```cpp
serialize(Serializer &serializer, const optional_ptr<FunctionData> bind_data, const AggregateFunction &function);
deserialize(Deserializer &deserializer, AggregateFunction &function);
```

Again like scalar functions, bound aggregates can be serialised as part of a query plan.
These functions save and restore the binding data from binary blobs.

### Ignore Nulls

The templating system needs to know whether the aggregate ignores nulls,
so the template generators require the `IgnoreNull` static method to be defined. 

## Ordered Aggregates

Some aggregates (e.g., `STRING_AGG`) are order-sensitive.
Unless marked otherwise by setting the `order_dependent` flag to `NOT_ORDER_DEPENDENT`,
the aggregate will be assumed to be order-sensitive.
If the aggregate is order-sensitive and the user specifies an `ORDER BY` clause in the arguments,
then it will be wrapped to make sure that the arguments are cached and sorted 
before being passed to the aggregate operations:

```sql
-- Concatenate the strings in alphabetical order 
STRING_AGG(code, ',' ORDER BY code)
```
