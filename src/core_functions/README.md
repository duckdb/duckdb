`core_functions` contains the set of functions that is included in the core system.
These functions are bundled with every installation of DuckDB.

In order to add new functions, add their definition to the `functions.json` file in the respective directory.
The function headers can then be generated the set of functions using the following command:

```python
python3 scripts/generate_functions.py
```

#### Function Format

Functions are defined according to the following format:

```json
{
    "name": "date_diff",
    "parameters": "part,startdate,enddate",
    "description": "The number of partition boundaries between the timestamps",
    "example": "date_diff('hour', TIMESTAMPTZ '1992-09-30 23:59:59', TIMESTAMPTZ '1992-10-01 01:58:00')",
    "type": "scalar_function_set",
    "struct": "DateDiffFun",
    "aliases": ["datediff"]
}
```

* *name* signifies the function name at the SQL level.
* *parameters* is a comma separated list of parameter names (for documentation purposes).
* *description* is a description of the function (for documentation purposes).
* *example* is an example of how to use the function (for documentation purposes).
* *type* is the type of function, e.g. `scalar_function`, `scalar_function_set`, `aggregate_function`, etc.
* *struct* is the **optional** name of the struct that holds the definition of the function in the generated header. By default the function name will be title cased with `Fun` added to the end, e.g. `date_diff` -> `DateDiffFun`.
* *aliases* is an **optional** list of aliases for the function at the SQL level.

##### Scalar Function
Scalar functions require the following function to be defined:

```cpp
ScalarFunction DateDiffFun::GetFunction() {
	return ...
}
```

##### Scalar Function Set
Scalar function sets require the following function to be defined:

```cpp
ScalarFunctionSet DateDiffFun::GetFunctions() {
	return ...
}
```
