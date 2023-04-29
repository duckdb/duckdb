The `core_functions` extension contains the set of functions that is included in the core system.
While this is an extension, these functions are bundled with every installation of DuckDB.

In order to add new functions, add their definition to the `functions.json` file in the respective directory.
The function headers can then be generated the set of functions using the following command:

```python
python3 scripts/generate_functions.py
```