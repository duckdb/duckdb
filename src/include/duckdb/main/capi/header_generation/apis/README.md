# How to add an entry to the extension API
The only way to add C API functionality to the C extension API is through the json files in this directory. From these
JSON files we generate the actual header files containg a struct of function pointers. 

To not break backwards compatibility of the API, we can only add functions to the end of these JSON files. To ensure extension
compatibility can be properly determined, the API version MUST be bumped every time a function is added to an API here.
In other words, modification of the api definition files must add a complete new versioned group of entries.

so for example consider the api currently:

```json
{
  "functions": [
    {
      "version": "v0.0.1",
      "entries": [
        { "name": "duckdb_open" },
        {"name": "duckdb_open_ext" }
      ]
    },
    {
      "version": "v0.0.2",
      "entries": [
        { "name": "duckdb_open" },
        { "name": "duckdb_create_scalar_function" }
      ]
    }
  ]
}
```

Let's say we want to add another function `my_function` to the extension api, we would have to bump the extension version:
```json
{
  "functions": [
    {
      "version": "v0.0.1",
      "entries": [
        { "name": "duckdb_open" },
        {"name": "duckdb_open_ext" }
      ]
    },
    {
      "version": "v0.0.2",
      "entries": [
        { "name": "duckdb_open" },
        { "name": "duckdb_create_scalar_function" }
      ]
    },
    {
      "version": "v0.0.3",
      "entries": [
        { "name": "my_function" }
      ]
    }
  ]
}
```
