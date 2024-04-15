# Using `duckdb` with Pyodide

[Pyodide](https://pyodide.org/en/stable/) is a WASM-based Python interpreter that runs in the browser.

DuckDB builds binary WASM wheels for Pyodide to allow use of the DuckDB Python
bindings with Pyodide.

## Usage

Here's a small snippet of HTML showing use of DuckDB inside a Pyodide script.

```html
<html>
  <head>
    <meta charset="utf-8" />
  </head>
  <body>
    <script type="text/javascript" src="https://cdn.jsdelivr.net/pyodide/v0.25.1/full/pyodide.js"></script>
    <script type="text/javascript">
      async function main() {
        let pyodide = await loadPyodide();
        await pyodide.loadPackage("micropip");
        const micropip = pyodide.pyimport("micropip");
        await micropip.install(["https://pyodide.duckdb.org/duckdb-0.10.2.dev479-cp311-cp311-emscripten_3_1_46_wasm32.whl"]);
        await pyodide.runPython(`
import duckdb

data = """\\
a,b,c
1,2,3
4,5,6
7,8,9"""

with open('data.csv', mode="w") as f:
    f.write(data)

print(duckdb.sql("SELECT COUNT(*) FROM 'data.csv'"))
      `);
      }
      main();
    </script>
  </body>
</html>
```

## Caveats

Only Pythons 3.10 and 3.11 are supported right now, with 3.12 support on the way.

Wheels are tied to a specific version of Pyodide. For example when using
Pyodide version 0.25.1, you must use the cp311-based wheel.

Some functionality is known to not work, such as extension downloading.

The default extensions (as well as the `httpfs` extension) that ship with
duckdb Python don't need to be `INSTALL`ed, but others, like `spatial`, won't
work because they cannot be downloaded in the pyodide runtime.
