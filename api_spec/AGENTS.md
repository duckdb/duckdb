# Changing the C API

Read this before changing anything under `api_spec/`, `src/main/capi/` or `tools/cpp/`, or a
generated C API header. `README.md` in this directory explains the specification and how V1 is
regenerated. `VERSIONING.md` explains the lifecycle of a function and the extension ABI.

## Two APIs, both generated from YAML

| | V1 | V2 |
|---|---|---|
| Specification | `api_spec/v1/` | `api_spec/v2/` |
| Generated files | `src/include/duckdb.h`, `src/include/duckdb_extension.h`, `src/include/duckdb/main/capi/extension_api.hpp` | `src/include/duckdb_v2.h`, `src/include/duckdb_extension_v2.h`, `src/include/duckdb/main/capi_v2/extension_api_v2.hpp`, `src/main/capi/v2/capi_v2_stubs.cpp` |
| Regenerate with | `make generate-files` or `./scripts/capi_v1_regen.sh` | `./scripts/capi_v2_regen.sh` (needs `uv`) |
| Implementation | `src/main/capi/v1/` | `src/main/capi/v2/` |
| Tests | `test/api/capi/v1/`, most of them under the tag `[capi]` | `test/api/capi/v2/`, tag `[capi_v2]` |

Never edit a generated file by hand. `make generate-files` does not regenerate V2, and nothing in
CI does either: your own run of its script is the only check that the committed V2 files match
the specification.

The C++ API in `tools/cpp/` (`duckdb_cpp.hpp`) is written on top of V2. Its tests live in
`tools/cpp/tests/`, are compiled into `unittest`, and carry the tag `[cpp_api]`.

## A change moves the whole chain

A change to the C API starts in the specification. Edit the YAML, regenerate, and bring every link
in line in the same change:

1. the YAML under `api_spec/`, with a `lifecycle` entry for anything new;
2. the generated files, whose diff is part of the change;
3. the implementation under `src/main/capi/v1/` or `src/main/capi/v2/`;
4. for V2, the C++ API in `tools/cpp/` when it exposes the function;
5. the tests: C tests for the function, and C++ tests when `tools/cpp/` changed. A new test file
   is added by name to `test/api/capi/v2/CMakeLists.txt` or `test/api/cpp/CMakeLists.txt`.

Before you finish, regenerate once more: it must leave no difference. CI fails when the committed
V1 headers do not match the specification.

## Rules for V2

- **C types only.** A V2 function takes and returns the primitives listed in
  `api_spec/v2/metadata.yaml`, opaque handles, enums, plain structs and callbacks. Conveniences
  such as overloads, owning classes, exceptions and containers belong in the C++ API in
  `tools/cpp/`, never in the C header.
- **No exception crosses the boundary.** An implementation in `src/main/capi/v2/` checks its
  arguments and runs the engine call inside `WithErrorHandler`, which turns anything thrown into
  an error code and an error info handle. The argument check is `DUCKDB_CHECK_ARG` for a pointer
  or a handle, and `DUCKDB_CHECK_STR_ARG` for a string given as pointer and length, where a null
  pointer with length 0 is the empty string.
- **Every function returns `DUCKDB_V2_ERROR`, and one that can fail takes
  `duckdb_v2_error_info_handle *err` as its last parameter.** Destructors and the accessors of
  the error info take no `err`. `DUCKDB_V2_ERROR_NONE` is the only success. After any other
  code a caller reads `err`, and only those out parameters that the function's documentation
  says are set on that failure: the functions that write text into a caller's buffer report the
  required length this way.
- **Out parameters follow one pattern.** A handle out parameter is set to null before the work.
  Any other out parameter follows the function's documentation. `err` and a caller's buffer are
  never cleared: a caller may pass an error info handle for reuse, and a buffer is documented as
  left untouched on failure.
- **An exposed enum value is pinned to the engine's.** Where the API exposes values of an engine
  enum, each exposed value has a `static_assert` beside the conversion, as in
  `src/main/capi/v2/capi_v2_expression.cpp`. A new engine value enters V2 only when the
  specification is meant to expose it. The log level, log scope and logical type id enums are
  converted with a plain cast and have no such assertion in `src/main/capi/v2/` yet.

## V1 keeps working

V1 is the released API, and the layout of its extension struct is a promise to extensions that are
already built. `VERSIONING.md` says what may change in which lifecycle state: follow it for
anything that renames, deprecates or removes a function.

- After a change under `src/main/capi/` or `api_spec/`, run the tests of both APIs and of the C++
  API with the `unittest` of the build that contains your change, for example
  `build/reldebug/test/unittest "[capi]"`, `"[capi_v2]"` and `"[cpp_api]"`.
- `[capi]` does not select every V1 test: some test cases in `test/api/capi/v1/` carry another tag
  or none. Also run the test files that cover what you changed by file name. The runner's `-#`
  option adds a tag per source file: `unittest -# "[#test_capi_appender]"`.
- After a change to `api_spec/v1/`, also run `python3 scripts/check_extension_abi.py`. It compares
  the generated extension struct with the releases in its `RELEASES` list. A pass prints one
  `pin ...: prefix-compatible` line per release. A run that prints `skipping` checked nothing:
  a release tag is missing from the clone.
