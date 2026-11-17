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

1. the YAML under `api_spec/`, with a `lifecycle` entry for anything new (see below);
2. the generated files, whose diff is part of the change;
3. the implementation under `src/main/capi/v1/` or `src/main/capi/v2/`;
4. for V2, the C++ API in `tools/cpp/` when it exposes the function;
5. the tests: C tests for the function, and C++ tests when `tools/cpp/` changed. A new test file
   is added by name to `test/api/capi/v1/CMakeLists.txt`, `test/api/capi/v2/CMakeLists.txt` or
   `test/api/cpp/CMakeLists.txt`, whichever builds the tests it joins.

Before you finish, regenerate once more: it must leave no difference. CI fails when the committed
V1 headers do not match the specification. Each regeneration script ends by formatting what it
generated. When only that last step fails, the generation itself has completed: check that the
generated files changed as you expect, and format them before you commit. For V2 that takes two
passes of `scripts/format.py`, as the script runs them: one pass does not settle the generated
struct members.

## Lifecycle of an entry

A new entry is `unstable`. A pull request that changes the lifecycle of an existing entry, or
adds a V2 entry in any other state than `unstable`, says so in its description and argues why.

## Rules for V2

- **Add to C only what cannot be composed.** Before adding a C function, ask whether a client
  could compose it from the existing functions without knowledge of the engine. If it could, the
  function belongs in the C++ API in `tools/cpp/`, or nowhere. An exception, for performance or to
  cover every kind of type with one function, is argued in the function's description in the
  specification.
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
  required length this way. A successful call leaves the slot behind `err` untouched, so an
  error info from an earlier failure can still be there: read success from the returned code,
  never from the slot. A callback that receives an `err` slot fills it only to signal a failure,
  and never destroys it.
- **Out parameters follow one pattern.** A handle out parameter is set to null before the work.
  Any other out parameter follows the function's documentation. `err` and a caller's buffer are
  never cleared: a caller may pass an error info handle for reuse, and a buffer is documented as
  left untouched on failure.
- **An enum converted by a cast is pinned to the engine's.** Where a V2 enum is converted to or
  from an engine enum by a cast, each exposed value has a `static_assert` beside the cast, as in
  `src/main/capi/v2/capi_v2_expression.cpp`. A conversion through a `switch` needs none, and its
  numbers may differ from the engine's. A new engine value enters V2 only when the specification
  is meant to expose it. Three casts have no assertion yet: the log level and the log scope in
  `capi_v2_log_sink.cpp`, and the logical type id in `capi_v2_logical_type.cpp`.

## Tests for V2

A V2 test builds its fixtures with the V2 API wherever V2 can express them, also when that takes
more code. V1 calls in a V2 test are for checking that the two APIs work together, or for what V2
cannot express yet. Never change the API to make a test easier to write.

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
  the generated extension struct with the releases in its `RELEASES` list. A pass exits with
  status 0 and ends with the line "all pinned builds are valid prefixes of every engine that
  accepts them". A run that prints `skipping` checked nothing: a release tag is missing from the
  clone.
