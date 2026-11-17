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
| Tests | `test/api/capi/v1/`, tag `[capi]` | `test/api/capi/v2/`, tag `[capi_v2]` |

Never edit a generated file by hand. `make generate-files` does not regenerate V2: run its script
yourself.

The C++ API in `tools/cpp/` (`duckdb_cpp.hpp`) is written on top of V2. Its tests live in
`tools/cpp/tests/`, are compiled into `unittest`, and carry the tag `[cpp_api]`.

## A change moves the whole chain

A change to the C API starts in the specification. Edit the YAML, regenerate, and bring every link
in line in the same change:

1. the YAML under `api_spec/`, with a `lifecycle` entry for anything new;
2. the generated files, whose diff is part of the change;
3. the implementation under `src/main/capi/v1/` or `src/main/capi/v2/`;
4. for V2, the C++ API in `tools/cpp/` when it exposes the function;
5. the tests: C tests for the function, and C++ tests when `tools/cpp/` changed.

Before you finish, regenerate once more: it must leave no difference. CI fails when the committed
V1 headers do not match the specification.

## Rules for V2

- **C types only.** A V2 function takes and returns the primitives listed in
  `api_spec/v2/metadata.yaml`, opaque handles, enums, plain structs and callbacks. Conveniences
  such as overloads, owning classes, exceptions and containers belong in the C++ API in
  `tools/cpp/`, never in the C header.
- **No exception crosses the boundary.** An implementation in `src/main/capi/v2/` checks its
  arguments with `DUCKDB_CHECK_ARG` and runs the engine call inside `WithErrorHandler`, which turns
  anything thrown into an error code and an error info handle.
- **A function that can fail returns `DUCKDB_V2_ERROR` and takes
  `duckdb_v2_error_info_handle *err` as its last parameter.** `DUCKDB_V2_ERROR_NONE` is the only
  success. The return code decides: after any other code, a caller reads no out parameter and
  assumes nothing succeeded. That holds for the tests and for the C++ API as well. Set every out
  parameter to null or zero before doing the work, as the existing functions do.
- **Enum values match the engine's.** Where a V2 enum mirrors an engine enum, the implementation
  pins each value with a `static_assert` (see `src/main/capi/v2/capi_v2_expression.cpp`). A new
  value in the engine enum gets its YAML entry, its V2 value and its assertion in the same change.

## V1 keeps working

V1 is the released API, and the layout of its extension struct is a promise to extensions that are
already built. `VERSIONING.md` says what may change in which lifecycle state: follow it for
anything that renames, deprecates or removes a function.

- After a change under `src/main/capi/` or `api_spec/`, run the tests of both APIs and of the C++
  API: `build/release/test/unittest "[capi]"`, `"[capi_v2]"` and `"[cpp_api]"`.
- After a change to `api_spec/v1/`, also run `python3 scripts/check_extension_abi.py`. It compares
  the generated extension struct with what each release shipped, and needs the release tags in
  the clone.
