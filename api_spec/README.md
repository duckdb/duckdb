# C API specification

This directory holds the declarative specification of DuckDB's C API. The
committed headers are generated from it:

- `src/include/duckdb.h`, the C API header
- `src/include/duckdb_extension.h`, the header for C extensions
- `src/include/duckdb/main/capi/extension_api.hpp`, the engine-side function
  pointer struct

Do not edit those three files by hand. Edit the YAML here and regenerate.

## Layout

- `v1/`: the spec. One directory per module (query, value, appender, ...),
  plus `metadata.yaml` (prefix, primitives, lifecycle states, known DuckDB
  versions) and `common/` (shared enums, structs, callbacks).
- `v1/extension/duckdb_extension.h.in`: the template for the extension
  header. Its member order is the extension ABI. The generator verifies every
  member against the spec and appends new functions at the marker comments.
  `extension_api.hpp` is derived from it, so both stay in lockstep.
- `pyproject.toml`: the generation environment. The `generate` dependency
  group pins capigen (the generator, https://github.com/duckdb/capigen) and
  the formatters. `uv.lock` records the exact resolution.

## Regenerating

Install the environment once, then regenerate:

    make generate-files-deps
    make generate-files

Or directly: `./scripts/capi_v1_regen.sh`. With uv instead of pip:
`uv run --project api_spec --group generate ./scripts/capi_v1_regen.sh`.
Requires Python 3.12 or newer. CI fails when the committed headers do not
match the spec, so regenerate before pushing.

## Adding a function

Declare it in the module YAML with a `lifecycle` entry naming the DuckDB
release it ships in. Add that release to `versions` in `metadata.yaml` if it
is not there yet. Regenerate. The function appears in `duckdb.h` and in the
unstable section of the extension struct. The diff in the generated files is
part of the review.

## Versioning

`schema_version` in `metadata.yaml` must be compatible with the installed
capigen (same major, spec minor at most the tool minor). To bump capigen:
update the pin in `pyproject.toml`, run `uv lock --project api_spec`,
regenerate, and commit the header diff together with the pin.

## Editor schema validation

The YAML here is validated against JSON Schemas that ship inside the pinned
capigen package. Point your editor at them for inline errors and completion
while editing. Because the schemas come from the pinned install, your editor
always checks the exact schema version the generator uses. Nothing is
vendored or committed, and the schemas move with the pin.

Resolve the schema directory (machine-specific, do not commit it; re-run
after changing the pin):

    uv run --project api_spec --group generate python -c \
        "import importlib.resources as r; print(r.files('capigen') / 'schema')"

With pip, run the same one-liner with the interpreter that has the
`generate` group installed.

Map two schemas in any editor backed by yaml-language-server (VS Code with
the Red Hat YAML extension, Neovim yamlls, Emacs, Sublime, Helix):

- `<schema-dir>/metadata.schema.json` validates `api_spec/*/metadata.yaml`
- `<schema-dir>/module.schema.json` validates the module files one level
  deeper, `api_spec/*/*/*.yaml`

The globs are workspace-relative and never overlap. In VS Code the mapping
is the `yaml.schemas` setting; keep it in your User settings, or in a
`.vscode/settings.json` listed in `.git/info/exclude`, since the path is
machine-specific. In Neovim the same map goes in the yamlls
`settings.yaml.schemas` table.

Editor validation is a convenience, not the gate. capigen validates every
file against these same schemas during regeneration, and CI fails on the
resulting drift, so an invalid spec cannot land regardless of editor setup.
