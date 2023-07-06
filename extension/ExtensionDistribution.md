# Extension Distribution conventions

This is a collection of good practices around extensions.

These are only partially enforced, but the goal of writing this down is that within these boundaries is what we consider properly supported, and we will try hard(er) not to break these conventions.
You might get away, and it's OK, with relaxed constraints, but we might at some point change implementation details assuming extensions developer follow these guidelines.

Get in contact if you have any feedback on these via opening an Issue or Discussion.

## Extension served via URLs

When performing `INSTALL name`, iff the file is not present locally or bundled via static linking, it will be downloaded from the network.
Extensions are served from URLs like: https://extensions.duckdb.org/v0.8.1/windows_arm64/name.duckdb_extension.gz

Unpacking this:
```
https://extensions.duckdb.org/              The extension registry URL (can have subfolders)
v0.8.1/                                     The version identifier
osx_arm64/                                  The platform this extension is compatible with
name                                        The default name of a given extension
.duckdb_extension                           Fixed file identifier
.gz                                         Extension served as gzipped file
```

Extension registry defaults to duckdb's one, but can be set via `SET custom_extension_repository="http://some.url.org/subfolder1/subfolder2/"`.

## Local extensions

`INSTALL name` will download, un-zip and save the extension locally at a path like `~/.duckdb/extensions/v0.8.1/osx_arm64/name.duckdb_extension`.

```
~/.duckdb/                                  The local configuration folder
extensions/                                 Fixed subfolder dedicated to extensions
v0.8.1/                                     The version identifier
osx_arm64/                                  The platform this extension is compatible with
name                                        The default name of a given extension
.duckdb_extension                           Fixed file identifier
```

Configuration folder defaults to be placed in home directory, but can be overwritten via `SET home_directory='/some/folder'`.

## WebAssembly loadable extensions (in flux)

```
https://extensions.duckdb.org/wasm/         The extension registry URL (can have subfolders)
v1.27.0/                                    The DuckDB-Wasm version identifier
webassembly_eh/                             The platform/feature-set this extension is compatible with
name                                        The default name of a given extension
.duckdb_extension                           Fixed file identifier
```

DuckDB-Wasm extensions are are downloaded by the browsers WITHOUT appening .gz, since decompression status is agreed using headers such as `Accept-Encoding: *` and `Content-Encoding: br`.

## Conventions

### Version identifier

Either the git tag (`v0.8.0`, `v0.8.1`, ...) or the git hash of a given duckdb version.
It's chosen at compile time and baked in DuckDB. A given duckdb executable or library is tied to a single version identifier.

### Plaform

Fixed at compile time via feature detection and backed in DuckDB.

### Extension name

Supported names shoud start with a letter, use only ascii lower case letters, numbers, dots ('.') or underscores ('_'), and have reasonable lenght (< 64 characters).
