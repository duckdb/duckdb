# Extension patches
Patches in this directory are used to smoothen the process of introducing changes to DuckDB that break compatibility with an
out-of-tree extension. Extensions installed from git urls can automatically apply patches found in this directory. The APPLY_PATCHES flag 
should be used to explicitly enable this feature. For example,
lets say our extension config looks like this:

```shell
duckdb_extension_load(spatial
    DONT_LINK
    GIT_URL https://github.com/duckdb/duckdb_spatial
    GIT_TAG f577b9441793f9170403e489f5d3587e023a945f
    APPLY_PATCHES
)
```
In this example, upon downloading the spatial extension, all patches in the `.github/patches/extensions/spatial/*.patch`
will be automatically applied.

Note that the reason for having the APPLY_PATCHES flag explicitly enabled is to make it easier for developers reading
the extension config to detect a patch is present. For this reason, the patching mechanism will actually fail if `APPLY_PATCHES`
is set with no patches in `.github/patches/extensions/<ext>/*.patch`.

# Workflow
Imagine a change to DuckDB is introduced that breaks compatibility with extension X. The
workflow for this is as follows:

### PR #1: breaking change to DuckDB
1. Commit breaking change to DuckDB
2. Git clone extension.
3. In DuckDB repo, go to .github/config/extensions/your_extension.cmake
4. If APPLY_PATCHES is not set in duckdb_extension_load, set it.
5. In cloned extension, checkout the GIT_TAG from the your_extension.cmake file.
6. In cloned extension, apply existing patches found in .github/patches/extensions
    ```bash 
    git apply [path to duckdb]/.github/patches/extensions/[your_extension]/fix.patch
    ```
7. Make necessary changes to extension.
8. Generate diff in cloned extension, and forward it to the original fix.patch location (or create the directory if it does not exist):
    ```bash
    git diff > [path to duckdb]/.github/patches/extensions/[your_extension]/fix.patch

9. Commit patch in `.github/patches/extensions/[your_extension]/*.patch` using a descriptive name

If the change is very minor, and you are confident everything will build, you can skip the next two steps.  

### PR #2: patch to extension X
- Apply (all) the patch(es) in `.github/patches/extensions/x/*.patch` to extension X.

### PR #3: update extension X in DuckDB
- Remove patches in `.github/patches/extensions/x/*.patch`
- Remove `APPLY_PATCHES` flag from config
- Update hash of extension in config