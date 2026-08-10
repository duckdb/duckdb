# C API versioning

Both `duckdb.h` and `duckdb_extension.h` are generated from a single lifecycle recorded in the spec, so the two can
never disagree about when a symbol appeared or when it was stabilized.
Targeting a version gives you the API as of that version, but there are some slight differences between direct consumers
of `duckdb.h` and extensions using `duckdb_extension.h` as extensions also require a fixed ABI layout.

## The lifecycle

Every entry in the API spec carries a stack of dated transitions, newest first:

```yaml
lifecycle:
  - [ "stable", "v1.5.6", "2026-07-30" ]
  - [ "unstable", "v1.4.0", "2025-09-12" ]
```

There are currently four states with the following semantics:

| State        | Meaning                                                       |
|--------------|---------------------------------------------------------------|
| `unstable`   | present but not promised; may change or vanish without notice |
| `stable`     | promised; signature and ABI slot are frozen from here on      |
| `deprecated` | still promised, but scheduled to go; callers should migrate   |
| `removed`    | gone from the library                                         |

Every function __must__ declare a lifecycle. A function's slot in the extension v-table is defined by the version it was
stabilized in (or at the end if unstable), and an undated function would therefore not have a defined position.

## Consumers of `duckdb.h`

E.g. client libraries or applications that link `libduckdb` directly.
When linking DuckDB, declarations do not have an inherent order they need to preserve, and can therefore be version
gated independently.
To control what symbols are visible to you, you can define the following macros:

- `DUCKDB_API_VERSION_MAJOR` / `_MINOR` / `_PATCH`

This is the version of the API that you want to target.
Defaults to the newest version the header describes. Define all three or none.
A declaration appears only if it had been stabilized by that version. This allows you to "get me the API as of
version X".

- `DUCKDB_API_ALLOW_DEPRECATED`

Defaults to `1`. Set to `0` and anything deprecated *as of your target* disappears, so the compiler finds your remaining
uses.
Deprecation is relative to the target, a symbol deprecated in v1.5.6 is still visible to a build targeting v1.5.4.

- `DUCKDB_API_ALLOW_UNSTABLE`

Defaults to `0`. Set to `1` to show symbols that have not been stabilized yet, accepting that they may change.
This requires targeting the newest version, see below.

For backwards compatability, the older `DUCKDB_API_NO_DEPRECATED` and `DUCKDB_EXTENSION_API_VERSION_UNSTABLE` macros
still work, and just set the new `DUCKDB_API_ALLOW_DEPRECATED`/`DUCKDB_API_ALLOW_UNSTABLE` macros accordingly.

### Unstable is a version of its own

A symbol is emitted from the version it was **stabilized** in, not the version it was introduced in.
Until then, it is not part of any released version: its signature may still change.
So asking for an older target *and* the unstable surface would hand you today's signatures under names that version never promised, which is the one case where "the API as of version X" would not be true.
The two are therefore mutually exclusive:

```c
#if DUCKDB_API_ALLOW_UNSTABLE && !DUCKDB_API_VERSION_AT_LEAST(1, 5, 6)
#error "the unstable surface requires targeting the newest API version"
#endif
```

Which leaves one rule for *both kinds of consumer*:
- **pick a version** and get the surface stabilized as of it, with signatures frozen and deprecation relative to it, or,
- **pick unstable** and get everything at the newest version, including unstable symbols, with no promises.

A gate therefore only ever reads `DUCKDB_API_VERSION_AT_LEAST(<stabilized>)`, or the bare switch for something not yet
stabilized.

## Extensions

An extension (when not statically linked) does not link the engine symbols directly. It receives a struct of function
pointers which every call is threaded through by a set of "indirection" macros.
On extension load, this struct is copied into a global static:

```c
duckdb_ext_api = *res;   /* copies sizeof(the extension's struct) */
```

This copy is sound if the extension's expected struct is a **prefix** of the one actually passed by DuckDB, so the
struct's layout is part of the ABI and cannot depend on anything that the engine cannot see.

### The V-Table struct is versioned

Functions in the v-table are grouped into contiguous bands by the version in which the function was **stabilized**, not
the version it was introduced.
Each band is emitted under a single gate:

```c
typedef struct {
	/* band v1.2.0 — 404 slots, always present */
#if DUCKDB_API_VERSION_AT_LEAST(1, 5, 6)
	/* band v1.5.6 — 142 slots */
#endif
#if DUCKDB_API_ALLOW_UNSTABLE
	/* any future unstable functions, always at the end */
#endif
} duckdb_ext_api_v1;
```

Targeting an older version therefore truncates the struct to exactly the slots that DuckDB shipped at that time.
The `scripts/check_extension_abi.py` can be used to verify this against every release tag.

Since the Extension-C-API struct was first introduced in version **v1.2.0**, nothing can predate it, and targeting
anything older is a compile error rather than an empty struct.

### The same two choices, picked by the build system

The rule explained in the "[Unstable is a version of its own](#unstable-is-a-version-of-its-own)" section above applies unchanged here: **pick a version**, or **pick unstable**.
What differs is that an extension does not set the macros itself and the choice is not only about which symbols it can
see: it also decides which DuckDB versions can load the result.

So the cmake build helper you call makes it for you:

**Versioned**: forward-loadable

```cmake
build_loadable_extension_capi(my_ext 1 5 6 ${SOURCES})
```

ABI type `C_STRUCT`. You state the minimum API version you need (which implicitly sets the
`DUCKDB_EXTENSION_API_VERSION` macros).
Any DuckDB at or above it can load your extension. You get the prefix of the v-table up to that target version, and
nothing more.
Pin the *lowest* version that has what you need: nothing was stabilized between v1.2.0 and v1.5.6, so every pin in
between yields the same 404 slots while needlessly excluding the DuckDBs before it.
`DUCKDB_API_ALLOW_UNSTABLE` is unavailable, by the rule above.
`DUCKDB_API_ALLOW_DEPRECATED` still works: disabling it drops the *names* (the indirection macros) but not the slots, so
it hides symbols without disturbing the v-table layout.

**Pinned**: locked to a specific DuckDB version

```cmake
build_loadable_extension_capi_unstable(my_ext ${SOURCES})
```

ABI type `C_STRUCT_UNSTABLE`. You can __not__ set any version macros.
You get the full v-table struct, plus the not-yet-stabilized tail.
DuckDB will only load your extension into if it has the exact same version you built against (enforced by the version
recorded in the extension's metadata footer).
Such a build reports the exact version to `get_api` rather than a semantic version, e.g. a release tag, or a git commit
hash for a dev build, taken from `DUCKDB_EXTENSION_API_VERSION_UNSTABLE`, which the build system sets.

### Why can't I get the unstable symbols _and_ pin a version?

The general reason is above: an unstable signature is not frozen, so no version can promise it. 
But for extensions this is extra important because the unstable tail of the v-table sits last after every version band, so its slot offsets depend on all the stable versions being compiled in first.
An extension that pinned v1.2.0 *and* compiled the tail would place its first tail slot at index 404 while the DuckDB puts it at 546, and every unstable call would go through the wrong pointer, silently.
The guard `duckdb.h` already emits prevents that too.

This costs nothing in practice, because the only build that can reach the tail is locked to a specific DuckDB version
anyway.

It also means a slot is frozen when it is **stabilized**, not when it is introduced (as unstable).
While a function is unstable it is only ever observed by a build locked to a single version, so it can still be
reordered, have its signature changed, or dropped entirely.
The moment it is set to `stable`, its slot is permanent.

This is important to keep in mind when adding C-API functions: an in-development function that turns out to be wrong
must be fixed **before** its `stable` version ships, or it costs a permanent slot forever.

### Renames

We should avoid renaming functions as much as possible, but it has unfortunately happened a couple time already.
A renamed symbol keeps its slot and the signature, and changes only the spelling, so it is ABI-compatible but not
source-compatible.
This can be recorded in the spec:

```yaml
create_bignum:
  renamed_from: { name: create_varint, version: "v1.4.0" }
```

The old spelling is then emitted as an alias gated `BELOW` that version (a `typedef` for a type, a `#define` for a
function) so it is reachable exactly while you target a version that still had it, and gone once you target the version
that renamed it.
The aliases live in `duckdb.h`, and the extension header includes it and its own mapping macro chains through the alias,
so a renamed function still resolves through the vtable rather than the library symbol.

`scripts/check_extension_abi.py` reads these aliases back out of the generated header, so a rename does not read as
though every later slot had shifted, and a future rename needs no change to the checker.

### Removal

A `removed` function has to keep its slot so the offsets after it do not move, but it loses its mapping macro, so the
name no longer compiles.
DuckDB is free to leave the actual function pointer `NULL`.

Note that removal is the one thing that can break an extension that was *already built*: its slot index is baked in, and
a newer DuckDB leaves that slot `NULL`, so the call crashes rather than failing to compile.
Nothing in the versioning scheme can prevent that, since the extension was compiled before the removal existed.
Prefer deprecation, which costs nothing at runtime: the slot stays filled, old binaries keep working, and new builds get
a compile error instead.

## Version macro forwarding

`duckdb_extension.h` resolves `DUCKDB_EXTENSION_API_VERSION_*` and forwards it to `DUCKDB_API_VERSION_*` before
including `duckdb.h`.
Without that, `duckdb.h` would declare the newest surface while the mapping macros followed the extension's older
target, and a name whose macro is absent would quietly resolve to the real `duckdb.h` declaration, making a loadable
extension reference an engine symbol directly.
Forwarding keeps the two in lockstep, so such a name is a compile error instead. Similarly, setting both explicitly to
different values is rejected.
