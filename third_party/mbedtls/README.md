In order to bump mbedtls, we can run the helper script to download a new version and copy over existing files:

```
python inline_mbedtls.py
```

We then need to make it work in C++ and as part of our amalgamation. We have a diff available that fixes these issues for the current version. It is possible / likely this does not map 1-1 to the new version, so rejects might need to be handled.

```
git apply inline_mbedtls.diff --reject
```

It is recommended to test regular compilation, as well as the amalgamation path:

```
python scripts/amalgamation.py
clang++ -std=c++11 -Isrc/amalgamation src/amalgamation/duckdb.cpp
```
