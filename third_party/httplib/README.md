In order to bump mbedtls, we can run the helper script to download a new version and copy over existing files:

```
python transform-httplib.py
```

We then need to make it work in C++. We have a diff available that fixes these issues for the current version. It is possible / likely this does not map 1-1 to the new version, so rejects might need to be handled.

```
git apply httplib.patch --reject
```

It is recommended to test regular compilation.