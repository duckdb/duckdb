To update cpp-httplib, change the version in the helper script and run it to download and transform the upstream header:

```
python transform-httplib.py
```

Then apply the ordered semantic patch series. A new upstream version may require patches to be refreshed.

```
git apply patches/*.patch
```

It is recommended to test regular compilation.