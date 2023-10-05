
It's required to have a JDK installed to build.
Make sure the `JAVA_HOME` environment variable is set.

If you are on a Mac and install `openjdk` via `brew` then additionally, it's required to set:
```
export JAVA_AWT_LIBRARY=$JAVA_HOME/libexec/openjdk.jdk/Contents/Home/lib
```
because the [`FindJNI.cmake`](https://cmake.org/cmake/help/latest/module/FindJNI.html) module doesn't look there for the `awt` library.

