
If the build can't find `$JAVA_HOME` You must set $JAVA_HOME yourself.

If you are on a Mac and install `openjdk` via `brew` then additionally, it's required to set:
```
export JAVA_AWT_LIBRARY=$JAVA_HOME/libexec/openjdk.jdk/Contents/Home/lib
```
because the FindJNI.cmake macro doesn't look there for the `awt` library.

