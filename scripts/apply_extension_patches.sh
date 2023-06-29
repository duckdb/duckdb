#!/bin/bash

FILES="$1/*.patch"

echo $FILES

for f in $FILES;
  do git apply $f;
done