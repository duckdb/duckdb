#!/bin/bash
for FILE in `grep -lR "_inc.h" .` 
do
	echo XX$FILE
	ORIG=$FILE.orig
	cp $FILE $ORIG
done


# find all the files having a _inc.h in them

# remove all the includes that don't have a _inc.h

# run clang++ -P -x c++ -E backward_references.cpp.org > backward_references.cpp

# add includes back

