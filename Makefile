


.PHONY: main clean all format test docs doxygen

all: main

clean:
	rm -rf build

main:
	mkdir -p build
	cd build && cmake -DCMAKE_BUILD_TYPE=Debug .. && make

test: main
	build/test/test

micro: main
	build/test/microbenchmark

docs:
	doxygen Doxyfile

doxygen: docs
	open docs/html/index.html
	
format:
	python format.py