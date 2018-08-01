


.PHONY: main clean all format test

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

doxygen:
	doxygen Doxyfile

docs: doxygen

format:
	python format.py