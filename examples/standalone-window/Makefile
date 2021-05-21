

.PHONY: duckdb clean main

all: duckdb main

clean:
	rm -rf build

duckdb:
	cd ../.. && make

main:
	mkdir -p build
	cd build && cmake .. -DCMAKE_BUILD_TYPE=Debug && cmake --build .
	build/example


