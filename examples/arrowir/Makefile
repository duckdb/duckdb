

.PHONY: duckdb clean main

all: duckdb main

clean:
	rm -rf build
	cd ../.. && make clean

duckdb:
	cd ../.. && BUILD_TPCH=1 DISABLE_SANITIZER=1 make

main:
	mkdir -p build
	cd build && cmake -DCMAKE_BUILD_TYPE=Debug .. && cmake --build .
	build/example


