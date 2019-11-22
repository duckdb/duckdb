

.PHONY: duckdb clean main

all: duckdb main

clean:
	rm -rf build

duckdb:
	make  -C ../../

main:
	mkdir -p build
	cd build && cmake .. && make
	build/programmating_querying_example


