


.PHONY: main clean all format test docs doxygen

all: main

clean:
	rm -rf build

main:
	mkdir -p build
	mkdir -p build/debug
	cd build/debug && cmake -DCMAKE_BUILD_TYPE=Debug ../.. && make

opt:
	mkdir -p build
	mkdir -p build/release
	cd build/release && cmake -DCMAKE_BUILD_TYPE=Release ../.. && make

test: main
	build/debug/test/test

unit: main
	build/debug/test/unittest

docs:
	mkdir -p build
	mkdir -p build/docs
	doxygen Doxyfile

doxygen: docs
	open build/docs/html/index.html
	
format:
	python format.py

sqlite: main
	./build/debug/test/sqlite/sqllogictest --engine DuckDB --halt --verify test/sqlite/select1.test
	./build/debug/test/sqlite/sqllogictest --engine DuckDB --halt --verify test/sqlite/select2.test
	./build/debug/test/sqlite/sqllogictest --engine DuckDB --halt --verify test/sqlite/select3.test
	./build/debug/test/sqlite/sqllogictest --engine DuckDB --halt --verify test/sqlite/select4.test
	./build/debug/test/sqlite/sqllogictest --engine DuckDB --halt --verify test/sqlite/select5.test
