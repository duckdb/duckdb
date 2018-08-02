


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

sqlite: main
	./build/test/sqlite/sqllogictest --engine DuckDB --halt --verify test/sqlite/select1.test
	./build/test/sqlite/sqllogictest --engine DuckDB --halt --verify test/sqlite/select2.test
	./build/test/sqlite/sqllogictest --engine DuckDB --halt --verify test/sqlite/select3.test
	./build/test/sqlite/sqllogictest --engine DuckDB --halt --verify test/sqlite/select4.test
	./build/test/sqlite/sqllogictest --engine DuckDB --halt --verify test/sqlite/select5.test
