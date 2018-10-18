


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

unittest: main
	build/debug/test/unittest

unit: unittest

allunit:main
	build/debug/test/unittest "*"

docs:
	mkdir -p build
	mkdir -p build/docs
	doxygen Doxyfile

doxygen: docs
	open build/docs/html/index.html
	
format:
	python format.py

third_party/sqllogictest:
	git clone --depth=1 https://github.com/cwida/sqllogictest.git third_party/sqllogictest

sqlite: main | third_party/sqllogictest
	git --git-dir third_party/sqllogictest/.git pull
	./build/debug/test/unittest "[sqlitelogic]"

sqlsmith: main
	./build/debug/third_party/sqlsmith/sqlsmith --duckdb=:memory:
