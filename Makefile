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

coverage: 
	rm -r build/coverage
	mkdir -p build/coverage
	(cd build/coverage && cmake -E env CXXFLAGS="--coverage" cmake -DCMAKE_BUILD_TYPE=Debug ../.. && make -j)
	build/coverage/test/unittest
	lcov -c -d build/coverage -o build/coverage/cov.info 
	lcov --remove build/coverage/cov.info  "/usr/include/*" "*/test/*" "*/third_party/*" -o build/coverage/cov-filtered.info 
	genhtml build/coverage/cov-filtered.info -o build/coverage/html --ignore-errors source -t "DuckDB Code Coverage Report `git show -s --format=%h`"
	
format:
	python format.py

third_party/sqllogictest:
	git clone --depth=1 https://github.com/cwida/sqllogictest.git third_party/sqllogictest

sqlite: main | third_party/sqllogictest
	git --git-dir third_party/sqllogictest/.git pull
	./build/debug/test/unittest "[sqlitelogic]"

sqlsmith: main
	./build/debug/third_party/sqlsmith/sqlsmith --duckdb=:memory:
