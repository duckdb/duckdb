.PHONY: all opt unit clean debug release test unittest allunit docs doxygen format sqlite

all: release
opt: release
unit: unittest

clean:
	rm -rf build

debug:
	mkdir -p build/debug && \
	cd build/debug && \
	cmake -DCMAKE_BUILD_TYPE=Debug ../.. && \
	cmake --build .

release:
	mkdir -p build/release && \
	cd build/release && \
	cmake -DCMAKE_BUILD_TYPE=RelWithDebInfo ../.. && \
	cmake --build .

unittest: debug
	build/debug/test/unittest

allunit: release # uses release build because otherwise allunit takes forever
	build/release/test/unittest "*"

docs:
	mkdir -p build/docs && \
	doxygen Doxyfile

doxygen: docs
	open build/docs/html/index.html

format:
	python format.py

third_party/sqllogictest:
	git clone --depth=1 https://github.com/cwida/sqllogictest.git third_party/sqllogictest

sqlite: release | third_party/sqllogictest
	git --git-dir third_party/sqllogictest/.git pull
	./build/release/test/unittest "[sqlitelogic]"

sqlsmith: debug
	./build/debug/third_party/sqlsmith/sqlsmith --duckdb=:memory:
