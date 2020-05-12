.PHONY: all opt unit clean debug release test unittest allunit docs doxygen format sqlite imdb

all: release
opt: release
unit: unittest
imdb: third_party/imdb/data

GENERATOR=
FORCE_COLOR=
WARNINGS_AS_ERRORS=
DISABLE_UNITY_FLAG=
ifeq ($(GEN),ninja)
	GENERATOR=-G "Ninja"
	FORCE_COLOR=-DFORCE_COLORED_OUTPUT=1
endif
ifeq (${TREAT_WARNINGS_AS_ERRORS}, 1)
	WARNINGS_AS_ERRORS=-DTREAT_WARNINGS_AS_ERRORS=1
endif
ifeq (${DISABLE_UNITY}, 1)
	DISABLE_UNITY_FLAG=-DDISABLE_UNITY=1
endif

clean:
	rm -rf build

debug:
	mkdir -p build/debug && \
	cd build/debug && \
	cmake $(GENERATOR) $(FORCE_COLOR) ${WARNINGS_AS_ERRORS} ${DISABLE_UNITY_FLAG} -DCMAKE_BUILD_TYPE=Debug ../.. && \
	cmake --build .

release:
	mkdir -p build/release && \
	cd build/release && \
	cmake $(GENERATOR) $(FORCE_COLOR) ${WARNINGS_AS_ERRORS} ${DISABLE_UNITY_FLAG} -DCMAKE_BUILD_TYPE=Release ../.. && \
	cmake --build .

unittest: debug
	build/debug/test/unittest
	build/debug/tools/sqlite3_api_wrapper/test_sqlite3_api_wrapper

allunit: release # uses release build because otherwise allunit takes forever
	build/release/test/unittest "*"

docs:
	mkdir -p build/docs && \
	doxygen Doxyfile

doxygen: docs
	open build/docs/html/index.html

amalgamation:
	mkdir -p build/amalgamation && \
	python scripts/amalgamation.py && \
	cd build/amalgamation && \
	cmake $(GENERATOR) $(FORCE_COLOR) -DAMALGAMATION_BUILD=1 -DCMAKE_BUILD_TYPE=Release ../.. && \
	cmake --build .

amaldebug:
	mkdir -p build/amaldebug && \
	python scripts/amalgamation.py && \
	cd build/amaldebug && \
	cmake $(GENERATOR) $(FORCE_COLOR) -DAMALGAMATION_BUILD=1 -DCMAKE_BUILD_TYPE=Debug ../.. && \
	cmake --build .

jdbc:
	mkdir -p build/release && \
	cd build/release && \
	cmake $(GENERATOR) $(FORCE_COLOR) ${WARNINGS_AS_ERRORS} ${DISABLE_UNITY_FLAG} -DJDBC_DRIVER=1 -DCMAKE_BUILD_TYPE=Release ../.. && \
	cmake --build .


jdbcdebug:
	mkdir -p build/jdbcdebug && \
	cd build/jdbcdebug && \
	cmake $(GENERATOR) $(FORCE_COLOR) -DJDBC_DRIVER=1 -DENABLE_SANITIZER=FALSE -DCMAKE_BUILD_TYPE=Debug ../.. && \
	cmake --build .

test_compile: # test compilation of individual cpp files
	python scripts/amalgamation.py --compile

format:
	python3 scripts/format.py

third_party/sqllogictest:
	git clone --depth=1 https://github.com/cwida/sqllogictest.git third_party/sqllogictest

third_party/imdb/data:
	wget -i "http://download.duckdb.org/imdb/list.txt" -P third_party/imdb/data

sqlite: release | third_party/sqllogictest
	git --git-dir third_party/sqllogictest/.git pull
	./build/release/test/unittest "[sqlitelogic]"

sqlsmith: debug
	./build/debug/third_party/sqlsmith/sqlsmith --duckdb=:memory:
