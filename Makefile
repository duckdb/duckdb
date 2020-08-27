.PHONY: all opt unit clean debug release release_expanded test unittest allunit docs doxygen format sqlite imdb

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
ifeq (${DISABLE_SANITIZER}, 1)
	DISABLE_SANITIZER_FLAG=-DENABLE_SANITIZER=FALSE
endif
EXTENSIONS=-DBUILD_PARQUET_EXTENSION=TRUE
ifeq (${BUILD_BENCHMARK}, 1)
	EXTENSIONS:=${EXTENSIONS} -DBUILD_BENCHMARKS=1
endif
ifeq (${BUILD_ICU}, 1)
	EXTENSIONS:=${EXTENSIONS} -DBUILD_ICU_EXTENSION=1
endif
ifeq (${BUILD_TPCH}, 1)
	EXTENSIONS:=${EXTENSIONS} -DBUILD_TPCH_EXTENSION=1
endif
ifeq (${BUILD_SQLSMITH}, 1)
	EXTENSIONS:=${EXTENSIONS} -DBUILD_SQLSMITH=1
endif
ifeq (${BUILD_TPCE}, 1)
	EXTENSIONS:=${EXTENSIONS} -DBUILD_TPCE=1
endif
ifeq (${BUILD_JDBC}, 1)
	EXTENSIONS:=${EXTENSIONS} -DJDBC_DRIVER=1
endif
ifeq (${BUILD_PYTHON}, 1)
	EXTENSIONS:=${EXTENSIONS} -DBUILD_PYTHON=1
endif
ifeq (${BUILD_R}, 1)
	EXTENSIONS:=${EXTENSIONS} -DBUILD_R=1
endif

clean:
	rm -rf build

debug:
	mkdir -p build/debug && \
	cd build/debug && \
	cmake $(GENERATOR) $(FORCE_COLOR) ${WARNINGS_AS_ERRORS} ${DISABLE_UNITY_FLAG} ${DISABLE_SANITIZER_FLAG} ${EXTENSIONS} -DCMAKE_BUILD_TYPE=Debug ../.. && \
	cmake --build .

release_expanded:
	mkdir -p build/release_expanded && \
	cd build/release_expanded && \
	cmake $(GENERATOR) $(FORCE_COLOR) ${WARNINGS_AS_ERRORS} ${DISABLE_UNITY_FLAG} ${DISABLE_SANITIZER_FLAG} ${EXTENSIONS} -DCMAKE_BUILD_TYPE=Release ../.. && \
	cmake --build .

cldebug:
	mkdir -p build/cldebug && \
	cd build/cldebug && \
	cmake $(GENERATOR) $(FORCE_COLOR) ${WARNINGS_AS_ERRORS} ${DISABLE_UNITY_FLAG} ${EXTENSIONS} -DBUILD_PYTHON=1 -DBUILD_R=1 -DENABLE_SANITIZER=0 -DCMAKE_BUILD_TYPE=Debug ../.. && \
	cmake --build .

clreldebug:
	mkdir -p build/clreldebug && \
	cd build/clreldebug && \
	cmake $(GENERATOR) $(FORCE_COLOR) ${WARNINGS_AS_ERRORS} ${DISABLE_UNITY_FLAG} ${EXTENSIONS} -DBUILD_PYTHON=1 -DBUILD_R=1 -DENABLE_SANITIZER=0 -DCMAKE_BUILD_TYPE=RelWithDebInfo ../.. && \
	cmake --build .

unittest: debug
	build/debug/test/unittest
	build/debug/tools/sqlite3_api_wrapper/test_sqlite3_api_wrapper

allunit: release_expanded # uses release build because otherwise allunit takes forever
	build/release_expanded/test/unittest "*"

docs:
	mkdir -p build/docs && \
	doxygen Doxyfile

doxygen: docs
	open build/docs/html/index.html

release:
	mkdir -p build/release && \
	python scripts/amalgamation.py && \
	cd build/release && \
	cmake $(GENERATOR) $(FORCE_COLOR) ${WARNINGS_AS_ERRORS} ${DISABLE_UNITY_FLAG} ${DISABLE_SANITIZER_FLAG} ${EXTENSIONS} -DCMAKE_BUILD_TYPE=Release -DAMALGAMATION_BUILD=1 ../.. && \
	cmake --build .

reldebug:
	mkdir -p build/reldebug && \
	cd build/reldebug && \
	cmake $(GENERATOR) $(FORCE_COLOR) ${WARNINGS_AS_ERRORS} ${DISABLE_UNITY_FLAG} ${DISABLE_SANITIZER_FLAG} ${EXTENSIONS} -DCMAKE_BUILD_TYPE=RelWithDebInfo ../.. && \
	cmake --build .

amaldebug:
	mkdir -p build/amaldebug && \
	python scripts/amalgamation.py && \
	cd build/amaldebug && \
	cmake $(GENERATOR) $(FORCE_COLOR) ${EXTENSIONS} -DAMALGAMATION_BUILD=1 -DCMAKE_BUILD_TYPE=Debug ../.. && \
	cmake --build .


test_compile: # test compilation of individual cpp files
	python scripts/amalgamation.py --compile

format:
	python3 scripts/format.py

third_party/sqllogictest:
	git clone --depth=1 https://github.com/cwida/sqllogictest.git third_party/sqllogictest

third_party/imdb/data:
	wget -i "http://download.duckdb.org/imdb/list.txt" -P third_party/imdb/data

sqlite: release_expanded | third_party/sqllogictest
	git --git-dir third_party/sqllogictest/.git pull
	./build/release_expanded/test/unittest "[sqlitelogic]"

sqlsmith: debug
	./build/debug/third_party/sqlsmith/sqlsmith --duckdb=:memory:
