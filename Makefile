.PHONY: all opt unit clean debug release test unittest allunit benchmark docs doxygen format sqlite

all: release
opt: release
unit: unittest

GENERATOR ?=
FORCE_COLOR ?=
WARNINGS_AS_ERRORS ?=
FORCE_WARN_UNUSED_FLAG ?=
DISABLE_UNITY_FLAG ?=
DISABLE_SANITIZER_FLAG ?=
OSX_BUILD_UNIVERSAL_FLAG ?=
FORCE_32_BIT_FLAG ?=

ifeq ($(GEN),ninja)
	GENERATOR=-G "Ninja"
	FORCE_COLOR=-DFORCE_COLORED_OUTPUT=1
endif
ifeq (${TREAT_WARNINGS_AS_ERRORS}, 1)
	WARNINGS_AS_ERRORS=-DTREAT_WARNINGS_AS_ERRORS=1
endif
ifeq (${OSX_BUILD_UNIVERSAL}, 1)
	OSX_BUILD_UNIVERSAL_FLAG=-DOSX_BUILD_UNIVERSAL=1
endif
ifeq (${FORCE_32_BIT}, 1)
	FORCE_32_BIT_FLAG=-DFORCE_32_BIT=1
endif
ifeq (${FORCE_WARN_UNUSED}, 1)
	FORCE_WARN_UNUSED_FLAG=-DFORCE_WARN_UNUSED=1
endif
ifeq (${DISABLE_UNITY}, 1)
	DISABLE_UNITY_FLAG=-DDISABLE_UNITY=1
endif
ifeq (${DISABLE_SANITIZER}, 1)
	DISABLE_SANITIZER_FLAG=-DENABLE_SANITIZER=FALSE -DENABLE_UBSAN=0
endif
ifeq (${DISABLE_UBSAN}, 1)
	DISABLE_SANITIZER_FLAG=-DENABLE_UBSAN=0
endif
ifeq (${DISABLE_VPTR_SANITIZER}, 1)
	DISABLE_SANITIZER_FLAG:=${DISABLE_SANITIZER_FLAG} -DDISABLE_VPTR_SANITIZER=1
endif
ifeq (${FORCE_SANITIZER}, 1)
	DISABLE_SANITIZER_FLAG:=${DISABLE_SANITIZER_FLAG} -DFORCE_SANITIZER=1
endif
ifeq (${THREADSAN}, 1)
	DISABLE_SANITIZER_FLAG:=${DISABLE_SANITIZER_FLAG} -DENABLE_THREAD_SANITIZER=1
endif
ifeq (${STATIC_LIBCPP}, 1)
	STATIC_LIBCPP=-DSTATIC_LIBCPP=TRUE
endif
EXTENSIONS ?=
EXTENSIONS += -DBUILD_PARQUET_EXTENSION=TRUE
ifeq (${DISABLE_PARQUET}, 1)
	EXTENSIONS:=
endif
ifeq (${DISABLE_MAIN_DUCKDB_LIBRARY}, 1)
	EXTENSIONS:=${EXTENSIONS} -DBUILD_MAIN_DUCKDB_LIBRARY=0
endif
ifeq (${EXTENSION_STATIC_BUILD}, 1)
	EXTENSIONS:=${EXTENSIONS} -DEXTENSION_STATIC_BUILD=1
endif
ifeq (${DISABLE_BUILTIN_EXTENSIONS}, 1)
	EXTENSIONS:=${EXTENSIONS} -DDISABLE_BUILTIN_EXTENSIONS=1
endif
ifeq (${BUILD_BENCHMARK}, 1)
	EXTENSIONS:=${EXTENSIONS} -DBUILD_BENCHMARKS=1
endif
ifeq (${BUILD_AUTOCOMPLETE}, 1)
	EXTENSIONS:=${EXTENSIONS} -DBUILD_AUTOCOMPLETE_EXTENSION=1
endif
ifeq (${BUILD_ICU}, 1)
	EXTENSIONS:=${EXTENSIONS} -DBUILD_ICU_EXTENSION=1
endif
ifeq (${BUILD_TPCH}, 1)
	EXTENSIONS:=${EXTENSIONS} -DBUILD_TPCH_EXTENSION=1
endif
ifeq (${BUILD_TPCDS}, 1)
	EXTENSIONS:=${EXTENSIONS} -DBUILD_TPCDS_EXTENSION=1
endif
ifeq (${BUILD_FTS}, 1)
	EXTENSIONS:=${EXTENSIONS} -DBUILD_FTS_EXTENSION=1
endif
ifeq (${BUILD_VISUALIZER}, 1)
	EXTENSIONS:=${EXTENSIONS} -DBUILD_VISUALIZER_EXTENSION=1
endif
ifeq (${BUILD_HTTPFS}, 1)
	EXTENSIONS:=${EXTENSIONS} -DBUILD_HTTPFS_EXTENSION=1
endif
ifeq (${BUILD_JSON}, 1)
	EXTENSIONS:=${EXTENSIONS} -DBUILD_JSON_EXTENSION=1
endif
ifeq (${BUILD_JEMALLOC}, 1)
	EXTENSIONS:=${EXTENSIONS} -DBUILD_JEMALLOC_EXTENSION=1
endif
ifeq (${BUILD_EXCEL}, 1)
	EXTENSIONS:=${EXTENSIONS} -DBUILD_EXCEL_EXTENSION=1
endif
ifeq (${BUILD_INET}, 1)
	EXTENSIONS:=${EXTENSIONS} -DBUILD_INET_EXTENSION=1
endif
ifeq (${STATIC_OPENSSL}, 1)
	EXTENSIONS:=${EXTENSIONS} -DOPENSSL_USE_STATIC_LIBS=1
endif
ifeq (${BUILD_SQLSMITH}, 1)
	EXTENSIONS:=${EXTENSIONS} -DBUILD_SQLSMITH_EXTENSION=1
endif
ifeq (${BUILD_TPCE}, 1)
	EXTENSIONS:=${EXTENSIONS} -DBUILD_TPCE=1
endif
ifeq (${BUILD_JDBC}, 1)
	EXTENSIONS:=${EXTENSIONS} -DJDBC_DRIVER=1
endif
ifneq ($(OVERRIDE_JDBC_OS_ARCH),)
	EXTENSIONS:=${EXTENSIONS} -DOVERRIDE_JDBC_OS_ARCH=$(OVERRIDE_JDBC_OS_ARCH)
endif
ifeq (${BUILD_ODBC}, 1)
	EXTENSIONS:=${EXTENSIONS} -DBUILD_ODBC_DRIVER=1
endif
ifneq ($(ODBC_CONFIG),)
	EXTENSIONS:=${EXTENSIONS} -DODBC_CONFIG=${ODBC_CONFIG}
endif
ifeq (${BUILD_PYTHON}, 1)
	EXTENSIONS:=${EXTENSIONS} -DBUILD_PYTHON=1 -DBUILD_JSON_EXTENSION=1 -DBUILD_FTS_EXTENSION=1 -DBUILD_TPCH_EXTENSION=1 -DBUILD_VISUALIZER_EXTENSION=1 -DBUILD_TPCDS_EXTENSION=1
endif
ifeq (${PYTHON_USER_SPACE}, 1)
	EXTENSIONS:=${EXTENSIONS} -DUSER_SPACE=1
endif
ifeq (${BUILD_R}, 1)
	EXTENSIONS:=${EXTENSIONS} -DBUILD_R=1
endif
ifeq (${BUILD_NODE}, 1)
	EXTENSIONS:=${EXTENSIONS} -DBUILD_NODE=1 -DBUILD_JSON_EXTENSION=1
endif
ifeq (${CONFIGURE_R}, 1)
	EXTENSIONS:=${EXTENSIONS} -DCONFIGURE_R=1
endif
ifneq ($(TIDY_THREADS),)
	TIDY_THREAD_PARAMETER := -j ${TIDY_THREADS}
endif
ifneq ($(TIDY_BINARY),)
	TIDY_BINARY_PARAMETER := -clang-tidy-binary ${TIDY_BINARY}
endif

ifneq ("${FORCE_QUERY_LOG}a", "a")
	EXTENSIONS:=${EXTENSIONS} -DFORCE_QUERY_LOG=${FORCE_QUERY_LOG}
endif
# TODO: deprecated, can be removed once all OOTEs use BUILD_OUT_OF_TREE_EXTENSIONS
ifneq ($(BUILD_OUT_OF_TREE_EXTENSION),)
	EXTENSIONS:=${EXTENSIONS} -DEXTERNAL_EXTENSION_DIRECTORIES="$(BUILD_OUT_OF_TREE_EXTENSION)"
endif
ifneq ($(BUILD_OUT_OF_TREE_EXTENSIONS),)
	EXTENSIONS:=${EXTENSIONS} -DDUCKDB_OOT_EXTENSION_NAMES="$(BUILD_OUT_OF_TREE_EXTENSIONS)"
endif

ifeq (${CRASH_ON_ASSERT}, 1)
	EXTENSIONS:=${EXTENSIONS} -DASSERT_EXCEPTION=0
endif
ifeq (${DISABLE_STRING_INLINE}, 1)
	EXTENSIONS:=${EXTENSIONS} -DDISABLE_STR_INLINE=1
endif
ifeq (${DISABLE_MEMORY_SAFETY}, 1)
	EXTENSIONS:=${EXTENSIONS} -DDISABLE_MEMORY_SAFETY=1
endif
ifeq (${DESTROY_UNPINNED_BLOCKS}, 1)
	EXTENSIONS:=${EXTENSIONS} -DDESTROY_UNPINNED_BLOCKS=1
endif
ifeq (${FORCE_ASYNC_SINK_SOURCE}, 1)
	EXTENSIONS:=${EXTENSIONS} -DFORCE_ASYNC_SINK_SOURCE=1
endif
ifeq (${ALTERNATIVE_VERIFY}, 1)
	EXTENSIONS:=${EXTENSIONS} -DALTERNATIVE_VERIFY=1
endif
ifeq (${DEBUG_MOVE}, 1)
	EXTENSIONS:=${EXTENSIONS} -DDEBUG_MOVE=1
endif
ifeq (${DEBUG_STACKTRACE}, 1)
	EXTENSIONS:=${EXTENSIONS} -DDEBUG_STACKTRACE=1
endif
ifeq (${DISABLE_CORE_FUNCTIONS}, 1)
	EXTENSIONS:=${EXTENSIONS} -DBUILD_CORE_FUNCTIONS_EXTENSION=0
endif

clean:
	rm -rf build

clean-python:
	tools/pythonpkg/clean.sh

debug:
	mkdir -p ./build/debug && \
	cd build/debug && \
	echo ${DUCKDB_OOT_EXTENSION_SUBSTRAIT_PATH} && \
	cmake $(GENERATOR) $(FORCE_COLOR) ${WARNINGS_AS_ERRORS} ${FORCE_32_BIT_FLAG} ${DISABLE_UNITY_FLAG} ${DISABLE_SANITIZER_FLAG} ${STATIC_LIBCPP} ${EXTENSIONS} ${EXTRA_CMAKE_VARIABLES} -DDEBUG_MOVE=1 -DCMAKE_BUILD_TYPE=Debug ../.. && \
	cmake --build . --config Debug

release:
	mkdir -p ./build/release && \
	cd build/release && \
	cmake $(GENERATOR) $(FORCE_COLOR) ${WARNINGS_AS_ERRORS} ${FORCE_WARN_UNUSED_FLAG} ${FORCE_32_BIT_FLAG} ${DISABLE_UNITY_FLAG} ${DISABLE_SANITIZER_FLAG} ${OSX_BUILD_UNIVERSAL_FLAG} ${STATIC_LIBCPP} ${EXTENSIONS} ${EXTRA_CMAKE_VARIABLES} -DCMAKE_BUILD_TYPE=Release ../.. && \
	cmake --build . --config Release

cldebug:
	mkdir -p ./build/cldebug && \
	cd build/cldebug && \
	cmake $(GENERATOR) $(FORCE_COLOR) ${WARNINGS_AS_ERRORS} ${FORCE_32_BIT_FLAG} ${DISABLE_UNITY_FLAG} ${EXTENSIONS} -DBUILD_PYTHON=1 -DBUILD_R=1 -DENABLE_SANITIZER=0 -DENABLE_UBSAN=0 ${EXTRA_CMAKE_VARIABLES} -DCMAKE_BUILD_TYPE=Debug ../.. && \
	cmake --build . --config Debug

clreldebug:
	mkdir -p ./build/clreldebug && \
	cd build/clreldebug && \
	cmake $(GENERATOR) $(FORCE_COLOR) ${WARNINGS_AS_ERRORS} ${FORCE_32_BIT_FLAG} ${DISABLE_UNITY_FLAG} ${STATIC_LIBCPP} ${EXTENSIONS} -DBUILD_PYTHON=1 -DBUILD_R=1 -DBUILD_FTS_EXTENSION=1 -DENABLE_SANITIZER=0 -DENABLE_UBSAN=0 ${EXTRA_CMAKE_VARIABLES} -DCMAKE_BUILD_TYPE=RelWithDebInfo ../.. && \
	cmake --build . --config RelWithDebInfo

unittest: debug
	build/debug/test/unittest
	build/debug/tools/sqlite3_api_wrapper/test_sqlite3_api_wrapper

unittestci:
	python3 scripts/run_tests_one_by_one.py build/debug/test/unittest
	build/debug/tools/sqlite3_api_wrapper/test_sqlite3_api_wrapper

unittestarrow:
	build/debug/test/unittest "[arrow]"


allunit: release # uses release build because otherwise allunit takes forever
	build/release/test/unittest "*"

docs:
	mkdir -p ./build/docs && \
	doxygen Doxyfile

doxygen: docs
	open build/docs/html/index.html

reldebug:
	mkdir -p ./build/reldebug && \
	cd build/reldebug && \
	cmake $(GENERATOR) $(FORCE_COLOR) ${WARNINGS_AS_ERRORS} ${FORCE_32_BIT_FLAG} ${DISABLE_UNITY_FLAG} ${DISABLE_SANITIZER_FLAG} ${STATIC_LIBCPP} ${EXTENSIONS} ${EXTRA_CMAKE_VARIABLES} -DCMAKE_BUILD_TYPE=RelWithDebInfo ../.. && \
	cmake --build . --config RelWithDebInfo

relassert:
	mkdir -p ./build/relassert && \
	cd build/relassert && \
	cmake $(GENERATOR) $(FORCE_COLOR) ${WARNINGS_AS_ERRORS} ${FORCE_32_BIT_FLAG} ${DISABLE_UNITY_FLAG} ${DISABLE_SANITIZER_FLAG} ${STATIC_LIBCPP} ${EXTENSIONS} -DFORCE_ASSERT=1 ${EXTRA_CMAKE_VARIABLES} -DCMAKE_BUILD_TYPE=RelWithDebInfo ../.. && \
	cmake --build . --config RelWithDebInfo

benchmark:
	mkdir -p ./build/release && \
	cd build/release && \
	cmake $(GENERATOR) $(FORCE_COLOR) ${WARNINGS_AS_ERRORS} ${FORCE_WARN_UNUSED_FLAG} ${FORCE_32_BIT_FLAG} ${DISABLE_UNITY_FLAG} ${DISABLE_SANITIZER_FLAG} ${OSX_BUILD_UNIVERSAL_FLAG} ${STATIC_LIBCPP} ${EXTENSIONS} -DBUILD_BENCHMARKS=1 ${EXTRA_CMAKE_VARIABLES} -DCMAKE_BUILD_TYPE=Release ../.. && \
	cmake --build . --config Release

amaldebug:
	mkdir -p ./build/amaldebug && \
	python3 scripts/amalgamation.py && \
	cd build/amaldebug && \
	cmake $(GENERATOR) $(FORCE_COLOR) ${STATIC_LIBCPP} ${EXTENSIONS} ${FORCE_32_BIT_FLAG} -DAMALGAMATION_BUILD=1 ${EXTRA_CMAKE_VARIABLES} -DCMAKE_BUILD_TYPE=Debug ../.. && \
	cmake --build . --config Debug

tidy-check:
	mkdir -p ./build/tidy && \
	cd build/tidy && \
	cmake -DCLANG_TIDY=1 -DDISABLE_UNITY=1 -DBUILD_PARQUET_EXTENSION=TRUE -DBUILD_PYTHON_PKG=TRUE ${EXTRA_CMAKE_VARIABLES} -DBUILD_SHELL=0 ../.. && \
	python3 ../../scripts/run-clang-tidy.py -quiet ${TIDY_THREAD_PARAMETER} ${TIDY_BINARY_PARAMETER}

tidy-fix:
	mkdir -p ./build/tidy && \
	cd build/tidy && \
	cmake -DCLANG_TIDY=1 -DDISABLE_UNITY=1 -DBUILD_PARQUET_EXTENSION=TRUE ${EXTRA_CMAKE_VARIABLES} -DBUILD_SHELL=0 ../.. && \
	python3 ../../scripts/run-clang-tidy.py -fix

test_compile: # test compilation of individual cpp files
	python3 scripts/amalgamation.py --compile

format-check:
	python3 scripts/format.py --all --check

format-check-silent:
	python3 scripts/format.py --all --check --silent

format-fix:
	rm -rf src/amalgamation/*
	python3 scripts/format.py --all --fix --noconfirm

format-head:
	python3 scripts/format.py HEAD --fix --noconfirm

format-changes:
	python3 scripts/format.py HEAD --fix --noconfirm

format-master:
	python3 scripts/format.py master --fix --noconfirm

third_party/sqllogictest:
	git clone --depth=1 --branch hawkfish-statistical-rounding https://github.com/cwida/sqllogictest.git third_party/sqllogictest

sqlite: release | third_party/sqllogictest
	git --git-dir third_party/sqllogictest/.git pull
	./build/release/test/unittest "[sqlitelogic]"

sqlsmith: debug
	./build/debug/third_party/sqlsmith/sqlsmith --duckdb=:memory:

# Bloaty: a size profiler for binaries, is a project backed by Google engineers, https://github.com/google/bloaty for more info
# works both on executable, libraries (-> .duckdb_extension) and on WebAssembly
bloaty/bloaty:
	git clone https://github.com/google/bloaty.git
	cd bloaty && git submodule update --init --recursive && cmake -B build -G Ninja -S . && cmake --build build
	mv bloaty/build/bloaty bloaty/bloaty

bloaty: reldebug bloaty/bloaty
	cd build/reldebug && dsymutil duckdb
	./bloaty/bloaty  build/reldebug/duckdb -d symbols -n 20 --debug-file=build/reldebug/duckdb.dSYM/Contents/Resources/DWARF/duckdb
	# ./bloaty/bloaty  build/reldebug/extension/parquet/parquet.duckdb_extension -d symbols -n 20 # to execute on extension

clangd:
	cmake -DCMAKE_BUILD_TYPE=Debug ${EXTENSIONS} -B build/clangd .
