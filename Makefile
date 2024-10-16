.PHONY: all opt unit clean debug release test unittest allunit benchmark docs doxygen format sqlite

all: release
opt: release
unit: unittest

EXTENSION_CONFIG_STEP ?=
ifdef USE_MERGED_VCPKG_MANIFEST
	EXTENSION_CONFIG_STEP = build/extension_configuration/vcpkg.json
endif

GENERATOR ?=
FORCE_COLOR ?=
WARNINGS_AS_ERRORS ?=
FORCE_WARN_UNUSED_FLAG ?=
DISABLE_UNITY_FLAG ?=
DISABLE_SANITIZER_FLAG ?=
FORCE_32_BIT_FLAG ?=

MKFILE_PATH := $(abspath $(lastword $(MAKEFILE_LIST)))
PROJ_DIR := $(dir $(MKFILE_PATH))

ifeq ($(GEN),ninja)
	GENERATOR=-G "Ninja"
	FORCE_COLOR=-DFORCE_COLORED_OUTPUT=1
endif
ifeq (${TREAT_WARNINGS_AS_ERRORS}, 1)
	WARNINGS_AS_ERRORS=-DTREAT_WARNINGS_AS_ERRORS=1
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
	DISABLE_SANITIZER_FLAG:=${DISABLE_SANITIZER_FLAG} -DENABLE_UBSAN=0
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

COMMON_CMAKE_VARS ?=
CMAKE_VARS_BUILD ?=
CMAKE_LLVM_VARS ?=
SKIP_EXTENSIONS ?=
BUILD_EXTENSIONS ?=
CORE_EXTENSIONS ?=
UNSAFE_NUMERIC_CAST ?=
ifdef OVERRIDE_GIT_DESCRIBE
        COMMON_CMAKE_VARS:=${COMMON_CMAKE_VARS} -DOVERRIDE_GIT_DESCRIBE="${OVERRIDE_GIT_DESCRIBE}"
else
        COMMON_CMAKE_VARS:=${COMMON_CMAKE_VARS} -DOVERRIDE_GIT_DESCRIBE=""
endif
ifneq (${CXX_STANDARD}, )
        CMAKE_VARS:=${CMAKE_VARS} -DCMAKE_CXX_STANDARD="${CXX_STANDARD}"
endif
ifneq (${DUCKDB_EXTENSIONS}, )
	BUILD_EXTENSIONS:=${DUCKDB_EXTENSIONS}
endif
ifneq (${CORE_EXTENSIONS}, )
	CORE_EXTENSIONS:=${CORE_EXTENSIONS}
endif
ifeq (${DISABLE_PARQUET}, 1)
	SKIP_EXTENSIONS:=${SKIP_EXTENSIONS};parquet
endif
ifeq (${DISABLE_MAIN_DUCKDB_LIBRARY}, 1)
	CMAKE_VARS:=${CMAKE_VARS} -DBUILD_MAIN_DUCKDB_LIBRARY=0
endif
ifeq (${EXTENSION_STATIC_BUILD}, 1)
	CMAKE_VARS:=${CMAKE_VARS} -DEXTENSION_STATIC_BUILD=1
endif
ifeq (${DISABLE_BUILTIN_EXTENSIONS}, 1)
	CMAKE_VARS:=${CMAKE_VARS} -DDISABLE_BUILTIN_EXTENSIONS=1
endif
ifneq (${ENABLE_EXTENSION_AUTOLOADING}, "")
	CMAKE_VARS:=${CMAKE_VARS} -DENABLE_EXTENSION_AUTOLOADING=${ENABLE_EXTENSION_AUTOLOADING}
endif
ifneq (${ENABLE_EXTENSION_AUTOINSTALL}, "")
	CMAKE_VARS:=${CMAKE_VARS} -DENABLE_EXTENSION_AUTOINSTALL=${ENABLE_EXTENSION_AUTOINSTALL}
endif
ifneq (${UNSAFE_NUMERIC_CAST}, )
	CMAKE_VARS:=${CMAKE_VARS} -DUNSAFE_NUMERIC_CAST=1
endif
ifeq (${BUILD_EXTENSIONS_ONLY}, 1)
	CMAKE_VARS:=${CMAKE_VARS} -DBUILD_EXTENSIONS_ONLY=1
endif

ifeq (${BUILD_BENCHMARK}, 1)
	CMAKE_VARS:=${CMAKE_VARS} -DBUILD_BENCHMARKS=1
endif
ifeq (${BUILD_AUTOCOMPLETE}, 1)
	BUILD_EXTENSIONS:=${BUILD_EXTENSIONS};autocomplete
endif
ifeq (${BUILD_ICU}, 1)
	BUILD_EXTENSIONS:=${BUILD_EXTENSIONS};icu
endif
ifeq (${BUILD_TPCH}, 1)
	BUILD_EXTENSIONS:=${BUILD_EXTENSIONS};tpch
endif
ifeq (${BUILD_TPCDS}, 1)
	BUILD_EXTENSIONS:=${BUILD_EXTENSIONS};tpcds
endif
ifeq (${BUILD_FTS}, 1)
	BUILD_EXTENSIONS:=${BUILD_EXTENSIONS};fts
endif
ifeq (${BUILD_HTTPFS}, 1)
	BUILD_EXTENSIONS:=${BUILD_EXTENSIONS};httpfs
endif
ifeq (${BUILD_JSON}, 1)
	BUILD_EXTENSIONS:=${BUILD_EXTENSIONS};json
endif
ifeq (${BUILD_JEMALLOC}, 1)
	BUILD_EXTENSIONS:=${BUILD_EXTENSIONS};jemalloc
endif
ifeq (${BUILD_ALL_EXT}, 1)
	CMAKE_VARS:=${CMAKE_VARS} -DDUCKDB_EXTENSION_CONFIGS=".github/config/in_tree_extensions.cmake;.github/config/out_of_tree_extensions.cmake"
else ifeq (${BUILD_ALL_IT_EXT}, 1)
	CMAKE_VARS:=${CMAKE_VARS} -DDUCKDB_EXTENSION_CONFIGS=".github/config/in_tree_extensions.cmake"
else ifeq (${BUILD_ALL_OOT_EXT}, 1)
	CMAKE_VARS:=${CMAKE_VARS} -DDUCKDB_EXTENSION_CONFIGS=".github/config/out_of_tree_extensions.cmake"
endif
ifeq (${STATIC_OPENSSL}, 1)
	CMAKE_VARS:=${CMAKE_VARS} -DOPENSSL_USE_STATIC_LIBS=1
endif
ifeq (${BUILD_TPCE}, 1)
	CMAKE_VARS:=${CMAKE_VARS} -DBUILD_TPCE=1
endif
ifeq (${BUILD_PYTHON}, 1)
	CMAKE_VARS:=${CMAKE_VARS} -DBUILD_PYTHON=1 -DDUCKDB_EXTENSION_CONFIGS="tools/pythonpkg/duckdb_extension_config.cmake"
endif
ifeq (${PYTHON_USER_SPACE}, 1)
	CMAKE_VARS:=${CMAKE_VARS} -DUSER_SPACE=1
endif
ifeq (${PYTHON_EDITABLE_BUILD}, 1)
	CMAKE_VARS:=${CMAKE_VARS} -DPYTHON_EDITABLE_BUILD=1
endif
ifeq (${CONFIGURE_R}, 1)
	CMAKE_VARS:=${CMAKE_VARS} -DCONFIGURE_R=1
endif
ifneq ($(TIDY_THREADS),)
	TIDY_THREAD_PARAMETER := -j ${TIDY_THREADS}
endif
ifneq ($(TIDY_BINARY),)
	TIDY_BINARY_PARAMETER := -clang-tidy-binary ${TIDY_BINARY}
endif
ifneq ($(TIDY_CHECKS),)
        TIDY_PERFORM_CHECKS := '-checks=${TIDY_CHECKS}'
endif
ifneq ("${FORCE_QUERY_LOG}a", "a")
	CMAKE_VARS:=${CMAKE_VARS} -DFORCE_QUERY_LOG=${FORCE_QUERY_LOG}
endif
ifneq ($(BUILD_EXTENSIONS),)
	CMAKE_VARS:=${CMAKE_VARS} -DBUILD_EXTENSIONS="$(BUILD_EXTENSIONS)"
endif
ifneq ($(CORE_EXTENSIONS),)
	CMAKE_VARS:=${CMAKE_VARS} -DCORE_EXTENSIONS="$(CORE_EXTENSIONS)"
endif
ifneq ($(SKIP_EXTENSIONS),)
	CMAKE_VARS:=${CMAKE_VARS} -DSKIP_EXTENSIONS="$(SKIP_EXTENSIONS)"
endif
ifneq ($(EXTENSION_CONFIGS),)
	CMAKE_VARS:=${CMAKE_VARS} -DDUCKDB_EXTENSION_CONFIGS="$(EXTENSION_CONFIGS)"
endif
ifeq ($(EXTENSION_TESTS_ONLY), 1)
	CMAKE_VARS:=${CMAKE_VARS} -DEXTENSION_TESTS_ONLY=1
endif
ifneq ($(EXTRA_CMAKE_VARIABLES),)
	CMAKE_VARS:=${CMAKE_VARS} ${EXTRA_CMAKE_VARIABLES}
endif
ifeq (${CRASH_ON_ASSERT}, 1)
	CMAKE_VARS:=${CMAKE_VARS} -DCRASH_ON_ASSERT=1
endif
ifeq (${FORCE_ASSERT}, 1)
	CMAKE_VARS:=${CMAKE_VARS} -DFORCE_ASSERT=1
endif
ifeq (${DISABLE_STRING_INLINE}, 1)
	CMAKE_VARS:=${CMAKE_VARS} -DDISABLE_STR_INLINE=1
endif
ifeq (${DISABLE_MEMORY_SAFETY}, 1)
	CMAKE_VARS:=${CMAKE_VARS} -DDISABLE_MEMORY_SAFETY=1
endif
ifeq (${DISABLE_ASSERTIONS}, 1)
	CMAKE_VARS:=${CMAKE_VARS} -DDISABLE_ASSERTIONS=1
endif
ifeq (${DESTROY_UNPINNED_BLOCKS}, 1)
	CMAKE_VARS:=${CMAKE_VARS} -DDESTROY_UNPINNED_BLOCKS=1
endif
ifeq (${FORCE_ASYNC_SINK_SOURCE}, 1)
	CMAKE_VARS:=${CMAKE_VARS} -DFORCE_ASYNC_SINK_SOURCE=1
endif
ifeq (${RUN_SLOW_VERIFIERS}, 1)
	CMAKE_VARS:=${CMAKE_VARS} -DRUN_SLOW_VERIFIERS=1
endif
ifeq (${ALTERNATIVE_VERIFY}, 1)
	CMAKE_VARS:=${CMAKE_VARS} -DALTERNATIVE_VERIFY=1
endif
ifeq (${LATEST_STORAGE}, 1)
	CMAKE_VARS:=${CMAKE_VARS} -DLATEST_STORAGE=1
endif
ifeq (${BLOCK_VERIFICATION}, 1)
	CMAKE_VARS:=${CMAKE_VARS} -DBLOCK_VERIFICATION=1
endif
ifneq (${VERIFY_VECTOR}, )
	CMAKE_VARS:=${CMAKE_VARS} -DVERIFY_VECTOR=${VERIFY_VECTOR}
endif
ifeq (${DEBUG_MOVE}, 1)
	CMAKE_VARS:=${CMAKE_VARS} -DDEBUG_MOVE=1
endif
ifeq (${DEBUG_ALLOCATION}, 1)
	CMAKE_VARS:=${CMAKE_VARS} -DDEBUG_ALLOCATION=1
endif
ifeq (${DEBUG_STACKTRACE}, 1)
	CMAKE_VARS:=${CMAKE_VARS} -DDEBUG_STACKTRACE=1
endif
ifeq (${DISABLE_CORE_FUNCTIONS}, 1)
	CMAKE_VARS:=${CMAKE_VARS} -DBUILD_CORE_FUNCTIONS_EXTENSION=0
endif
ifeq (${DISABLE_EXTENSION_LOAD}, 1)
	CMAKE_VARS:=${CMAKE_VARS} -DDISABLE_EXTENSION_LOAD=1
endif
CMAKE_VARS:=${CMAKE_VARS} -DLOCAL_EXTENSION_REPO="${LOCAL_EXTENSION_REPO}"
ifneq (${OSX_BUILD_ARCH}, )
	CMAKE_VARS:=${CMAKE_VARS} -DOSX_BUILD_ARCH=${OSX_BUILD_ARCH}
endif
ifeq (${OSX_BUILD_UNIVERSAL}, 1)
	CMAKE_VARS:=${CMAKE_VARS} -DOSX_BUILD_UNIVERSAL=1
endif
ifneq ("${CUSTOM_LINKER}", "")
	CMAKE_VARS:=${CMAKE_VARS} -DCUSTOM_LINKER=${CUSTOM_LINKER}
endif
ifdef SKIP_PLATFORM_UTIL
	CMAKE_VARS:=${CMAKE_VARS} -DSKIP_PLATFORM_UTIL=1
endif
ifdef DEBUG_STACKTRACE
	CMAKE_VARS:=${CMAKE_VARS} -DDEBUG_STACKTRACE=1
endif

# Optional overrides
ifneq (${STANDARD_VECTOR_SIZE}, )
	CMAKE_VARS:=${CMAKE_VARS} -DSTANDARD_VECTOR_SIZE=${STANDARD_VECTOR_SIZE}
endif
ifneq (${BLOCK_ALLOC_SIZE}, )
	CMAKE_VARS:=${CMAKE_VARS} -DBLOCK_ALLOC_SIZE=${BLOCK_ALLOC_SIZE}
endif

# Enable VCPKG for this build
ifneq ("${VCPKG_TOOLCHAIN_PATH}", "")
	CMAKE_VARS_BUILD:=${CMAKE_VARS_BUILD} -DCMAKE_TOOLCHAIN_FILE='${VCPKG_TOOLCHAIN_PATH}' -DVCPKG_BUILD=1
endif
ifneq ("${VCPKG_TARGET_TRIPLET}", "")
	CMAKE_VARS_BUILD:=${CMAKE_VARS_BUILD} -DVCPKG_TARGET_TRIPLET='${VCPKG_TARGET_TRIPLET}'
endif
ifeq (${USE_MERGED_VCPKG_MANIFEST}, 1)
	CMAKE_VARS:=${CMAKE_VARS} -DVCPKG_MANIFEST_DIR='${PROJ_DIR}build/extension_configuration'
endif

ifneq ("${LTO}", "")
	CMAKE_VARS:=${CMAKE_VARS} -DCMAKE_LTO='${LTO}'
endif
ifneq ("${CMAKE_LLVM_PATH}", "")
	CMAKE_VARS:=${CMAKE_VARS} -DCMAKE_RANLIB='${CMAKE_LLVM_PATH}/bin/llvm-ranlib' -DCMAKE_AR='${CMAKE_LLVM_PATH}/bin/llvm-ar' -DCMAKE_CXX_COMPILER='${CMAKE_LLVM_PATH}/bin/clang++' -DCMAKE_C_COMPILER='${CMAKE_LLVM_PATH}/bin/clang'
endif

CMAKE_VARS:=${CMAKE_VARS} ${COMMON_CMAKE_VARS}

ifdef DUCKDB_PLATFORM
	ifneq ("${DUCKDB_PLATFORM}", "")
		CMAKE_VARS:=${CMAKE_VARS} -DDUCKDB_EXPLICIT_PLATFORM='${DUCKDB_PLATFORM}'
	endif
endif

clean:
	rm -rf build

clean-python:
	tools/pythonpkg/clean.sh

debug: ${EXTENSION_CONFIG_STEP}
	mkdir -p ./build/debug && \
	cd build/debug && \
	echo ${DUCKDB_EXTENSION_SUBSTRAIT_PATH} && \
	cmake $(GENERATOR) $(FORCE_COLOR) ${WARNINGS_AS_ERRORS} ${FORCE_32_BIT_FLAG} ${DISABLE_UNITY_FLAG} ${DISABLE_SANITIZER_FLAG} ${STATIC_LIBCPP} ${CMAKE_VARS} ${CMAKE_VARS_BUILD} -DDEBUG_MOVE=1 -DCMAKE_BUILD_TYPE=Debug ../.. && \
	cmake --build . --config Debug

release: ${EXTENSION_CONFIG_STEP}
	mkdir -p ./build/release && \
	cd build/release && \
	cmake $(GENERATOR) $(FORCE_COLOR) ${WARNINGS_AS_ERRORS} ${FORCE_WARN_UNUSED_FLAG} ${FORCE_32_BIT_FLAG} ${DISABLE_UNITY_FLAG} ${DISABLE_SANITIZER_FLAG} ${STATIC_LIBCPP} ${CMAKE_VARS} ${CMAKE_VARS_BUILD} -DCMAKE_BUILD_TYPE=Release ../.. && \
	cmake --build . --config Release

wasm_mvp: ${EXTENSION_CONFIG_STEP}
	mkdir -p ./build/wasm_mvp && \
	emcmake cmake $(GENERATOR) -DWASM_LOADABLE_EXTENSIONS=1 -DBUILD_EXTENSIONS_ONLY=1 -Bbuild/wasm_mvp -DCMAKE_CXX_FLAGS="-DDUCKDB_CUSTOM_PLATFORM=wasm_mvp" -DDUCKDB_EXPLICIT_PLATFORM="wasm_mvp" ${COMMON_CMAKE_VARS} ${TOOLCHAIN_FLAGS} && \
	emmake make -j8 -Cbuild/wasm_mvp

wasm_eh: ${EXTENSION_CONFIG_STEP}
	mkdir -p ./build/wasm_eh && \
	emcmake cmake $(GENERATOR) -DWASM_LOADABLE_EXTENSIONS=1 -DBUILD_EXTENSIONS_ONLY=1 -Bbuild/wasm_eh -DCMAKE_CXX_FLAGS="-fwasm-exceptions -DWEBDB_FAST_EXCEPTIONS=1 -DDUCKDB_CUSTOM_PLATFORM=wasm_eh" -DDUCKDB_EXPLICIT_PLATFORM="wasm_eh" ${COMMON_CMAKE_VARS} ${TOOLCHAIN_FLAGS} && \
	emmake make -j8 -Cbuild/wasm_eh

wasm_threads: ${EXTENSION_CONFIG_STEP}
	mkdir -p ./build/wasm_threads && \
	emcmake cmake $(GENERATOR) -DWASM_LOADABLE_EXTENSIONS=1 -DBUILD_EXTENSIONS_ONLY=1 -Bbuild/wasm_threads -DCMAKE_CXX_FLAGS="-fwasm-exceptions -DWEBDB_FAST_EXCEPTIONS=1 -DWITH_WASM_THREADS=1 -DWITH_WASM_SIMD=1 -DWITH_WASM_BULK_MEMORY=1 -DDUCKDB_CUSTOM_PLATFORM=wasm_threads -pthread" -DDUCKDB_EXPLICIT_PLATFORM="wasm_threads" ${COMMON_CMAKE_VARS} -DUSE_WASM_THREADS=1 -DCMAKE_C_FLAGS="-pthread" ${TOOLCHAIN_FLAGS} && \
	emmake make -j8 -Cbuild/wasm_threads

cldebug: ${EXTENSION_CONFIG_STEP}
	mkdir -p ./build/cldebug && \
	cd build/cldebug && \
	cmake $(GENERATOR) $(FORCE_COLOR) ${WARNINGS_AS_ERRORS} ${FORCE_32_BIT_FLAG} ${DISABLE_UNITY_FLAG} ${CMAKE_VARS} ${CMAKE_VARS_BUILD} -DBUILD_PYTHON=1 -DENABLE_SANITIZER=0 -DENABLE_UBSAN=0 -DCMAKE_BUILD_TYPE=Debug ../.. && \
	cmake --build . --config Debug

clreldebug:
	mkdir -p ./build/clreldebug && \
	cd build/clreldebug && \
	cmake $(GENERATOR) $(FORCE_COLOR) ${WARNINGS_AS_ERRORS} ${FORCE_32_BIT_FLAG} ${DISABLE_UNITY_FLAG} ${STATIC_LIBCPP} ${CMAKE_VARS} -DBUILD_PYTHON=1 -DBUILD_FTS_EXTENSION=1 -DENABLE_SANITIZER=0 -DENABLE_UBSAN=0 -DCMAKE_BUILD_TYPE=RelWithDebInfo ../.. && \
	cmake --build . --config RelWithDebInfo

extension_configuration: build/extension_configuration/vcpkg.json

extension/extension_config_local.cmake:
	touch extension/extension_config_local.cmake

build/extension_configuration/vcpkg.json: extension/extension_config_local.cmake extension/extension_config.cmake
	mkdir -p ./build/extension_configuration && \
	cd build/extension_configuration && \
	cmake $(GENERATOR) $(FORCE_COLOR) ${CMAKE_VARS} -DEXTENSION_CONFIG_BUILD=TRUE -DVCPKG_BUILD=1 -DCMAKE_BUILD_TYPE=Debug ../.. && \
	cmake --build . --config RelWithDebInfo

unittest: debug
	build/debug/test/unittest
	build/debug/tools/sqlite3_api_wrapper/test_sqlite3_api_wrapper

unittest_release: release
	build/release/test/unittest
	build/release/tools/sqlite3_api_wrapper/test_sqlite3_api_wrapper

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

reldebug: ${EXTENSION_CONFIG_STEP}
	mkdir -p ./build/reldebug && \
	cd build/reldebug && \
	cmake $(GENERATOR) $(FORCE_COLOR) ${WARNINGS_AS_ERRORS} ${FORCE_32_BIT_FLAG} ${DISABLE_UNITY_FLAG} ${DISABLE_SANITIZER_FLAG} ${STATIC_LIBCPP} ${CMAKE_VARS} ${CMAKE_VARS_BUILD} -DCMAKE_BUILD_TYPE=RelWithDebInfo ../.. && \
	cmake --build . --config RelWithDebInfo

relassert: ${EXTENSION_CONFIG_STEP}
	mkdir -p ./build/relassert && \
	cd build/relassert && \
	cmake $(GENERATOR) $(FORCE_COLOR) ${WARNINGS_AS_ERRORS} ${FORCE_32_BIT_FLAG} ${DISABLE_UNITY_FLAG} ${DISABLE_SANITIZER_FLAG} ${STATIC_LIBCPP} ${CMAKE_VARS} ${CMAKE_VARS_BUILD} -DFORCE_ASSERT=1 -DCMAKE_BUILD_TYPE=RelWithDebInfo ../.. && \
	cmake --build . --config RelWithDebInfo

benchmark:
	mkdir -p ./build/release && \
	cd build/release && \
	cmake $(GENERATOR) $(FORCE_COLOR) ${WARNINGS_AS_ERRORS} ${FORCE_WARN_UNUSED_FLAG} ${FORCE_32_BIT_FLAG} ${DISABLE_UNITY_FLAG} ${DISABLE_SANITIZER_FLAG} ${STATIC_LIBCPP} ${CMAKE_VARS} -DBUILD_BENCHMARKS=1 -DCMAKE_BUILD_TYPE=Release ../.. && \
	cmake --build . --config Release

amaldebug:
	mkdir -p ./build/amaldebug && \
	python3 scripts/amalgamation.py && \
	cd build/amaldebug && \
	cmake $(GENERATOR) $(FORCE_COLOR) ${STATIC_LIBCPP} ${CMAKE_VARS} ${FORCE_32_BIT_FLAG} -DAMALGAMATION_BUILD=1 -DCMAKE_BUILD_TYPE=Debug ../.. && \
	cmake --build . --config Debug

tidy-check:
	mkdir -p ./build/tidy && \
	cd build/tidy && \
	cmake -DCLANG_TIDY=1 -DDISABLE_UNITY=1 -DBUILD_EXTENSIONS=parquet -DBUILD_PYTHON_PKG=TRUE -DBUILD_SHELL=0 ../.. && \
	python3 ../../scripts/run-clang-tidy.py -quiet ${TIDY_THREAD_PARAMETER} ${TIDY_BINARY_PARAMETER} ${TIDY_PERFORM_CHECKS}

tidy-check-diff:
	mkdir -p ./build/tidy && \
	cd build/tidy && \
	cmake -DCLANG_TIDY=1 -DDISABLE_UNITY=1 -DBUILD_EXTENSIONS=parquet -DBUILD_PYTHON_PKG=TRUE -DBUILD_SHELL=0 ../.. && \
	cd ../../ && \
	git diff origin/main . ':(exclude)tools' ':(exclude)extension' ':(exclude)test' ':(exclude)benchmark' ':(exclude)third_party' ':(exclude)src/common/adbc' ':(exclude)src/main/capi' | python3 scripts/clang-tidy-diff.py -path build/tidy -quiet ${TIDY_THREAD_PARAMETER} ${TIDY_BINARY_PARAMETER} ${TIDY_PERFORM_CHECKS} -p1

tidy-fix:
	mkdir -p ./build/tidy && \
	cd build/tidy && \
	cmake -DCLANG_TIDY=1 -DDISABLE_UNITY=1 -DBUILD_EXTENSIONS=parquet -DBUILD_SHELL=0 ../.. && \
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

format-main:
	python3 scripts/format.py main --fix --noconfirm

format-feature:
	python3 scripts/format.py feature --fix --noconfirm

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
	cd bloaty && git submodule update --init --recursive && cmake -Bm build -G Ninja -S . && cmake --build build
	mv bloaty/build/bloaty bloaty/bloaty

bloaty: reldebug bloaty/bloaty
	cd build/reldebug && dsymutil duckdb
	./bloaty/bloaty  build/reldebug/duckdb -d symbols -n 20 --debug-file=build/reldebug/duckdb.dSYM/Contents/Resources/DWARF/duckdb
	# ./bloaty/bloaty  build/reldebug/extension/parquet/parquet.duckdb_extension -d symbols -n 20 # to execute on extension

clangd:
	cmake -DCMAKE_BUILD_TYPE=Debug ${CMAKE_VARS} -B build/clangd .

coverage-check:
	./scripts/coverage_check.sh

generate-files:
	python3 scripts/generate_c_api.py
	python3 scripts/generate_functions.py
	python3 scripts/generate_serialization.py
	python3 scripts/generate_enum_util.py
	-@python3 tools/pythonpkg/scripts/generate_connection_code.py || echo "Warning: generate_connection_code.py failed, cxxheaderparser & pcpp are required to perform this step"
# Run the formatter again after (re)generating the files
	$(MAKE) format-main

bundle-library: release
	cd build/release && \
	mkdir -p bundle && \
	cp src/libduckdb_static.a bundle/. && \
	cp third_party/*/libduckdb_*.a bundle/. && \
	cp extension/*/lib*_extension.a bundle/. && \
	cd bundle && \
	find . -name '*.a' -exec ${AR} -x {} \; && \
	${AR} cr ../libduckdb_bundle.a *.o
