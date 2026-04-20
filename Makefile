.PHONY: all opt unit clean debug release test unittest allunit benchmark docs doxygen format sqlite smoke runnertests sync_out_of_tree_extensions

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
CONFIGS_DIR = ./test/configs


MKFILE_PATH := $(abspath $(lastword $(MAKEFILE_LIST)))
PROJ_DIR := $(dir $(MKFILE_PATH))

PYTHON ?= python3
EXE_SUFFIX :=
ifeq ($(OS),Windows_NT)
EXE_SUFFIX := .exe
endif
UNITTEST_BINARY ?= test/unittest$(EXE_SUFFIX)
SMOKE_UNITTEST ?= build/relassert/$(UNITTEST_BINARY)
UNITTEST_SLOW_FLAGS ?= --batch-timeout=1800 --track-runtime=300
UNITTEST_HUGE_FLAGS ?= --batch-size=1 --workers=50% $(UNITTEST_SLOW_FLAGS)

# Allow setting extra unit test parameters using `make smoke T=...`.
T ?=

CI_CPU_COUNT := $(shell nproc 2>/dev/null || sysctl -n hw.ncpu 2>/dev/null || printf '%s\n' "$${NUMBER_OF_PROCESSORS:-1}")
CI_BUILD_JOBS := $(shell jobs=$$(( $(CI_CPU_COUNT) * 80 / 100 )); [ $$jobs -lt 1 ] && jobs=1; echo $$jobs)
export CI_BUILD_JOBS
ifndef CMAKE_BUILD_PARALLEL_LEVEL
CMAKE_BUILD_PARALLEL_LEVEL := $(CI_BUILD_JOBS)
endif
export CMAKE_BUILD_PARALLEL_LEVEL
export CI_TIDY_JOBS := $(shell jobs=$$(( $(CI_CPU_COUNT) * 25 / 100 )); [ $$jobs -lt 1 ] && jobs=1; echo $$jobs)

# Assume Ninja is the default generator (if missing), but verify ninja exists.
# Cache Ninja detection so we only probe `ninja --version` once.
ifeq ($(GEN),)
NINJA_VERSION_FILE := build/ninja_version.txt
ifeq ($(wildcard $(NINJA_VERSION_FILE)),)
NINJA_DETECTED := $(strip $(shell mkdir -p build >/dev/null 2>&1; \
	v=$$(ninja --version 2>/dev/null | head -n 1); \
	if [ -n "$$v" ]; then \
		printf '%s\n' "$$v" > "$(NINJA_VERSION_FILE)"; \
		echo 1; \
	fi))
ifneq ($(NINJA_DETECTED),)
GEN := ninja
endif
else
GEN := ninja
endif
endif

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
ifeq (${RELEASE_SANITIZER}, 1)
	DISABLE_SANITIZER_FLAG:=${DISABLE_SANITIZER_FLAG} -DRELEASE_SANITIZER=1
endif
ifeq (${THREADSAN}, 1)
	DISABLE_SANITIZER_FLAG:=${DISABLE_SANITIZER_FLAG} -DENABLE_THREAD_SANITIZER=1
endif
ifeq (${STATIC_LIBCPP}, 1)
	STATIC_LIBCPP=-DSTATIC_LIBCPP=TRUE
endif
GIT_BASE_BRANCH:=main
ifneq (${DUCKDB_GIT_BASE_BRANCH}, )
	GIT_BASE_BRANCH:=${DUCKDB_GIT_BASE_BRANCH}
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

ifdef DUCKDB_EXPLICIT_VERSION
        COMMON_CMAKE_VARS:=${COMMON_CMAKE_VARS} -DDUCKDB_EXPLICIT_VERSION="${DUCKDB_EXPLICIT_VERSION}"
else
        COMMON_CMAKE_VARS:=${COMMON_CMAKE_VARS} -DDUCKDB_EXPLICIT_VERSION=""
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
ifneq (${EXTENSION_STATIC_BUILD}, )
	CMAKE_VARS:=${CMAKE_VARS} -DEXTENSION_STATIC_BUILD=${EXTENSION_STATIC_BUILD}
endif
ifeq (${DISABLE_GCC_FUNCTION_SECTIONS}, 1)
	CMAKE_VARS:=${CMAKE_VARS} -DDISABLE_GCC_FUNCTION_SECTIONS=1
endif
ifeq (${DISABLE_BUILTIN_EXTENSIONS}, 1)
	CMAKE_VARS:=${CMAKE_VARS} -DDISABLE_BUILTIN_EXTENSIONS=1
endif
ifeq (${GENERATE_EXTENSION_ENTRIES}, 1)
	CMAKE_VARS:=${CMAKE_VARS} -DGENERATE_EXTENSION_ENTRIES=1
endif
ifneq ("${ENABLE_EXTENSION_AUTOLOADING}", "")
	CMAKE_VARS:=${CMAKE_VARS} -DENABLE_EXTENSION_AUTOLOADING=${ENABLE_EXTENSION_AUTOLOADING}
endif
ifneq ("${ENABLE_EXTENSION_AUTOINSTALL}", "")
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
ifdef CORE_EXTENSIONS
	BUILD_EXTENSIONS:=${BUILD_EXTENSIONS};${CORE_EXTENSIONS}
endif
ifeq (${BUILD_ALL_EXT}, 1)
	EXTENSION_CONFIGS:=.github/config/in_tree_extensions.cmake;.github/config/out_of_tree_extensions.cmake;.github/config/rust_based_extensions.cmake
else ifeq (${BUILD_ALL_IT_EXT}, 1)
	EXTENSION_CONFIGS:=.github/config/in_tree_extensions.cmake
else ifeq (${BUILD_ALL_OOT_EXT}, 1)
	EXTENSION_CONFIGS:=.github/config/out_of_tree_extensions.cmake
endif
ifeq (${STATIC_OPENSSL}, 1)
	CMAKE_VARS:=${CMAKE_VARS} -DOPENSSL_USE_STATIC_LIBS=1
endif
ifeq (${BUILD_TPCE}, 1)
	CMAKE_VARS:=${CMAKE_VARS} -DBUILD_TPCE=1
endif
ifeq (${CONFIGURE_R}, 1)
	CMAKE_VARS:=${CMAKE_VARS} -DCONFIGURE_R=1
endif
ifneq ($(TIDY_BINARY),)
	TIDY_BINARY_PARAMETER := -clang-tidy-binary ${TIDY_BINARY}
endif
CLANGD_TIDY_VERSION := 1.1.1
CLANGD_TIDY_VENV ?= $(abspath build/clangd-tidy-venv)
ifeq ($(CLANGD_TIDY_BINARY),)
ifneq ($(wildcard $(CLANGD_TIDY_VENV)/bin/clangd-tidy),)
CLANGD_TIDY_BINARY := $(CLANGD_TIDY_VENV)/bin/clangd-tidy
endif
endif
ifeq ($(CLANGD_BINARY),)
ifneq ("${CMAKE_LLVM_PATH}", "")
CLANGD_BINARY := ${CMAKE_LLVM_PATH}/bin/clangd
endif
endif
ifneq ($(CLANGD_TIDY_BINARY),)
	CLANGD_TIDY_BINARY_PARAMETER := --clangd-tidy-binary ${CLANGD_TIDY_BINARY}
endif
ifneq ($(CLANGD_BINARY),)
	CLANGD_BINARY_PARAMETER := --clangd-binary ${CLANGD_BINARY}
endif
ifneq ($(CLANGD_TIDY_QUERY_DRIVER),)
	CLANGD_TIDY_QUERY_DRIVER_PARAMETER := --query-driver ${CLANGD_TIDY_QUERY_DRIVER}
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
ifeq ($(SHADOW_FORBIDDEN_FUNCTIONS),1)
	CMAKE_VARS:=${CMAKE_VARS} -DSHADOW_FORBIDDEN_FUNCTIONS=1
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
ifeq (${FORCE_DEBUG}, 1)
	CMAKE_VARS:=${CMAKE_VARS} -DFORCE_DEBUG=1
endif
ifeq (${SMALLER_BINARY}, 1)
	CMAKE_VARS:=${CMAKE_VARS} -DSMALLER_BINARY=1
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
ifeq (${DISABLE_POINTER_SALT}, 1)
	CMAKE_VARS:=${CMAKE_VARS} -DDISABLE_POINTER_SALT=1
endif
ifeq (${HASH_ZERO}, 1)
	CMAKE_VARS:=${CMAKE_VARS} -DHASH_ZERO=1
endif
ifeq (${LATEST_STORAGE}, 1)
	CMAKE_VARS:=${CMAKE_VARS} -DLATEST_STORAGE=1
endif
ifeq (${BLOCK_VERIFICATION}, 1)
	CMAKE_VARS:=${CMAKE_VARS} -DBLOCK_VERIFICATION=1
endif
ifneq (${DISABLE_CPP_UNITTESTS}, )
	CMAKE_VARS:=${CMAKE_VARS} -DENABLE_UNITTEST_CPP_TESTS=0
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
	SKIP_EXTENSIONS:=${SKIP_EXTENSIONS};core_functions
endif
ifeq (${DISABLE_EXTENSION_LOAD}, 1)
	CMAKE_VARS:=${CMAKE_VARS} -DDISABLE_EXTENSION_LOAD=1
endif
ifeq (${DISABLE_SHELL}, 1)
	CMAKE_VARS:=${CMAKE_VARS} -DBUILD_SHELL=0
endif
CMAKE_VARS:=${CMAKE_VARS} -DLOCAL_EXTENSION_REPO="${LOCAL_EXTENSION_REPO}"
ifneq (${OSX_BUILD_ARCH}, )
	CMAKE_VARS:=${CMAKE_VARS} -DOSX_BUILD_ARCH=${OSX_BUILD_ARCH}
endif
ifeq (${OSX_BUILD_UNIVERSAL}, 1)
	CMAKE_VARS:=${CMAKE_VARS} -DOSX_BUILD_UNIVERSAL=1
endif
ifneq ("${DUCKDB_LINKER}", "")
	CMAKE_VARS:=${CMAKE_VARS} -DDUCKDB_LINKER=${DUCKDB_LINKER}
endif
ifdef SKIP_PLATFORM_UTIL
	CMAKE_VARS:=${CMAKE_VARS} -DSKIP_PLATFORM_UTIL=1
endif
ifdef DEBUG_STACKTRACE
	CMAKE_VARS:=${CMAKE_VARS} -DDEBUG_STACKTRACE=1
endif
ifeq (${NATIVE_ARCH}, 1)
	CMAKE_VARS:=${CMAKE_VARS} -DNATIVE_ARCH=1
endif
ifeq (${OVERRIDE_NEW_DELETE}, 1)
	CMAKE_VARS:=${CMAKE_VARS} -DOVERRIDE_NEW_DELETE=1
endif
ifeq (${MAIN_BRANCH_VERSIONING}, 0)
	CMAKE_VARS:=${CMAKE_VARS} -DMAIN_BRANCH_VERSIONING=0
endif
ifeq (${MAIN_BRANCH_VERSIONING}, 1)
	CMAKE_VARS:=${CMAKE_VARS} -DMAIN_BRANCH_VERSIONING=1
endif
ifeq (${STANDALONE_DEBUG}, 1)
	CMAKE_VARS:=${CMAKE_VARS} -DSTANDALONE_DEBUG=1
endif
ifneq (${DUCKDB_PREBUILT_LIBRARY}, )
	CMAKE_VARS:=${CMAKE_VARS} -DPREBUILT_BINARY=${DUCKDB_PREBUILT_LIBRARY}
endif
ifdef REDUCE_SYMBOLS
	CMAKE_VARS:=${CMAKE_VARS} -DREDUCE_SYMBOLS=1
endif


# Optional overrides
ifneq (${STANDARD_VECTOR_SIZE}, )
	CMAKE_VARS:=${CMAKE_VARS} -DSTANDARD_VECTOR_SIZE=${STANDARD_VECTOR_SIZE}
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
sync_extensions_into =
vcpkg_cmake_flag =
ifdef DUCKDB_NEW_EXTENSION_BUILD
sync_extensions_into = $(PYTHON) scripts/sync_out_of_tree_extensions.py $(if $(BUILD_EXTENSIONS),--build-extensions "$(BUILD_EXTENSIONS)") $(if $(EXTENSION_CONFIGS),--extension-configs "$(EXTENSION_CONFIGS)") --output-dir '$(1)' &&
ifneq ("${VCPKG_TOOLCHAIN_PATH}", "")
vcpkg_cmake_flag = -DVCPKG_MANIFEST_DIR='$(1)'
endif
endif

ifneq ("${LTO}", "")
	CMAKE_VARS:=${CMAKE_VARS} -DCMAKE_LTO='${LTO}'
endif
ifeq (${EXPORT_DYNAMIC_SYMBOLS}, 1)
	CMAKE_VARS:=${CMAKE_VARS} -DEXPORT_DYNAMIC_SYMBOLS=1
endif
ifneq ("${CMAKE_LLVM_PATH}", "")
	CMAKE_VARS:=${CMAKE_VARS} -DCMAKE_RANLIB='${CMAKE_LLVM_PATH}/bin/llvm-ranlib' -DCMAKE_AR='${CMAKE_LLVM_PATH}/bin/llvm-ar' -DCMAKE_CXX_COMPILER='${CMAKE_LLVM_PATH}/bin/clang++' -DCMAKE_C_COMPILER='${CMAKE_LLVM_PATH}/bin/clang' -DCMAKE_EXE_LINKER_FLAGS_INIT='-L${CMAKE_LLVM_PATH}/lib -L${CMAKE_LLVM_PATH}/lib/c++' -DCMAKE_SHARED_LINKER_FLAGS_INIT='-L${CMAKE_LLVM_PATH}/lib -L${CMAKE_LLVM_PATH}/lib/c++'  -DCMAKE_MODULE_LINKER_FLAGS_INIT='-L${CMAKE_LLVM_PATH}/lib -L${CMAKE_LLVM_PATH}/lib/c++'
endif

CMAKE_VARS:=${CMAKE_VARS} ${COMMON_CMAKE_VARS}

ifdef DUCKDB_PLATFORM
	ifneq ("${DUCKDB_PLATFORM}", "")
		CMAKE_VARS:=${CMAKE_VARS} -DDUCKDB_EXPLICIT_PLATFORM='${DUCKDB_PLATFORM}'
	endif
endif

clean:
	rm -rf build

EXTENSION_REPOSITORY_PATH ?= build/release/repository
EXTENSION_BUCKET ?= duckdb-core-extensions

.PHONY: upload-extensions
upload-extensions:
	CI_CPU_COUNT="$(CI_CPU_COUNT)" ./scripts/extension-upload-repository.sh "$(EXTENSION_REPOSITORY_PATH)" "$(EXTENSION_BUCKET)"

define cmake_build
	mkdir -p ./$(1) && \
	$(call sync_extensions_into,${PROJ_DIR}$(1)) \
	cd $(1) && \
	cmake $(GENERATOR) $(FORCE_COLOR) ${WARNINGS_AS_ERRORS} ${FORCE_32_BIT_FLAG} ${DISABLE_UNITY_FLAG} ${DISABLE_SANITIZER_FLAG} ${STATIC_LIBCPP} ${CMAKE_VARS} ${CMAKE_VARS_BUILD} $(call vcpkg_cmake_flag,${PROJ_DIR}$(1)) $(3) -DCMAKE_BUILD_TYPE=$(2) ../.. && \
	cmake --build . --config $(2)
endef

debug: ${EXTENSION_CONFIG_STEP}
	$(call cmake_build,build/debug,Debug,-DDEBUG_MOVE=1)

release: ${EXTENSION_CONFIG_STEP}
	$(call cmake_build,build/release,Release,${FORCE_WARN_UNUSED_FLAG})

.PHONY: fuzzer_tools fuzzer fuzzer_smoke
fuzzer_tools:
	@uname_s="$$(uname -s)"; \
	if [ "$$uname_s" != "Linux" ]; then \
		echo "Error: fuzzer_tools is Linux-only"; \
		exit 1; \
	fi
	sudo apt-get update -y -qq
	sudo apt-get install -y -qq build-essential python3-dev automake cmake git flex bison libglib2.0-dev libpixman-1-dev python3-setuptools cargo libgtk-3-dev
	sudo apt-get install -y -qq lld-18 llvm-18 llvm-18-dev clang-18 || sudo apt-get install -y -qq lld llvm llvm-dev clang
	gcc_major="$$(gcc --version | head -n1 | sed 's/\..*//' | sed 's/.* //')"; \
	sudo apt-get install -y -qq "gcc-$${gcc_major}-plugin-dev" "libstdc++-$${gcc_major}-dev"
	./scripts/ci/afl_build.sh

fuzzer: ${EXTENSION_CONFIG_STEP}
	@eval "$$(./scripts/ci/afl_detect_cxx.sh)"; \
	rm -rf ./build/fuzzer && \
	mkdir -p ./build/fuzzer && \
	cd build/fuzzer && \
	cmake $(GENERATOR) $(FORCE_COLOR) ${WARNINGS_AS_ERRORS} ${FORCE_WARN_UNUSED_FLAG} ${FORCE_32_BIT_FLAG} ${DISABLE_UNITY_FLAG} ${DISABLE_SANITIZER_FLAG} ${STATIC_LIBCPP} ${CMAKE_VARS} $$AFL_LTO_CMAKE_VAR ${CMAKE_VARS_BUILD} -DCMAKE_C_COMPILER=$$CC -DCMAKE_CXX_COMPILER=$$CXX -DDUCKDB_FUZZER=1 -DFORCE_DEBUG=1 -DBUILD_EXTENSIONS="jemalloc" -DBUILD_UNITTESTS=1 -DENABLE_UNITTEST_CPP_TESTS=0 -DCMAKE_BUILD_TYPE=Release ../.. && \
	cmake --build . --config Release --target unittest

FUZZ_SMOKE_SECS ?= 30
fuzzer_smoke:
	@command -v afl-fuzz >/dev/null 2>&1 || { echo "Error: afl-fuzz is required (run: brew install afl++)"; exit 1; }
	@mkdir -p build/fuzzer/smoke/in && \
	printf "statement ok\nSELECT 42;\n\nquery I\nSELECT 7;\n----\n7\n" > build/fuzzer/smoke/in/seed1.test && \
	OUT_DIR=build/fuzzer/smoke/out-$$(date +%s) && \
	AFL_SKIP_CPUFREQ=1 python3 ./scripts/ci/afl_sandbox.py --writable-dir "$$OUT_DIR" -- \
	afl-fuzz -i build/fuzzer/smoke/in -o $$OUT_DIR -V ${FUZZ_SMOKE_SECS} -- ./build/fuzzer/test/unittest --writable-dir "$$OUT_DIR"

.PHONY: fuzz_sql_corpus fuzz_sql_dict
fuzz_sql_corpus:
	: "$${CORPUS_SQL_CMIN:?Error: CORPUS_SQL_CMIN is required}" && \
	export AFL_MAP_SIZE="$$(sh ./scripts/ci/afl_map_size.sh)" && \
	python3 -u scripts/ci/afl_corpus.py \
	--target "./build/fuzzer/test/unittest --writable-dir \"$${CORPUS_SQL_CMIN}\"" \
	--afl-cmin "python3 ./scripts/ci/afl_sandbox.py --writable-dir \"$${CORPUS_SQL_CMIN}\" -- afl-cmin -e -t 1000" \
	--output-dir "$${CORPUS_SQL_CMIN}"

FUZZ_SQL_DICT_SECS ?= 60
fuzz_sql_dict:
	: "$${CORPUS_SQL_CMIN:?Error: CORPUS_SQL_CMIN is required}" && \
	: "$${CORPUS_SQL_DICT:?Error: CORPUS_SQL_DICT is required}" && \
	export AFL_MAP_SIZE="$$(sh ./scripts/ci/afl_map_size.sh)" && AFL_SKIP_CPUFREQ=1 \
	python3 -u scripts/ci/afl_dict.py \
	--target "./build/fuzzer/test/unittest --writable-dir \"$$(dirname "$${CORPUS_SQL_DICT}")\"" \
	--input-dir "$${CORPUS_SQL_CMIN}" \
	--output-file "$${CORPUS_SQL_DICT}" \
	--fuzz-secs "${FUZZ_SQL_DICT_SECS}" \
	--afl-fuzz "python3 ./scripts/ci/afl_sandbox.py --writable-dir \"$$(dirname "$${CORPUS_SQL_DICT}")\" -- afl-fuzz"

WINDOWS_GENERATOR_PLATFORM ?= x64
BUNDLED_EXTENSIONS_CONFIGS ?= $(PWD)/.github/config/bundled_extensions.cmake
windows_release: ${EXTENSION_CONFIG_STEP}
	$(call sync_extensions_into,${PROJ_DIR}) \
	cmake $(GENERATOR) $(FORCE_COLOR) $(if $(filter ninja,$(GEN)),,-DCMAKE_GENERATOR_PLATFORM=$(WINDOWS_GENERATOR_PLATFORM)) ${WARNINGS_AS_ERRORS} ${FORCE_WARN_UNUSED_FLAG} ${FORCE_32_BIT_FLAG} ${DISABLE_SANITIZER_FLAG} ${STATIC_LIBCPP} ${CMAKE_VARS} ${CMAKE_VARS_BUILD} $(call vcpkg_cmake_flag,${PROJ_DIR}) -DCMAKE_BUILD_TYPE=Release -DENABLE_EXTENSION_AUTOLOADING=1 -DENABLE_EXTENSION_AUTOINSTALL=1 -DDUCKDB_EXTENSION_CONFIGS="$(BUNDLED_EXTENSIONS_CONFIGS)" . && \
	cmake --build . --config Release

windows_release_32: ${EXTENSION_CONFIG_STEP}
	$(call sync_extensions_into,${PROJ_DIR}) \
	cmake $(GENERATOR) $(FORCE_COLOR) $(if $(filter ninja,$(GEN)),,-DCMAKE_GENERATOR_PLATFORM=Win32) ${WARNINGS_AS_ERRORS} ${FORCE_WARN_UNUSED_FLAG} ${FORCE_32_BIT_FLAG} ${DISABLE_SANITIZER_FLAG} ${STATIC_LIBCPP} ${CMAKE_VARS} ${CMAKE_VARS_BUILD} $(call vcpkg_cmake_flag,${PROJ_DIR}) -DCMAKE_BUILD_TYPE=Release -DDUCKDB_EXTENSION_CONFIGS="$(BUNDLED_EXTENSIONS_CONFIGS)" . && \
	cmake --build . --config Release

wasm_mvp: ${EXTENSION_CONFIG_STEP}
	mkdir -p ./build/wasm_mvp && \
	emcmake cmake $(GENERATOR) -DWASM_LOADABLE_EXTENSIONS=1 -DBUILD_EXTENSIONS_ONLY=1 -Bbuild/wasm_mvp -DCMAKE_CXX_FLAGS="-DDUCKDB_CUSTOM_PLATFORM=wasm_mvp" -DDUCKDB_EXPLICIT_PLATFORM="wasm_mvp" ${COMMON_CMAKE_VARS} ${TOOLCHAIN_FLAGS} && \
	emmake make -j${CI_BUILD_JOBS} -Cbuild/wasm_mvp

wasm_eh: WASM_EH_CMAKE_VARS=-DBUILD_EXTENSIONS_ONLY=1
wasm_ci: WASM_EH_CMAKE_VARS=
wasm_eh wasm_ci: ${EXTENSION_CONFIG_STEP}
	mkdir -p ./build/wasm_eh && \
	emcmake cmake $(GENERATOR) -DWASM_LOADABLE_EXTENSIONS=1 $(WASM_EH_CMAKE_VARS) -Bbuild/wasm_eh -DCMAKE_CXX_FLAGS="-fwasm-exceptions -DDUCKDB_NO_THREADS=1 -DWEBDB_FAST_EXCEPTIONS=1 -DDUCKDB_CUSTOM_PLATFORM=wasm_eh" -DDUCKDB_EXPLICIT_PLATFORM="wasm_eh" ${COMMON_CMAKE_VARS} ${TOOLCHAIN_FLAGS} && \
	emmake make -j${CI_BUILD_JOBS} -Cbuild/wasm_eh

wasm_threads: ${EXTENSION_CONFIG_STEP}
	mkdir -p ./build/wasm_threads && \
	emcmake cmake $(GENERATOR) -DWASM_LOADABLE_EXTENSIONS=1 -DBUILD_EXTENSIONS_ONLY=1 -Bbuild/wasm_threads -DCMAKE_CXX_FLAGS="-fwasm-exceptions -DWEBDB_FAST_EXCEPTIONS=1 -DWITH_WASM_THREADS=1 -DWITH_WASM_SIMD=1 -DWITH_WASM_BULK_MEMORY=1 -DDUCKDB_CUSTOM_PLATFORM=wasm_threads -pthread" -DDUCKDB_EXPLICIT_PLATFORM="wasm_threads" ${COMMON_CMAKE_VARS} -DUSE_WASM_THREADS=1 -DCMAKE_C_FLAGS="-pthread" ${TOOLCHAIN_FLAGS} && \
	emmake make -j${CI_BUILD_JOBS} -Cbuild/wasm_threads

cldebug: ${EXTENSION_CONFIG_STEP}
	$(call cmake_build,build/cldebug,Debug,-DENABLE_SANITIZER=0 -DENABLE_UBSAN=0)

clreldebug:
	mkdir -p ./build/clreldebug && \
	cd build/clreldebug && \
	cmake $(GENERATOR) $(FORCE_COLOR) ${WARNINGS_AS_ERRORS} ${FORCE_32_BIT_FLAG} ${DISABLE_UNITY_FLAG} ${STATIC_LIBCPP} ${CMAKE_VARS} -DBUILD_FTS_EXTENSION=1 -DENABLE_SANITIZER=0 -DENABLE_UBSAN=0 -DCMAKE_BUILD_TYPE=RelWithDebInfo ../.. && \
	cmake --build . --config RelWithDebInfo

SYNC_OUTPUT_DIR ?= build
sync_out_of_tree_extensions:
	$(PYTHON) scripts/sync_out_of_tree_extensions.py $(if $(BUILD_EXTENSIONS),--build-extensions "$(BUILD_EXTENSIONS)") $(if $(EXTENSION_CONFIGS),--extension-configs "$(EXTENSION_CONFIGS)") --output-dir '${PROJ_DIR}$(SYNC_OUTPUT_DIR)'

extension_configuration: build/extension_configuration/vcpkg.json

extension/extension_config_local.cmake:
	touch extension/extension_config_local.cmake

build/extension_configuration/vcpkg.json: extension/extension_config_local.cmake extension/extension_config.cmake
	mkdir -p ./build/extension_configuration && \
	cd build/extension_configuration && \
	cmake $(GENERATOR) $(FORCE_COLOR) ${CMAKE_VARS} -DEXTENSION_CONFIG_BUILD=TRUE -DVCPKG_BUILD=1 -DCMAKE_BUILD_TYPE=Release ../.. && \
	cmake --build . --config Release

unittest: debug
	$(PYTHON) scripts/ci/run_tests.py build/debug/$(UNITTEST_BINARY) $(T)

unittest_reldebug:
	$(PYTHON) scripts/ci/run_tests.py build/reldebug/$(UNITTEST_BINARY) $(T)

ifneq ($(SKIP_BUILD),1)
unittest_release: release
endif
unittest_release:
	$(PYTHON) scripts/ci/run_tests.py build/release/$(UNITTEST_BINARY) $(T)

.PHONY: alltest_release_tag test_release_tag
alltest_release_tag:
	$(PYTHON) scripts/ci/run_tests.py --test-flags="--select-tag release" ./build/release/$(UNITTEST_BINARY) '*' $(T)

test_release_tag:
	$(PYTHON) scripts/ci/run_tests.py --test-flags="--select-tag release" ./build/release/$(UNITTEST_BINARY) $(T)

unittest_relassert:
	$(PYTHON) scripts/ci/run_tests.py build/relassert/$(UNITTEST_BINARY) $(T)

smoke:
	$(PYTHON) scripts/ci/run_tests.py --batch-timeout 120 --test-list test/smoke_tests.list $(SMOKE_UNITTEST) $(T)

unittestarrow:
	$(PYTHON) scripts/ci/run_tests.py build/debug/$(UNITTEST_BINARY) "[arrow]"

allunit:
	$(PYTHON) scripts/ci/run_tests.py --workers=50% build/release/$(UNITTEST_BINARY) '*' $(T)
ifndef CI
allunit: release
endif

unittest_threadsan: export TSAN_OPTIONS ?= "suppressions=./.sanitizer-thread-suppressions.txt"
unittest_threadsan: unittest_reldebug
	$(PYTHON) scripts/ci/run_tests.py $(UNITTEST_HUGE_FLAGS) build/reldebug/$(UNITTEST_BINARY) "[intraquery],[interquery],[detailed_profiler],test/sql/tpch/tpch_sf01.test_slow" $(T)
	$(PYTHON) scripts/ci/run_tests.py $(UNITTEST_HUGE_FLAGS) --test-flags="--force-storage --force-reload" build/reldebug/$(UNITTEST_BINARY) "[interquery]" $(T)

.PHONY: unittest_threadsan_extra
unittest_threadsan_extra: export TSAN_OPTIONS ?= "suppressions=./.sanitizer-thread-suppressions.txt"
unittest_threadsan_extra: unittest_reldebug
	$(PYTHON) scripts/ci/run_tests.py --batch-size=1 --workers=50% --batch-timeout=1800 --track-runtime=300 --test-flags="--force-storage" build/reldebug/$(UNITTEST_BINARY) "[interquery]" $(T)

docs:
	mkdir -p ./build/docs && \
	doxygen Doxyfile

doxygen: docs
	open build/docs/html/index.html

reldebug: ${EXTENSION_CONFIG_STEP}
	$(call cmake_build,build/reldebug,RelWithDebInfo,)

relassert: ${EXTENSION_CONFIG_STEP}
	$(call cmake_build,build/relassert,RelWithDebInfo,-DFORCE_ASSERT=1)

.PHONY: relassert-artifact

relassert-artifact:
	bash scripts/prepare_build_artifact.sh relassert

.PHONY: release-artifact

release-artifact:
	bash scripts/prepare_build_artifact.sh release

.PHONY: symbol-checks symbol-leakage-check banned-symbol-check

symbol-checks: symbol-leakage-check banned-symbol-check

symbol-leakage-check:
	$(PYTHON) scripts/exported_symbols_check.py build/release/src/libduckdb*.so

banned-symbol-check:
	$(PYTHON) scripts/banned_symbols_check.py --directory build/release/src

define ensure_apt_commands
	missing=0; \
	for cmd in $(1); do \
		command -v $$cmd >/dev/null 2>&1 || missing=1; \
	done; \
	if [ $$missing -eq 1 ]; then \
		sudo apt-get update -y -qq; \
		sudo apt-get install -y -qq $(2); \
	fi
endef

.PHONY: toolsci format_tools enum-integrity-check

toolsci:
	$(call ensure_apt_commands,ninja mold ccache pkg-config pigz,ninja-build mold ccache pkg-config pigz)
	pkg-config --exists libcurl || { \
		sudo apt-get update -y -qq; \
		sudo apt-get install -y -qq libcurl4-openssl-dev; \
	}
	ls -lh /usr/bin/gcc* /usr/bin/g++*
	gcc --version
	g++ --version

test_ci:
	python3 -m unittest discover --buffer --start-directory scripts/ci $(T)

format_tools:
	$(call ensure_apt_commands,ninja clang-format,ninja-build clang-format-11)
	sudo pip3 install --break-system-packages cmake-format 'black==24.*' cxxheaderparser pcpp 'clang_format==11.0.1'
	@echo "::group::Installed Python packages"
	pip3 freeze
	@echo "::endgroup::"
	@echo "::group::Formatter versions and config"
	clang-format --version
	clang-format --dump-config
	black --version
	@echo "::endgroup::"

enum-integrity-check:
	$(PYTHON) scripts/verify_enum_integrity.py src/include/duckdb.h

benchmark:
	mkdir -p ./build/release && \
	cd build/release && \
	cmake $(GENERATOR) $(FORCE_COLOR) ${WARNINGS_AS_ERRORS} ${FORCE_WARN_UNUSED_FLAG} ${FORCE_32_BIT_FLAG} ${DISABLE_UNITY_FLAG} ${DISABLE_SANITIZER_FLAG} ${STATIC_LIBCPP} ${CMAKE_VARS} -DBUILD_BENCHMARKS=1 -DCMAKE_BUILD_TYPE=Release ../.. && \
	cmake --build . --config Release

amaldebug:
	mkdir -p ./build/amaldebug && \
	$(PYTHON) scripts/amalgamation.py && \
	cd build/amaldebug && \
	cmake $(GENERATOR) $(FORCE_COLOR) ${STATIC_LIBCPP} ${CMAKE_VARS} ${FORCE_32_BIT_FLAG} -DAMALGAMATION_BUILD=1 -DCMAKE_BUILD_TYPE=Debug ../.. && \
	cmake --build . --config Debug

tidy-check:
	mkdir -p ./build/tidy && \
	cd build/tidy && \
	cmake -DCLANG_TIDY=1 -DDISABLE_UNITY=1 -DBUILD_EXTENSIONS=parquet -DBUILD_SHELL=0 ../.. && \
	$(PYTHON) ../../scripts/run-clang-tidy.py -quiet -j $(CI_CPU_COUNT) ${TIDY_BINARY_PARAMETER} ${TIDY_PERFORM_CHECKS}

install-clangd-tidy:
	mkdir -p $(dir $(CLANGD_TIDY_VENV)) && \
	$(PYTHON) -m venv $(CLANGD_TIDY_VENV) && \
	$(CLANGD_TIDY_VENV)/bin/pip install --upgrade 'clangd-tidy==$(CLANGD_TIDY_VERSION)'

tidy-check-clangd:
	mkdir -p ./build/tidy && \
	cd build/tidy && \
	cmake $(GENERATOR) $(FORCE_COLOR) ${STATIC_LIBCPP} ${CMAKE_VARS} -DCLANG_TIDY=1 -DDISABLE_UNITY=1 -DBUILD_EXTENSIONS=parquet -DBUILD_SHELL=0 ../.. && \
	trap 'rm -rf ./pchs' EXIT && \
	$(PYTHON) -u ../../scripts/run-clangd-tidy.py -j $(CI_TIDY_JOBS) ${CLANGD_TIDY_BINARY_PARAMETER} ${CLANGD_BINARY_PARAMETER} ${CLANGD_TIDY_QUERY_DRIVER_PARAMETER}

tidy-check-diff:
	mkdir -p ./build/tidy && \
	cd build/tidy && \
	cmake -DCLANG_TIDY=1 -DDISABLE_UNITY=1 -DBUILD_EXTENSIONS=parquet -DBUILD_SHELL=0 ../.. && \
	cd ../../ && \
	git diff origin/${GIT_BASE_BRANCH} . ':(exclude)tools' ':(exclude)extension' ':(exclude)test' ':(exclude)benchmark' ':(exclude)third_party' ':(exclude)src/common/adbc' ':(exclude)src/main/capi' | $(PYTHON) scripts/clang-tidy-diff.py -path build/tidy -quiet -j $(CI_CPU_COUNT) ${TIDY_BINARY_PARAMETER} ${TIDY_PERFORM_CHECKS} -p1

tidy-fix:
	mkdir -p ./build/tidy && \
	cd build/tidy && \
	cmake -DCLANG_TIDY=1 -DDISABLE_UNITY=1 -DBUILD_EXTENSIONS=parquet -DBUILD_SHELL=0 ../.. && \
	$(PYTHON) ../../scripts/run-clang-tidy.py -fix

test_compile: # test compilation of individual cpp files
	$(PYTHON) scripts/amalgamation.py --compile

format-check:
	$(PYTHON) scripts/format.py --all --check

format-check-silent:
	$(PYTHON) scripts/format.py --all --check --silent

format-fix:
	rm -rf src/amalgamation/*
	$(PYTHON) scripts/format.py --all --fix --noconfirm

.PHONY: check-extension-entries
check-extension-entries: extension_configuration
	$(PYTHON) scripts/generate_extensions_function.py
	$(PYTHON) scripts/format.py src/include/duckdb/main/extension_entries.hpp --fix --noconfirm
	@git diff -- src/include/duckdb/main/extension_entries.hpp > extension_entries.hpp.diff
	@if [ -s extension_entries.hpp.diff ]; then \
		cat extension_entries.hpp.diff; \
		echo "error: differences found in src/include/duckdb/main/extension_entries.hpp"; \
		exit 1; \
	else \
		rm -f extension_entries.hpp.diff; \
		echo "No differences found"; \
	fi

format-head:
	$(PYTHON) scripts/format.py HEAD --fix --noconfirm

format-changes:
	$(PYTHON) scripts/format.py HEAD --fix --noconfirm

format-main:
	$(PYTHON) scripts/format.py main --fix --noconfirm

format-feature:
	$(PYTHON) scripts/format.py feature --fix --noconfirm

format-configs:
	$(foreach file, $(wildcard $(CONFIGS_DIR)/*), jq . < "$(file)" > "$(file).tmp" && mv "$(file).tmp" "$(file)" ;)


third_party/sqllogictest:
	git clone --depth=1 --branch hawkfish-statistical-rounding https://github.com/duckdb/sqllogictest.git third_party/sqllogictest

sqlite: release | third_party/sqllogictest
	git --git-dir third_party/sqllogictest/.git pull
	$(PYTHON) scripts/ci/run_tests.py ./build/release/$(UNITTEST_BINARY) "[sqlitelogic]"

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

# Generate compile commands without actually building
clangd:
	cmake -DCMAKE_BUILD_TYPE=Debug ${CMAKE_VARS} -B .cache/clangd/debug .
	cp .cache/clangd/debug/compile_commands.json .cache/clangd/compile_commands.json

coverage-check:
	./scripts/coverage_check.sh

generate-files-deps:
	pip install cxxheaderparser pcpp

generate-files:
	$(PYTHON) scripts/generate_c_api.py
	$(PYTHON) scripts/generate_functions.py
	$(PYTHON) scripts/generate_settings.py
	$(PYTHON) scripts/generate_serialization.py
	$(PYTHON) scripts/generate_storage_info.py
	$(PYTHON) scripts/generate_metric_enums.py
	$(PYTHON) scripts/generate_enum_util.py
# Run the formatter again after (re)generating the files
	$(MAKE) format-main

bundle-setup:
	cd build/release && \
	rm -rf bundle && \
	mkdir -p bundle && \
	cp src/libduckdb_static.a bundle/. && \
	cp third_party/*/libduckdb_*.a bundle/. && \
	cp extension/libduckdb_generated_extension_loader.a bundle/. && \
	cp extension/*/lib*_extension.a bundle/. && \
	mkdir -p vcpkg_installed && \
	find vcpkg_installed -name '*.a' -exec cp {} bundle/. \; && \
	mkdir -p _deps && \
	if [ -f linked_libs.txt ]; then \
		while IFS= read -r libline || [ -n "$$libline" ]; do \
			find _deps -path "*/$$libline" -exec cp {} bundle/. \; 2>/dev/null || true; \
		done < linked_libs.txt; \
	fi && \
	cd bundle && \
	find . -name '*.a' -exec mkdir -p {}.objects \; -exec mv {} {}.objects \; && \
	find . -name '*.a' -execdir ${AR} -x {} \;

bundle-library-o: bundle-setup
	cd build/release/bundle && \
	echo ./*/*.o | xargs ${AR} cr ../libduckdb_bundle.a

bundle-library-obj: bundle-setup
	cd build/release/bundle && \
	echo ./*/*.obj | xargs ${AR} cr ../libduckdb_bundle.a

bundle-library: release
	make bundle-library-o

gather-libs: release
	cd build/release && \
	rm -rf libs && \
	mkdir -p libs && \
	cp src/libduckdb_static.a libs/. && \
	cp third_party/*/libduckdb_*.a libs/. && \
	cp extension/libduckdb_generated_extension_loader.a libs/. && \
	cp extension/*/lib*_extension.a libs/.

#### Setup VCPKG to correct version 2025.12.12 tag is 84bab45d415d22042bd0b9081aea57f362da3f35
vcpkg/scripts/buildsystems/vcpkg.cmake:
	git -C vcpkg fetch || git clone --branch 2025.12.12 https://github.com/microsoft/vcpkg
	cd vcpkg && ./bootstrap-vcpkg.sh

setup-vcpkg: vcpkg/scripts/buildsystems/vcpkg.cmake
	@echo 'Consider exporting VCPKG_TOOLCHAIN_PATH=$(PWD)/vcpkg/scripts/buildsystems/vcpkg.cmake'

cleanup-vcpkg:
	rm -rf vcpkg

test-utils:
	make release EXTENSION_CONFIGS='.github/config/extensions/httpfs.cmake;.github/config/extensions/test-utils.cmake;.github/config/extensions/inet.cmake' DUCKDB_EXTENSIONS='tpcds;icu;autocomplete;tpch;json'
