#include <string>

// duplicated from string_util.h to avoid linking issues
#ifndef DUCKDB_QUOTE_DEFINE
#define DUCKDB_QUOTE_DEFINE_IMPL(x) #x
#define DUCKDB_QUOTE_DEFINE(x)      DUCKDB_QUOTE_DEFINE_IMPL(x)
#endif

namespace duckdb {

std::string DuckDBPlatform() { // NOLINT: allow definition in header
#if defined(DUCKDB_CUSTOM_PLATFORM)
	return DUCKDB_QUOTE_DEFINE(DUCKDB_CUSTOM_PLATFORM);
#endif
#if defined(DUCKDB_WASM_VERSION)
	// DuckDB-Wasm requires CUSTOM_PLATFORM to be defined
	static_assert(0, "DUCKDB_WASM_VERSION should rely on CUSTOM_PLATFORM being provided");
#endif
	std::string os = "linux";
#if INTPTR_MAX == INT64_MAX
	std::string arch = "amd64";
#elif INTPTR_MAX == INT32_MAX
	std::string arch = "i686";
#else
#error Unknown pointer size or missing size macros!
#endif
	std::string postfix = "";

#ifdef _WIN32
	os = "windows";
#elif defined(__APPLE__)
	os = "osx";
#elif defined(__FreeBSD__)
	os = "freebsd";
#endif
#if defined(__aarch64__) || defined(__ARM_ARCH_ISA_A64)
	arch = "arm64";
#endif

#if !defined(_GLIBCXX_USE_CXX11_ABI) || _GLIBCXX_USE_CXX11_ABI == 0
	if (os == "linux") {
		postfix = "_gcc4";
	}
#endif
#if defined(__ANDROID__)
	postfix += "_android"; // using + because it may also be gcc4
#endif
#ifdef __MINGW32__
	postfix = "_mingw";
#endif
// this is used for the windows R builds which use a separate build environment
#ifdef DUCKDB_PLATFORM_RTOOLS
	postfix = "_rtools";
#endif
	return os + "_" + arch + postfix;
}

} // namespace duckdb
