//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/platform.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once
#include <string>

// duplicated from string_util.h to avoid linking issues
#ifndef DUCKDB_QUOTE_DEFINE
#define DUCKDB_QUOTE_DEFINE_IMPL(x) #x
#define DUCKDB_QUOTE_DEFINE(x)      DUCKDB_QUOTE_DEFINE_IMPL(x)
#endif

#if defined(_WIN32) || defined(__APPLE__) || defined(__FreeBSD__)
#else
#if !defined(_GNU_SOURCE)
#define _GNU_SOURCE
#include <features.h>
#ifndef __USE_GNU
#define __MUSL__
#endif
#undef _GNU_SOURCE /* don't contaminate other includes unnecessarily */
#else
#include <features.h>
#ifndef __USE_GNU
#define __MUSL__
#endif
#endif
#endif

namespace duckdb {

std::string DuckDBPlatform() { // NOLINT: allow definition in header
#if defined(DUCKDB_CUSTOM_PLATFORM)
	return DUCKDB_QUOTE_DEFINE(DUCKDB_CUSTOM_PLATFORM);
#else
#if defined(DUCKDB_WASM_VERSION)
	// DuckDB-Wasm requires CUSTOM_PLATFORM to be defined
	static_assert(0, "DUCKDB_WASM_VERSION should rely on CUSTOM_PLATFORM being provided");
#endif

#if defined(__x86_64__) || defined(_M_X64)
	std::string arch = "amd64";
#elif defined(__i386__) || defined(_M_IX86)
	std::string arch = "i686";
#elif defined(__aarch64__)
	std::string arch = "arm64";
#elif defined(__loongarch64)
	std::string arch = "loong64";
#elif defined(__powerpc64__) && defined(__LITTLE_ENDIAN__)
	std::string arch = "ppc64el";
#elif defined(__riscv) && __riscv_xlen == 64
	std::string arch = "riscv64";
#else
	std::string arch = "unknown_arch";
#endif
	std::string postfix = "";

	std::string os = "linux";
#ifdef _WIN32
	os = "windows";
#elif defined(__APPLE__)
	os = "osx";
#elif defined(__FreeBSD__)
	os = "freebsd";
#endif

#if defined(__MUSL__)
	if (os == "linux") {
		postfix = "_musl";
	}
#elif (!defined(__clang__) && defined(__GNUC__) && __GNUC__ < 5) ||                                                    \
    (defined(_GLIBCXX_USE_CXX11_ABI) && _GLIBCXX_USE_CXX11_ABI == 0)
#error                                                                                                                 \
    "DuckDB does not provide extensions for this (legacy) CXX ABI - Explicitly set DUCKDB_PLATFORM (Makefile) / DUCKDB_EXPLICIT_PLATFORM (CMake) to build anyway. "
#endif

#if defined(__ANDROID__)
	postfix = "_android";
#endif
#ifdef __MINGW32__
	postfix = "_mingw";
#endif
// this is used for the windows R builds which use `mingw` equivalent extensions
#ifdef DUCKDB_PLATFORM_RTOOLS
	postfix = "_mingw";
#endif
	return os + "_" + arch + postfix;
#endif
}

} // namespace duckdb
