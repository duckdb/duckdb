//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/dl.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/windows.hpp"
#include "duckdb/common/local_file_system.hpp"
#include "duckdb/common/windows_util.hpp"

#ifndef _WIN32
#include <dlfcn.h>
#else
#define RTLD_NOW   0
#define RTLD_LOCAL 0
#define RTLD_LAZY  0
#endif

namespace duckdb {

#ifdef _WIN32

inline void *dlopen(const char *file, int mode) {
	if (!file) {
		// If nullptr, return handle of main executable
		return (void *)GetModuleHandle(nullptr);
	}
	auto fpath = WindowsUtil::UTF8ToUnicode(file);
	return (void *)LoadLibraryW(fpath.c_str());
}

inline void *dlsym(void *handle, const char *name) {
	D_ASSERT(handle);
	return (void *)GetProcAddress((HINSTANCE)handle, name);
}

inline void dlclose(void *handle) {
	D_ASSERT(handle);
	FreeLibrary((HINSTANCE)handle);
}

inline std::string GetDLError(void) {
	return LocalFileSystem::GetLastErrorAsString();
}

#else

inline std::string GetDLError(void) {
	return dlerror();
}

#endif

} // namespace duckdb
