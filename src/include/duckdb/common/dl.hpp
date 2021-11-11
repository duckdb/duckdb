#pragma once

#ifndef _WIN32
#include <dlfcn.h>
#else
#define RTLD_LAZY  0
#define RTLD_LOCAL 0
#endif

namespace duckdb {

#ifdef _WIN32

void *dlopen(const char *file, int mode) {
	D_ASSERT(file);
	return (void *)LoadLibrary(file);
}

void *dlsym(void *handle, const char *name) {
	D_ASSERT(handle);
	return (void *)GetProcAddress((HINSTANCE)handle, name);
}
#endif

} // namespace duckdb
