#include "duckdb/execution/operator/helper/physical_load.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/client_context.hpp"

#ifndef _WIN32
#include <dlfcn.h>
#else
#define WIN32_LEAN_AND_MEAN
#include <windows.h>
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

void PhysicalLoad::GetChunkInternal(ExecutionContext &context, DataChunk &chunk, PhysicalOperatorState *state) {
	if (!FileSystem::GetFileSystem(context.client).FileExists(info->filename)) {
		throw InvalidInputException("File %s not found", info->filename);
	}
	auto lib_hdl = dlopen(info->filename.c_str(), RTLD_LAZY | RTLD_LOCAL);
	if (lib_hdl == nullptr) {
		throw InvalidInputException("File %s could not be loaded", info->filename);
	}

	std::string sep = "";
	if (StringUtil::Contains(info->filename, "/") && !StringUtil::Contains(info->filename, "\\")) {
		sep = "/";
	} else if (StringUtil::Contains(info->filename, "\\") && !StringUtil::Contains(info->filename, "/")) {
		sep = "\\";
	} else {
		sep = FileSystem::GetFileSystem(context.client).PathSeparator();
	}

	auto vec = StringUtil::Split(StringUtil::Split(info->filename, sep).back(), ".");
	auto basename = vec[0];
	auto init_fun_name = basename + "_init";
	auto version_fun_name = basename + "_version";

	void (*init_fun)(DatabaseInstance &);
	const char *(*version_fun)(void);

	*(void **)(&init_fun) = dlsym(lib_hdl, init_fun_name.c_str());
	if (init_fun == nullptr) {
		throw InvalidInputException("File %s did not contain initialization function %s", info->filename,
		                            init_fun_name);
	}

	*(void **)(&version_fun) = dlsym(lib_hdl, version_fun_name.c_str());
	if (init_fun == nullptr) {
		throw InvalidInputException("File %s did not contain version function %s", info->filename, version_fun_name);
	}
	auto extension_version = std::string((*version_fun)());
	auto engine_version = DuckDB::LibraryVersion();
	if (extension_version != engine_version) {
		throw InvalidInputException("Extension %s version (%s) does not match DuckDB version (%s)", info->filename,
		                            extension_version, engine_version);
	}

	try {
		(*init_fun)(*context.client.db);
	} catch (Exception &e) {
		throw InvalidInputException("Initialization function %s from file %s threw an exception: %s", init_fun_name,
		                            info->filename, e.what());
	}
	state->finished = true;
}

} // namespace duckdb
