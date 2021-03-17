#include "duckdb/execution/operator/helper/physical_load.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/client_context.hpp"

#include <dlfcn.h>

namespace duckdb {

void PhysicalLoad::GetChunkInternal(ExecutionContext &context, DataChunk &chunk, PhysicalOperatorState *state) {
	if (!FileSystem::GetFileSystem(context.client).FileExists(info->filename)) {
		throw InvalidInputException("File %s not found", info->filename);
	}
	auto lib_hdl = dlopen(info->filename.c_str(), RTLD_LAZY | RTLD_LOCAL);
	if (lib_hdl == nullptr) {
		throw InvalidInputException("File %s could not be loaded", info->filename);
	}

	// FIXME
	auto init_fun_name = "loadable_extension_demo_init";

	void (*init_fun)(DatabaseInstance &);
	*(void **)(&init_fun) = dlsym(lib_hdl, init_fun_name);
	if (init_fun == nullptr) {
		throw InvalidInputException("File %s did not contain initialization function %s", info->filename,
		                            init_fun_name);
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
