#include "duckdb/execution/operator/helper/physical_load.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/windows.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/client_context.hpp"
#include "httplib.hpp"

#include <fstream>

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

// TODO add force install / update install

static vector<string> path_components = {".duckdb", "extensions", DuckDB::SourceID(), DuckDB::Platform()};

void PhysicalLoad::DoInstall(ExecutionContext &context) const {
	auto &fs = FileSystem::GetFileSystem(context.client);

	// todo get platform from cmake like jdbc

	string local_path = fs.GetHomeDirectory();
	if (!fs.DirectoryExists(local_path)) {
		throw InternalException("Can't find the home directory at " + local_path);
	}
	for (auto &path_ele : path_components) {
		local_path = fs.JoinPath(local_path, path_ele);
		if (!fs.DirectoryExists(local_path)) {
			fs.CreateDirectory(local_path);
		}
	}

	auto extension_name = fs.ExtractBaseName(info->filename);

	string local_extension_path = fs.JoinPath(local_path, extension_name + ".duckdb_extension");
	if (fs.FileExists(local_extension_path)) {
		return;
	}

	if (fs.FileExists(info->filename)) {
		std::ifstream in(info->filename, std::ios::binary);
		if (in.bad()) {
			throw IOException("Failed to read extension from %s", info->filename);
		}
		std::ofstream out(local_extension_path, std::ios::binary);
		out << in.rdbuf();
		if (out.bad()) {
			throw IOException("Failed to write extension to %s", local_extension_path);
		}
		in.close();
		out.close();
		return;
	} else if (StringUtil::Contains(info->filename, "/")) {
		throw IOException("Failed to read extension from %s", info->filename);
	}

	auto url_local_part = string(string(DuckDB::SourceID()) + "/osx-arm64/" + extension_name + ".duckdb_extension");
	auto url_base = "http://extension.duckdb.org";
	httplib::Client cli(url_base);
	auto res = cli.Get(url_local_part.c_str());
	if (!res || res->status != 200) {
		throw IOException("Failed to download extension %s/%s", url_base, url_local_part);
	}
	std::ofstream out(local_extension_path, std::ios::binary);
	out.write(res->body.c_str(), res->body.size());
	if (out.bad()) {
		throw IOException("Failed to write extension to %s", local_extension_path);
	}
}

void PhysicalLoad::DoLoad(ExecutionContext &context) const {
	auto &fs = FileSystem::GetFileSystem(context.client);
	auto filename = fs.ConvertSeparators(info->filename);

	// shorthand case
	if (!StringUtil::Contains(info->filename, ".") && !StringUtil::Contains(info->filename, fs.PathSeparator())) {
		string local_path = fs.GetHomeDirectory();
		for (auto &path_ele : path_components) {
			local_path = fs.JoinPath(local_path, path_ele);
		}
		filename = fs.JoinPath(local_path, info->filename + ".duckdb_extension");
	}

	if (!fs.FileExists(filename)) {
		throw InvalidInputException("File %s not found", filename);
	}
	auto lib_hdl = dlopen(filename.c_str(), RTLD_LAZY | RTLD_LOCAL);
	if (!lib_hdl) {
		throw InvalidInputException("File %s could not be loaded", filename);
	}

	auto basename = fs.ExtractBaseName(filename);
	auto init_fun_name = basename + "_init";
	auto version_fun_name = basename + "_version";

	void (*init_fun)(DatabaseInstance &);
	const char *(*version_fun)(void);

	*(void **)(&init_fun) = dlsym(lib_hdl, init_fun_name.c_str());
	if (init_fun == nullptr) {
		throw InvalidInputException("File %s did not contain initialization function %s", filename, init_fun_name);
	}

	*(void **)(&version_fun) = dlsym(lib_hdl, version_fun_name.c_str());
	if (init_fun == nullptr) {
		throw InvalidInputException("File %s did not contain version function %s", filename, version_fun_name);
	}
	auto extension_version = std::string((*version_fun)());
	auto engine_version = DuckDB::LibraryVersion();
	if (extension_version != engine_version) {
		throw InvalidInputException("Extension %s version (%s) does not match DuckDB version (%s)", filename,
		                            extension_version, engine_version);
	}

	try {
		(*init_fun)(*context.client.db);
	} catch (Exception &e) {
		throw InvalidInputException("Initialization function %s from file %s threw an exception: %s", init_fun_name,
		                            filename, e.what());
	}
}

void PhysicalLoad::GetData(ExecutionContext &context, DataChunk &chunk, GlobalSourceState &gstate_p,
                           LocalSourceState &lstate) const {
	if (!context.client.db->config.enable_external_access) {
		throw Exception("Loading extensions is disabled");
	}
	if (info->install) {
		DoInstall(context);
	} else {
		DoLoad(context);
	}
}

} // namespace duckdb
