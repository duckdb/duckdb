#include "duckdb/main/extension_helper.hpp"
#include "duckdb/common/dl.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Load External Extension
//===--------------------------------------------------------------------===//
typedef void (*ext_init_fun_t)(DatabaseInstance &);
typedef const char *(*ext_version_fun_t)(void);

template <class T>
static T LoadFunctionFromDLL(void *dll, const string &function_name, const string &filename) {
	auto function = dlsym(dll, function_name.c_str());
	if (!function) {
		throw IOException("File \"%s\" did not contain function \"%s\": %s", filename, function_name, GetDLError());
	}
	return (T)function;
}

void ExtensionHelper::LoadExternalExtension(DatabaseInstance &db, const string &extension) {
	auto &config = DBConfig::GetConfig(db);
	if (!config.enable_external_access) {
		throw PermissionException("Loading external extensions is disabled through configuration");
	}
	auto &fs = FileSystem::GetFileSystem(db);
	auto filename = fs.ConvertSeparators(extension);

	// shorthand case
	if (!StringUtil::Contains(extension, ".") && !StringUtil::Contains(extension, fs.PathSeparator())) {
		string local_path = fs.GetHomeDirectory();
		auto path_components = PathComponents();
		for (auto &path_ele : path_components) {
			local_path = fs.JoinPath(local_path, path_ele);
		}
		filename = fs.JoinPath(local_path, extension + ".duckdb_extension");
	}

	if (!fs.FileExists(filename)) {
		throw IOException("File \"%s\" not found", filename);
	}
	auto lib_hdl = dlopen(filename.c_str(), RTLD_NOW | RTLD_LOCAL);
	if (!lib_hdl) {
		throw IOException("File \"%s\" could not be loaded: %s", filename, GetDLError());
	}

	auto basename = fs.ExtractBaseName(filename);
	auto init_fun_name = basename + "_init";
	auto version_fun_name = basename + "_version";

	ext_init_fun_t init_fun;
	ext_version_fun_t version_fun;

	init_fun = LoadFunctionFromDLL<ext_init_fun_t>(lib_hdl, init_fun_name, filename);
	version_fun = LoadFunctionFromDLL<ext_version_fun_t>(lib_hdl, version_fun_name, filename);

	std::string engine_version = std::string(DuckDB::LibraryVersion());

	auto version_fun_result = (*version_fun)();
	if (version_fun_result == nullptr) {
		throw InvalidInputException("Extension \"%s\" returned a nullptr", filename);
	}
	std::string extension_version = std::string(version_fun_result);

	// Trim v's if necessary
	std::string extension_version_trimmed = extension_version;
	std::string engine_version_trimmed = engine_version;
	if (extension_version.length() > 0 && extension_version[0] == 'v') {
		extension_version_trimmed = extension_version.substr(1);
	}
	if (engine_version.length() > 0 && engine_version[0] == 'v') {
		engine_version_trimmed = engine_version.substr(1);
	}

	if (extension_version_trimmed != engine_version_trimmed) {
		throw InvalidInputException("Extension \"%s\" version (%s) does not match DuckDB version (%s)", filename,
		                            extension_version, engine_version);
	}

	try {
		(*init_fun)(db);
	} catch (std::exception &e) {
		throw InvalidInputException("Initialization function \"%s\" from file \"%s\" threw an exception: \"%s\"",
		                            init_fun_name, filename, e.what());
	}
}

} // namespace duckdb
