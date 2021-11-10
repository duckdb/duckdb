#include "duckdb/main/extension_helper.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/windows.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/common/gzip_file_system.hpp"
#include "duckdb/common/dl.hpp"

#include "httplib.hpp"

#include <fstream>

#ifdef BUILD_ICU_EXTENSION
#include "icu-extension.hpp"
#endif

#ifdef BUILD_PARQUET_EXTENSION
#include "parquet-extension.hpp"
#endif

#ifdef BUILD_TPCH_EXTENSION
#include "tpch-extension.hpp"
#endif

#ifdef BUILD_TPCDS_EXTENSION
#include "tpcds-extension.hpp"
#endif

#ifdef BUILD_FTS_EXTENSION
#include "fts-extension.hpp"
#endif

#ifdef BUILD_HTTPFS_EXTENSION
#include "httpfs-extension.hpp"
#endif

#ifdef BUILD_VISUALIZER_EXTENSION
#include "visualizer-extension.hpp"
#endif

namespace duckdb {

void ExtensionHelper::LoadAllExtensions(DuckDB &db) {
	unordered_set<string> extensions {"parquet", "icu", "tpch", "tpcds", "fts", "httpfs", "visualizer"};
	for (auto &ext : extensions) {
		LoadExtension(db, ext);
	}
}

//===--------------------------------------------------------------------===//
// Load Statically Compiled Extension
//===--------------------------------------------------------------------===//
ExtensionLoadResult ExtensionHelper::LoadExtension(DuckDB &db, const std::string &extension) {
	if (extension == "parquet") {
#ifdef BUILD_PARQUET_EXTENSION
		db.LoadExtension<ParquetExtension>();
#else
		// parquet extension required but not build: skip this test
		return ExtensionLoadResult::NOT_LOADED;
#endif
	} else if (extension == "icu") {
#ifdef BUILD_ICU_EXTENSION
		db.LoadExtension<ICUExtension>();
#else
		// icu extension required but not build: skip this test
		return ExtensionLoadResult::NOT_LOADED;
#endif
	} else if (extension == "tpch") {
#ifdef BUILD_TPCH_EXTENSION
		db.LoadExtension<TPCHExtension>();
#else
		// icu extension required but not build: skip this test
		return ExtensionLoadResult::NOT_LOADED;
#endif
	} else if (extension == "tpcds") {
#ifdef BUILD_TPCDS_EXTENSION
		db.LoadExtension<TPCDSExtension>();
#else
		// icu extension required but not build: skip this test
		return ExtensionLoadResult::NOT_LOADED;
#endif
	} else if (extension == "fts") {
#ifdef BUILD_FTS_EXTENSION
		db.LoadExtension<FTSExtension>();
#else
		// fts extension required but not build: skip this test
		return ExtensionLoadResult::NOT_LOADED;
#endif
	} else if (extension == "httpfs") {
#ifdef BUILD_HTTPFS_EXTENSION
		db.LoadExtension<HTTPFsExtension>();
#else
		return ExtensionLoadResult::NOT_LOADED;
#endif
	} else if (extension == "visualizer") {
#ifdef BUILD_VISUALIZER_EXTENSION
		db.LoadExtension<VisualizerExtension>();
#else
		// visualizer extension required but not build: skip this test
		return ExtensionLoadResult::NOT_LOADED;
#endif
	} else {
		// unknown extension
		return ExtensionLoadResult::EXTENSION_UNKNOWN;
	}
	return ExtensionLoadResult::LOADED_EXTENSION;
}

//===--------------------------------------------------------------------===//
// Install Extension
//===--------------------------------------------------------------------===//
static vector<string> path_components = {".duckdb", "extensions", DuckDB::SourceID(), DuckDB::Platform()};

void ExtensionHelper::InstallExtension(DatabaseInstance &db, const string &extension, bool force_install) {
	auto &config = DBConfig::GetConfig(db);
	if (!config.enable_external_access) {
		throw Exception("Installing extensions is disabled");
	}
	auto &fs = FileSystem::GetFileSystem(db);

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

	auto extension_name = fs.ExtractBaseName(extension);

	string local_extension_path = fs.JoinPath(local_path, extension_name + ".duckdb_extension");
	if (fs.FileExists(local_extension_path) && !force_install) {
		return;
	}

	auto is_http_url = StringUtil::Contains(extension, "http://");
	if (fs.FileExists(extension)) {
		std::ifstream in(extension, std::ios::binary);
		if (in.bad()) {
			throw IOException("Failed to read extension from \"%s\"", extension);
		}
		std::ofstream out(local_extension_path, std::ios::binary);
		out << in.rdbuf();
		if (out.bad()) {
			throw IOException("Failed to write extension to \"%s\"", local_extension_path);
		}
		in.close();
		out.close();
		return;
	} else if (StringUtil::Contains(extension, "/") && !is_http_url) {
		throw IOException("Failed to read extension from \"%s\": no such file", extension);
	}

	string url_template = "http://extensions.duckdb.org/${REVISION}/${PLATFORM}/${NAME}.duckdb_extension.gz";

	if (is_http_url) {
		url_template = extension;
		extension_name = "";
	}

	auto url = StringUtil::Replace(url_template, "${REVISION}", DuckDB::SourceID());
	url = StringUtil::Replace(url, "${PLATFORM}", DuckDB::Platform());
	url = StringUtil::Replace(url, "${NAME}", extension_name);

	string no_http = StringUtil::Replace(url, "http://", "");

	idx_t next = no_http.find('/', 0);
	if (next == string::npos) {
		throw IOException("No slash in URL template");
	}

	// Push the substring [last, next) on to splits
	auto hostname_without_http = no_http.substr(0, next);
	auto url_local_part = no_http.substr(next);

	auto url_base = "http://" + hostname_without_http;
	duckdb_httplib::Client cli(url_base.c_str());

	duckdb_httplib::Headers headers = {{"User-Agent", StringUtil::Format("DuckDB %s %s %s", DuckDB::LibraryVersion(),
	                                                              DuckDB::SourceID(), DuckDB::Platform())}};

	auto res = cli.Get(url_local_part.c_str(), headers);
	if (!res || res->status != 200) {
		throw IOException("Failed to download extension %s%s", url_base, url_local_part);
	}
	auto decompressed_body = GZipFileSystem::UncompressGZIPString(res->body);
	std::ofstream out(local_extension_path, std::ios::binary);
	out.write(decompressed_body.data(), decompressed_body.size());
	if (out.bad()) {
		throw IOException("Failed to write extension to %s", local_extension_path);
	}
}

//===--------------------------------------------------------------------===//
// Load External Extension
//===--------------------------------------------------------------------===//
template<class T>
static T LoadFunctionFromDLL(void *dll, const string &function_name, const string &filename) {
	auto function = dlsym(dll, function_name.c_str());
	if (!function) {
		throw IOException("File \"%s\" did not contain function \"%s\"", filename, function_name);
	}
	return (T) function;
}

void ExtensionHelper::LoadExternalExtension(DatabaseInstance &db, const string &extension) {
	auto &config = DBConfig::GetConfig(db);
	if (!config.enable_external_access) {
		throw Exception("Loading external extensions is disabled");
	}
	auto &fs = FileSystem::GetFileSystem(db);
	auto filename = fs.ConvertSeparators(extension);

	// shorthand case
	if (!StringUtil::Contains(extension, ".") && !StringUtil::Contains(extension, fs.PathSeparator())) {
		string local_path = fs.GetHomeDirectory();
		for (auto &path_ele : path_components) {
			local_path = fs.JoinPath(local_path, path_ele);
		}
		filename = fs.JoinPath(local_path, extension + ".duckdb_extension");
	}

	if (!fs.FileExists(filename)) {
		throw IOException("File \"%s\" not found", filename);
	}
	auto lib_hdl = dlopen(filename.c_str(), RTLD_LAZY | RTLD_LOCAL);
	if (!lib_hdl) {
		throw IOException("File \"%s\" could not be loaded", filename);
	}

	auto basename = fs.ExtractBaseName(filename);
	auto init_fun_name = basename + "_init";
	auto version_fun_name = basename + "_version";

	typedef void (*ext_init_fun_t)(DatabaseInstance &);
	typedef const char *(*ext_version_fun_t)(void);

	ext_init_fun_t init_fun;
	ext_version_fun_t version_fun;

	init_fun = LoadFunctionFromDLL<ext_init_fun_t>(lib_hdl, init_fun_name, filename);
	version_fun = LoadFunctionFromDLL<ext_version_fun_t>(lib_hdl, version_fun_name, filename);

	auto extension_version = std::string((*version_fun)());
	auto engine_version = DuckDB::LibraryVersion();
	if (extension_version != engine_version) {
		throw InvalidInputException("Extension \"%s\" version (%s) does not match DuckDB version (%s)", filename,
		                            extension_version, engine_version);
	}

	try {
		(*init_fun)(db);
	} catch (std::exception &e) {
		throw InvalidInputException("Initialization function \"%s\" from file \"%s\" threw an exception: \"%s\"", init_fun_name,
		                            filename, e.what());
	}
}

} // namespace duckdb
