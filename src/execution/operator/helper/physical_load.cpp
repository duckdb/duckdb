#include "duckdb/execution/operator/helper/physical_load.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/windows.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/common/gzip_file_system.hpp"
#include "duckdb/common/dl.hpp"

#include "httplib.hpp"

#include <fstream>
#include "miniz.hpp"

namespace duckdb {

static vector<string> path_components = {".duckdb", "extensions", DuckDB::SourceID(), DuckDB::Platform()};

void PhysicalLoad::DoInstall(ExecutionContext &context) const {
	auto &fs = FileSystem::GetFileSystem(context.client);

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
	if (fs.FileExists(local_extension_path) && info->load_type != LoadType::FORCE_INSTALL) {
		return;
	}

	auto is_http_url = StringUtil::Contains(info->filename, "http://");

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
	} else if (StringUtil::Contains(info->filename, "/") && !is_http_url) {
		throw IOException("Failed to read extension from %s", info->filename);
	}

	string url_template = "http://extensions.duckdb.org/${REVISION}/${PLATFORM}/${NAME}.duckdb_extension.gz";

	if (is_http_url) {
		url_template = info->filename;
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
	if (info->load_type == LoadType::INSTALL || info->load_type == LoadType::FORCE_INSTALL) {
		DoInstall(context);
	} else {
		DoLoad(context);
	}
}

} // namespace duckdb
