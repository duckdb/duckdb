#include "duckdb/main/extension_helper.hpp"
#include "duckdb/common/gzip_file_system.hpp"

#ifndef DISABLE_DUCKDB_REMOTE_INSTALL
#include "httplib.hpp"
#endif
#include "duckdb/common/windows_undefs.hpp"

#include <fstream>

namespace duckdb {

//===--------------------------------------------------------------------===//
// Install Extension
//===--------------------------------------------------------------------===//
const vector<string> ExtensionHelper::PathComponents() {
	return vector<string> {".duckdb", "extensions", DuckDB::SourceID(), DuckDB::Platform()};
}

void ExtensionHelper::InstallExtension(DatabaseInstance &db, const string &extension, bool force_install) {
	auto &config = DBConfig::GetConfig(db);
	if (!config.enable_external_access) {
		throw PermissionException("Installing extensions is disabled through configuration");
	}
	auto &fs = FileSystem::GetFileSystem(db);

	string local_path = fs.GetHomeDirectory();
	if (!fs.DirectoryExists(local_path)) {
		throw InternalException("Can't find the home directory at " + local_path);
	}
	auto path_components = PathComponents();
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

#ifdef DISABLE_DUCKDB_REMOTE_INSTALL
	throw BinderException("Remote extension installation is disabled through configuration");
#else
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
#endif
}

} // namespace duckdb
