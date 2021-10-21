#include "duckdb/execution/operator/helper/physical_load.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/windows.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/client_context.hpp"
#include "httplib.hpp"

#include <fstream>
#include "miniz.hpp"

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

static constexpr const uint8_t GZIP_COMPRESSION_DEFLATE = 0x08;

static constexpr const uint8_t GZIP_FLAG_ASCII = 0x1;
static constexpr const uint8_t GZIP_FLAG_MULTIPART = 0x2;
static constexpr const uint8_t GZIP_FLAG_EXTRA = 0x4;
static constexpr const uint8_t GZIP_FLAG_NAME = 0x8;
static constexpr const uint8_t GZIP_FLAG_COMMENT = 0x10;
static constexpr const uint8_t GZIP_FLAG_ENCRYPT = 0x20;

static constexpr const uint8_t GZIP_HEADER_MINSIZE = 10;

static constexpr const unsigned char GZIP_FLAG_UNSUPPORTED =
    GZIP_FLAG_ASCII | GZIP_FLAG_MULTIPART | GZIP_FLAG_EXTRA | GZIP_FLAG_COMMENT | GZIP_FLAG_ENCRYPT;

static string uncompress_gzip_string(string &in) {
	// decompress file
	auto body_ptr = in.data();

	auto mz_stream_ptr = new duckdb_miniz::mz_stream();
	memset(mz_stream_ptr, 0, sizeof(duckdb_miniz::mz_stream));

	uint8_t gzip_hdr[GZIP_HEADER_MINSIZE];

	// check for incorrectly formatted files
	// LCOV_EXCL_START

	// TODO this is mostly the same as gzip_file_system.cpp
	if (in.size() < GZIP_HEADER_MINSIZE) {
		throw IOException("Input is not a GZIP stream");
	}
	memcpy(gzip_hdr, body_ptr, GZIP_HEADER_MINSIZE);
	body_ptr += GZIP_HEADER_MINSIZE;

	if (gzip_hdr[0] != 0x1F || gzip_hdr[1] != 0x8B) { // magic header
		throw Exception("Input is not a GZIP stream");
	}
	if (gzip_hdr[2] != GZIP_COMPRESSION_DEFLATE) { // compression method
		throw Exception("Unsupported GZIP compression method");
	}
	if (gzip_hdr[3] & GZIP_FLAG_UNSUPPORTED) {
		throw Exception("Unsupported GZIP archive");
	}
	// LCOV_EXCL_STOP

	if (gzip_hdr[3] & GZIP_FLAG_NAME) {
		char c;
		do {
			c = *body_ptr;
			body_ptr++;
		} while (c != '\0' && (idx_t)(body_ptr - in.data()) < in.size());
	}

	// stream is now set to beginning of payload data
	auto status = duckdb_miniz::mz_inflateInit2(mz_stream_ptr, -MZ_DEFAULT_WINDOW_BITS);
	if (status != duckdb_miniz::MZ_OK) {
		throw InternalException("Failed to initialize miniz");
	}

	auto bytes_remaining = in.size() - (body_ptr - in.data());
	mz_stream_ptr->next_in = (unsigned char *)body_ptr;
	mz_stream_ptr->avail_in = bytes_remaining;

	unsigned char decompress_buffer[BUFSIZ];
	string decompressed;

	while (status == duckdb_miniz::MZ_OK) {
		mz_stream_ptr->next_out = decompress_buffer;
		mz_stream_ptr->avail_out = sizeof(decompress_buffer);
		status = mz_inflate(mz_stream_ptr, duckdb_miniz::MZ_NO_FLUSH);
		if (status != duckdb_miniz::MZ_STREAM_END && status != duckdb_miniz::MZ_OK) {
			throw IOException("Failed to uncompress");
		}
		decompressed.append((char *)decompress_buffer, mz_stream_ptr->total_out - decompressed.size());
	}
	duckdb_miniz::mz_inflateEnd(mz_stream_ptr);
	if (decompressed.size() == 0) {
		throw IOException("Failed to uncompress");
	}
	return decompressed;
}

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

	auto url_local_part =
	    string("/" + string(DuckDB::SourceID()) + "/osx-arm64/" + extension_name + ".duckdb_extension.gz");
	auto url_base = "http://extensions.duckdb.org";
	httplib::Client cli(url_base);

	httplib::Headers headers = {{"User-Agent", StringUtil::Format("DuckDB %s %s %s", DuckDB::LibraryVersion(),
	                                                              DuckDB::SourceID(), DuckDB::Platform())}};

	auto res = cli.Get(url_local_part.c_str(), headers);
	if (!res || res->status != 200) {
		throw IOException("Failed to download extension %s%s", url_base, url_local_part);
	}
	auto decompressed_body = uncompress_gzip_string(res->body);
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
	if (info->install) {
		DoInstall(context);
	} else {
		DoLoad(context);
	}
}

} // namespace duckdb
