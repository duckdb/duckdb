#include "duckdb/main/replacement_opens.hpp"
#include "duckdb.hpp"
#include "duckdb/common/virtual_file_system.hpp"
#include "duckdb/main/extension_helper.hpp"

#include "re2/re2.h"

namespace duckdb {
struct ParquetReplacementOpenData : public ReplacementOpenData {
	ParquetReplacementOpenData(string path) : path(path) {
	}
	string path;
};

static unique_ptr<ReplacementOpenData> ParquetReplacementPreOpen(DBConfig &config) {
	FileSystem *fs = nullptr;
	VirtualFileSystem fallback_file_system; // config may not contain one yet
	if (config.file_system) {
		fs = config.file_system.get();
	} else {
		fs = &fallback_file_system;
	}
	// TODO check if parquet extension is installed in the first place?
	auto parquet_path = config.options.database_path;
	if (!fs->FileExists(parquet_path)) {
		return nullptr;
	}
	auto handle = fs->OpenFile(parquet_path, FileFlags::FILE_FLAGS_READ);
	char read_buf[4];
	auto read = handle->Read(read_buf, 4);
	if (read != 4) {
		return nullptr;
	}
	if (memcmp(read_buf, "PAR1", 4)) {
		return nullptr;
	}
	config.options.database_path = string(); // db runs in in-memory mode
	return make_unique<ParquetReplacementOpenData>(parquet_path);
}

static void ParquetReplacementPostOpen(DatabaseInstance &instance, ReplacementOpenData *open_data) {
	if (!open_data) {
		return;
	}
	D_ASSERT(open_data);
	auto parquet_open_data = (ParquetReplacementOpenData *)open_data;

	// TODO make a better parquet file check, maybe by instantiating reader in try/catch?
	DuckDB db(instance);
	// TODO potentially install extension
	ExtensionHelper::LoadExtension(db, "parquet");
	Connection con(db);
	// TODO better name, although I like _
	auto res =
	    con.Query(StringUtil::Format("CREATE VIEW _ AS SELECT * FROM PARQUET_SCAN('%s')", parquet_open_data->path));
	if (res->HasError()) {
		res->ThrowError(); // TODO is an error here appropriate?
	}
}

ParquetReplacementOpen::ParquetReplacementOpen()
    : ReplacementOpen(ParquetReplacementPreOpen, ParquetReplacementPostOpen) {
}

struct ExtensionPrefixOpenData : public ReplacementOpenData {
	ExtensionPrefixOpenData(string extension, string path, unique_ptr<ReplacementOpenData> data)
	    : extension(extension), path(path), data(move(data)) {
	}
	string extension;
	string path;
	unique_ptr<ReplacementOpenData> data;
};

static unique_ptr<ReplacementOpenData> ExtensionPrefixPreOpen(DBConfig &config) {
	auto path = config.options.database_path;
	duckdb_re2::RE2 pattern("([a-zA-Z_]{2,}):.+");
	D_ASSERT(pattern.ok());

	string extension;
	string s;
	if (duckdb_re2::RE2::FullMatch(path, pattern, &s)) {
		extension = s;
	} else {
		return nullptr;
	}

	// TODO at this point we are not yet sure we want to abandon the path yet but well
	//	config.options.database_path = string(); // db runs in in-memory mode

	// TODO should we really use the config we are passed here?
	// dummy instance just to install/load the extension
	DBConfig dummy_config;
	// TODO what other options do we need to pass on
	dummy_config.options.allow_unsigned_extensions = config.options.allow_unsigned_extensions;
	DuckDB db(nullptr, &dummy_config);
	Connection con(db);
	// TODO how do we check if extension is installed?

	auto extension_data = ExtensionHelper::ReplacementOpenPre(*con.context, extension, config);
	return make_unique<ExtensionPrefixOpenData>(extension, path, move(extension_data));
}

static void ExtensionPrefixPostOpen(DatabaseInstance &instance, ReplacementOpenData *open_data) {
	if (!open_data) {
		return;
	}
	D_ASSERT(open_data);
	auto prefix_open_data = (ExtensionPrefixOpenData *)open_data;

	Connection con(instance);
	ExtensionHelper::LoadExternalExtension(*con.context, prefix_open_data->extension);
	ExtensionHelper::ReplacementOpenPost(*con.context, prefix_open_data->extension, instance,
	                                     prefix_open_data->data.get());
}

ExtensionPrefixReplacementOpen::ExtensionPrefixReplacementOpen()
    : ReplacementOpen(ExtensionPrefixPreOpen, ExtensionPrefixPostOpen) {
}

} // namespace duckdb
