#include "duckdb/main/extension_helper.hpp"

#include "duckdb/common/file_system.hpp"
#include "duckdb/common/serializer/binary_deserializer.hpp"
#include "duckdb/common/serializer/buffered_file_reader.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/windows.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/extension.hpp"
#include "duckdb/main/extension_install_info.hpp"

// Note that c++ preprocessor doesn't have a nice way to clean this up so we need to set the defines we use to false
// explicitly when they are undefined
#ifndef DUCKDB_EXTENSION_CORE_FUNCTIONS_LINKED
#define DUCKDB_EXTENSION_CORE_FUNCTIONS_LINKED false
#endif

#ifndef DUCKDB_EXTENSION_ICU_LINKED
#define DUCKDB_EXTENSION_ICU_LINKED false
#endif

#ifndef DUCKDB_EXTENSION_EXCEL_LINKED
#define DUCKDB_EXTENSION_EXCEL_LINKED false
#endif

#ifndef DUCKDB_EXTENSION_PARQUET_LINKED
#define DUCKDB_EXTENSION_PARQUET_LINKED false
#endif

#ifndef DUCKDB_EXTENSION_TPCH_LINKED
#define DUCKDB_EXTENSION_TPCH_LINKED false
#endif

#ifndef DUCKDB_EXTENSION_TPCDS_LINKED
#define DUCKDB_EXTENSION_TPCDS_LINKED false
#endif

#ifndef DUCKDB_EXTENSION_HTTPFS_LINKED
#define DUCKDB_EXTENSION_HTTPFS_LINKED false
#endif

#ifndef DUCKDB_EXTENSION_JSON_LINKED
#define DUCKDB_EXTENSION_JSON_LINKED false
#endif

#ifndef DUCKDB_EXTENSION_JEMALLOC_LINKED
#define DUCKDB_EXTENSION_JEMALLOC_LINKED false
#endif

#ifndef DUCKDB_EXTENSION_AUTOCOMPLETE_LINKED
#define DUCKDB_EXTENSION_AUTOCOMPLETE_LINKED false
#endif

// Load the generated header file containing our list of extension headers
#if defined(GENERATED_EXTENSION_HEADERS) && GENERATED_EXTENSION_HEADERS && !defined(DUCKDB_AMALGAMATION)
#include "duckdb/main/extension/generated_extension_loader.hpp"
#else
// TODO: rewrite package_build.py to allow also loading out-of-tree extensions in non-cmake builds, after that
//		 these can be removed
#if DUCKDB_EXTENSION_CORE_FUNCTIONS_LINKED
#include "core_functions_extension.hpp"
#endif

#if DUCKDB_EXTENSION_ICU_LINKED
#include "icu_extension.hpp"
#endif

#if DUCKDB_EXTENSION_PARQUET_LINKED
#include "parquet_extension.hpp"
#endif

#if DUCKDB_EXTENSION_TPCH_LINKED
#include "tpch_extension.hpp"
#endif

#if DUCKDB_EXTENSION_TPCDS_LINKED
#include "tpcds_extension.hpp"
#endif

#if DUCKDB_EXTENSION_JSON_LINKED
#include "json_extension.hpp"
#endif

#if DUCKDB_EXTENSION_JEMALLOC_LINKED
#include "jemalloc_extension.hpp"
#endif

#if DUCKDB_EXTENSION_AUTOCOMPLETE_LINKED
#include "autocomplete_extension.hpp"
#endif

#endif

namespace duckdb {

//===--------------------------------------------------------------------===//
// Default Extensions
//===--------------------------------------------------------------------===//
static const DefaultExtension internal_extensions[] = {
    {"core_functions", "Core function library", DUCKDB_EXTENSION_CORE_FUNCTIONS_LINKED},
    {"icu", "Adds support for time zones and collations using the ICU library", DUCKDB_EXTENSION_ICU_LINKED},
    {"excel", "Adds support for Excel-like format strings", DUCKDB_EXTENSION_EXCEL_LINKED},
    {"parquet", "Adds support for reading and writing parquet files", DUCKDB_EXTENSION_PARQUET_LINKED},
    {"tpch", "Adds TPC-H data generation and query support", DUCKDB_EXTENSION_TPCH_LINKED},
    {"tpcds", "Adds TPC-DS data generation and query support", DUCKDB_EXTENSION_TPCDS_LINKED},
    {"httpfs", "Adds support for reading and writing files over a HTTP(S) connection", DUCKDB_EXTENSION_HTTPFS_LINKED},
    {"json", "Adds support for JSON operations", DUCKDB_EXTENSION_JSON_LINKED},
    {"jemalloc", "Overwrites system allocator with JEMalloc", DUCKDB_EXTENSION_JEMALLOC_LINKED},
    {"autocomplete", "Adds support for autocomplete in the shell", DUCKDB_EXTENSION_AUTOCOMPLETE_LINKED},
    {"motherduck", "Enables motherduck integration with the system", false},
    {"mysql_scanner", "Adds support for connecting to a MySQL database", false},
    {"sqlite_scanner", "Adds support for reading and writing SQLite database files", false},
    {"postgres_scanner", "Adds support for connecting to a Postgres database", false},
    {"inet", "Adds support for IP-related data types and functions", false},
    {"spatial", "Geospatial extension that adds support for working with spatial data and functions", false},
    {"aws", "Provides features that depend on the AWS SDK", false},
    {"arrow", "A zero-copy data integration between Apache Arrow and DuckDB", false},
    {"azure", "Adds a filesystem abstraction for Azure blob storage to DuckDB", false},
    {"iceberg", "Adds support for Apache Iceberg", false},
    {"vss", "Adds indexing support to accelerate Vector Similarity Search", false},
    {"delta", "Adds support for Delta Lake", false},
    {"fts", "Adds support for Full-Text Search Indexes", false},
    {nullptr, nullptr, false}};

idx_t ExtensionHelper::DefaultExtensionCount() {
	idx_t index;
	for (index = 0; internal_extensions[index].name != nullptr; index++) {
	}
	return index;
}

DefaultExtension ExtensionHelper::GetDefaultExtension(idx_t index) {
	D_ASSERT(index < DefaultExtensionCount());
	return internal_extensions[index];
}

//===--------------------------------------------------------------------===//
// Allow Auto-Install Extensions
//===--------------------------------------------------------------------===//
static const char *const auto_install[] = {"motherduck", "postgres_scanner", "mysql_scanner", "sqlite_scanner",
                                           "delta",      "iceberg",          "uc_catalog",    nullptr};

// TODO: unify with new autoload mechanism
bool ExtensionHelper::AllowAutoInstall(const string &extension) {
	auto extension_name = ApplyExtensionAlias(extension);
	for (idx_t i = 0; auto_install[i]; i++) {
		if (extension_name == auto_install[i]) {
			return true;
		}
	}
	return false;
}

bool ExtensionHelper::CanAutoloadExtension(const string &ext_name) {
#ifdef DUCKDB_DISABLE_EXTENSION_LOAD
	return false;
#endif

	if (ext_name.empty()) {
		return false;
	}
	for (const auto &ext : AUTOLOADABLE_EXTENSIONS) {
		if (ext_name == ext) {
			return true;
		}
	}
	return false;
}

string ExtensionHelper::AddExtensionInstallHintToErrorMsg(ClientContext &context, const string &base_error,
                                                          const string &extension_name) {

	return AddExtensionInstallHintToErrorMsg(DatabaseInstance::GetDatabase(context), base_error, extension_name);
}
string ExtensionHelper::AddExtensionInstallHintToErrorMsg(DatabaseInstance &db, const string &base_error,
                                                          const string &extension_name) {
	string install_hint;

	auto &config = db.config;

	if (!ExtensionHelper::CanAutoloadExtension(extension_name)) {
		install_hint = "Please try installing and loading the " + extension_name + " extension:\nINSTALL " +
		               extension_name + ";\nLOAD " + extension_name + ";\n\n";
	} else if (!config.options.autoload_known_extensions) {
		install_hint =
		    "Please try installing and loading the " + extension_name + " extension by running:\nINSTALL " +
		    extension_name + ";\nLOAD " + extension_name +
		    ";\n\nAlternatively, consider enabling auto-install "
		    "and auto-load by running:\nSET autoinstall_known_extensions=1;\nSET autoload_known_extensions=1;";
	} else if (!config.options.autoinstall_known_extensions) {
		install_hint =
		    "Please try installing the " + extension_name + " extension by running:\nINSTALL " + extension_name +
		    ";\n\nAlternatively, consider enabling autoinstall by running:\nSET autoinstall_known_extensions=1;";
	}

	if (!install_hint.empty()) {
		return base_error + "\n\n" + install_hint;
	}

	return base_error;
}

bool ExtensionHelper::TryAutoLoadExtension(ClientContext &context, const string &extension_name) noexcept {
	if (context.db->ExtensionIsLoaded(extension_name)) {
		return true;
	}
	auto &dbconfig = DBConfig::GetConfig(context);
	try {
		if (dbconfig.options.autoinstall_known_extensions) {
			auto &config = DBConfig::GetConfig(context);
			auto autoinstall_repo = ExtensionRepository::GetRepositoryByUrl(
			    StringValue::Get(config.GetSetting<AutoinstallExtensionRepositorySetting>(context)));
			ExtensionInstallOptions options;
			options.repository = autoinstall_repo;
			ExtensionHelper::InstallExtension(context, extension_name, options);
		}
		ExtensionHelper::LoadExternalExtension(context, extension_name);
		return true;
	} catch (...) {
		return false;
	}
}

bool ExtensionHelper::TryAutoLoadExtension(DatabaseInstance &instance, const string &extension_name) noexcept {
	if (instance.ExtensionIsLoaded(extension_name)) {
		return true;
	}
	auto &dbconfig = DBConfig::GetConfig(instance);
	try {
		auto &fs = FileSystem::GetFileSystem(instance);
		if (dbconfig.options.autoinstall_known_extensions) {
			auto autoinstall_repo =
			    ExtensionRepository::GetRepositoryByUrl(dbconfig.options.autoinstall_extension_repo);
			ExtensionInstallOptions options;
			options.repository = autoinstall_repo;
			ExtensionHelper::InstallExtension(instance, fs, extension_name, options);
		}
		ExtensionHelper::LoadExternalExtension(instance, fs, extension_name);
		return true;
	} catch (...) {
		return false;
	}
}

static ExtensionUpdateResult UpdateExtensionInternal(ClientContext &context, DatabaseInstance &db, FileSystem &fs,
                                                     const string &full_extension_path, const string &extension_name) {
	ExtensionUpdateResult result;
	result.extension_name = extension_name;

	auto &config = DBConfig::GetConfig(db);

	if (!fs.FileExists(full_extension_path)) {
		result.tag = ExtensionUpdateResultTag::NOT_INSTALLED;
		return result;
	}

	// Extension exists, check for .info file
	const string info_file_path = full_extension_path + ".info";
	if (!fs.FileExists(info_file_path)) {
		result.tag = ExtensionUpdateResultTag::MISSING_INSTALL_INFO;
		return result;
	}

	// Parse the version of the extension before updating
	auto ext_binary_handle = fs.OpenFile(full_extension_path, FileOpenFlags::FILE_FLAGS_READ);
	auto parsed_metadata = ExtensionHelper::ParseExtensionMetaData(*ext_binary_handle);
	if (!parsed_metadata.AppearsValid() && !config.options.allow_extensions_metadata_mismatch) {
		throw IOException(
		    "Failed to update extension: '%s', the metadata of the extension appears invalid! To resolve this, either "
		    "reinstall the extension using 'FORCE INSTALL %s', manually remove the file '%s', or enable '"
		    "SET allow_extensions_metadata_mismatch=true'",
		    extension_name, extension_name, full_extension_path);
	}

	result.prev_version = parsed_metadata.AppearsValid() ? parsed_metadata.extension_version : "";

	auto extension_install_info = ExtensionInstallInfo::TryReadInfoFile(fs, info_file_path, extension_name);

	// Early out: no info file found
	if (extension_install_info->mode == ExtensionInstallMode::UNKNOWN) {
		result.tag = ExtensionUpdateResultTag::MISSING_INSTALL_INFO;
		return result;
	}

	// Early out: we can only update extensions from repositories
	if (extension_install_info->mode != ExtensionInstallMode::REPOSITORY) {
		result.tag = ExtensionUpdateResultTag::NOT_A_REPOSITORY;
		result.installed_version = result.prev_version;
		return result;
	}

	auto repository_from_info = ExtensionRepository::GetRepositoryByUrl(extension_install_info->repository_url);
	result.repository = repository_from_info.ToReadableString();

	// Force install the full url found in this file, enabling etags to ensure efficient updating
	ExtensionInstallOptions options;
	options.repository = repository_from_info;
	options.force_install = true;
	options.use_etags = true;

	unique_ptr<ExtensionInstallInfo> install_result;
	try {
		install_result = ExtensionHelper::InstallExtension(context, extension_name, options);
	} catch (std::exception &e) {
		ErrorData error(e);
		error.Throw("Extension updating failed when trying to install '" + extension_name + "', original error: ");
	}

	result.installed_version = install_result->version;

	if (result.installed_version.empty()) {
		result.tag = ExtensionUpdateResultTag::REDOWNLOADED;
	} else if (result.installed_version != result.prev_version) {
		result.tag = ExtensionUpdateResultTag::UPDATED;
	} else {
		result.tag = ExtensionUpdateResultTag::NO_UPDATE_AVAILABLE;
	}

	return result;
}

vector<ExtensionUpdateResult> ExtensionHelper::UpdateExtensions(ClientContext &context) {
	auto &fs = FileSystem::GetFileSystem(context);

	vector<ExtensionUpdateResult> result;
	DatabaseInstance &db = DatabaseInstance::GetDatabase(context);

#ifndef WASM_LOADABLE_EXTENSIONS
	case_insensitive_set_t seen_extensions;

	// scan the install directory for installed extensions
	auto ext_directory = ExtensionHelper::ExtensionDirectory(db, fs);
	fs.ListFiles(ext_directory, [&](const string &path, bool is_directory) {
		if (!StringUtil::EndsWith(path, ".duckdb_extension")) {
			return;
		}

		auto extension_file_name = StringUtil::GetFileName(path);
		auto extension_name = StringUtil::Split(extension_file_name, ".")[0];

		seen_extensions.insert(extension_name);

		result.push_back(UpdateExtensionInternal(context, db, fs, fs.JoinPath(ext_directory, path), extension_name));
	});
#endif

	return result;
}

ExtensionUpdateResult ExtensionHelper::UpdateExtension(ClientContext &context, const string &extension_name) {
	auto &fs = FileSystem::GetFileSystem(context);
	DatabaseInstance &db = DatabaseInstance::GetDatabase(context);
	auto ext_directory = ExtensionHelper::ExtensionDirectory(db, fs);

	auto full_extension_path = fs.JoinPath(ext_directory, extension_name + ".duckdb_extension");

	auto update_result = UpdateExtensionInternal(context, db, fs, full_extension_path, extension_name);

	if (update_result.tag == ExtensionUpdateResultTag::NOT_INSTALLED) {
		throw InvalidInputException("Failed to update the extension '%s', the extension is not installed!",
		                            extension_name);
	} else if (update_result.tag == ExtensionUpdateResultTag::UNKNOWN) {
		throw InternalException("Failed to update extension '%s', an unknown error occurred", extension_name);
	}
	return update_result;
}

void ExtensionHelper::AutoLoadExtension(ClientContext &context, const string &extension_name) {
	return ExtensionHelper::AutoLoadExtension(*context.db, extension_name);
}

void ExtensionHelper::AutoLoadExtension(DatabaseInstance &db, const string &extension_name) {
	if (db.ExtensionIsLoaded(extension_name)) {
		// Avoid downloading again
		return;
	}
	auto &dbconfig = DBConfig::GetConfig(db);
	try {
		auto fs = FileSystem::CreateLocal();
#ifndef DUCKDB_WASM
		if (dbconfig.options.autoinstall_known_extensions) {
			//! Get the autoloading repository
			auto repository = ExtensionRepository::GetRepositoryByUrl(dbconfig.options.autoinstall_extension_repo);
			ExtensionInstallOptions options;
			options.repository = repository;
			ExtensionHelper::InstallExtension(db, *fs, extension_name, options);
		}
#endif
		ExtensionHelper::LoadExternalExtension(db, *fs, extension_name);
		DUCKDB_LOG_INFO(db, "duckdb.Extensions.ExtensionAutoloaded", extension_name);

	} catch (std::exception &e) {
		ErrorData error(e);
		throw AutoloadException(extension_name, error.RawMessage());
	}
}

//===--------------------------------------------------------------------===//
// Load Statically Compiled Extension
//===--------------------------------------------------------------------===//
void ExtensionHelper::LoadAllExtensions(DuckDB &db) {
	// The in-tree extensions that we check. Non-cmake builds are currently limited to these for static linking
	// TODO: rewrite package_build.py to allow also loading out-of-tree extensions in non-cmake builds, after that
	//		 these can be removed
	vector<string> extensions {"parquet", "icu",  "tpch",     "tpcds",        "httpfs",        "json",
	                           "excel",   "inet", "jemalloc", "autocomplete", "core_functions"};
	for (auto &ext : extensions) {
		LoadExtensionInternal(db, ext, true);
	}

#if defined(GENERATED_EXTENSION_HEADERS) && GENERATED_EXTENSION_HEADERS
	for (const auto &ext : LinkedExtensions()) {
		LoadExtensionInternal(db, ext, true);
	}
#endif
}

ExtensionLoadResult ExtensionHelper::LoadExtension(DuckDB &db, const std::string &extension) {
	return LoadExtensionInternal(db, extension, false);
}

ExtensionLoadResult ExtensionHelper::LoadExtensionInternal(DuckDB &db, const std::string &extension,
                                                           bool initial_load) {
#ifdef DUCKDB_TEST_REMOTE_INSTALL
	if (!initial_load && StringUtil::Contains(DUCKDB_TEST_REMOTE_INSTALL, extension)) {
		Connection con(db);
		auto result = con.Query("INSTALL " + extension);
		if (result->HasError()) {
			result->Print();
			return ExtensionLoadResult::EXTENSION_UNKNOWN;
		}
		result = con.Query("LOAD " + extension);
		if (result->HasError()) {
			result->Print();
			return ExtensionLoadResult::EXTENSION_UNKNOWN;
		}
		return ExtensionLoadResult::LOADED_EXTENSION;
	}
#endif

#ifdef DUCKDB_EXTENSIONS_TEST_WITH_LOADABLE
	// Note: weird comma's are on purpose to do easy string contains on a list of extension names
	if (!initial_load && StringUtil::Contains(DUCKDB_EXTENSIONS_TEST_WITH_LOADABLE, "," + extension + ",")) {
		Connection con(db);
		auto result = con.Query((string) "LOAD '" + DUCKDB_EXTENSIONS_BUILD_PATH + "/" + extension + "/" + extension +
		                        ".duckdb_extension'");
		if (result->HasError()) {
			result->Print();
			return ExtensionLoadResult::EXTENSION_UNKNOWN;
		}
		return ExtensionLoadResult::LOADED_EXTENSION;
	}
#endif

	// This is the main extension loading mechanism that loads the extension that are statically linked.
#if defined(GENERATED_EXTENSION_HEADERS) && GENERATED_EXTENSION_HEADERS
	if (TryLoadLinkedExtension(db, extension)) {
		return ExtensionLoadResult::LOADED_EXTENSION;
	} else {
		return ExtensionLoadResult::NOT_LOADED;
	}
#endif

	// This is the fallback to the "old" extension loading mechanism for non-cmake builds
	// TODO: rewrite package_build.py to allow also loading out-of-tree extensions in non-cmake builds
	if (extension == "parquet") {
#if DUCKDB_EXTENSION_PARQUET_LINKED
		db.LoadStaticExtension<ParquetExtension>();
#else
		// parquet extension required but not build: skip this test
		return ExtensionLoadResult::NOT_LOADED;
#endif
	} else if (extension == "icu") {
#if DUCKDB_EXTENSION_ICU_LINKED
		db.LoadStaticExtension<IcuExtension>();
#else
		// icu extension required but not build: skip this test
		return ExtensionLoadResult::NOT_LOADED;
#endif
	} else if (extension == "tpch") {
#if DUCKDB_EXTENSION_TPCH_LINKED
		db.LoadStaticExtension<TpchExtension>();
#else
		// icu extension required but not build: skip this test
		return ExtensionLoadResult::NOT_LOADED;
#endif
	} else if (extension == "tpcds") {
#if DUCKDB_EXTENSION_TPCDS_LINKED
		db.LoadStaticExtension<TpcdsExtension>();
#else
		// icu extension required but not build: skip this test
		return ExtensionLoadResult::NOT_LOADED;
#endif
	} else if (extension == "httpfs") {
#if DUCKDB_EXTENSION_HTTPFS_LINKED
		db.LoadStaticExtension<HttpfsExtension>();
#else
		return ExtensionLoadResult::NOT_LOADED;
#endif
	} else if (extension == "json") {
#if DUCKDB_EXTENSION_JSON_LINKED
		db.LoadStaticExtension<JsonExtension>();
#else
		// json extension required but not build: skip this test
		return ExtensionLoadResult::NOT_LOADED;
#endif
	} else if (extension == "excel") {
#if DUCKDB_EXTENSION_EXCEL_LINKED
		db.LoadStaticExtension<ExcelExtension>();
#else
		// excel extension required but not build: skip this test
		return ExtensionLoadResult::NOT_LOADED;
#endif
	} else if (extension == "jemalloc") {
#if DUCKDB_EXTENSION_JEMALLOC_LINKED
		db.LoadStaticExtension<JemallocExtension>();
#else
		// jemalloc extension required but not build: skip this test
		return ExtensionLoadResult::NOT_LOADED;
#endif
	} else if (extension == "autocomplete") {
#if DUCKDB_EXTENSION_AUTOCOMPLETE_LINKED
		db.LoadStaticExtension<AutocompleteExtension>();
#else
		// autocomplete extension required but not build: skip this test
		return ExtensionLoadResult::NOT_LOADED;
#endif
	} else if (extension == "inet") {
#if DUCKDB_EXTENSION_INET_LINKED
		db.LoadStaticExtension<InetExtension>();
#else
		// inet extension required but not build: skip this test
		return ExtensionLoadResult::NOT_LOADED;
#endif
	} else if (extension == "core_functions") {
#if DUCKDB_EXTENSION_CORE_FUNCTIONS_LINKED
		db.LoadStaticExtension<CoreFunctionsExtension>();
#else
		// core_functions extension required but not build: skip this test
		return ExtensionLoadResult::NOT_LOADED;
#endif
	}

	return ExtensionLoadResult::LOADED_EXTENSION;
}

static const char *const public_keys[] = {
    R"(
-----BEGIN PUBLIC KEY-----
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA6aZuHUa1cLR9YDDYaEfi
UDbWY8m2t7b71S+k1ZkXfHqu+5drAxm+dIDzdOHOKZSIdwnJbT3sSqwFoG6PlXF3
g3dsJjax5qESIhbVvf98nyipwNINxoyHCkcCIPkX17QP2xpnT7V59+CqcfDJXLqB
ymjqoFSlaH8dUCHybM4OXlWnAtVHW/nmw0khF8CetcWn4LxaTUHptByaBz8CasSs
gWpXgSfaHc3R9eArsYhtsVFGyL/DEWgkEHWolxY3Llenhgm/zOf3s7PsAMe7EJX4
qlSgiXE6OVBXnqd85z4k20lCw/LAOe5hoTMmRWXIj74MudWe2U91J6GrrGEZa7zT
7QIDAQAB
-----END PUBLIC KEY-----
)",
    R"(
-----BEGIN PUBLIC KEY-----
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAq8Gg1S/LI6ApMAYsFc9m
PrkFIY+nc0LXSpxm77twU8D5M0Xkz/Av4f88DQmj1OE3164bEtR7sl7xDPZojFHj
YYyucJxEI97l5OU1d3Pc1BdKXL4+mnW5FlUGj218u8qD+G1hrkySXQkrUzIjPPNw
o6knF3G/xqQF+KI+tc7ajnTni8CAlnUSxfnstycqbVS86m238PLASVPK9/SmIRgO
XCEV+ZNMlerq8EwsW4cJPHH0oNVMcaG+QT4z79roW1rbJghn9ubAVdQU6VLUAikI
b8keUyY+D0XdY9DpDBeiorb1qPYt8BPLOAQrIUAw1CgpMM9KFp9TNvW47KcG4bcB
dQIDAQAB
-----END PUBLIC KEY-----
)",
    R"(
-----BEGIN PUBLIC KEY-----
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAyYATA9KOQ0Azf97QAPfY
Jc/WeZyE4E1qlRgKWKqNtYSXZqk5At0V7w2ntAWtYSpczFrVepCJ0oPMDpZTigEr
NgOgfo5LEhPx5XmtCf62xY/xL3kgtfz9Mm5TBkuQy4KwY4z1npGr4NYYDXtF7kkf
LQE+FnD8Yr4E0wHBib7ey7aeeKWmwqvUjzDqG+TzaqwzO/RCUsSctqSS0t1oo2hv
4q1ofanUXsV8MXk/ujtgxu7WkVvfiSpK1zRazgeZjcrQFO9qL/pla0vBUxa1U8He
GMLnL0oRfcMg7yKrbIMrvlEl2ZmiR9im44dXJWfY42quObwr1PuEkEoCMcMisSWl
jwIDAQAB
-----END PUBLIC KEY-----
)",
    R"(
-----BEGIN PUBLIC KEY-----
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA4RvbWx3zLblDHH/lGUF5
Q512MT+v3YPriuibROMllv8WiCLAMeJ0QXbVaIzBOeHDeLx8yvoZZN+TENKxtT6u
IfMMneUzxHBqy0AQNfIsSsOnG5nqoeE/AwbS6VqCdH1aLfoCoPffacHYa0XvTcsi
aVlZfr+UzJS+ty8pRmFVi1UKSOADDdK8XfIovJl/zMP2TxYX2Y3fnjeLtl8Sqs2e
P+eHDoy7Wi4EPTyY7tNTCfxwKNHn1HQ5yrv5dgvMxFWIWXGz24yikFvtwLGHe8uJ
Wi+fBX+0PF0diZ6pIthZ149VU8qCqYAXjgpxZ0EZdrsiF6Ewz0cfg20SYApFcmW4
pwIDAQAB
-----END PUBLIC KEY-----
)",
    R"(
-----BEGIN PUBLIC KEY-----
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAyhd5AfwrUohG3O4DE0K9
O3FmgB7zE4aDnkL8UUfGCh5kdP8q7ewMjekY+c6LwWOmpdJpSwqhfV1q5ZU1l6rk
3hlt03LO3sgs28kcfOVH15hqfxts6Sg5KcRjxStE50ORmXGwXDcS9vqkJ60J1EHA
lcZqbCRSO73ZPLhdepfd0/C6tM0L7Ge6cAE62/MTmYNGv8fDzwQr/kYIJMdoS8Zp
thRpctFZJtPs3b0fffZA/TCLVKMvEVgTWs48751qKid7N/Lm/iEGx/tOf4o23Nec
Pz1IQaGLP+UOLVQbqQBHJWNOqigm7kWhDgs3N4YagWgxPEQ0WVLtFji/ZjlKZc7h
dwIDAQAB
-----END PUBLIC KEY-----
)",
    R"(
-----BEGIN PUBLIC KEY-----
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAnFDg3LhyV6BVE2Z3zQvN
6urrKvPhygTa5+wIPGwYTzJ8DfGALqlsX3VOXMvcJTca6SbuwwkoXHuSU5wQxfcs
bt4jTXD3NIoRwQPl+D9IbgIMuX0ACl27rJmr/f9zkY7qui4k1X82pQkxBe+/qJ4r
TBwVNONVx1fekTMnSCEhwg5yU3TNbkObu0qlQeJfuMWLDQbW/8v/qfr/Nz0JqHDN
yYKfKvFMlORxyJYiOyeOsbzNGEhkGQGOmKhRUhS35kD+oA0jqwPwMCM9O4kFg/L8
iZbpBBX2By1K3msejWMRAewTOyPas6YMQOYq9BMmWQqzVtG5xcaSJwN/YnMpJyqb
sQIDAQAB
-----END PUBLIC KEY-----
)",
    R"(
-----BEGIN PUBLIC KEY-----
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA1z0RU8vGrfEkrscEoZKA
GiOcGh2EMcKwjQpl4nKuR9H4o/dg+CZregVSHg7MP2f8mhLZZyoFev49oWOV4Rmi
qs99UNxm7DyKW1fF1ovowsUW5lsDoKYLvpuzHo0s4laiV4AnIYP7tHGLdzsnK2Os
Cp5dSuMwKHPZ9N25hXxFB/dRrAdIiXHvbSqr4N29XzfQloQpL3bGHLKY6guFHluH
X5dJ9eirVakWWou7BR2rnD0k9vER6oRdVnJ6YKb5uhWEOQ3NmV961oyr+uiDTcep
qqtGHWuFhENixtiWGjFJJcACwqxEAW3bz9lyrfnPDsHSW/rlQVDIAkik+fOp+R7L
kQIDAQAB
-----END PUBLIC KEY-----
)",
    R"(
-----BEGIN PUBLIC KEY-----
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAxwO27e1vnbNcpiDg7Wwx
K/w5aEGukXotu3529ieq+O39H0+Bak4vIbzGhDUh3/ElmxaFMAs4PYrWe/hc2WFD
H4JCOoFIn4y9gQeE855DGGFgeIVd1BnSs5S+5wUEMxLNyHdHSmINN6FsoZ535iUg
KdYjRh1iZevezg7ln8o/O36uthu925ehFBXSy6jLJgQlwmq0KxZJE0OAZhuDBM60
MtIunNa/e5y+Gw3GknFwtRLmn/nEckZx1nEtepYvvUa7UGy+8KuGuhOerCZTutbG
k8liCVgGenRve8unA2LrBbpL+AUf3CrZU/uAxxTqWmw6Z/S6TeW5ozeeyOCh8ii6
TwIDAQAB
-----END PUBLIC KEY-----
)",
    R"(
-----BEGIN PUBLIC KEY-----
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAsGIFOfIQ4RI5qu4klOxf
ge6eXwBMAkuTXyhyIIJDtE8CurnwQvUXVlt+Kf0SfuIFW6MY5ErcWE/vMFbc81IR
9wByOAAV2CTyiLGZT63uE8pN6FSHd6yGYCLjXd3P3cnP3Qj5pBncpLuAUDfHG4wP
bs9jIADw3HysD+eCNja8p7ZC7CzWxTcO7HsEu9deAAU19YywdpagXvQ0pJ9zV5qU
jrHxBygl31t6TmmX+3d+azjGu9Hu36E+5wcSOOhuwAFXDejb40Ixv53ItJ3fZzzH
PF2nj9sQvQ8c5ptjyOvQCBRdqkEWXIVHClxqWb+o59pDIh1G0UGcmiDN7K9Gz5HA
ZQIDAQAB
-----END PUBLIC KEY-----
)",
    R"(
-----BEGIN PUBLIC KEY-----
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAt9uUnlW/CoUXT68yaZh9
SeXHzGRCPNEI98Tara+dgYxDX1z7nfOh8o15liT0QsAzx34EewZOxcKCNiV/dZX5
z4clCkD8uUbZut6IVx8Eu+7Qcd5jZthRc6hQrN9Ltv7ZQEh7KGXOHa53kT2K01ws
4jbVmd/7Nx7y0Yyqhja01pIu/CUaTkODfQxBXwriLdIzp7y/iJeF/TLqCwZWHKQx
QOZnsPEveB1F00Va9MeAtTlXFUJ/TQXquqTjeLj4HuIRtbyuNgWoc0JyF+mcafAl
bnrNEBIfxZhAT81aUCIAzRJp6AqfdeZxnZ/WwohtZQZLXAxFQPTWCcP+Z9M7OIQL
WwIDAQAB
-----END PUBLIC KEY-----
)",
    R"(
-----BEGIN PUBLIC KEY-----
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA56NhfACkeCyZM07l2wmd
iTp24E2tLLKU3iByKlIRWRAvXsOejRMJTHTNHWa3cQ7uLP++Tf2St7ksNsyPMNZy
9QRTLNCYr9rN9loLwdb2sMWxFBwwzCaAOTahGI7GJQy30UB7FEND0X/5U2rZvQij
Q6K+O4aa+K9M5qyOHNMmXywmTnAgWKNaNxQHPRtD2+dSj60T6zXdtIuCrPfcNGg5
gj07qWGEXX83V/L7nSqCiIVYg/wqds1x52Yjk1nhXYNBTqlnhmOd8LynGxz/sXC7
h2Q9XsHjXIChW4FHyLIOl6b4zPMBSxzCigYm3QZJWfAkZv5PBRtnq7vhYOLHzLQj
CwIDAQAB
-----END PUBLIC KEY-----
)",
    R"(
-----BEGIN PUBLIC KEY-----
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAmfPLe0IWGYC0MZC6YiM3
QGfhT6zSKB0I2DW44nlBlWUcF+32jW2bFJtgE76qGGKFeU4kJBWYr99ufHoAodNg
M1Ehl/JfQ5KmbC1WIqnFTrgbmqJde79jeCvCpbFLuqnzidwO1PbXDbfRFQcgWaXT
mDVLNNVmLxA0GkCv+kydE2gtcOD9BDceg7F/56TDvclyI5QqAnjE2XIRMPZlXQP4
oF2kgz4Cn7LxLHYmkU2sS9NYLzHoyUqFplWlxkQjA4eQ0neutV1Ydmc1IX8W7R38
A7nFtaT8iI8w6Vkv7ijYN6xf5cVBPKZ3Dv7AdwPet86JD5mf5v+r7iwg5xl3r77Z
iwIDAQAB
-----END PUBLIC KEY-----
)",
    R"(
-----BEGIN PUBLIC KEY-----
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAoB1kWsX8YmCcFOD9ilBY
xK076HmUAN026uJ8JpmU9Hz+QT1FNXOsnj1h2G6U6btYVIdHUTHy/BvAumrDKqRz
qcEAzCuhxUjPjss54a/Zqu6nQcoIPHuG/Er39oZHIVkPR1WCvWj8wmyYv6T//dPH
unO6tW29sXXxS+J1Gah6vpbtJw1pI/liah1DZzb13KWPDI6ZzviTNnW4S05r6js/
30He+Yud6aywrdaP/7G90qcrteEFcjFy4Xf+5vG960oKoGoDplwX5poay1oCP9tb
g8AC8VSRAGi3oviTeSWZcrLXS8AtJhGvF48cXQj2q+8YeVKVDpH6fPQxJ9Sh9aeU
awIDAQAB
-----END PUBLIC KEY-----
)",
    R"(
-----BEGIN PUBLIC KEY-----
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA4NTMAIYIlCMID00ufy/I
AZXc8pocDx9N1Q5x5/cL3aIpLmx02AKo9BvTJaJuHiTjlwYhPtlhIrHV4HUVTkOX
sISp8B8v9i2I1RIvCTAcvy3gcH6rdRWZ0cdTUiMEqnnxBX9zdzl8oMzZcyauv19D
BeqJvzflIT96b8g8K3mvgJHs9a1j9f0gN8FuTA0c52DouKnrh8UwH7mlrumYerJw
6goJGQuK1HEOt6bcQuvogkbgJWOoEYwjNrPwQvIcP4wyrgSnOHg1yXOFE84oVynJ
czQEOz9ke42I3h8wrnQxilEYBVo2uX8MenqTyfGnE32lPRt3Wv1iEVQls8Cxiuy2
CQIDAQAB
-----END PUBLIC KEY-----
)",
    R"(
-----BEGIN PUBLIC KEY-----
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA3bUtfp66OtRyvIF/oucn
id8mo7gvbNEH04QMLO3Ok43dlWgWI3hekJAqOYc0mvoI5anqr98h8FI7aCYZm/bY
vpz0I1aXBaEPh3aWh8f/w9HME7ykBvmhMe3J+VFGWWL4eswfRl//GCtnSMBzDFhM
SaQOTvADWHkC0njeI5yXjf/lNm6fMACP1cnhuvCtnx7VP/DAtvUk9usDKG56MJnZ
UoVM3HHjbJeRwxCdlSWe12ilCdwMRKSDY92Hk38/zBLenH04C3HRQLjBGewACUmx
uvNInehZ4kSYFGa+7UxBxFtzJhlKzGR73qUjpWzZivCe1K0WfRVP5IWsKNCCESJ/
nQIDAQAB
-----END PUBLIC KEY-----
)",
    R"(
-----BEGIN PUBLIC KEY-----
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAyV2dE/CRUAUE8ybq/DoS
Lc7QlYXh04K+McbhN724TbHahLTuDk5mR5TAunA8Nea4euRzknKdMFAz1eh9gyy3
5x4UfXQW1fIZqNo6WNrGxYJgWAXU+pov+OvxsMQWzqS4jrTHDHbblCCLKp1akwJk
aFNyqgjAL373PcqXC+XAn8vHx4xHFoFP5lq4lLcJCOW5ee9v9El3w0USLwS+t1cF
RY3kuV6Njlr4zsRH9iM6/zaSuCALYWJ/JrPEurSJXzFZnWsvn6aQdeNeAn08+z0F
k2NwaauEo0xmLqzqTRGzjHqKKmeefN3/+M/FN2FrApDlxWQfhD2Y3USdAiN547Nj
1wIDAQAB
-----END PUBLIC KEY-----
)",
    R"(
-----BEGIN PUBLIC KEY-----
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAvm2+kTrEQWZXuxhWzBdl
PCbQGqbrukbeS6JKSlQLJDC8ayZIxFxatqg1Q8UPyv89MVRsHOGlG1OqFaOEtPjQ
Oo6j/moFwB4GPyJhJHOGpCKa4CLB5clhfDCLJw6ty7PcDU3T6yW4X4Qc5k4LRRWy
yzC8lVHfBdarN+1iEe0ALMOGoeiJjVn6i/AFxktRwgd8njqv/oWQyfjJZXkNMsb6
7ZDxNVAUrp/WXpE4Kq694bB9xa/pWsqv7FjQJUgTnEzvbN+qXnVPtA7dHcOYYJ8Z
SbrJUfHrf8TS5B54AiopFpWG+hIbjqqdigqabBqFpmjiRDZgDy4zJJj52xJZMnrp
rwIDAQAB
-----END PUBLIC KEY-----
)",
    R"(
-----BEGIN PUBLIC KEY-----
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAwEAcVmY3589O02pLA22f
MlarLyJUgy0BeJDG5AUsi17ct8sHZzRiv9zKQVCBk1CtZY//jyqnrM7iCBLWsyby
TiTOtGYHHApaLnNjjtaHdQ6zplhbc3g2XLy+4ab8GNKG3zc8iXpsQM6r+JO5n9pm
V9vollz9dkFxS9l+1P17lZdIgCh9O3EIFJv5QCd5c9l2ezHAan2OhkWhiDtldnH/
MfRXbz7X5sqlwWLa/jhPtvY45x7dZaCHGqNzbupQZs0vHnAVdDu3vAWDmT/3sXHG
vmGxswKA9tPU0prSvQWLz4LUCnGi/cC5R+fiu+fovFM/BwvaGtqBFIF/1oWVq7bZ
4wIDAQAB
-----END PUBLIC KEY-----
)",
    R"(
-----BEGIN PUBLIC KEY-----
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA25qGwNO1+qHygC8mjm8L
3I66mV/IzslgBDHC91mE8YcI5Fq0sdrtsbUhK3z89wIN/zOhbHX0NEiXm2GxUnsI
vb5tDZXAh7AbTnXTMVbxO/e/8sPLUiObGjDvjVzyzrxOeG87yK/oIiilwk9wTsIb
wMn2Grj4ht9gVKx3oGHYV7STNdWBlzSaJj4Ou7+5M1InjPDRFZG1K31D2d3IHByX
lmcRPZtPFTa5C1uVJw00fI4F4uEFlPclZQlR5yA0G9v+0uDgLcjIUB4eqwMthUWc
dHhlmrPp04LI19eksWHCtG30RzmUaxDiIC7J2Ut0zHDqUe7aXn8tOVI7dE9tTKQD
KQIDAQAB
-----END PUBLIC KEY-----
)",
    R"(
-----BEGIN PUBLIC KEY-----
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA7EC2bx7aRnf3TcRg5gmw
QOKNCUheCelK8hoXLMsKSJqmufyJ+IHUejpXGOpvyYRbACiJ5GiNcww20MVpTBU7
YESWB2QSU2eEJJXMq84qsZSO8WGmAuKpUckI+hNHKQYJBEDOougV6/vVVEm5c5bc
SLWQo0+/ciQ21Zwz5SwimX8ep1YpqYirO04gcyGZzAfGboXRvdUwA+1bZvuUXdKC
4zsCw2QALlcVpzPwjB5mqA/3a+SPgdLAiLOwWXFDRMnQw44UjsnPJFoXgEZiUpZm
EMS5gLv50CzQqJXK9mNzPuYXNUIc4Pw4ssVWe0OfN3Od90gl5uFUwk/G9lWSYnBN
3wIDAQAB
-----END PUBLIC KEY-----
)", nullptr};

static const char *const community_public_keys[] = {
    R"(
-----BEGIN PUBLIC KEY-----
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAtXl28loGwAH3ZGQXXgJQ
3omhIEiUb3z9Petjl+jmdtEQnMNUFEZiXkfJB02UFWBL1OoKKnjiGhcr5oGiIZKR
CoaL6SfmWe//7o8STM44stE0exzZcv8W4tWwjrzSWQnwh2JgSnHN64xoDQjdvG3X
9uQ1xXMXghWOKqEpgArpJQkHoPW3CD5sCS2NLFrBG6KgX0W+GTV5HaKhTMr2754F
l260drcBJZhLFCeesze2DXtQC+R9D25Zwn2ehHHd2Fd1M10ZL/iKN8NeerB4Jnph
w6E3orA0DusDLDLtpJUHhmpLoU/1eYQFQOpGw2ce5I88Tkx7SKnCRy1UiE7BA82W
YQIDAQAB
-----END PUBLIC KEY-----
)",
    R"(
-----BEGIN PUBLIC KEY-----
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAvTgQ+mJs8vG/TQTJ6sV+
tACTZTbmp8NkgTuwEyHZSNhX6W8FYwAqPzbePo7wudsUdBWV8j+kUYaBiqeiPUp0
7neO/3oTUQkMJLq9FeIXfoYkS3+/5CIuvsfas6PJP9U2ge6MV1Ndgbd7a12cmX8V
4eNwQRDv/H4zgL7YI2ZZSG1loxgMffZrpflNB87t/f0QYdmnwphMC5RqxiCkDZPA
a5/5KbmD6kjLh8RRRw3lAZbPQe5r7o2Xqqwg9gc6rQ/WFBB1Oj+Q5Bggqznl6dCB
JcLOA7rhYatv/mvt1h6ogQwQ9FGRM3PifV9boZxOQGBAkMD6ngpd5kVoOxdygC7v
twIDAQAB
-----END PUBLIC KEY-----
)",
    R"(
-----BEGIN PUBLIC KEY-----
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA7KvnA+Ixj4ZCLR+aXSFz
ICGbQdVrZ/hhjImDQcWgWY+z/bEbybslDvy5KEPrxTNxKZ0VfFFAVEUj2cw8B5KI
naK8U2VIpdD6LpEJvkOuWKg3bym4COhyAcRNqKKu/GPzS90wICJ2aaayF1mVoCIL
dsp2ZShSIVRJa55gVvfRN1ZEkqBnZryKNt/h3DNqqq2Sn3n3HIZ8H9oEO+L+2Efe
kyET7o9OHy6QZXhf4SJ8QlQAwxxe/L4bln8CBlBHKrUNNqxpjhC37EnY2jpuu3a9
EZcNFj8R4qIJx7hcltntZyKrEIXqc6I6x4oZ4qhZj3RQ5Lr+pJ++idoc1LmBS3k5
yQIDAQAB
-----END PUBLIC KEY-----
)",
    R"(
-----BEGIN PUBLIC KEY-----
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA7SF+5RZ9jXyruBkhxhk2
BSWPbohevxxv++7Uw0HXC/3Xw4jzii0tYaJ6O8QWXyggEAkvmONblAN1rfiz+h5M
oJUQwHjTTZ8BmKUmWrNayVokUXLu4IpCAHk4uSXfx4U/AINnNfWW7z8mUJf6nGsM
XePuKPBRUsw+JmTWOXEIVrkc/66B+gpgi+DwRFLUPh96D8XRAhp7QbHE9UMD3HpA
mPMX7ICVsVS+NGdCHNsdWfH4noaESjgmMdApKekgeeo8Zu1pvQ3y8iew1xOQVBoR
V+PCGWAJYB7ulqBBkRz+NhPLWw7wRA4yLNcZVlZuDFxH9EoavWdfIyYYUn4efSz9
tQIDAQAB
-----END PUBLIC KEY-----
)",
    R"(
-----BEGIN PUBLIC KEY-----
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAszmZ6Slv/oJvFpOLtSMx
58AKMia9y+qcVfw77/Alb3b+Qi5L2uy6nHfJElT7RIeeXhJ8mFglZ70MecTfj0jl
5WhW+yMg6jmPCJL2JMt/oeC4iY4Cf/3C9RHU4IO13VN4dnVQ5S+SEEmSbXnno9Pe
06yyVgZeJ0REJMV1JZj9gOPc/wbeLHsx4UC5qsu32Ammy6J7tS+k7JvRc9CPOEpe
IhWoZmpONydcI6IRfyH2xl4uLY3hWDrRei0I2zGH45G2hPNeTtRh27t+SzXO7h9j
y072CgHytRgQBiH711i8fe4bHMmtVPhPjFrbuzbJSgE7SyikrWIHMDsnPz443bdR
cQIDAQAB
-----END PUBLIC KEY-----
)",
    R"(
-----BEGIN PUBLIC KEY-----
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAleywAb7xZKYTFE08gGA9
ffTeYPRcECl/J060fUziIiFu0NHTOZO+a4BH2X+E1WjjNNQkbn00g+op4nqg3/U+
UaKuXNjWY2Rvd8s91fUD0YOdRpPmsTm2QqhgmYYzO8Oh3YXBNRpXaqALbjL9Nahw
YEAsI3o5yenZGUIEk3JaZFHsAZPL5wGgDVpZgmVUHJ0EO8N5LQh01aHxnP5+ey2z
L5h6IdWLubb07wEBk5bnmIvdhd6dIBzUql27BAqvxKJbW0/okjrhIgcIANDCavfV
L8UP7MCGnfozK7VIl5DG85gCQVAD8+lGUDzOuhzZjl7XKpkFAIWaS8pl4AJbJuG8
nwIDAQAB
-----END PUBLIC KEY-----
)",
    R"(
-----BEGIN PUBLIC KEY-----
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAxiKgcR7Kb1CGTNczbuX+
S7OFpnVLDD5XGVKvYWxL+2By2QRFPWtMs8c24omLIgZ/CWBFPraMiNKS4+V9ar2C
wJhToJnAOKyayA0Gw2wNZx1mgHAZ/5mT+ImfkmZu2HPwtzJmJDQlESD4p40BWBNa
ZpWFGPMKn4GqvOOSGevC/r9inXm6NaPkM+B/piVDEgiJ7g/kpoqImmNb/c2/3XG5
3kbDIHdbd2m3A3jWCjNGSANKsR5C0/rZtvsA8tjDlNWIuKmkU3C2nfj3UduU4dNP
Cisod/pDY8ov0U9sdkM9XZsTXjtbAIGLzMshmOv4ajRFUueGnsZW0GRqp9DSnKmj
2QIDAQAB
-----END PUBLIC KEY-----
)",
    R"(
-----BEGIN PUBLIC KEY-----
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAuh334hUmJcdDJUSmeXqE
GUfGnASD2QrnuoS+gsXgW5BQW8YMDFASvADQhoDUdcwZMlAF+p+CxKCX/gBp40nC
5eyPXv1e0K6PFcCdHtJq8MhGYAr1sy+7cOpzv0r9whobYUykGoHjdwZeu3VbA3uz
go80oYQlwY+v4zZFafCz3cXw8u7n/9PlddgeqHuIPsNZLocICuBUxwg5rHTzycg2
Pa68CRselONGN12V0/wlOg+NZpKCym58CM9SS/0v4YZ6LnmINo8gdRYnGE2zhvey
pHR8IJ8WSJXbl8NwyIY1AmtT/Z0dbAclfD8Wt/w5KA/sttnQzrB7fPsLRyLP1Alq
iQIDAQAB
-----END PUBLIC KEY-----
)",
    R"(
-----BEGIN PUBLIC KEY-----
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAvWuRMEbez/Ud2o/0KA04
K9u3HePWud9rEqMsPv2HlclH3k+cezoUJzVre0lopv3R4aG3LDoFETrgGgUVrfPG
z3Zh7vyk0kb4IGkv+kLQu/cWQXyNzigxV+WQnpIWQ28vrP45y5f+GhwwgzFaDAQR
u1o1HH1FEnP7SSzHVvisNTecY95+F5AOvtOOUg4VlegXdUeGZHEza/0D9V8gODPL
DzbOJDDiqX8ahhRnIZyGEg6y7QqftZFz7j0siCHTXXYJBOcPjD4TqTUNpGvBox44
wgLlLcDsZ/n2Ck4doLXxVz9F80VKOriHSk+qIwseykKVzWQDQTOMOsjCmQsDvram
RwIDAQAB
-----END PUBLIC KEY-----
)",
    R"(
-----BEGIN PUBLIC KEY-----
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAyJmGd1GuBv/WD80IcVyr
dZcmuYe/7azLuV1wsgtH4gsUx+ifUwLZUhLFGOTAPFitbFYPPdhQKncO+BcbvOIo
9FGKj9jGVpMU6C+0JQfi+koESevtO1tYzG8c2dMOGNUO0Hlj2Hezm3tZY4nAbo1J
DYqQSY7qvOYZPFvOS/zL+q2vMx93w9jDHJK4iU02ovAqK9xCWfTp4W7rtbDeTgiX
W/75rMG8DWI1ZHA2JXAOFPsiOHa0/yyvCvUIWvRuNHqTTN5NFiJRIcbTCKKbNwNM
xcNkBQCx4xwOqD9TkDbHpBOC/pfW7j3ygJdYRjFFqm10+KwPACYo/f0n4n4DI8Zz
twIDAQAB
-----END PUBLIC KEY-----
)",
    R"(
-----BEGIN PUBLIC KEY-----
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAnmxbunsK+2pL8Cva9F8E
9o/dQ35TuIqcgpl9/oIc++x+6G5/8UT5mgGCQTITJRIAPnHsZ9XEnMxTAuSCDkYG
CA3JMl1MT7Zxu8TQJBPiXxOaAE1UmA13JuQ2Uu0v7T6TucQxR9KMvcdCxOZ5cBU4
uyJObnZVy/WjM2vWcWDUaYGfMss3eYxcDpavspBANdtSZfv11+8/VC+gEGBOe+oW
zDR+BlQx//MAzwSP5HVQcmLHsT073IvkoUWJUxSCCwlLe60ylpY16BLT6dB0RU8B
sxFcIwmYg0kq19EEPPvZLvRKjG/TJRm1MFzOE5LP2VxLGdMltWYEVsBZHTcWU7HR
8wIDAQAB
-----END PUBLIC KEY-----
)",
    R"(
-----BEGIN PUBLIC KEY-----
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAlo7eDZOpCptanajUtDK3
q8Q/ykxmDDw6lVSiLBm54zwMxaqfM+tV/xqalvIVv3BrucRkCs6H+R0bpd7XhbE5
a7ZFSrWCBf1V6y/NZrEn4qcRbk/WsG4UFqu7CG4r+EgQ4nmoIH/A5+e8FUcur3Y8
2ie9Foi1CUpZojWYZJeHKbb2yYn4MFHszEb5w9HVxY+i9jR1B8Rvn6OEK3OYDrtA
KnPXp4OiDx6CviYEmipX815PPj7Sv8KKL96JqGWjC4kYw6ALgV/GxiX++tv6rh2O
paW9MBv1y+5oZ8ls5S2T/LXbxDpjUEKC9guSSWmsPHRMxOumXsw0H43grC3Ce8Ui
CwIDAQAB
-----END PUBLIC KEY-----
)",
    R"(
-----BEGIN PUBLIC KEY-----
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA0ACgf0kJeQgDh+vHj2aj
K/6FQ794MknLxlwTARGlktoVwZgW/qc6vMZsILRUP1gb/gPXdpSTqqad/GLG4f5R
1Ji1It6BniJOPWu1YyTz0C/BXzTGWbwPnIaawbpQE8n4A+tjGGvAoauPtzr0bWfV
XOXPfIW9XB51dcaVTZgHN55Y8Yd/Pcu9/lqXqXyE23tDLXR/QgGpwK9VxTSbRmuC
WspwqWY6L3MIw+3HIXERTM1uNhc9oHxMOCRbJmUghG0wCWB0ed3Xhbnl9mHlX+l1
rfCJAP4lVWKFjkKBNUejaf+WHxASMjrQubgHLZ2fpf3Ra8TfI3rgPABsAqEIFw3T
QwIDAQAB
-----END PUBLIC KEY-----
)",
    R"(
-----BEGIN PUBLIC KEY-----
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAt635/P50bMbEDTapjAQz
ARTb3y8jMHxVruX0tJU1tycmkX3J8tBALmc6TkSHNTJcQmR8L8Sj3h76l/vuL373
HFSGZ4xghBQqR1lUd2kVomoh+rzEte+0rHWm0JMhjmTQBx+AkDCOw4z3vi5AxWx0
4EbYpQm2akVGKXQrQPyds0UirmdLACCH6WM6exgAXr75DB4PUpG85oI9Q+5ee1Km
+4atVJ4FNa6ZnjWccrlMYT0W7a0Y7feJPAPvfizrs2MG9/ijyBX34eCWA5dtUSIm
2uqI6DxITZlLTvXVDSKQGlq5TEGMvRULWTatqWy4g+tOZ8rSbRuj32pcBnXlwuVu
7QIDAQAB
-----END PUBLIC KEY-----
)",
    R"(
-----BEGIN PUBLIC KEY-----
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAwqO3yWSLKqz1uQ54iFd/
VcQzgT6chLVuhktt7EFvi3tKaQqz2h2KPkDR+MssRV/BZ/41GNlR6r6p5CaPVDDe
Cuj5IcxrIFZIOBMBi1YZ/bknF9edJacINxNfGK/lXBNEAdUvxcOxX8WeP69uvl2l
SKyO3yAdx6HOyL9if95bYQD19HYPZzbfccPX1aD4pjnej6uMfd7yZErH7i8y0oj4
eSKSe1CisjFlR9NzRGO42jU9rtqnAFH9sK5wU9xKQ7bQwlz7yKBF2RuuQweMpXb6
lSObI7ZqYN+7jkf9F5hKRx4kX3+MMBeYmFOy1aYZ08u6sdJ2ua/hFNSDRg7e/UCe
AwIDAQAB
-----END PUBLIC KEY-----
)",
    R"(
-----BEGIN PUBLIC KEY-----
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAkJihnfMECaa6YCg6avam
cb8Sy1GshJ7c7+EW6C4vnspSSvEi04AEBB29pnEF9+VO6VSUHLxunVCpbmKFaLH+
5fDLnc/wCkjPQww49da9MEScCmVGjROlmog65cxQbv4lfxyw55sFV3s/5CPcGlVc
1gojHRABrx4YocpeYies04mEVoOYg1DBG4Uf+aFd5+hm3ZtBa4mqTK2iQa4ILkHa
a0/Us1drRuDjjI4zSbgRzy9x0JVDvqDdLubHyaEf7d7SdrKzodhydG84qpsPFxIj
LK7Bu5v7P4ZTJmxMG3PBM2kB//hlYVR4vO4VEu66mQIM6km+vT9cwxz77qIJhLn3
ywIDAQAB
-----END PUBLIC KEY-----
)",
    R"(
-----BEGIN PUBLIC KEY-----
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA9NbP7ijUxZh4j0NVF6yO
IZ0rzROwl4pP4HGeN+Woyi9+qpdE874WlVoquGEpsshF4Ojzbu2BtXuihb783awa
GLx66MYPeID1FjTKmuCJ2aluOP+DkVo6K1EoqVJXyeIxZzVSqhSIuAdb/vmPlgLz
Fzdk3FgNNOERuGV363DRGz1YxZVnJeSs76g+/9ddhMk8cqIRup5S4YgTOSr0vKem
1E6lyE8IbLoq9J7w5Ur8VjzE2cI+eLKGFqr46Q8pf0pJq72gd+Z3mH5D2LmvEtAR
9jAQXVlLfHauQR2M0K6mqDy9GxL19OU4tGO+GY86VvDTU+wZppAZRz9AKoL1fwfI
BQIDAQAB
-----END PUBLIC KEY-----
)",
    R"(
-----BEGIN PUBLIC KEY-----
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAjrI16GdC2zJriLbyzcre
AqvckBSTMd4bdGaodUBNBTBVbITsOw/k7D62y2hSZHt2nHOyEVkJINJHADrpNZuY
ybS4ssEXxD8+NnjATqQxDMuSz8lUj/Jnf49uzLh84fep3DTksDcQX6Nvio5q8Xbh
HRgvl5I+tPfLtme0oW9cVuVja2i5lHB3SzYCW9Kk/V4/d2WiceYf91a1Nae6m7QV
5bmbYoHmsxT8refTQq+5lAhzVXYU9QRgiKdbE8sSmkV+YiZEtGijefUXgmOxx3I9
B3y03796WBS/RHpSzdMNJw/xPWJcSEMqaUdSYr0DuPCnrn7ojFeF/EFC47CBq5DU
swIDAQAB
-----END PUBLIC KEY-----
)",
    R"(
-----BEGIN PUBLIC KEY-----
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAjS1+My6OhQCCD1DgrzKu
db4Fvc3aqqEhQyjqMLnalp0uoGFpSLoPsZiPGloTE8FSs1ZBFKQ8h2SsGwSdhRKF
xIqoOnS0B/ORjGJxTj7Q2YWjzkCZUD4Ul2AxIbv3TmZM2LeyHJL3A71tSuck8EQY
PE2aj1tLzXsSfRaByy5xwXiU6UpnwCY1xb8tK8QxavRCo5T9Si9tNsolStoNVXV0
k9EbTcRNnxCvab/oqjvgyRuSmIES00v8jZOGQZQUpw02RN6yCBeX2i8GPsGjj/T9
6Gu1Z3G4zUjLlJxl8vjo8KIDaQ8NVWT0j7gx9Knvb5tWnAORI1aJA8AHQvaoOT1W
1wIDAQAB
-----END PUBLIC KEY-----
)", nullptr};

const vector<string> ExtensionHelper::GetPublicKeys(bool allow_community_extensions) {
	vector<string> keys;
	for (idx_t i = 0; public_keys[i]; i++) {
		keys.emplace_back(public_keys[i]);
	}
	if (allow_community_extensions) {
		for (idx_t i = 0; community_public_keys[i]; i++) {
			keys.emplace_back(community_public_keys[i]);
		}
	}
	return keys;
}

} // namespace duckdb
