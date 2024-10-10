#include "duckdb.h"
#include "duckdb/common/dl.hpp"
#include "duckdb/common/operator/cast_operators.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/virtual_file_system.hpp"
#include "duckdb/main/capi/capi_internal.hpp"
#include "duckdb/main/capi/extension_api.hpp"
#include "duckdb/main/error_manager.hpp"
#include "duckdb/main/extension_helper.hpp"
#include "mbedtls_wrapper.hpp"

#ifndef DUCKDB_NO_THREADS
#include <thread>
#endif // DUCKDB_NO_THREADS

#ifdef WASM_LOADABLE_EXTENSIONS
#include <emscripten.h>
#endif

namespace duckdb {

//===--------------------------------------------------------------------===//
// Extension C API
//===--------------------------------------------------------------------===//

//! State that is kept during the load phase of a C API extension
struct DuckDBExtensionLoadState {
	explicit DuckDBExtensionLoadState(DatabaseInstance &db_p) : db(db_p), database_data(nullptr) {
	}

	//! Create a DuckDBExtensionLoadState reference from a C API opaque pointer
	static DuckDBExtensionLoadState &Get(duckdb_extension_info info) {
		D_ASSERT(info);
		return *reinterpret_cast<duckdb::DuckDBExtensionLoadState *>(info);
	}

	//! Convert to an opaque C API pointer
	duckdb_extension_info ToCStruct() {
		return reinterpret_cast<duckdb_extension_info>(this);
	}

	//! Ref to the database being loaded
	DatabaseInstance &db;

	//! This is the duckdb_database struct that will be passed to the extension during initialization. Note that the
	//! extension does not need to free it.
	unique_ptr<DatabaseData> database_data;

	//! The function pointer struct passed to the extension. The extension is expected to copy this struct during
	//! initialization
	duckdb_ext_api_v0 api_struct;

	//! Error handling
	bool has_error = false;
	//! The stored error from the loading process
	ErrorData error_data;
};

//! Contains the callbacks that are passed to CAPI extensions to allow initialization
struct ExtensionAccess {
	//! Create the struct of function pointers to pass to the extension for initialization
	static duckdb_extension_access CreateAccessStruct() {
		return {SetError, GetDatabase, GetAPI};
	}

	//! Called by the extension to indicate failure to initialize the extension
	static void SetError(duckdb_extension_info info, const char *error) {
		auto &load_state = DuckDBExtensionLoadState::Get(info);

		load_state.has_error = true;
		load_state.error_data = ErrorData(ExceptionType::UNKNOWN_TYPE, error);
	}

	//! Called by the extension get a pointer to the database that is loading it
	static duckdb_database *GetDatabase(duckdb_extension_info info) {
		auto &load_state = DuckDBExtensionLoadState::Get(info);

		try {
			// Create the duckdb_database
			load_state.database_data = make_uniq<DatabaseData>();
			load_state.database_data->database = make_uniq<DuckDB>(load_state.db);
			return reinterpret_cast<duckdb_database *>(load_state.database_data.get());
		} catch (std::exception &ex) {
			load_state.error_data = ErrorData(ex);
			return nullptr;
		} catch (...) {
			load_state.error_data =
			    ErrorData(ExceptionType::UNKNOWN_TYPE, "Unknown error in GetDatabase when trying to load extension!");
			return nullptr;
		}
	}

	//! Called by the extension get a pointer the correctly versioned extension C API struct.
	static const void *GetAPI(duckdb_extension_info info, const char *version) {

		string version_string = version;
		idx_t major, minor, patch;
		auto parsed = VersioningUtils::ParseSemver(version_string, major, minor, patch);

		auto &load_state = DuckDBExtensionLoadState::Get(info);

		if (!parsed || !VersioningUtils::IsSupportedCAPIVersion(major, minor, patch)) {
			load_state.has_error = true;
			load_state.error_data =
			    ErrorData(ExceptionType::UNKNOWN_TYPE,
			              "Unsupported C CAPI version detected during extension initialization: " + string(version));
			return nullptr;
		}

		load_state.api_struct = load_state.db.GetExtensionAPIV0();
		return &load_state.api_struct;
	}
};

//===--------------------------------------------------------------------===//
// Load External Extension
//===--------------------------------------------------------------------===//
#ifndef DUCKDB_DISABLE_EXTENSION_LOAD
// The C++ init function
typedef void (*ext_init_fun_t)(DatabaseInstance &);
// The C init function
typedef void (*ext_init_c_api_fun_t)(duckdb_extension_info info, duckdb_extension_access *access);
typedef const char *(*ext_version_fun_t)(void);
typedef bool (*ext_is_storage_t)(void);

template <class T>
static T LoadFunctionFromDLL(void *dll, const string &function_name, const string &filename) {
	auto function = dlsym(dll, function_name.c_str());
	if (!function) {
		throw IOException("File \"%s\" did not contain function \"%s\": %s", filename, function_name, GetDLError());
	}
	return (T)function;
}
#endif

template <class T>
static T TryLoadFunctionFromDLL(void *dll, const string &function_name, const string &filename) {
	auto function = dlsym(dll, function_name.c_str());
	if (!function) {
		return nullptr;
	}
	return (T)function;
}

static void ComputeSHA256String(const string &to_hash, string *res) {
	// Invoke MbedTls function to actually compute sha256
	*res = duckdb_mbedtls::MbedTlsWrapper::ComputeSha256Hash(to_hash);
}

static void ComputeSHA256FileSegment(FileHandle *handle, const idx_t start, const idx_t end, string *res) {
	idx_t iter = start;
	const idx_t segment_size = 1024ULL * 8ULL;

	duckdb_mbedtls::MbedTlsWrapper::SHA256State state;

	string to_hash;
	while (iter < end) {
		idx_t len = std::min(end - iter, segment_size);
		to_hash.resize(len);
		handle->Read((void *)to_hash.data(), len, iter);

		state.AddString(to_hash);

		iter += segment_size;
	}

	*res = state.Finalize();
}

static string FilterZeroAtEnd(string s) {
	while (!s.empty() && s.back() == '\0') {
		s.pop_back();
	}
	return s;
}

ParsedExtensionMetaData ExtensionHelper::ParseExtensionMetaData(const char *metadata) noexcept {
	ParsedExtensionMetaData result;

	vector<string> metadata_field;
	for (idx_t i = 0; i < 8; i++) {
		string field = string(metadata + i * 32, 32);
		metadata_field.emplace_back(field);
	}

	std::reverse(metadata_field.begin(), metadata_field.end());

	// Fetch the magic value and early out if this is invalid: the rest will just be bogus
	result.magic_value = FilterZeroAtEnd(metadata_field[0]);
	if (!result.AppearsValid()) {
		return result;
	}

	result.platform = FilterZeroAtEnd(metadata_field[1]);

	result.extension_version = FilterZeroAtEnd(metadata_field[3]);

	auto extension_abi_metadata = FilterZeroAtEnd(metadata_field[4]);

	if (extension_abi_metadata == "C_STRUCT") {
		result.abi_type = ExtensionABIType::C_STRUCT;
		result.duckdb_capi_version = FilterZeroAtEnd(metadata_field[2]);
	} else if (extension_abi_metadata == "CPP" || extension_abi_metadata.empty()) {
		result.abi_type = ExtensionABIType::CPP;
		result.duckdb_version = FilterZeroAtEnd(metadata_field[2]);
	} else {
		result.abi_type = ExtensionABIType::UNKNOWN;
		result.duckdb_version = "unknown";
		result.extension_abi_metadata = extension_abi_metadata;
	}

	result.signature = string(metadata, ParsedExtensionMetaData::FOOTER_SIZE - ParsedExtensionMetaData::SIGNATURE_SIZE);
	return result;
}

ParsedExtensionMetaData ExtensionHelper::ParseExtensionMetaData(FileHandle &handle) {
	const string engine_version = string(ExtensionHelper::GetVersionDirectoryName());
	const string engine_platform = string(DuckDB::Platform());

	string metadata_segment;
	metadata_segment.resize(ParsedExtensionMetaData::FOOTER_SIZE);

	if (handle.GetFileSize() < ParsedExtensionMetaData::FOOTER_SIZE) {
		throw InvalidInputException(
		    "File '%s' is not a DuckDB extension. Valid DuckDB extensions must be at least %llu bytes", handle.path,
		    ParsedExtensionMetaData::FOOTER_SIZE);
	}

	handle.Read((void *)metadata_segment.data(), metadata_segment.size(),
	            handle.GetFileSize() - ParsedExtensionMetaData::FOOTER_SIZE);

	return ParseExtensionMetaData(metadata_segment.data());
}

bool ExtensionHelper::CheckExtensionSignature(FileHandle &handle, ParsedExtensionMetaData &parsed_metadata,
                                              const bool allow_community_extensions) {
	auto signature_offset = handle.GetFileSize() - ParsedExtensionMetaData::SIGNATURE_SIZE;

	const idx_t maxLenChunks = 1024ULL * 1024ULL;
	const idx_t numChunks = (signature_offset + maxLenChunks - 1) / maxLenChunks;
	vector<string> hash_chunks(numChunks);
	vector<idx_t> splits(numChunks + 1);

	for (idx_t i = 0; i < numChunks; i++) {
		splits[i] = maxLenChunks * i;
	}
	splits.back() = signature_offset;

#ifndef DUCKDB_NO_THREADS
	vector<std::thread> threads;
	threads.reserve(numChunks);
	for (idx_t i = 0; i < numChunks; i++) {
		threads.emplace_back(ComputeSHA256FileSegment, &handle, splits[i], splits[i + 1], &hash_chunks[i]);
	}

	for (auto &thread : threads) {
		thread.join();
	}
#else
	for (idx_t i = 0; i < numChunks; i++) {
		ComputeSHA256FileSegment(&handle, splits[i], splits[i + 1], &hash_chunks[i]);
	}
#endif // DUCKDB_NO_THREADS

	string hash_concatenation;
	hash_concatenation.reserve(32 * numChunks); // 256 bits -> 32 bytes per chunk

	for (auto &hash_chunk : hash_chunks) {
		hash_concatenation += hash_chunk;
	}

	string two_level_hash;
	ComputeSHA256String(hash_concatenation, &two_level_hash);

	// TODO maybe we should do a stream read / hash update here
	handle.Read((void *)parsed_metadata.signature.data(), parsed_metadata.signature.size(), signature_offset);

	for (auto &key : ExtensionHelper::GetPublicKeys(allow_community_extensions)) {
		if (duckdb_mbedtls::MbedTlsWrapper::IsValidSha256Signature(key, parsed_metadata.signature, two_level_hash)) {
			return true;
			break;
		}
	}

	return false;
}

bool ExtensionHelper::TryInitialLoad(DatabaseInstance &db, FileSystem &fs, const string &extension,
                                     ExtensionInitResult &result, string &error) {
#ifdef DUCKDB_DISABLE_EXTENSION_LOAD
	throw PermissionException("Loading external extensions is disabled through a compile time flag");
#else
	if (!db.config.options.enable_external_access) {
		throw PermissionException("Loading external extensions is disabled through configuration");
	}
	auto filename = fs.ConvertSeparators(extension);

	bool direct_load;

	// shorthand case
	if (!ExtensionHelper::IsFullPath(extension)) {
		direct_load = false;
		string extension_name = ApplyExtensionAlias(extension);
#ifdef WASM_LOADABLE_EXTENSIONS
		string url_template = ExtensionUrlTemplate(&config, "");
		string url = ExtensionFinalizeUrlTemplate(url_template, extension_name);

		char *str = (char *)EM_ASM_PTR(
		    {
			    var jsString = ((typeof runtime == 'object') && runtime && (typeof runtime.whereToLoad == 'function') &&
			                    runtime.whereToLoad)
			                       ? runtime.whereToLoad(UTF8ToString($0))
			                       : (UTF8ToString($1));
			    var lengthBytes = lengthBytesUTF8(jsString) + 1;
			    // 'jsString.length' would return the length of the string as UTF-16
			    // units, but Emscripten C strings operate as UTF-8.
			    var stringOnWasmHeap = _malloc(lengthBytes);
			    stringToUTF8(jsString, stringOnWasmHeap, lengthBytes);
			    return stringOnWasmHeap;
		    },
		    filename.c_str(), url.c_str());
		string address(str);
		free(str);

		filename = address;
#else

		string local_path = !db.config.options.extension_directory.empty()
		                        ? db.config.options.extension_directory
		                        : ExtensionHelper::DefaultExtensionFolder(fs);

		// convert random separators to platform-canonic
		local_path = fs.ConvertSeparators(local_path);
		// expand ~ in extension directory
		local_path = fs.ExpandPath(local_path);
		auto path_components = PathComponents();
		for (auto &path_ele : path_components) {
			local_path = fs.JoinPath(local_path, path_ele);
		}
		filename = fs.JoinPath(local_path, extension_name + ".duckdb_extension");
#endif
	} else {
		direct_load = true;
		filename = fs.ExpandPath(filename);
	}
	if (!fs.FileExists(filename)) {
		string message;
		bool exact_match = ExtensionHelper::CreateSuggestions(extension, message);
		if (exact_match) {
			message += "\nInstall it first using \"INSTALL " + extension + "\".";
		}
		error = StringUtil::Format("Extension \"%s\" not found.\n%s", filename, message);
		return false;
	}

	auto handle = fs.OpenFile(filename, FileFlags::FILE_FLAGS_READ);

	// Parse the extension metadata from the extension binary
	auto parsed_metadata = ParseExtensionMetaData(*handle);

	auto metadata_mismatch_error = parsed_metadata.GetInvalidMetadataError();

	if (!metadata_mismatch_error.empty()) {
		metadata_mismatch_error = StringUtil::Format("Failed to load '%s', %s", extension, metadata_mismatch_error);
	}

	if (!db.config.options.allow_unsigned_extensions) {
		bool signature_valid;
		if (parsed_metadata.AppearsValid()) {
			signature_valid =
			    CheckExtensionSignature(*handle, parsed_metadata, db.config.options.allow_community_extensions);
		} else {
			signature_valid = false;
		}

		if (!signature_valid) {
			throw IOException(db.config.error_manager->FormatException(ErrorType::UNSIGNED_EXTENSION, filename) +
			                  metadata_mismatch_error);
		}

		if (!metadata_mismatch_error.empty()) {
			// Signed extensions perform the full check
			throw InvalidInputException(metadata_mismatch_error);
		}
	} else if (!db.config.options.allow_extensions_metadata_mismatch) {
		if (!metadata_mismatch_error.empty()) {
			// Unsigned extensions AND configuration allowing n, loading allowed, mainly for
			// debugging purposes
			throw InvalidInputException(metadata_mismatch_error);
		}
	}

	auto filebase = fs.ExtractBaseName(filename);

#ifdef WASM_LOADABLE_EXTENSIONS
	EM_ASM(
	    {
		    // Next few lines should argubly in separate JavaScript-land function call
		    // TODO: move them out / have them configurable
		    const xhr = new XMLHttpRequest();
		    xhr.open("GET", UTF8ToString($0), false);
		    xhr.responseType = "arraybuffer";
		    xhr.send(null);
		    var uInt8Array = xhr.response;
		    WebAssembly.validate(uInt8Array);
		    console.log('Loading extension ', UTF8ToString($1));

		    // Here we add the uInt8Array to Emscripten's filesystem, for it to be found by dlopen
		    FS.writeFile(UTF8ToString($1), new Uint8Array(uInt8Array));
	    },
	    filename.c_str(), filebase.c_str());
	auto dopen_from = filebase;
#else
	auto dopen_from = filename;
#endif

	auto lib_hdl = dlopen(dopen_from.c_str(), RTLD_NOW | RTLD_LOCAL);
	if (!lib_hdl) {
		throw IOException("Extension \"%s\" could not be loaded: %s", filename, GetDLError());
	}

	auto lowercase_extension_name = StringUtil::Lower(filebase);

	// Initialize the ExtensionInitResult
	result.filebase = lowercase_extension_name;
	result.filename = filename;
	result.lib_hdl = lib_hdl;

	if (!direct_load) {
		auto info_file_name = filename + ".info";

		result.install_info = ExtensionInstallInfo::TryReadInfoFile(fs, info_file_name, lowercase_extension_name);

		if (result.install_info->mode == ExtensionInstallMode::UNKNOWN) {
			// The info file was missing, we just set the version, since we have it from the parsed footer
			result.install_info->version = parsed_metadata.extension_version;
		}

		if (result.install_info->version != parsed_metadata.extension_version) {
			throw IOException("Metadata mismatch detected when loading extension '%s'\nPlease try reinstalling the "
			                  "extension using `FORCE INSTALL '%s'`",
			                  filename, extension);
		}
	} else {
		result.install_info = make_uniq<ExtensionInstallInfo>();
		result.install_info->mode = ExtensionInstallMode::NOT_INSTALLED;
		result.install_info->full_path = filename;
		result.install_info->version = parsed_metadata.extension_version;
	}

	return true;
#endif
}

ExtensionInitResult ExtensionHelper::InitialLoad(DatabaseInstance &db, FileSystem &fs, const string &extension) {
	string error;
	ExtensionInitResult result;
	if (!TryInitialLoad(db, fs, extension, result, error)) {
		if (!ExtensionHelper::AllowAutoInstall(extension)) {
			throw IOException(error);
		}
		// the extension load failed - try installing the extension
		ExtensionInstallOptions options;
		ExtensionHelper::InstallExtension(db, fs, extension, options);
		// try loading again
		if (!TryInitialLoad(db, fs, extension, result, error)) {
			throw IOException(error);
		}
	}
	return result;
}

bool ExtensionHelper::IsFullPath(const string &extension) {
	return StringUtil::Contains(extension, ".") || StringUtil::Contains(extension, "/") ||
	       StringUtil::Contains(extension, "\\");
}

string ExtensionHelper::GetExtensionName(const string &original_name) {
	auto extension = StringUtil::Lower(original_name);
	if (!IsFullPath(extension)) {
		return ExtensionHelper::ApplyExtensionAlias(extension);
	}
	auto splits = StringUtil::Split(StringUtil::Replace(extension, "\\", "/"), '/');
	if (splits.empty()) {
		return ExtensionHelper::ApplyExtensionAlias(extension);
	}
	splits = StringUtil::Split(splits.back(), '.');
	if (splits.empty()) {
		return ExtensionHelper::ApplyExtensionAlias(extension);
	}
	return ExtensionHelper::ApplyExtensionAlias(splits.front());
}

void ExtensionHelper::LoadExternalExtension(DatabaseInstance &db, FileSystem &fs, const string &extension) {
	if (db.ExtensionIsLoaded(extension)) {
		return;
	}
#ifdef DUCKDB_DISABLE_EXTENSION_LOAD
	throw PermissionException("Loading external extensions is disabled through a compile time flag");
#else
	auto res = InitialLoad(db, fs, extension);
	auto init_fun_name = res.filebase + "_init";

	// "OLD WAY" of loading extensions. If the <ext_name>_init exists, we choose that
	ext_init_fun_t init_fun = TryLoadFunctionFromDLL<ext_init_fun_t>(res.lib_hdl, init_fun_name, res.filename);
	if (init_fun) {
		try {
			(*init_fun)(db);
		} catch (std::exception &e) {
			ErrorData error(e);
			throw InvalidInputException("Initialization function \"%s\" from file \"%s\" threw an exception: \"%s\"",
			                            init_fun_name, res.filename, error.RawMessage());
		}

		D_ASSERT(res.install_info);

		db.SetExtensionLoaded(extension, *res.install_info);
		return;
	}

	// TODO: make this the only way of calling extensions?
	// "NEW WAY" of loading extensions enabling C API only
	init_fun_name = res.filebase + "_init_c_api";
	ext_init_c_api_fun_t init_fun_capi =
	    TryLoadFunctionFromDLL<ext_init_c_api_fun_t>(res.lib_hdl, init_fun_name, res.filename);

	if (!init_fun_capi) {
		throw IOException("File \"%s\" did not contain function \"%s\": %s", res.filename, init_fun_name, GetDLError());
	}

	// Create the load state
	DuckDBExtensionLoadState load_state(db);

	auto access = ExtensionAccess::CreateAccessStruct();
	(*init_fun_capi)(load_state.ToCStruct(), &access);

	// Throw any error that the extension might have encountered
	if (load_state.has_error) {
		load_state.error_data.Throw("An error was thrown during initialization of the extension '" + extension + "': ");
	}

	D_ASSERT(res.install_info);

	db.SetExtensionLoaded(extension, *res.install_info);
#endif
}

void ExtensionHelper::LoadExternalExtension(ClientContext &context, const string &extension) {
	LoadExternalExtension(DatabaseInstance::GetDatabase(context), FileSystem::GetFileSystem(context), extension);
}

string ExtensionHelper::ExtractExtensionPrefixFromPath(const string &path) {
	auto first_colon = path.find(':');
	if (first_colon == string::npos || first_colon < 2) { // needs to be at least two characters because windows c: ...
		return "";
	}
	auto extension = path.substr(0, first_colon);

	if (path.substr(first_colon, 3) == "://") {
		// these are not extensions
		return "";
	}

	D_ASSERT(extension.size() > 1);
	// needs to be alphanumeric
	for (auto &ch : extension) {
		if (!isalnum(ch) && ch != '_') {
			return "";
		}
	}
	return extension;
}

} // namespace duckdb
