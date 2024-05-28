#include "duckdb/common/dl.hpp"
#include "duckdb/common/virtual_file_system.hpp"
#include "duckdb/main/error_manager.hpp"
#include "duckdb/main/extension_helper.hpp"
#include "duckdb/common/serializer/buffered_file_reader.hpp"
#include "duckdb/common/serializer/binary_deserializer.hpp"
#include "mbedtls_wrapper.hpp"

#ifndef DUCKDB_NO_THREADS
#include <thread>
#endif // DUCKDB_NO_THREADS

#ifdef WASM_LOADABLE_EXTENSIONS
#include <emscripten.h>
#endif

namespace duckdb {

//===--------------------------------------------------------------------===//
// Load External Extension
//===--------------------------------------------------------------------===//
#ifndef DUCKDB_DISABLE_EXTENSION_LOAD
typedef void (*ext_init_fun_t)(DatabaseInstance &);
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
#endif

static string FilterZeroAtEnd(string s) {
	while (!s.empty() && s.back() == '\0') {
		s.pop_back();
	}
	return s;
}

ParsedExtensionMetaData ExtensionHelper::ParseExtensionMetaData(const char *metadata) {
	ParsedExtensionMetaData result;

	vector<string> metadata_field;
	for (idx_t i = 0; i < 8; i++) {
		string field = string(metadata + i * 32, 32);
		metadata_field.emplace_back(field);
	}

	std::reverse(metadata_field.begin(), metadata_field.end());

	result.magic_value = FilterZeroAtEnd(metadata_field[0]);
	result.platform = FilterZeroAtEnd(metadata_field[1]);
	result.duckdb_version = FilterZeroAtEnd(metadata_field[2]);
	result.extension_version = FilterZeroAtEnd(metadata_field[3]);

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

bool ExtensionHelper::TryInitialLoad(DBConfig &config, FileSystem &fs, const string &extension,
                                     ExtensionInitResult &result, string &error) {
#ifdef DUCKDB_DISABLE_EXTENSION_LOAD
	throw PermissionException("Loading external extensions is disabled through a compile time flag");
#else
	if (!config.options.enable_external_access) {
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

		string local_path = !config.options.extension_directory.empty() ? config.options.extension_directory
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

	if (!config.options.allow_unsigned_extensions) {
		bool signature_valid =
		    CheckExtensionSignature(*handle, parsed_metadata, config.options.allow_community_extensions);

		if (!signature_valid) {
			throw IOException(config.error_manager->FormatException(ErrorType::UNSIGNED_EXTENSION, filename) +
			                  metadata_mismatch_error);
		}

		if (!metadata_mismatch_error.empty()) {
			// Signed extensions perform the full check
			throw InvalidInputException(metadata_mismatch_error);
		}
	} else if (!config.options.allow_extensions_metadata_mismatch) {
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

ExtensionInitResult ExtensionHelper::InitialLoad(DBConfig &config, FileSystem &fs, const string &extension) {
	string error;
	ExtensionInitResult result;
	if (!TryInitialLoad(config, fs, extension, result, error)) {
		if (!ExtensionHelper::AllowAutoInstall(extension)) {
			throw IOException(error);
		}
		// the extension load failed - try installing the extension
		ExtensionHelper::InstallExtension(config, fs, extension, false);
		// try loading again
		if (!TryInitialLoad(config, fs, extension, result, error)) {
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
	auto res = InitialLoad(DBConfig::GetConfig(db), fs, extension);
	auto init_fun_name = res.filebase + "_init";

	ext_init_fun_t init_fun;
	init_fun = LoadFunctionFromDLL<ext_init_fun_t>(res.lib_hdl, init_fun_name, res.filename);

	try {
		(*init_fun)(db);
	} catch (std::exception &e) {
		ErrorData error(e);
		throw InvalidInputException("Initialization function \"%s\" from file \"%s\" threw an exception: \"%s\"",
		                            init_fun_name, res.filename, error.RawMessage());
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
