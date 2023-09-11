#include "duckdb/common/dl.hpp"
#include "duckdb/common/virtual_file_system.hpp"
#include "duckdb/main/extension_helper.hpp"
#include "duckdb/main/error_manager.hpp"
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

static void ComputeSHA256String(const std::string &to_hash, std::string *res) {
	// Invoke MbedTls function to actually compute sha256
	*res = duckdb_mbedtls::MbedTlsWrapper::ComputeSha256Hash(to_hash);
}

static void ComputeSHA256FileSegment(FileHandle *handle, const idx_t start, const idx_t end, std::string *res) {
	idx_t iter = start;
	const idx_t segment_size = 1024 * 8;

	duckdb_mbedtls::MbedTlsWrapper::SHA256State state;

	std::string to_hash;
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

bool ExtensionHelper::TryInitialLoad(DBConfig &config, FileSystem &fs, const string &extension,
                                     ExtensionInitResult &result, string &error,
                                     optional_ptr<const ClientConfig> client_config) {
#ifdef DUCKDB_DISABLE_EXTENSION_LOAD
	throw PermissionException("Loading external extensions is disabled through a compile time flag");
#else
	if (!config.options.enable_external_access) {
		throw PermissionException("Loading external extensions is disabled through configuration");
	}
	auto filename = fs.ConvertSeparators(extension);

	// shorthand case
	if (!ExtensionHelper::IsFullPath(extension)) {
		string extension_name = ApplyExtensionAlias(extension);
#ifdef WASM_LOADABLE_EXTENSIONS
		string url_template = ExtensionUrlTemplate(client_config, "");
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
		std::string address(str);
		free(str);

		filename = address;
#else

		string local_path =
		    !config.options.extension_directory.empty() ? config.options.extension_directory : fs.GetHomeDirectory();

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
	if (!config.options.allow_unsigned_extensions) {
		auto handle = fs.OpenFile(filename, FileFlags::FILE_FLAGS_READ);

		// signature is the last 256 bytes of the file

		string signature;
		signature.resize(256);

		auto signature_offset = handle->GetFileSize() - signature.size();

		const idx_t maxLenChunks = 1024ULL * 1024ULL;
		const idx_t numChunks = (signature_offset + maxLenChunks - 1) / maxLenChunks;
		std::vector<std::string> hash_chunks(numChunks);
		std::vector<idx_t> splits(numChunks + 1);

		for (idx_t i = 0; i < numChunks; i++) {
			splits[i] = maxLenChunks * i;
		}
		splits.back() = signature_offset;

#ifndef DUCKDB_NO_THREADS
		std::vector<std::thread> threads;
		threads.reserve(numChunks);
		for (idx_t i = 0; i < numChunks; i++) {
			threads.emplace_back(ComputeSHA256FileSegment, handle.get(), splits[i], splits[i + 1], &hash_chunks[i]);
		}

		for (auto &thread : threads) {
			thread.join();
		}
#else
		for (idx_t i = 0; i < numChunks; i++) {
			ComputeSHA256FileSegment(handle.get(), splits[i], splits[i + 1], &hash_chunks[i]);
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
		handle->Read((void *)signature.data(), signature.size(), signature_offset);

		bool any_valid = false;
		for (auto &key : ExtensionHelper::GetPublicKeys()) {
			if (duckdb_mbedtls::MbedTlsWrapper::IsValidSha256Signature(key, signature, two_level_hash)) {
				any_valid = true;
				break;
			}
		}
		if (!any_valid) {
			throw IOException(config.error_manager->FormatException(ErrorType::UNSIGNED_EXTENSION, filename));
		}
	}
	auto basename = fs.ExtractBaseName(filename);

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
	    filename.c_str(), basename.c_str());
	auto dopen_from = basename;
#else
	auto dopen_from = filename;
#endif

	auto lib_hdl = dlopen(dopen_from.c_str(), RTLD_NOW | RTLD_LOCAL);
	if (!lib_hdl) {
		throw IOException("Extension \"%s\" could not be loaded: %s", filename, GetDLError());
	}

	ext_version_fun_t version_fun;
	auto version_fun_name = basename + "_version";

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

	result.basename = basename;
	result.filename = filename;
	result.lib_hdl = lib_hdl;
	return true;
#endif
}

ExtensionInitResult ExtensionHelper::InitialLoad(DBConfig &config, FileSystem &fs, const string &extension,
                                                 optional_ptr<const ClientConfig> client_config) {
	string error;
	ExtensionInitResult result;
	if (!TryInitialLoad(config, fs, extension, result, error, client_config)) {
		if (!ExtensionHelper::AllowAutoInstall(extension)) {
			throw IOException(error);
		}
		// the extension load failed - try installing the extension
		ExtensionHelper::InstallExtension(config, fs, extension, false);
		// try loading again
		if (!TryInitialLoad(config, fs, extension, result, error, client_config)) {
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

void ExtensionHelper::LoadExternalExtension(DatabaseInstance &db, FileSystem &fs, const string &extension,
                                            optional_ptr<const ClientConfig> client_config) {
	if (db.ExtensionIsLoaded(extension)) {
		return;
	}
#ifdef DUCKDB_DISABLE_EXTENSION_LOAD
	throw PermissionException("Loading external extensions is disabled through a compile time flag");
#else
	auto res = InitialLoad(DBConfig::GetConfig(db), fs, extension, client_config);
	auto init_fun_name = res.basename + "_init";

	ext_init_fun_t init_fun;
	init_fun = LoadFunctionFromDLL<ext_init_fun_t>(res.lib_hdl, init_fun_name, res.filename);

	try {
		(*init_fun)(db);
	} catch (std::exception &e) {
		throw InvalidInputException("Initialization function \"%s\" from file \"%s\" threw an exception: \"%s\"",
		                            init_fun_name, res.filename, e.what());
	}

	db.SetExtensionLoaded(extension);
#endif
}

void ExtensionHelper::LoadExternalExtension(ClientContext &context, const string &extension) {
	LoadExternalExtension(DatabaseInstance::GetDatabase(context), FileSystem::GetFileSystem(context), extension,
	                      &ClientConfig::GetConfig(context));
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
