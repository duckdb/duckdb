#include "duckdb/common/dl.hpp"
#include "duckdb/common/virtual_file_system.hpp"
#include "duckdb/main/extension_helper.hpp"
#include "duckdb/main/error_manager.hpp"
#include "mbedtls_wrapper.hpp"

#ifdef WASM_LOADABLE_EXTENSIONS
#include <emscripten.h>
#endif

namespace duckdb {

//===--------------------------------------------------------------------===//
// Load External Extension
//===--------------------------------------------------------------------===//
typedef void (*ext_init_fun_t)(DatabaseInstance &);
typedef const char *(*ext_version_fun_t)(void);
typedef void (*ext_storage_init_t)(DBConfig &);

template <class T>
static T LoadFunctionFromDLL(void *dll, const string &function_name, const string &filename) {
	auto function = dlsym(dll, function_name.c_str());
	if (!function) {
		throw IOException("File \"%s\" did not contain function \"%s\": %s", filename, function_name, GetDLError());
	}
	return (T)function;
}

bool ExtensionHelper::TryInitialLoad(DBConfig &config, FileOpener *opener, const string &extension,
                                     ExtensionInitResult &result, string &error) {
	if (!config.options.enable_external_access) {
		throw PermissionException("Loading external extensions is disabled through configuration");
	}
	VirtualFileSystem fallback_file_system; // config may not contain one yet
	auto &fs = config.file_system ? *config.file_system : fallback_file_system;
	auto filename = fs.ConvertSeparators(extension);

	// shorthand case
	if (!ExtensionHelper::IsFullPath(extension)) {
		string local_path = !config.options.extension_directory.empty() ? config.options.extension_directory
		                                                                : fs.GetHomeDirectory(opener);

		// convert random separators to platform-canonic
		local_path = fs.ConvertSeparators(local_path);
		// expand ~ in extension directory
		local_path = fs.ExpandPath(local_path, opener);
		auto path_components = PathComponents();
		for (auto &path_ele : path_components) {
			local_path = fs.JoinPath(local_path, path_ele);
		}
		string extension_name = ApplyExtensionAlias(extension);
		filename = fs.JoinPath(local_path, extension_name + ".duckdb_extension");
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

		string file_content;
		file_content.resize(signature_offset);
		handle->Read((void *)file_content.data(), signature_offset, 0);

		// TODO maybe we should do a stream read / hash update here
		handle->Read((void *)signature.data(), signature.size(), signature_offset);

		auto hash = duckdb_mbedtls::MbedTlsWrapper::ComputeSha256Hash(file_content);

		bool any_valid = false;
		for (auto &key : ExtensionHelper::GetPublicKeys()) {
			if (duckdb_mbedtls::MbedTlsWrapper::IsValidSha256Signature(key, signature, hash)) {
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
}

ExtensionInitResult ExtensionHelper::InitialLoad(DBConfig &config, FileOpener *opener, const string &extension) {
	string error;
	ExtensionInitResult result;
	if (!TryInitialLoad(config, opener, extension, result, error)) {
		throw IOException(error);
	}
	return result;
}

bool ExtensionHelper::IsFullPath(const string &extension) {
	return StringUtil::Contains(extension, ".") || StringUtil::Contains(extension, "/") ||
	       StringUtil::Contains(extension, "\\");
}

string ExtensionHelper::GetExtensionName(const string &extension) {
	if (!IsFullPath(extension)) {
		return extension;
	}
	auto splits = StringUtil::Split(StringUtil::Replace(extension, "\\", "/"), '/');
	if (splits.empty()) {
		return extension;
	}
	splits = StringUtil::Split(splits.back(), '.');
	if (splits.empty()) {
		return extension;
	}
	return StringUtil::Lower(splits.front());
}

void ExtensionHelper::LoadExternalExtension(DatabaseInstance &db, FileOpener *opener, const string &extension) {
	if (db.ExtensionIsLoaded(extension)) {
		return;
	}

	auto res = InitialLoad(DBConfig::GetConfig(db), opener, extension);
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
}

void ExtensionHelper::LoadExternalExtension(ClientContext &context, const string &extension) {
	LoadExternalExtension(DatabaseInstance::GetDatabase(context), FileSystem::GetFileOpener(context), extension);
}

void ExtensionHelper::StorageInit(string &extension, DBConfig &config) {
	extension = ExtensionHelper::ApplyExtensionAlias(extension);
	ExtensionInitResult res;
	string error;
	if (!TryInitialLoad(config, nullptr, extension, res, error)) {
		if (!ExtensionHelper::AllowAutoInstall(extension)) {
			throw IOException(error);
		}
		// the extension load failed - try installing the extension
		if (!config.file_system) {
			throw InternalException("Attempting to install an extension without a file system");
		}
		ExtensionHelper::InstallExtension(config, *config.file_system, extension, false);
		// try loading again
		if (!TryInitialLoad(config, nullptr, extension, res, error)) {
			throw IOException(error);
		}
	}
	auto storage_fun_name = res.basename + "_storage_init";

	ext_storage_init_t storage_init_fun;
	storage_init_fun = LoadFunctionFromDLL<ext_storage_init_t>(res.lib_hdl, storage_fun_name, res.filename);

	try {
		(*storage_init_fun)(config);
	} catch (std::exception &e) {
		throw InvalidInputException(
		    "Storage initialization function \"%s\" from file \"%s\" threw an exception: \"%s\"", storage_fun_name,
		    res.filename, e.what());
	}
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
