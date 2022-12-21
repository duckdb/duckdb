#include "duckdb/common/dl.hpp"
#include "duckdb/common/virtual_file_system.hpp"
#include "duckdb/function/replacement_open.hpp"
#include "duckdb/main/extension_helper.hpp"
#include "duckdb/main/error_manager.hpp"
#include "mbedtls_wrapper.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Load External Extension
//===--------------------------------------------------------------------===//
typedef void (*ext_init_fun_t)(DatabaseInstance &);
typedef const char *(*ext_version_fun_t)(void);

template <class T>
static T LoadFunctionFromDLL(void *dll, const string &function_name, const string &filename) {
	auto function = dlsym(dll, function_name.c_str());
	if (!function) {
		throw IOException("File \"%s\" did not contain function \"%s\": %s", filename, function_name, GetDLError());
	}
	return (T)function;
}

ExtensionInitResult ExtensionHelper::InitialLoad(DBConfig &config, FileOpener *opener, const string &extension) {
	if (!config.options.enable_external_access) {
		throw PermissionException("Loading external extensions is disabled through configuration");
	}
	VirtualFileSystem fallback_file_system; // config may not contain one yet
	auto &fs = config.file_system ? *config.file_system : fallback_file_system;
	auto filename = fs.ConvertSeparators(extension);

	// shorthand case
	if (!StringUtil::Contains(extension, ".") && !StringUtil::Contains(extension, fs.PathSeparator())) {
		string local_path = fs.GetHomeDirectory(opener);
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
		throw IOException("Extension \"%s\" not found.\n%s", filename, message);
	}
	{
		auto handle = fs.OpenFile(filename, FileFlags::FILE_FLAGS_READ);

		// signature is the last 265 bytes of the file

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
		if (!any_valid && !config.options.allow_unsigned_extensions) {
			throw IOException(config.error_manager->FormatException(ErrorType::UNSIGNED_EXTENSION, filename));
		}
	}
	auto lib_hdl = dlopen(filename.c_str(), RTLD_NOW | RTLD_LOCAL);
	if (!lib_hdl) {
		throw IOException("Extension \"%s\" could not be loaded: %s", filename, GetDLError());
	}

	auto basename = fs.ExtractBaseName(filename);

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

	ExtensionInitResult res;
	res.basename = basename;
	res.filename = filename;
	res.lib_hdl = lib_hdl;
	return res;
}

void ExtensionHelper::LoadExternalExtension(ClientContext &context, const string &extension) {
	auto &db = DatabaseInstance::GetDatabase(context);
	auto &loaded_extensions = db.LoadedExtensions();
	if (loaded_extensions.find(extension) != loaded_extensions.end()) {
		return;
	}

	auto res = InitialLoad(DBConfig::GetConfig(context), FileSystem::GetFileOpener(context), extension);
	auto init_fun_name = res.basename + "_init";

	ext_init_fun_t init_fun;
	init_fun = LoadFunctionFromDLL<ext_init_fun_t>(res.lib_hdl, init_fun_name, res.filename);

	try {
		(*init_fun)(db);
	} catch (std::exception &e) {
		throw InvalidInputException("Initialization function \"%s\" from file \"%s\" threw an exception: \"%s\"",
		                            init_fun_name, res.filename, e.what());
	}

	DatabaseInstance::GetDatabase(context).SetExtensionLoaded(extension);
}

unique_ptr<ReplacementOpenData> ExtensionHelper::ReplacementOpenPre(const string &extension, DBConfig &config) {

	auto res = InitialLoad(config, nullptr, extension); // TODO opener
	auto init_fun_name = res.basename + "_replacement_open_pre";

	replacement_open_pre_t open_pre_fun;
	open_pre_fun = LoadFunctionFromDLL<replacement_open_pre_t>(res.lib_hdl, init_fun_name, res.filename);

	try {
		return (*open_pre_fun)(config, nullptr);
	} catch (std::exception &e) {
		throw InvalidInputException("Initialization function \"%s\" from file \"%s\" threw an exception: \"%s\"",
		                            init_fun_name, res.filename, e.what());
	}
}

void ExtensionHelper::ReplacementOpenPost(ClientContext &context, const string &extension, DatabaseInstance &instance,
                                          ReplacementOpenData *open_data) {
	auto res = InitialLoad(DBConfig::GetConfig(context), FileSystem::GetFileOpener(context), extension);
	auto init_fun_name = res.basename + "_replacement_open_post";

	replacement_open_post_t open_post_fun;
	open_post_fun = LoadFunctionFromDLL<replacement_open_post_t>(res.lib_hdl, init_fun_name, res.filename);

	try {
		(*open_post_fun)(instance, open_data);
	} catch (std::exception &e) {
		throw InvalidInputException("Initialization function \"%s\" from file \"%s\" threw an exception: \"%s\"",
		                            init_fun_name, res.filename, e.what());
	}
}

string ExtensionHelper::ExtractExtensionPrefixFromPath(const string &path) {
	auto first_colon = path.find(':');
	if (first_colon == string::npos || first_colon < 2) { // needs to be at least two characters because windows c: ...
		return "";
	}
	auto extension = path.substr(0, first_colon);
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
