#include "duckdb/main/extension_repository_manager.hpp"

#include "duckdb/common/exception/catalog_exception.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/types/blob.hpp"
#include "duckdb/common/serializer/buffered_file_writer.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/extension_helper.hpp"
#include "duckdb/main/settings.hpp"
#include "mbedtls_wrapper.hpp"
#include "yyjson.hpp"

using namespace duckdb_yyjson; // NOLINT

namespace duckdb {

static constexpr const char *PEM_HEADER = "-----BEGIN PUBLIC KEY-----";
static constexpr const char *PEM_FOOTER = "-----END PUBLIC KEY-----";
//! The number of base64 characters that are written per line in a PEM encoded key
static constexpr const idx_t PEM_LINE_LENGTH = 64;
//! Sanity limit on the size of a public key file that is fetched from a repository
static constexpr const idx_t MAX_PUBLIC_KEY_SIZE = 4096;

//===--------------------------------------------------------------------===//
// Public keys
//===--------------------------------------------------------------------===//
string ExtensionRepositoryManager::ToPEMPublicKey(const string &compact_key) {
	string result = string(PEM_HEADER) + "\n";
	for (idx_t pos = 0; pos < compact_key.size(); pos += PEM_LINE_LENGTH) {
		result += compact_key.substr(pos, PEM_LINE_LENGTH) + "\n";
	}
	result += string(PEM_FOOTER) + "\n";
	return result;
}

string ExtensionRepositoryManager::ToCompactPublicKey(const string &public_key) {
	string key = public_key;

	// strip the PEM header and footer if the key is provided in PEM form
	auto header_pos = key.find(PEM_HEADER);
	if (header_pos != string::npos) {
		auto footer_pos = key.find(PEM_FOOTER, header_pos);
		if (footer_pos == string::npos) {
			throw InvalidInputException("Invalid public key: found '%s' but no matching '%s'", PEM_HEADER, PEM_FOOTER);
		}
		auto body_start = header_pos + strlen(PEM_HEADER);
		key = key.substr(body_start, footer_pos - body_start);
	} else if (StringUtil::Contains(key, "-----BEGIN")) {
		throw InvalidInputException(
		    "Invalid public key: only keys in SubjectPublicKeyInfo format (starting with '%s') are supported",
		    PEM_HEADER);
	}

	// the compact representation is the base64 body of the PEM encoding without any whitespace
	string result;
	for (auto c : key) {
		if (c == ' ' || c == '\t' || c == '\n' || c == '\r') {
			continue;
		}
		if (!StringUtil::CharacterIsAlpha(c) && !StringUtil::CharacterIsDigit(c) && c != '+' && c != '/' && c != '=') {
			throw InvalidInputException("Invalid public key: unexpected character '%c' in base64 encoded key", c);
		}
		result += c;
	}

	if (!duckdb_mbedtls::MbedTlsWrapper::IsValidRSA2048PublicKey(ToPEMPublicKey(result))) {
		throw InvalidInputException("Invalid public key: extensions are signed using 2048 bit RSA keys, provide the "
		                            "public key either in PEM format or as the base64 encoded body of the PEM format");
	}
	return result;
}

string ExtensionRepositoryManager::GetPublicKeyFingerprint(const string &compact_key) {
	auto der_key = Blob::FromBase64(string_t(compact_key));

	char hash[duckdb_mbedtls::MbedTlsWrapper::SHA256_HASH_LENGTH_BYTES];
	duckdb_mbedtls::MbedTlsWrapper::ComputeSha256Hash(der_key.data(), der_key.size(), hash);

	char hex[duckdb_mbedtls::MbedTlsWrapper::SHA256_HASH_LENGTH_TEXT];
	duckdb_mbedtls::MbedTlsWrapper::ToBase16(hash, hex, duckdb_mbedtls::MbedTlsWrapper::SHA256_HASH_LENGTH_BYTES);

	return "sha256:" + string(hex, duckdb_mbedtls::MbedTlsWrapper::SHA256_HASH_LENGTH_TEXT);
}

//===--------------------------------------------------------------------===//
// Repository storage
//===--------------------------------------------------------------------===//
//! Fetch the public key that the repository publishes at its prefix. Note that the key is pinned when the repository
//! is created: the fingerprint that is reported back is what identifies the key from there on
string ExtensionRepositoryManager::FetchPublicKey(DatabaseInstance &db, optional_ptr<ClientContext> context,
                                                  const string &prefix) {
	auto key_path = prefix;
	StringUtil::RTrim(key_path, "/");
	key_path += "/" + string(PUBLIC_KEY_FILE);

	auto &fs = db.GetFileSystem();
	// remote file systems are provided by extensions - autoload them if necessary
	string required_extension;
	if (context && fs.IsRemoteFile(key_path, required_extension) && !db.ExtensionIsLoaded(required_extension) &&
	    Settings::Get<AutoloadKnownExtensionsSetting>(*context)) {
		ExtensionHelper::AutoLoadExtension(*context, required_extension);
	}

	string contents;
	try {
		auto handle = fs.OpenFile(key_path, FileFlags::FILE_FLAGS_READ);
		auto file_size = handle->GetFileSize();
		if (file_size > MAX_PUBLIC_KEY_SIZE) {
			throw IOException("File is %llu bytes, which is too large to be a public key", file_size);
		}
		auto buffer = make_unsafe_uniq_array<char>(file_size);
		handle->Read(buffer.get(), file_size, 0);
		contents = string(buffer.get(), file_size);
	} catch (std::exception &ex) {
		ErrorData error(ex);
		throw IOException("Failed to read the public key of the extension repository from '%s': %s\nProvide the key "
		                  "directly using `USING PUBLIC KEY '<key>'` if the repository does not serve it",
		                  key_path, error.RawMessage());
	}
	return contents;
}

void ExtensionRepositoryManager::ThrowIfDisabled(DatabaseInstance &db) {
	if (!Settings::Get<AllowExtensionRepositoriesSetting>(db)) {
		throw PermissionException("Custom extension repositories are disabled through configuration");
	}
	// modifying the trusted repositories changes the set of keys that are trusted to sign extensions, so it is gated
	// by the same lock as the setting that governs the feature: a locked configuration freezes the trust store
	db.config.CheckLock(AllowExtensionRepositoriesSetting::Name);
}

bool ExtensionRepositoryManager::TryNormalizeRepositoryName(const string &name, string &result) {
	if (name.empty()) {
		return false;
	}
	result = StringUtil::Lower(name);
	for (auto c : result) {
		if (!StringUtil::CharacterIsAlpha(c) && !StringUtil::CharacterIsDigit(c) && c != '_' && c != '-') {
			return false;
		}
	}
	return true;
}

string ExtensionRepositoryManager::NormalizeRepositoryName(const string &name) {
	string result;
	if (!TryNormalizeRepositoryName(name, result)) {
		throw InvalidInputException("Invalid extension repository name '%s': names cannot be empty and can only "
		                            "contain letters, digits, '_' and '-'",
		                            name);
	}
	return result;
}

string ExtensionRepositoryManager::GetRepositoryDirectory(DatabaseInstance &db, FileSystem &fs) {
	auto directory = Settings::Get<ExtensionRepositoryDirectorySetting>(db);
	if (directory.empty()) {
		directory = fs.JoinPath(fs.JoinPath(fs.GetHomeDirectory(), ".duckdb"), "extension_repositories");
	}
	return fs.ExpandPath(fs.ConvertSeparators(directory));
}

string ExtensionRepositoryManager::GetRepositoryPath(DatabaseInstance &db, FileSystem &fs, const string &name) {
	return fs.JoinPath(GetRepositoryDirectory(db, fs), name + FILE_EXTENSION);
}

//! Wrapper that ensures the yyjson document is freed when an exception is thrown
struct JSONDocument {
	explicit JSONDocument(yyjson_doc *doc_p) : doc(doc_p) {
	}
	~JSONDocument() {
		yyjson_doc_free(doc);
	}
	yyjson_doc *doc;
};

//! Wrapper that ensures the mutable yyjson document is freed when an exception is thrown
struct MutableJSONDocument {
	MutableJSONDocument() : doc(yyjson_mut_doc_new(nullptr)) {
	}
	~MutableJSONDocument() {
		yyjson_mut_doc_free(doc);
	}
	yyjson_mut_doc *doc;
};

static string ReadJSONString(yyjson_val *root, const char *field, const string &path) {
	auto val = yyjson_obj_get(root, field);
	if (!val || !yyjson_is_str(val)) {
		throw IOException("Failed to read extension repository file '%s': missing or invalid field '%s'", path, field);
	}
	return string(yyjson_get_str(val), yyjson_get_len(val));
}

ExtensionRepository ExtensionRepositoryManager::ReadRepository(FileSystem &fs, const string &path) {
	auto handle = fs.OpenFile(path, FileFlags::FILE_FLAGS_READ);
	auto file_size = handle->GetFileSize();
	auto buffer = make_unsafe_uniq_array<char>(file_size);
	handle->Read(buffer.get(), file_size, 0);
	string contents(buffer.get(), file_size);

	JSONDocument document(yyjson_read(contents.c_str(), contents.size(), 0));
	if (!document.doc) {
		throw IOException("Failed to read extension repository file '%s': the file is not valid json", path);
	}
	auto root = yyjson_doc_get_root(document.doc);
	if (!root || !yyjson_is_obj(root)) {
		throw IOException("Failed to read extension repository file '%s': expected a json object", path);
	}

	ExtensionRepository result;
	result.name = ReadJSONString(root, "name", path);
	result.path = ReadJSONString(root, "url", path);
	result.public_key = ToCompactPublicKey(ReadJSONString(root, "public_key", path));
	result.type = ExtensionRepositoryType::USER_PROVIDED;

	auto expected_name = fs.ExtractBaseName(path);
	if (NormalizeRepositoryName(result.name) != expected_name) {
		throw IOException("Failed to read extension repository file '%s': the file contains repository '%s', but '%s' "
		                  "was expected based on the file name",
		                  path, result.name, expected_name);
	}
	// a user provided repository must never shadow a built-in repository: those are resolved first, so a file named
	// after a built-in repository would silently redirect installs to an attacker controlled key and url
	if (!ExtensionRepository::TryGetRepositoryUrl(result.name).empty()) {
		throw IOException("Failed to read extension repository file '%s': '%s' is a built-in repository name", path,
		                  result.name);
	}
	return result;
}

ExtensionRepository ExtensionRepositoryManager::CreateRepository(DatabaseInstance &db, FileSystem &fs,
                                                                 optional_ptr<ClientContext> context,
                                                                 const ExtensionRepository &repository,
                                                                 OnCreateConflict on_conflict) {
	ThrowIfDisabled(db);

	auto name = NormalizeRepositoryName(repository.name);
	if (!ExtensionRepository::TryGetRepositoryUrl(name).empty()) {
		throw InvalidInputException("Cannot create extension repository '%s': this is a built-in repository name",
		                            name);
	}
	if (repository.path.empty()) {
		throw InvalidInputException("Cannot create extension repository '%s': the prefix cannot be empty", name);
	}
	auto directory = GetRepositoryDirectory(db, fs);
	auto file_path = fs.JoinPath(directory, name + FILE_EXTENSION);
	if (fs.FileExists(file_path)) {
		switch (on_conflict) {
		case OnCreateConflict::ERROR_ON_CONFLICT:
			throw CatalogException(
			    "Extension repository with name \"%s\" already exists!\nUse CREATE OR REPLACE to overwrite it", name);
		case OnCreateConflict::IGNORE_ON_CONFLICT:
			return ReadRepository(fs, file_path);
		default:
			break;
		}
	}

	// if no key was provided, the repository is expected to serve it at its prefix. The key is pinned here: it is
	// stored alongside the repository and never fetched again
	auto public_key =
	    repository.public_key.empty() ? FetchPublicKey(db, context, repository.path) : repository.public_key;
	// this validates the public key
	public_key = ToCompactPublicKey(public_key);

	if (!fs.DirectoryExists(directory)) {
		fs.CreateDirectoriesRecursive(directory);
	}

	MutableJSONDocument document;
	auto root = yyjson_mut_obj(document.doc);
	yyjson_mut_doc_set_root(document.doc, root);
	yyjson_mut_obj_add_uint(document.doc, root, "version", FORMAT_VERSION);
	yyjson_mut_obj_add_strcpy(document.doc, root, "name", name.c_str());
	yyjson_mut_obj_add_strcpy(document.doc, root, "url", repository.path.c_str());
	yyjson_mut_obj_add_strcpy(document.doc, root, "public_key", public_key.c_str());

	size_t json_size;
	auto json = yyjson_mut_write(document.doc, YYJSON_WRITE_PRETTY, &json_size);
	if (!json) {
		throw InternalException("Failed to serialize extension repository '%s'", name);
	}
	string contents(json, json_size);
	free(json);

	// write to a temporary file first, then move it in place
	auto temp_path = file_path + ".tmp-" + UUID::ToString(UUID::GenerateRandomUUID());
	fs.TryRemoveFile(temp_path);
	{
		auto file_writer =
		    BufferedFileWriter(fs, temp_path, FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_FILE_CREATE_NEW);
		file_writer.WriteData(const_data_ptr_cast(contents.c_str()), contents.size());
		file_writer.Close();
	}
	fs.TryRemoveFile(file_path);
	fs.MoveFile(temp_path, file_path);

	ExtensionRepository result(name, repository.path, public_key);
	result.type = ExtensionRepositoryType::USER_PROVIDED;
	return result;
}

void ExtensionRepositoryManager::DropRepository(DatabaseInstance &db, FileSystem &fs, const string &name_p,
                                                OnEntryNotFound on_entry_not_found) {
	ThrowIfDisabled(db);

	auto name = NormalizeRepositoryName(name_p);
	auto file_path = GetRepositoryPath(db, fs, name);
	if (!fs.FileExists(file_path)) {
		if (on_entry_not_found == OnEntryNotFound::RETURN_NULL) {
			return;
		}
		throw CatalogException("Extension repository with name \"%s\" does not exist!", name);
	}
	fs.RemoveFile(file_path);
}

vector<ExtensionRepository> ExtensionRepositoryManager::GetRepositories(DatabaseInstance &db, FileSystem &fs) {
	vector<ExtensionRepository> result;
	if (!Settings::Get<AllowExtensionRepositoriesSetting>(db)) {
		return result;
	}
	auto directory = GetRepositoryDirectory(db, fs);
	if (!fs.DirectoryExists(directory)) {
		return result;
	}
	vector<string> files;
	fs.ListFiles(directory, [&](const string &file_name, bool is_dir) {
		if (!is_dir && StringUtil::EndsWith(file_name, FILE_EXTENSION)) {
			files.push_back(file_name);
		}
	});
	// sort the files to get a deterministic order
	std::sort(files.begin(), files.end());
	for (auto &file_name : files) {
		auto base_name = fs.ExtractBaseName(file_name);
		// ignore files that are named after a built-in repository: those are always served by the built-in table
		string normalized;
		if (TryNormalizeRepositoryName(base_name, normalized) &&
		    !ExtensionRepository::TryGetRepositoryUrl(normalized).empty()) {
			continue;
		}
		result.push_back(ReadRepository(fs, fs.JoinPath(directory, file_name)));
	}
	return result;
}

bool ExtensionRepositoryManager::TryGetRepository(DatabaseInstance &db, FileSystem &fs, const string &name_p,
                                                  ExtensionRepository &result) {
	if (!Settings::Get<AllowExtensionRepositoriesSetting>(db)) {
		return false;
	}
	string name;
	if (!TryNormalizeRepositoryName(name_p, name)) {
		// this is not a valid repository name - it might still be a url
		return false;
	}
	if (!ExtensionRepository::TryGetRepositoryUrl(name).empty()) {
		// this is a built-in repository - it is always resolved through the built-in table, never through a file that
		// happens to be named after it
		return false;
	}
	auto file_path = GetRepositoryPath(db, fs, name);
	if (!fs.FileExists(file_path)) {
		return false;
	}
	result = ReadRepository(fs, file_path);
	return true;
}

} // namespace duckdb
