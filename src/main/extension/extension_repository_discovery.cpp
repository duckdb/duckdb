#include "duckdb/main/extension/extension_repository_discovery.hpp"

#include "duckdb/common/error_data.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/extension_helper.hpp"

#include "mbedtls_wrapper.hpp"
#include "yyjson.hpp"

using namespace duckdb_yyjson; // NOLINT

namespace duckdb {

string ExtensionRepositoryDiscovery::NormalizeDiscoveryUrl(const string &url) {
	if (StringUtil::EndsWith(StringUtil::Lower(url), ".json")) {
		return url;
	}
	string base = url;
	while (!base.empty() && base.back() == '/') {
		base.pop_back();
	}
	return base + WELL_KNOWN_PATH;
}

string ExtensionRepositoryDiscovery::KeyFingerprint(const string &public_key) {
	auto hash = duckdb_mbedtls::MbedTlsWrapper::ComputeSha256Hash(public_key);
	static const char *HEX = "0123456789abcdef";
	string hex;
	hex.reserve(hash.size() * 2);
	for (auto c : hash) {
		auto byte = static_cast<uint8_t>(c);
		hex += HEX[byte >> 4];
		hex += HEX[byte & 0x0F];
	}
	return "SHA256:" + hex;
}

ExtensionRepositoryDiscovery ExtensionRepositoryDiscovery::Parse(const string &json_content, const string &source) {
	ExtensionRepositoryDiscovery result;

	yyjson_doc *doc = yyjson_read(json_content.c_str(), json_content.size(), 0);
	if (!doc) {
		throw IOException("Failed to parse extension repository discovery document at '%s': invalid JSON", source);
	}

	try {
		yyjson_val *root = yyjson_doc_get_root(doc);
		if (!root || !yyjson_is_obj(root)) {
			throw IOException("Extension repository discovery document at '%s' is not a JSON object", source);
		}

		auto schema = yyjson_obj_get(root, "schema_version");
		if (schema && yyjson_is_int(schema)) {
			result.schema_version = yyjson_get_sint(schema);
		}
		auto repository = yyjson_obj_get(root, "repository");
		if (repository && yyjson_is_str(repository)) {
			result.repository = yyjson_get_str(repository);
		}
		auto url = yyjson_obj_get(root, "url");
		if (url && yyjson_is_str(url)) {
			result.url = yyjson_get_str(url);
		}
		auto url_template = yyjson_obj_get(root, "url_template");
		if (url_template && yyjson_is_str(url_template)) {
			result.url_template = yyjson_get_str(url_template);
		}

		auto signing_keys = yyjson_obj_get(root, "signing_keys");
		if (signing_keys && yyjson_is_arr(signing_keys)) {
			size_t idx, max;
			yyjson_val *key;
			yyjson_arr_foreach(signing_keys, idx, max, key) {
				if (!yyjson_is_obj(key)) {
					continue;
				}
				ExtensionRepositorySigningKey sk;
				auto public_key = yyjson_obj_get(key, "public_key");
				if (public_key && yyjson_is_str(public_key)) {
					sk.public_key = yyjson_get_str(public_key);
				}
				auto kid = yyjson_obj_get(key, "kid");
				if (kid && yyjson_is_str(kid)) {
					sk.kid = yyjson_get_str(kid);
				}
				auto algorithm = yyjson_obj_get(key, "algorithm");
				if (algorithm && yyjson_is_str(algorithm)) {
					sk.algorithm = yyjson_get_str(algorithm);
				}
				auto valid_to = yyjson_obj_get(key, "valid_to");
				if (valid_to && yyjson_is_str(valid_to)) {
					sk.valid_to = yyjson_get_str(valid_to);
				}
				if (!sk.public_key.empty()) {
					result.signing_keys.push_back(std::move(sk));
				}
			}
		}
	} catch (...) {
		yyjson_doc_free(doc);
		throw;
	}
	yyjson_doc_free(doc);

	if (result.signing_keys.empty()) {
		throw IOException("Extension repository discovery document at '%s' contains no usable signing_keys", source);
	}
	return result;
}

ExtensionRepositoryDiscovery ExtensionRepositoryDiscovery::FetchAndParse(ClientContext &context,
                                                                         const string &discovery_url) {
	auto &fs = FileSystem::GetFileSystem(context);

	// For remote discovery urls (https/s3/...), make sure the handling extension (httpfs) is available.
	string required_extension;
	if (FileSystem::IsRemoteFile(discovery_url, required_extension)) {
		if (!required_extension.empty() &&
		    !DatabaseInstance::GetDatabase(context).ExtensionIsLoaded(required_extension)) {
			ExtensionHelper::AutoLoadExtension(context, required_extension);
		}
	}

	unique_ptr<FileHandle> handle;
	try {
		handle = fs.OpenFile(discovery_url, FileFlags::FILE_FLAGS_READ);
	} catch (std::exception &ex) {
		ErrorData error(ex);
		throw IOException("Failed to open extension repository discovery document at '%s': %s", discovery_url,
		                  error.RawMessage());
	}

	auto file_size = handle->GetFileSize();
	if (file_size <= 0) {
		throw IOException("Extension repository discovery document at '%s' is empty", discovery_url);
	}
	static constexpr int64_t MAX_DISCOVERY_SIZE = 1 << 20; // 1 MiB
	if (file_size > MAX_DISCOVERY_SIZE) {
		throw IOException("Extension repository discovery document at '%s' is too large (%lld bytes)", discovery_url,
		                  file_size);
	}

	string content;
	content.resize(NumericCast<idx_t>(file_size));
	handle->Read(const_cast<char *>(content.data()), NumericCast<idx_t>(file_size), 0);

	return Parse(content, discovery_url);
}

} // namespace duckdb
