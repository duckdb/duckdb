#include "duckdb/function/function_set.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "httpfs-extension.hpp"
#include "duckdb/main/client_data.hpp"
#include "httpfs.hpp"
#include "s3fs.hpp"

#include "duckdb/common/http_state.hpp"

namespace duckdb {

void CacheRemoteFile::CacheRemoteFileFunction(duckdb::DataChunk &args, duckdb::ExpressionState &state,
                                              duckdb::Vector &result) {
	D_ASSERT(args.ColumnCount() == 1);
	auto &url_vector = args.data[0];
	auto &context = state.GetContext();
	auto &url_cache = context.client_data->registered_url;
	auto &fs = FileSystem::GetFileSystem(context);

	auto &client_config = ClientConfig::GetConfig(context);
	Value force_download(false);
	Value true_value(true);
	if (client_config.set_variables.find("force_download") != client_config.set_variables.end()) {
		force_download = client_config.set_variables["force_download"];
	}

	for (idx_t i = 0; i < args.size(); i++) {
		auto url = url_vector.GetValue(i).GetValue<string>();
		if (url_cache.find(url) != url_cache.end()) {
			throw InvalidInputException("The URL: %s is already cached", url);
		}
		if (HTTPFileSystem::ValidURL(url) || S3FileSystem::ValidURL(url)) {
			// this is an HTTP URL
			client_config.set_variables["force_download"] = true_value;
			auto fh = fs.OpenFile(url.c_str(), FileFlags::FILE_FLAGS_READ, FileLockType::NO_LOCK,
			                      FileCompressionType::AUTO_DETECT, FileSystem::GetFileOpener(context));
			auto hfh = (HTTPFileHandle *)fh.get();
			url_cache[url] = hfh->state->cached_files[url];
			client_config.set_variables["force_download"] = force_download;
		} else {
			throw InvalidInputException("HTTPFS can't handle this URL");
		}
	}
	// TODO; should we just return false for URLs we can't register instead of throwing errors?
	result.SetVectorType(duckdb::VectorType::CONSTANT_VECTOR);
	result.SetValue(0, true);
}

void DeleteCachedFile::DeleteCachedFileFunction(duckdb::DataChunk &args, duckdb::ExpressionState &state,
                                                duckdb::Vector &result) {
	D_ASSERT(args.ColumnCount() == 1);
	auto &url_vector = args.data[0];
	auto &context = state.GetContext();
	auto &url_cache = context.client_data->registered_url;

	for (idx_t i = 0; i < args.size(); i++) {
		auto url = url_vector.GetValue(i).GetValue<string>();
		if (url_cache.find(url) == url_cache.end()) {
			throw InvalidInputException("The URL: %s is not yet cached, nothing to remove.", url);
		}
		url_cache.erase(url);
	}
	// TODO; should we just return false for URLs we can't unregister instead of throwing errors?
	result.SetVectorType(duckdb::VectorType::CONSTANT_VECTOR);
	result.SetValue(0, true);
}

} // namespace duckdb
