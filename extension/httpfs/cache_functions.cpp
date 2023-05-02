#include "duckdb/function/function_set.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "httpfs-extension.hpp"
#include "duckdb/main/client_data.hpp"
#include "httpfs.hpp"
#include "s3fs.hpp"

#include "duckdb/common/http_state.hpp"

namespace duckdb {

struct CacheFunctionData : public TableFunctionData {
	CacheFunctionData() = default;
	string url;
};

unique_ptr<FunctionData> RemoteFileGeneric::Bind(ClientContext &context, TableFunctionBindInput &input,
                                                 vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_uniq<CacheFunctionData>();
	result->url = input.inputs[0].GetValueUnsafe<string>();
	return_types.emplace_back(LogicalType::BOOLEAN);
	names.emplace_back("Success");
	return std::move(result);
}

void CacheRemoteFile::Function(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto url = ((CacheFunctionData &)*data.bind_data).url;

	auto &url_cache = context.client_data->registered_url;
	auto &fs = FileSystem::GetFileSystem(context);

	auto &client_config = ClientConfig::GetConfig(context);
	Value force_download(false);
	Value true_value(true);
	if (client_config.set_variables.find("force_download") != client_config.set_variables.end()) {
		force_download = client_config.set_variables["force_download"];
	}

	if (url_cache.find(url) != url_cache.end()) {
		throw InvalidInputException("The URL: %s is already cached", url);
	}
	if (HTTPFileSystem::ValidURL(url) || S3FileSystem::ValidURL(url)) {
		// this is an HTTP URL
		client_config.set_variables["force_download"] = true_value;
		unique_ptr<FileHandle> fh;
		try {
			fh = fs.OpenFile(url.c_str(), FileFlags::FILE_FLAGS_READ, FileLockType::NO_LOCK,
			                 FileCompressionType::AUTO_DETECT, FileSystem::GetFileOpener(context));
		} catch (const Exception &e) {
			// Exception caught, reset configuration and re-throwing it
			client_config.set_variables["force_download"] = force_download;
			throw e;
		}
		client_config.set_variables["force_download"] = force_download;
		auto hfh = (HTTPFileHandle *)fh.get();
		url_cache[url] = hfh->state->cached_files[url];

	} else {
		throw InvalidInputException("HTTPFS can't handle this URL");
	}
}

void DeleteCachedFile::Function(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto url = ((CacheFunctionData &)*data.bind_data).url;

	auto &url_cache = context.client_data->registered_url;
	if (url_cache.find(url) == url_cache.end()) {
		throw InvalidInputException("The URL: %s is not yet cached, nothing to remove.", url);
	}
	url_cache.erase(url);
}

} // namespace duckdb
