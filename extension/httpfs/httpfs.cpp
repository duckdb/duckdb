#include "httpfs.hpp"

#include "duckdb/common/atomic.hpp"
#include "duckdb/common/exception/http_exception.hpp"
#include "duckdb/common/file_opener.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/http_util.hpp"
#include "duckdb/common/thread.hpp"
#include "duckdb/common/types/hash.hpp"
#include "duckdb/function/scalar/strftime_format.hpp"
#include "duckdb/logging/http_logger.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/secret/secret_manager.hpp"
#include "http_state.hpp"

#include <chrono>
#include <map>
#include <string>
#include <thread>

#define CPPHTTPLIB_OPENSSL_SUPPORT
#include "httplib.hpp"

namespace duckdb {

duckdb::unique_ptr<duckdb_httplib_openssl::Headers> HTTPFileSystem::InitializeHeaders(HeaderMap &header_map,
                                                                                      const HTTPParams &http_params) {
	auto headers = make_uniq<duckdb_httplib_openssl::Headers>();
	for (auto &entry : header_map) {
		headers->insert(entry);
	}

	for (auto &entry : http_params.extra_headers) {
		headers->insert(entry);
	}

	return headers;
}

HTTPParams HTTPParams::ReadFrom(optional_ptr<FileOpener> opener, optional_ptr<FileOpenerInfo> info) {
	auto result = HTTPParams();

	// No point in continueing without an opener
	if (!opener) {
		return result;
	}

	Value value;

	// Setting lookups
	FileOpener::TryGetCurrentSetting(opener, "http_timeout", result.timeout, info);
	FileOpener::TryGetCurrentSetting(opener, "force_download", result.force_download, info);
	FileOpener::TryGetCurrentSetting(opener, "http_retries", result.retries, info);
	FileOpener::TryGetCurrentSetting(opener, "http_retry_wait_ms", result.retry_wait_ms, info);
	FileOpener::TryGetCurrentSetting(opener, "http_retry_backoff", result.retry_backoff, info);
	FileOpener::TryGetCurrentSetting(opener, "http_keep_alive", result.keep_alive, info);
	FileOpener::TryGetCurrentSetting(opener, "enable_server_cert_verification", result.enable_server_cert_verification,
	                                 info);
	FileOpener::TryGetCurrentSetting(opener, "ca_cert_file", result.ca_cert_file, info);
	FileOpener::TryGetCurrentSetting(opener, "hf_max_per_page", result.hf_max_per_page, info);

	// HTTP Secret lookups
	KeyValueSecretReader settings_reader(*opener, info, "http");

	string proxy_setting;
	if (settings_reader.TryGetSecretKeyOrSetting<string>("http_proxy", "http_proxy", proxy_setting) &&
	    !proxy_setting.empty()) {
		idx_t port;
		string host;
		HTTPUtil::ParseHTTPProxyHost(proxy_setting, host, port);
		result.http_proxy = host;
		result.http_proxy_port = port;
	}
	settings_reader.TryGetSecretKeyOrSetting<string>("http_proxy_username", "http_proxy_username",
	                                                 result.http_proxy_username);
	settings_reader.TryGetSecretKeyOrSetting<string>("http_proxy_password", "http_proxy_password",
	                                                 result.http_proxy_password);
	settings_reader.TryGetSecretKey<string>("bearer_token", result.bearer_token);

	Value extra_headers;
	if (settings_reader.TryGetSecretKey("extra_http_headers", extra_headers)) {
		auto children = MapValue::GetChildren(extra_headers);
		for (const auto &child : children) {
			auto kv = StructValue::GetChildren(child);
			D_ASSERT(kv.size() == 2);
			result.extra_headers[kv[0].GetValue<string>()] = kv[1].GetValue<string>();
		}
	}

	return result;
}

unique_ptr<duckdb_httplib_openssl::Client> HTTPClientCache::GetClient() {
	lock_guard<mutex> lck(lock);
	if (clients.size() == 0) {
		return nullptr;
	}

	auto client = std::move(clients.back());
	clients.pop_back();
	return client;
}

void HTTPClientCache::StoreClient(unique_ptr<duckdb_httplib_openssl::Client> client) {
	lock_guard<mutex> lck(lock);
	clients.push_back(std::move(client));
}

void HTTPFileSystem::ParseUrl(string &url, string &path_out, string &proto_host_port_out) {
	if (url.rfind("http://", 0) != 0 && url.rfind("https://", 0) != 0) {
		throw IOException("URL needs to start with http:// or https://");
	}
	auto slash_pos = url.find('/', 8);
	if (slash_pos == string::npos) {
		throw IOException("URL needs to contain a '/' after the host");
	}
	proto_host_port_out = url.substr(0, slash_pos);

	path_out = url.substr(slash_pos);

	if (path_out.empty()) {
		throw IOException("URL needs to contain a path");
	}
}

// Retry the request performed by fun using the exponential backoff strategy defined in params. Before retry, the
// retry callback is called
duckdb::unique_ptr<ResponseWrapper>
HTTPFileSystem::RunRequestWithRetry(const std::function<duckdb_httplib_openssl::Result(void)> &request, string &url,
                                    string method, const HTTPParams &params,
                                    const std::function<void(void)> &retry_cb) {
	idx_t tries = 0;
	while (true) {
		std::exception_ptr caught_e = nullptr;
		duckdb_httplib_openssl::Error err;
		duckdb_httplib_openssl::Response response;
		int status;

		try {
			auto res = request();
			err = res.error();
			if (err == duckdb_httplib_openssl::Error::Success) {
				status = res->status;
				response = res.value();
			}
		} catch (IOException &e) {
			caught_e = std::current_exception();
		}

		// Note: all duckdb_httplib_openssl::Error types will be retried.
		if (err == duckdb_httplib_openssl::Error::Success) {
			switch (status) {
			case 408: // Request Timeout
			case 418: // Server is pretending to be a teapot
			case 429: // Rate limiter hit
			case 500: // Server has error
			case 503: // Server has error
			case 504: // Server has error
				break;
			default:
				return make_uniq<ResponseWrapper>(response, url);
			}
		}

		tries += 1;

		if (tries <= params.retries) {
			if (tries > 1) {
				uint64_t sleep_amount = (uint64_t)((float)params.retry_wait_ms * pow(params.retry_backoff, tries - 2));
				std::this_thread::sleep_for(std::chrono::milliseconds(sleep_amount));
			}
			if (retry_cb) {
				retry_cb();
			}
		} else {
			if (caught_e) {
				std::rethrow_exception(caught_e);
			} else if (err == duckdb_httplib_openssl::Error::Success) {
				throw HTTPException(response, "Request returned HTTP %d for HTTP %s to '%s'", status, method, url);
			} else {
				throw IOException("%s error for HTTP %s to '%s'", to_string(err), method, url);
			}
		}
	}
}

unique_ptr<ResponseWrapper> HTTPFileSystem::PostRequest(FileHandle &handle, string url, HeaderMap header_map,
                                                        duckdb::unique_ptr<char[]> &buffer_out, idx_t &buffer_out_len,
                                                        char *buffer_in, idx_t buffer_in_len, string params) {
	auto &hfh = handle.Cast<HTTPFileHandle>();
	string path, proto_host_port;
	ParseUrl(url, path, proto_host_port);
	auto headers = InitializeHeaders(header_map, hfh.http_params);
	idx_t out_offset = 0;

	std::function<duckdb_httplib_openssl::Result(void)> request([&]() {
		auto client = GetClient(hfh.http_params, proto_host_port.c_str(), &hfh);

		if (hfh.state) {
			hfh.state->post_count++;
			hfh.state->total_bytes_sent += buffer_in_len;
		}

		// We use a custom Request method here, because there is no Post call with a contentreceiver in httplib
		duckdb_httplib_openssl::Request req;
		req.method = "POST";
		req.path = path;
		req.headers = *headers;
		req.headers.emplace("Content-Type", "application/octet-stream");
		req.content_receiver = [&](const char *data, size_t data_length, uint64_t /*offset*/,
		                           uint64_t /*total_length*/) {
			if (hfh.state) {
				hfh.state->total_bytes_received += data_length;
			}
			if (out_offset + data_length > buffer_out_len) {
				// Buffer too small, increase its size by at least 2x to fit the new value
				auto new_size = MaxValue<idx_t>(out_offset + data_length, buffer_out_len * 2);
				auto tmp = duckdb::unique_ptr<char[]> {new char[new_size]};
				memcpy(tmp.get(), buffer_out.get(), buffer_out_len);
				buffer_out = std::move(tmp);
				buffer_out_len = new_size;
			}
			memcpy(buffer_out.get() + out_offset, data, data_length);
			out_offset += data_length;
			return true;
		};
		req.body.assign(buffer_in, buffer_in_len);
		return client->send(req);
	});

	return RunRequestWithRetry(request, url, "POST", hfh.http_params);
}

unique_ptr<duckdb_httplib_openssl::Client> HTTPFileSystem::GetClient(const HTTPParams &http_params,
                                                                     const char *proto_host_port,
                                                                     optional_ptr<HTTPFileHandle> hfh) {
	auto client = make_uniq<duckdb_httplib_openssl::Client>(proto_host_port);
	client->set_follow_location(true);
	client->set_keep_alive(http_params.keep_alive);
	if (!http_params.ca_cert_file.empty()) {
		client->set_ca_cert_path(http_params.ca_cert_file.c_str());
	}
	client->enable_server_certificate_verification(http_params.enable_server_cert_verification);
	client->set_write_timeout(http_params.timeout);
	client->set_read_timeout(http_params.timeout);
	client->set_connection_timeout(http_params.timeout);
	client->set_decompress(false);
	if (hfh && hfh->http_logger) {
		client->set_logger(
		    hfh->http_logger->GetLogger<duckdb_httplib_openssl::Request, duckdb_httplib_openssl::Response>());
	}
	if (!http_params.bearer_token.empty()) {
		client->set_bearer_token_auth(http_params.bearer_token.c_str());
	}

	if (!http_params.http_proxy.empty()) {
		client->set_proxy(http_params.http_proxy, http_params.http_proxy_port);

		if (!http_params.http_proxy_username.empty()) {
			client->set_proxy_basic_auth(http_params.http_proxy_username, http_params.http_proxy_password);
		}
	}

	return client;
}

unique_ptr<ResponseWrapper> HTTPFileSystem::PutRequest(FileHandle &handle, string url, HeaderMap header_map,
                                                       char *buffer_in, idx_t buffer_in_len, string params) {
	auto &hfh = handle.Cast<HTTPFileHandle>();
	string path, proto_host_port;
	ParseUrl(url, path, proto_host_port);
	auto headers = InitializeHeaders(header_map, hfh.http_params);

	std::function<duckdb_httplib_openssl::Result(void)> request([&]() {
		auto client = GetClient(hfh.http_params, proto_host_port.c_str(), &hfh);
		if (hfh.state) {
			hfh.state->put_count++;
			hfh.state->total_bytes_sent += buffer_in_len;
		}
		return client->Put(path.c_str(), *headers, buffer_in, buffer_in_len, "application/octet-stream");
	});

	return RunRequestWithRetry(request, url, "PUT", hfh.http_params);
}

unique_ptr<ResponseWrapper> HTTPFileSystem::HeadRequest(FileHandle &handle, string url, HeaderMap header_map) {
	auto &hfh = handle.Cast<HTTPFileHandle>();
	string path, proto_host_port;
	ParseUrl(url, path, proto_host_port);
	auto headers = InitializeHeaders(header_map, hfh.http_params);
	auto http_client = hfh.GetClient(nullptr);

	std::function<duckdb_httplib_openssl::Result(void)> request([&]() {
		if (hfh.state) {
			hfh.state->head_count++;
		}
		return http_client->Head(path.c_str(), *headers);
	});

	// Refresh the client on retries
	std::function<void(void)> on_retry(
	    [&]() { http_client = GetClient(hfh.http_params, proto_host_port.c_str(), &hfh); });

	auto response = RunRequestWithRetry(request, url, "HEAD", hfh.http_params, on_retry);
	hfh.StoreClient(std::move(http_client));
	return response;
}

unique_ptr<ResponseWrapper> HTTPFileSystem::GetRequest(FileHandle &handle, string url, HeaderMap header_map) {
	auto &hfh = handle.Cast<HTTPFileHandle>();
	string path, proto_host_port;
	ParseUrl(url, path, proto_host_port);
	auto headers = InitializeHeaders(header_map, hfh.http_params);

	D_ASSERT(hfh.cached_file_handle);

	auto http_client = hfh.GetClient(nullptr);

	std::function<duckdb_httplib_openssl::Result(void)> request([&]() {
		D_ASSERT(hfh.state);
		hfh.state->get_count++;
		return http_client->Get(
		    path.c_str(), *headers,
		    [&](const duckdb_httplib_openssl::Response &response) {
			    if (response.status >= 400) {
				    string error = "HTTP GET error on '" + url + "' (HTTP " + to_string(response.status) + ")";
				    if (response.status == 416) {
					    error += " This could mean the file was changed. Try disabling the duckdb http metadata cache "
					             "if enabled, and confirm the server supports range requests.";
				    }
				    throw IOException(error);
			    }
			    return true;
		    },
		    [&](const char *data, size_t data_length) {
			    D_ASSERT(hfh.state);
			    if (hfh.state) {
				    hfh.state->total_bytes_received += data_length;
			    }
			    if (!hfh.cached_file_handle->GetCapacity()) {
				    hfh.cached_file_handle->AllocateBuffer(data_length);
				    hfh.length = data_length;
				    hfh.cached_file_handle->Write(data, data_length);
			    } else {
				    auto new_capacity = hfh.cached_file_handle->GetCapacity();
				    while (new_capacity < hfh.length + data_length) {
					    new_capacity *= 2;
				    }
				    // Grow buffer when running out of space
				    if (new_capacity != hfh.cached_file_handle->GetCapacity()) {
					    hfh.cached_file_handle->GrowBuffer(new_capacity, hfh.length);
				    }
				    // We can just copy stuff
				    hfh.cached_file_handle->Write(data, data_length, hfh.length);
				    hfh.length += data_length;
			    }
			    return true;
		    });
	});

	std::function<void(void)> on_retry(
	    [&]() { http_client = GetClient(hfh.http_params, proto_host_port.c_str(), &hfh); });

	auto response = RunRequestWithRetry(request, url, "GET", hfh.http_params, on_retry);
	hfh.StoreClient(std::move(http_client));
	return response;
}

unique_ptr<ResponseWrapper> HTTPFileSystem::GetRangeRequest(FileHandle &handle, string url, HeaderMap header_map,
                                                            idx_t file_offset, char *buffer_out, idx_t buffer_out_len) {
	auto &hfh = handle.Cast<HTTPFileHandle>();
	string path, proto_host_port;
	ParseUrl(url, path, proto_host_port);
	auto headers = InitializeHeaders(header_map, hfh.http_params);

	// send the Range header to read only subset of file
	string range_expr = "bytes=" + to_string(file_offset) + "-" + to_string(file_offset + buffer_out_len - 1);
	headers->insert(pair<string, string>("Range", range_expr));

	auto http_client = hfh.GetClient(nullptr);

	idx_t out_offset = 0;

	std::function<duckdb_httplib_openssl::Result(void)> request([&]() {
		if (hfh.state) {
			hfh.state->get_count++;
		}
		return http_client->Get(
		    path.c_str(), *headers,
		    [&](const duckdb_httplib_openssl::Response &response) {
			    if (response.status >= 400) {
				    string error = "HTTP GET error on '" + url + "' (HTTP " + to_string(response.status) + ")";
				    if (response.status == 416) {
					    error += " This could mean the file was changed. Try disabling the duckdb http metadata cache "
					             "if enabled, and confirm the server supports range requests.";
				    }
				    throw HTTPException(response, error);
			    }
			    if (response.status < 300) { // done redirecting
				    out_offset = 0;
				    if (response.has_header("Content-Length")) {
					    auto content_length = stoll(response.get_header_value("Content-Length", 0));
					    if ((idx_t)content_length != buffer_out_len) {
						    throw IOException("HTTP GET error: Content-Length from server mismatches requested "
						                      "range, server may not support range requests.");
					    }
				    }
			    }
			    return true;
		    },
		    [&](const char *data, size_t data_length) {
			    if (hfh.state) {
				    hfh.state->total_bytes_received += data_length;
			    }
			    if (buffer_out != nullptr) {
				    if (data_length + out_offset > buffer_out_len) {
					    // As of v0.8.2-dev4424 we might end up here when very big files are served from servers
					    // that returns more data than requested via range header. This is an uncommon but legal
					    // behaviour, so we have to improve logic elsewhere to properly handle this case.

					    // To avoid corruption of memory, we bail out.
					    throw IOException("Server sent back more data than expected, `SET force_download=true` might "
					                      "help in this case");
				    }
				    memcpy(buffer_out + out_offset, data, data_length);
				    out_offset += data_length;
			    }
			    return true;
		    });
	});

	std::function<void(void)> on_retry(
	    [&]() { http_client = GetClient(hfh.http_params, proto_host_port.c_str(), &hfh); });

	auto response = RunRequestWithRetry(request, url, "GET Range", hfh.http_params, on_retry);
	hfh.StoreClient(std::move(http_client));
	return response;
}

HTTPFileHandle::HTTPFileHandle(FileSystem &fs, const string &path, FileOpenFlags flags, const HTTPParams &http_params)
    : FileHandle(fs, path), http_params(http_params), flags(flags), length(0), buffer_available(0), buffer_idx(0),
      file_offset(0), buffer_start(0), buffer_end(0) {
}

unique_ptr<HTTPFileHandle> HTTPFileSystem::CreateHandle(const string &path, FileOpenFlags flags,
                                                        optional_ptr<FileOpener> opener) {
	D_ASSERT(flags.Compression() == FileCompressionType::UNCOMPRESSED);

	FileOpenerInfo info;
	info.file_path = path;
	auto params = HTTPParams::ReadFrom(opener, info);

	auto secret_manager = FileOpener::TryGetSecretManager(opener);
	auto transaction = FileOpener::TryGetCatalogTransaction(opener);
	if (secret_manager && transaction) {
		auto secret_match = secret_manager->LookupSecret(*transaction, path, "bearer");

		if (secret_match.HasMatch()) {
			const auto &kv_secret = dynamic_cast<const KeyValueSecret &>(*secret_match.secret_entry->secret);
			params.bearer_token = kv_secret.TryGetValue("token", true).ToString();
		}
	}

	auto result = duckdb::make_uniq<HTTPFileHandle>(*this, path, flags, params);

	auto client_context = FileOpener::TryGetClientContext(opener);
	if (client_context && ClientConfig::GetConfig(*client_context).enable_http_logging) {
		result->http_logger = client_context->client_data->http_logger.get();
	}

	return result;
}

unique_ptr<FileHandle> HTTPFileSystem::OpenFile(const string &path, FileOpenFlags flags,
                                                optional_ptr<FileOpener> opener) {
	D_ASSERT(flags.Compression() == FileCompressionType::UNCOMPRESSED);

	if (flags.ReturnNullIfNotExists()) {
		try {
			auto handle = CreateHandle(path, flags, opener);
			handle->Initialize(opener);
			return std::move(handle);
		} catch (...) {
			return nullptr;
		}
	}

	auto handle = CreateHandle(path, flags, opener);
	handle->Initialize(opener);
	return std::move(handle);
}

// Buffered read from http file.
// Note that buffering is disabled when FileFlags::FILE_FLAGS_DIRECT_IO is set
void HTTPFileSystem::Read(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) {
	auto &hfh = handle.Cast<HTTPFileHandle>();

	D_ASSERT(hfh.state);
	if (hfh.cached_file_handle) {
		if (!hfh.cached_file_handle->Initialized()) {
			throw InternalException("Cached file not initialized properly");
		}
		memcpy(buffer, hfh.cached_file_handle->GetData() + location, nr_bytes);
		hfh.file_offset = location + nr_bytes;
		return;
	}

	idx_t to_read = nr_bytes;
	idx_t buffer_offset = 0;

	// Don't buffer when DirectIO is set or when we are doing parallel reads
	bool skip_buffer = hfh.flags.DirectIO() || hfh.flags.RequireParallelAccess();
	if (skip_buffer && to_read > 0) {
		GetRangeRequest(hfh, hfh.path, {}, location, (char *)buffer, to_read);
		hfh.buffer_available = 0;
		hfh.buffer_idx = 0;
		hfh.file_offset = location + nr_bytes;
		return;
	}

	if (location >= hfh.buffer_start && location < hfh.buffer_end) {
		hfh.file_offset = location;
		hfh.buffer_idx = location - hfh.buffer_start;
		hfh.buffer_available = (hfh.buffer_end - hfh.buffer_start) - hfh.buffer_idx;
	} else {
		// reset buffer
		hfh.buffer_available = 0;
		hfh.buffer_idx = 0;
		hfh.file_offset = location;
	}
	while (to_read > 0) {
		auto buffer_read_len = MinValue<idx_t>(hfh.buffer_available, to_read);
		if (buffer_read_len > 0) {
			D_ASSERT(hfh.buffer_start + hfh.buffer_idx + buffer_read_len <= hfh.buffer_end);
			memcpy((char *)buffer + buffer_offset, hfh.read_buffer.get() + hfh.buffer_idx, buffer_read_len);

			buffer_offset += buffer_read_len;
			to_read -= buffer_read_len;

			hfh.buffer_idx += buffer_read_len;
			hfh.buffer_available -= buffer_read_len;
			hfh.file_offset += buffer_read_len;
		}

		if (to_read > 0 && hfh.buffer_available == 0) {
			auto new_buffer_available = MinValue<idx_t>(hfh.READ_BUFFER_LEN, hfh.length - hfh.file_offset);

			// Bypass buffer if we read more than buffer size
			if (to_read > new_buffer_available) {
				GetRangeRequest(hfh, hfh.path, {}, location + buffer_offset, (char *)buffer + buffer_offset, to_read);
				hfh.buffer_available = 0;
				hfh.buffer_idx = 0;
				hfh.file_offset += to_read;
				break;
			} else {
				GetRangeRequest(hfh, hfh.path, {}, hfh.file_offset, (char *)hfh.read_buffer.get(),
				                new_buffer_available);
				hfh.buffer_available = new_buffer_available;
				hfh.buffer_idx = 0;
				hfh.buffer_start = hfh.file_offset;
				hfh.buffer_end = hfh.buffer_start + new_buffer_available;
			}
		}
	}
}

int64_t HTTPFileSystem::Read(FileHandle &handle, void *buffer, int64_t nr_bytes) {
	auto &hfh = (HTTPFileHandle &)handle;
	idx_t max_read = hfh.length - hfh.file_offset;
	nr_bytes = MinValue<idx_t>(max_read, nr_bytes);
	Read(handle, buffer, nr_bytes, hfh.file_offset);
	return nr_bytes;
}

void HTTPFileSystem::Write(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) {
	throw NotImplementedException("Writing to HTTP files not implemented");
}

int64_t HTTPFileSystem::Write(FileHandle &handle, void *buffer, int64_t nr_bytes) {
	auto &hfh = (HTTPFileHandle &)handle;
	Write(handle, buffer, nr_bytes, hfh.file_offset);
	return nr_bytes;
}

void HTTPFileSystem::FileSync(FileHandle &handle) {
	throw NotImplementedException("FileSync for HTTP files not implemented");
}

int64_t HTTPFileSystem::GetFileSize(FileHandle &handle) {
	auto &sfh = handle.Cast<HTTPFileHandle>();
	return sfh.length;
}

time_t HTTPFileSystem::GetLastModifiedTime(FileHandle &handle) {
	auto &sfh = handle.Cast<HTTPFileHandle>();
	return sfh.last_modified;
}

bool HTTPFileSystem::FileExists(const string &filename, optional_ptr<FileOpener> opener) {
	try {
		auto handle = OpenFile(filename, FileFlags::FILE_FLAGS_READ, opener);
		auto &sfh = handle->Cast<HTTPFileHandle>();
		if (sfh.length == 0) {
			return false;
		}
		return true;
	} catch (...) {
		return false;
	};
}

bool HTTPFileSystem::CanHandleFile(const string &fpath) {
	return fpath.rfind("https://", 0) == 0 || fpath.rfind("http://", 0) == 0;
}

void HTTPFileSystem::Seek(FileHandle &handle, idx_t location) {
	auto &sfh = handle.Cast<HTTPFileHandle>();
	sfh.file_offset = location;
}

idx_t HTTPFileSystem::SeekPosition(FileHandle &handle) {
	auto &sfh = handle.Cast<HTTPFileHandle>();
	return sfh.file_offset;
}

optional_ptr<HTTPMetadataCache> HTTPFileSystem::GetGlobalCache() {
	lock_guard<mutex> lock(global_cache_lock);
	if (!global_metadata_cache) {
		global_metadata_cache = make_uniq<HTTPMetadataCache>(false, true);
	}
	return global_metadata_cache.get();
}

// Get either the local, global, or no cache depending on settings
static optional_ptr<HTTPMetadataCache> TryGetMetadataCache(optional_ptr<FileOpener> opener, HTTPFileSystem &httpfs) {
	auto db = FileOpener::TryGetDatabase(opener);
	auto client_context = FileOpener::TryGetClientContext(opener);
	if (!db) {
		return nullptr;
	}

	bool use_shared_cache = db->config.options.http_metadata_cache_enable;
	if (use_shared_cache) {
		return httpfs.GetGlobalCache();
	} else if (client_context) {
		return client_context->registered_state->GetOrCreate<HTTPMetadataCache>("http_cache", true, true).get();
	}
	return nullptr;
}

void HTTPFileHandle::Initialize(optional_ptr<FileOpener> opener) {
	auto &hfs = file_system.Cast<HTTPFileSystem>();
	state = HTTPState::TryGetState(opener);
	if (!state) {
		state = make_shared_ptr<HTTPState>();
	}

	auto client_context = FileOpener::TryGetClientContext(opener);
	if (client_context && ClientConfig::GetConfig(*client_context).enable_http_logging) {
		http_logger = client_context->client_data->http_logger.get();
	}

	auto current_cache = TryGetMetadataCache(opener, hfs);

	bool should_write_cache = false;
	if (!http_params.force_download && current_cache && !flags.OpenForWriting()) {

		HTTPMetadataCacheEntry value;
		bool found = current_cache->Find(path, value);

		if (found) {
			last_modified = value.last_modified;
			length = value.length;

			if (flags.OpenForReading()) {
				read_buffer = duckdb::unique_ptr<data_t[]>(new data_t[READ_BUFFER_LEN]);
			}
			return;
		}

		should_write_cache = true;
	}

	// If we're writing to a file, we might as well remove it from the cache
	if (current_cache && flags.OpenForWriting()) {
		current_cache->Erase(path);
	}

	auto res = hfs.HeadRequest(*this, path, {});
	string range_length;

	if (res->code != 200) {
		if (flags.OpenForWriting() && res->code == 404) {
			if (!flags.CreateFileIfNotExists() && !flags.OverwriteExistingFile()) {
				throw IOException("Unable to open URL \"" + path +
				                  "\" for writing: file does not exist and CREATE flag is not set");
			}
			length = 0;
			return;
		} else {
			// HEAD request fail, use Range request for another try (read only one byte)
			if (flags.OpenForReading() && res->code != 404) {
				auto range_res = hfs.GetRangeRequest(*this, path, {}, 0, nullptr, 2);
				if (range_res->code != 206) {
					throw IOException("Unable to connect to URL \"%s\": %d (%s)", path, res->code, res->error);
				}
				auto range_find = range_res->headers["Content-Range"].find("/");

				if (range_find == std::string::npos || range_res->headers["Content-Range"].size() < range_find + 1) {
					throw IOException("Unknown Content-Range Header \"The value of Content-Range Header\":  (%s)",
					                  range_res->headers["Content-Range"]);
				}

				range_length = range_res->headers["Content-Range"].substr(range_find + 1);
				if (range_length == "*") {
					throw IOException("Unknown total length of the document \"%s\": %d (%s)", path, res->code,
					                  res->error);
				}
				res = std::move(range_res);
			} else {
				throw HTTPException(*res, "Unable to connect to URL \"%s\": %s (%s)", res->http_url,
				                    to_string(res->code), res->error);
			}
		}
	}

	// Initialize the read buffer now that we know the file exists
	if (flags.OpenForReading()) {
		read_buffer = duckdb::unique_ptr<data_t[]>(new data_t[READ_BUFFER_LEN]);
	}

	if (res->headers.find("Content-Length") == res->headers.end() || res->headers["Content-Length"].empty()) {
		// There was no content-length header, we can not do range requests here, so we set the length to 0
		length = 0;
	} else {
		try {
			if (res->headers.find("Content-Range") == res->headers.end() || res->headers["Content-Range"].empty()) {
				length = std::stoll(res->headers["Content-Length"]);
			} else {
				length = std::stoll(range_length);
			}
		} catch (std::invalid_argument &e) {
			throw IOException("Invalid Content-Length header received: %s", res->headers["Content-Length"]);
		} catch (std::out_of_range &e) {
			throw IOException("Invalid Content-Length header received: %s", res->headers["Content-Length"]);
		}
	}
	if (state && (length == 0 || http_params.force_download)) {
		auto &cache_entry = state->GetCachedFile(path);
		cached_file_handle = cache_entry->GetHandle();
		if (!cached_file_handle->Initialized()) {
			// Try to fully download the file first
			auto full_download_result = hfs.GetRequest(*this, path, {});
			if (full_download_result->code != 200) {
				throw HTTPException(*res, "Full download failed to to URL \"%s\": %s (%s)",
				                    full_download_result->http_url, to_string(full_download_result->code),
				                    full_download_result->error);
			}

			// Mark the file as initialized, set its final length, and unlock it to allowing parallel reads
			cached_file_handle->SetInitialized(length);

			// We shouldn't write these to cache
			should_write_cache = false;
		} else {
			length = cached_file_handle->GetSize();
		}
	}

	if (!res->headers["Last-Modified"].empty()) {
		auto result = StrpTimeFormat::Parse("%a, %d %h %Y %T %Z", res->headers["Last-Modified"]);

		struct tm tm {};
		tm.tm_year = result.data[0] - 1900;
		tm.tm_mon = result.data[1] - 1;
		tm.tm_mday = result.data[2];
		tm.tm_hour = result.data[3];
		tm.tm_min = result.data[4];
		tm.tm_sec = result.data[5];
		tm.tm_isdst = 0;
		last_modified = mktime(&tm);
	}

	if (should_write_cache) {
		current_cache->Insert(path, {length, last_modified});
	}
}

unique_ptr<duckdb_httplib_openssl::Client> HTTPFileHandle::GetClient(optional_ptr<ClientContext> context) {
	// Try to fetch a cached client
	auto cached_client = client_cache.GetClient();
	if (cached_client) {
		return cached_client;
	}

	// Create a new client
	return CreateClient(context);
}

unique_ptr<duckdb_httplib_openssl::Client> HTTPFileHandle::CreateClient(optional_ptr<ClientContext> context) {
	// Create a new client
	string path_out, proto_host_port;
	HTTPFileSystem::ParseUrl(path, path_out, proto_host_port);
	auto http_client = HTTPFileSystem::GetClient(this->http_params, proto_host_port.c_str(), this);
	if (context && ClientConfig::GetConfig(*context).enable_http_logging) {
		http_logger = context->client_data->http_logger.get();
		http_client->set_logger(
		    http_logger->GetLogger<duckdb_httplib_openssl::Request, duckdb_httplib_openssl::Response>());
	}
	return http_client;
}

void HTTPFileHandle::StoreClient(unique_ptr<duckdb_httplib_openssl::Client> client) {
	client_cache.StoreClient(std::move(client));
}

ResponseWrapper::ResponseWrapper(duckdb_httplib_openssl::Response &res, string &original_url) {
	code = res.status;
	error = res.reason;
	for (auto &h : res.headers) {
		headers[h.first] = h.second;
	}
	http_url = original_url;
	body = res.body;
}

HTTPFileHandle::~HTTPFileHandle() = default;
} // namespace duckdb
