#include "httpfs.hpp"

#include "duckdb/common/atomic.hpp"
#include "duckdb/common/file_opener.hpp"
#include "duckdb/common/http_stats.hpp"
#include "duckdb/common/thread.hpp"
#include "duckdb/function/scalar/strftime.hpp"
#include "duckdb/common/types/hash.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"

#include <chrono>
#include <thread>
#include <string>

#define CPPHTTPLIB_OPENSSL_SUPPORT
#include "httplib.hpp"

#include <map>

namespace duckdb {

static unique_ptr<duckdb_httplib_openssl::Headers> initialize_http_headers(HeaderMap &header_map) {
	auto headers = make_unique<duckdb_httplib_openssl::Headers>();
	for (auto &entry : header_map) {
		headers->insert(entry);
	}
	return headers;
}

HTTPParams HTTPParams::ReadFrom(FileOpener *opener) {
	uint64_t timeout = DEFAULT_TIMEOUT;
	uint64_t retries = DEFAULT_RETRIES;
	uint64_t retry_wait_ms = DEFAULT_RETRY_WAIT_MS;
	float retry_backoff = DEFAULT_RETRY_BACKOFF;
	Value value;
	if (FileOpener::TryGetCurrentSetting(opener, "http_timeout", value)) {
		timeout = value.GetValue<uint64_t>();
	}
	if (FileOpener::TryGetCurrentSetting(opener, "http_retries", value)) {
		retries = value.GetValue<uint64_t>();
	}
	if (FileOpener::TryGetCurrentSetting(opener, "http_retry_wait_ms", value)) {
		retry_wait_ms = value.GetValue<uint64_t>();
	}
	if (FileOpener::TryGetCurrentSetting(opener, "http_retry_backoff", value)) {
		retry_backoff = value.GetValue<float>();
	}
	return {timeout, retries, retry_wait_ms, retry_backoff};
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
static unique_ptr<ResponseWrapper>
RunRequestWithRetry(const std::function<duckdb_httplib_openssl::Result(void)> &request, string &url, string method,
                    const HTTPParams &params, const std::function<void(void)> &retry_cb = {}) {
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
			case 503: // Server has error
			case 504: // Server has error
				break;
			default:
				return make_unique<ResponseWrapper>(response);
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
				throw IOException("Request returned HTTP " + to_string(status) + " for HTTP " + method + " to '" + url +
				                  "'");
			} else {
				throw IOException(to_string(err) + " error for " + "HTTP " + method + " to '" + url + "'");
			}
		}
	}
}

unique_ptr<ResponseWrapper> HTTPFileSystem::PostRequest(FileHandle &handle, string url, HeaderMap header_map,
                                                        unique_ptr<char[]> &buffer_out, idx_t &buffer_out_len,
                                                        char *buffer_in, idx_t buffer_in_len) {
	auto &hfs = (HTTPFileHandle &)handle;
	string path, proto_host_port;
	ParseUrl(url, path, proto_host_port);
	auto headers = initialize_http_headers(header_map);
	idx_t out_offset = 0;

	std::function<duckdb_httplib_openssl::Result(void)> request([&]() {
		auto client = GetClient(hfs.http_params, proto_host_port.c_str());

		if (hfs.stats) {
			hfs.stats->post_count++;
			hfs.stats->total_bytes_sent += buffer_in_len;
		}

		// We use a custom Request method here, because there is no Post call with a contentreceiver in httplib
		duckdb_httplib_openssl::Request req;
		req.method = "POST";
		req.path = path;
		req.headers = *headers;
		req.headers.emplace("Content-Type", "application/octet-stream");
		req.content_receiver = [&](const char *data, size_t data_length, uint64_t /*offset*/,
		                           uint64_t /*total_length*/) {
			if (hfs.stats) {
				hfs.stats->total_bytes_received += data_length;
			}
			if (out_offset + data_length > buffer_out_len) {
				// Buffer too small, increase its size by at least 2x to fit the new value
				auto new_size = MaxValue<idx_t>(out_offset + data_length, buffer_out_len * 2);
				auto tmp = unique_ptr<char[]> {new char[new_size]};
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

	return RunRequestWithRetry(request, url, "POST", hfs.http_params);
}

unique_ptr<duckdb_httplib_openssl::Client> HTTPFileSystem::GetClient(const HTTPParams &http_params,
                                                                     const char *proto_host_port) {
	auto client = make_unique<duckdb_httplib_openssl::Client>(proto_host_port);
	client->set_follow_location(true);
	client->set_keep_alive(true);
	client->enable_server_certificate_verification(false);
	client->set_write_timeout(http_params.timeout);
	client->set_read_timeout(http_params.timeout);
	client->set_connection_timeout(http_params.timeout);
	return client;
}

unique_ptr<ResponseWrapper> HTTPFileSystem::PutRequest(FileHandle &handle, string url, HeaderMap header_map,
                                                       char *buffer_in, idx_t buffer_in_len) {
	auto &hfs = (HTTPFileHandle &)handle;
	string path, proto_host_port;
	ParseUrl(url, path, proto_host_port);
	auto headers = initialize_http_headers(header_map);

	std::function<duckdb_httplib_openssl::Result(void)> request([&]() {
		auto client = GetClient(hfs.http_params, proto_host_port.c_str());
		if (hfs.stats) {
			hfs.stats->put_count++;
			hfs.stats->total_bytes_sent += buffer_in_len;
		}
		return client->Put(path.c_str(), *headers, buffer_in, buffer_in_len, "application/octet-stream");
	});

	return RunRequestWithRetry(request, url, "PUT", hfs.http_params);
}

unique_ptr<ResponseWrapper> HTTPFileSystem::HeadRequest(FileHandle &handle, string url, HeaderMap header_map) {
	auto &hfs = (HTTPFileHandle &)handle;
	string path, proto_host_port;
	ParseUrl(url, path, proto_host_port);
	auto headers = initialize_http_headers(header_map);

	std::function<duckdb_httplib_openssl::Result(void)> request([&]() {
		if (hfs.stats) {
			hfs.stats->head_count++;
		}
		return hfs.http_client->Head(path.c_str(), *headers);
	});

	std::function<void(void)> on_retry(
	    [&]() { hfs.http_client = GetClient(hfs.http_params, proto_host_port.c_str()); });

	return RunRequestWithRetry(request, url, "HEAD", hfs.http_params, on_retry);
}

unique_ptr<ResponseWrapper> HTTPFileSystem::GetRangeRequest(FileHandle &handle, string url, HeaderMap header_map,
                                                            idx_t file_offset, char *buffer_out, idx_t buffer_out_len) {
	auto &hfs = (HTTPFileHandle &)handle;
	string path, proto_host_port;
	ParseUrl(url, path, proto_host_port);
	auto headers = initialize_http_headers(header_map);

	// send the Range header to read only subset of file
	string range_expr = "bytes=" + to_string(file_offset) + "-" + to_string(file_offset + buffer_out_len - 1);
	headers->insert(pair<string, string>("Range", range_expr));

	idx_t out_offset = 0;

	std::function<duckdb_httplib_openssl::Result(void)> request([&]() {
		if (hfs.stats) {
			hfs.stats->get_count++;
		}
		return hfs.http_client->Get(
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
			    if (response.status < 300) { // done redirecting
				    out_offset = 0;
				    auto content_length = stoll(response.get_header_value("Content-Length", 0));
				    if ((idx_t)content_length != buffer_out_len) {
					    throw IOException("HTTP GET error: Content-Length from server mismatches requested "
					                      "range, server may not support range requests.");
				    }
			    }
			    return true;
		    },
		    [&](const char *data, size_t data_length) {
			    if (hfs.stats) {
				    hfs.stats->total_bytes_received += data_length;
			    }
			    memcpy(buffer_out + out_offset, data, data_length);
			    out_offset += data_length;
			    return true;
		    });
	});

	std::function<void(void)> on_retry(
	    [&]() { hfs.http_client = GetClient(hfs.http_params, proto_host_port.c_str()); });

	return RunRequestWithRetry(request, url, "GET Range", hfs.http_params, on_retry);
}

HTTPFileHandle::HTTPFileHandle(FileSystem &fs, string path, uint8_t flags, const HTTPParams &http_params)
    : FileHandle(fs, path), http_params(http_params), flags(flags), length(0), buffer_available(0), buffer_idx(0),
      file_offset(0), buffer_start(0), buffer_end(0) {
}

unique_ptr<HTTPFileHandle> HTTPFileSystem::CreateHandle(const string &path, const string &query_param, uint8_t flags,
                                                        FileLockType lock, FileCompressionType compression,
                                                        FileOpener *opener) {
	D_ASSERT(compression == FileCompressionType::UNCOMPRESSED);
	return duckdb::make_unique<HTTPFileHandle>(*this, query_param.empty() ? path : path + "?" + query_param, flags,
	                                           HTTPParams::ReadFrom(opener));
}

unique_ptr<FileHandle> HTTPFileSystem::OpenFile(const string &path, uint8_t flags, FileLockType lock,
                                                FileCompressionType compression, FileOpener *opener) {
	D_ASSERT(compression == FileCompressionType::UNCOMPRESSED);

	// splitting query params from base path
	string stripped_path, query_param;

	auto question_pos = path.find_last_of('?');
	if (question_pos == string::npos) {
		stripped_path = path;
		query_param = "";
	} else {
		stripped_path = path.substr(0, question_pos);
		query_param = path.substr(question_pos + 1);
	}

	auto handle = CreateHandle(stripped_path, query_param, flags, lock, compression, opener);
	handle->Initialize(opener);
	return std::move(handle);
}

// Buffered read from http file.
// Note that buffering is disabled when FileFlags::FILE_FLAGS_DIRECT_IO is set
void HTTPFileSystem::Read(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) {
	auto &hfh = (HTTPFileHandle &)handle;
	idx_t to_read = nr_bytes;
	idx_t buffer_offset = 0;
	if (location + nr_bytes > hfh.length) {
		throw IOException("out of file");
	}

	// Don't buffer when DirectIO is set.
	if (hfh.flags & FileFlags::FILE_FLAGS_DIRECT_IO && to_read > 0) {
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
	auto &sfh = (HTTPFileHandle &)handle;
	return sfh.length;
}

time_t HTTPFileSystem::GetLastModifiedTime(FileHandle &handle) {
	auto &sfh = (HTTPFileHandle &)handle;
	return sfh.last_modified;
}

bool HTTPFileSystem::FileExists(const string &filename) {
	try {
		auto handle = OpenFile(filename.c_str(), FileFlags::FILE_FLAGS_READ);
		auto &sfh = (HTTPFileHandle &)*handle;
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
	auto &sfh = (HTTPFileHandle &)handle;
	sfh.file_offset = location;
}

// Get either the local, global, or no cache depending on settings
static HTTPMetadataCache *TryGetMetadataCache(FileOpener *opener, HTTPFileSystem &httpfs) {
	auto client_context = FileOpener::TryGetClientContext(opener);
	if (!client_context) {
		return nullptr;
	}

	bool use_shared_cache = client_context->db->config.options.http_metadata_cache_enable;

	if (use_shared_cache) {
		if (!httpfs.global_metadata_cache) {
			httpfs.global_metadata_cache = make_unique<HTTPMetadataCache>(false, true);
		}
		return httpfs.global_metadata_cache.get();
	} else {
		auto lookup = client_context->registered_state.find("http_cache");
		if (lookup == client_context->registered_state.end()) {
			auto cache = make_shared<HTTPMetadataCache>(true, false);
			client_context->registered_state["http_cache"] = cache;
			return cache.get();
		} else {
			return (HTTPMetadataCache *)lookup->second.get();
		}
	}
}

void HTTPFileHandle::Initialize(FileOpener *opener) {
	InitializeClient();
	auto &hfs = (HTTPFileSystem &)file_system;

	HTTPMetadataCache *current_cache = TryGetMetadataCache(opener, hfs);
	stats = HTTPStats::TryGetStats(opener);

	bool should_write_cache = false;
	if (current_cache && !(flags & FileFlags::FILE_FLAGS_WRITE)) {

		HTTPMetadataCacheEntry value;
		bool found = current_cache->Find(path, value);

		if (found) {
			last_modified = value.last_modified;
			length = value.length;

			if (flags & FileFlags::FILE_FLAGS_READ) {
				read_buffer = unique_ptr<data_t[]>(new data_t[READ_BUFFER_LEN]);
			}
			return;
		}

		should_write_cache = true;
	}

	// If we're writing to a file, we might as well remove it from the cache
	if (current_cache && flags & FileFlags::FILE_FLAGS_WRITE) {
		current_cache->Erase(path);
	}

	auto res = hfs.HeadRequest(*this, path, {});

	if (res->code != 200) {
		if ((flags & FileFlags::FILE_FLAGS_WRITE) && res->code == 404) {
			if (!(flags & FileFlags::FILE_FLAGS_FILE_CREATE) && !(flags & FileFlags::FILE_FLAGS_FILE_CREATE_NEW)) {
				throw IOException("Unable to open URL \"" + path +
				                  "\" for writing: file does not exist and CREATE flag is not set");
			}
			length = 0;
			return;
		} else {
			throw IOException("Unable to connect to URL \"" + path + "\": " + to_string(res->code) + " (" + res->error +
			                  ")");
		}
	}

	// Initialize the read buffer now that we know the file exists
	if (flags & FileFlags::FILE_FLAGS_READ) {
		read_buffer = unique_ptr<data_t[]>(new data_t[READ_BUFFER_LEN]);
	}

	if (res->headers.find("Content-Length") == res->headers.end() || res->headers["Content-Length"].empty()) {
		// There was no content-length header, we can not do range requests here
		throw IOException("Server did not send Content-Length header, can not read from this file.");
	}

	try {
		length = std::stoll(res->headers["Content-Length"]);
	} catch (std::invalid_argument &e) {
		throw IOException("Invalid Content-Length header received: %s", res->headers["Content-Length"]);
	} catch (std::out_of_range &e) {
		throw IOException("Invalid Content-Length header received: %s", res->headers["Content-Length"]);
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

void HTTPFileHandle::InitializeClient() {
	string path_out, proto_host_port;
	HTTPFileSystem::ParseUrl(path, path_out, proto_host_port);
	http_client = HTTPFileSystem::GetClient(this->http_params, proto_host_port.c_str());
}

ResponseWrapper::ResponseWrapper(duckdb_httplib_openssl::Response &res) {
	code = res.status;
	error = res.reason;
	for (auto &h : res.headers) {
		headers[h.first] = h.second;
	}
}

HTTPFileHandle::~HTTPFileHandle() = default;
} // namespace duckdb
