#include "httpfs.hpp"

#include "duckdb/common/atomic.hpp"
#include "duckdb/common/file_opener.hpp"
#include "duckdb/common/http_state.hpp"
#include "duckdb/common/thread.hpp"
#include "duckdb/common/types/hash.hpp"
#include "duckdb/function/scalar/strftime_format.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"

#include <chrono>
#include <string>
#include <thread>

#define CPPHTTPLIB_OPENSSL_SUPPORT
#include "httplib.hpp"

#include <map>

namespace duckdb {

static duckdb::unique_ptr<duckdb_httplib_openssl::Headers> initialize_http_headers(HeaderMap &header_map) {
	auto headers = make_uniq<duckdb_httplib_openssl::Headers>();
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
	bool force_download = DEFAULT_FORCE_DOWNLOAD;
	Value value;
	if (FileOpener::TryGetCurrentSetting(opener, "http_timeout", value)) {
		timeout = value.GetValue<uint64_t>();
	}
	if (FileOpener::TryGetCurrentSetting(opener, "force_download", value)) {
		force_download = value.GetValue<bool>();
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

	return {timeout, retries, retry_wait_ms, retry_backoff, force_download};
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
static duckdb::unique_ptr<ResponseWrapper>
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
	auto &hfs = (HTTPFileHandle &)handle;
	string path, proto_host_port;
	ParseUrl(url, path, proto_host_port);
	auto headers = initialize_http_headers(header_map);
	idx_t out_offset = 0;

	std::function<duckdb_httplib_openssl::Result(void)> request([&]() {
		auto client = GetClient(hfs.http_params, proto_host_port.c_str());

		if (hfs.state) {
			hfs.state->post_count++;
			hfs.state->total_bytes_sent += buffer_in_len;
		}

		// We use a custom Request method here, because there is no Post call with a contentreceiver in httplib
		duckdb_httplib_openssl::Request req;
		req.method = "POST";
		req.path = path;
		req.headers = *headers;
		req.headers.emplace("Content-Type", "application/octet-stream");
		req.content_receiver = [&](const char *data, size_t data_length, uint64_t /*offset*/,
		                           uint64_t /*total_length*/) {
			if (hfs.state) {
				hfs.state->total_bytes_received += data_length;
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

	return RunRequestWithRetry(request, url, "POST", hfs.http_params);
}

unique_ptr<duckdb_httplib_openssl::Client> HTTPFileSystem::GetClient(const HTTPParams &http_params,
                                                                     const char *proto_host_port) {
	auto client = make_uniq<duckdb_httplib_openssl::Client>(proto_host_port);
	client->set_follow_location(true);
	client->set_keep_alive(true);
	client->enable_server_certificate_verification(false);
	client->set_write_timeout(http_params.timeout);
	client->set_read_timeout(http_params.timeout);
	client->set_connection_timeout(http_params.timeout);
	client->set_decompress(false);
	return client;
}

unique_ptr<ResponseWrapper> HTTPFileSystem::PutRequest(FileHandle &handle, string url, HeaderMap header_map,
                                                       char *buffer_in, idx_t buffer_in_len, string params) {
	auto &hfs = (HTTPFileHandle &)handle;
	string path, proto_host_port;
	ParseUrl(url, path, proto_host_port);
	auto headers = initialize_http_headers(header_map);

	std::function<duckdb_httplib_openssl::Result(void)> request([&]() {
		auto client = GetClient(hfs.http_params, proto_host_port.c_str());
		if (hfs.state) {
			hfs.state->put_count++;
			hfs.state->total_bytes_sent += buffer_in_len;
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
		if (hfs.state) {
			hfs.state->head_count++;
		}
		return hfs.http_client->Head(path.c_str(), *headers);
	});

	std::function<void(void)> on_retry(
	    [&]() { hfs.http_client = GetClient(hfs.http_params, proto_host_port.c_str()); });

	return RunRequestWithRetry(request, url, "HEAD", hfs.http_params, on_retry);
}

unique_ptr<ResponseWrapper> HTTPFileSystem::GetRequest(FileHandle &handle, string url, HeaderMap header_map) {
	auto &hfh = (HTTPFileHandle &)handle;
	string path, proto_host_port;
	ParseUrl(url, path, proto_host_port);
	auto headers = initialize_http_headers(header_map);

	D_ASSERT(hfh.cached_file_handle);

	std::function<duckdb_httplib_openssl::Result(void)> request([&]() {
		D_ASSERT(hfh.state);
		hfh.state->get_count++;
		return hfh.http_client->Get(
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
	    [&]() { hfh.http_client = GetClient(hfh.http_params, proto_host_port.c_str()); });

	return RunRequestWithRetry(request, url, "GET", hfh.http_params, on_retry);
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
		if (hfs.state) {
			hfs.state->get_count++;
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
			    if (hfs.state) {
				    hfs.state->total_bytes_received += data_length;
			    }
			    if (buffer_out != nullptr) {
				    memcpy(buffer_out + out_offset, data, data_length);
				    out_offset += data_length;
			    }
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

unique_ptr<HTTPFileHandle> HTTPFileSystem::CreateHandle(const string &path, uint8_t flags, FileLockType lock,
                                                        FileCompressionType compression, FileOpener *opener) {
	D_ASSERT(compression == FileCompressionType::UNCOMPRESSED);
	return duckdb::make_uniq<HTTPFileHandle>(*this, path, flags, HTTPParams::ReadFrom(opener));
}

unique_ptr<FileHandle> HTTPFileSystem::OpenFile(const string &path, uint8_t flags, FileLockType lock,
                                                FileCompressionType compression, FileOpener *opener) {
	D_ASSERT(compression == FileCompressionType::UNCOMPRESSED);

	auto handle = CreateHandle(path, flags, lock, compression, opener);
	handle->Initialize(opener);
	return std::move(handle);
}

// Buffered read from http file.
// Note that buffering is disabled when FileFlags::FILE_FLAGS_DIRECT_IO is set
void HTTPFileSystem::Read(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) {
	auto &hfh = (HTTPFileHandle &)handle;

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

idx_t HTTPFileSystem::SeekPosition(FileHandle &handle) {
	auto &sfh = (HTTPFileHandle &)handle;
	return sfh.file_offset;
}

// Get either the local, global, or no cache depending on settings
static optional_ptr<HTTPMetadataCache> TryGetMetadataCache(FileOpener *opener, HTTPFileSystem &httpfs) {
	auto client_context = FileOpener::TryGetClientContext(opener);
	if (!client_context) {
		return nullptr;
	}

	bool use_shared_cache = client_context->db->config.options.http_metadata_cache_enable;

	if (use_shared_cache) {
		if (!httpfs.global_metadata_cache) {
			httpfs.global_metadata_cache = make_uniq<HTTPMetadataCache>(false, true);
		}
		return httpfs.global_metadata_cache.get();
	} else {
		auto lookup = client_context->registered_state.find("http_cache");
		if (lookup == client_context->registered_state.end()) {
			auto cache = make_shared<HTTPMetadataCache>(true, true);
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
	state = HTTPState::TryGetState(opener);
	if (!state) {
		if (!opener) {
			// If opener is not available (e.g., FileExists()), we create the HTTPState here.
			state = make_shared<HTTPState>();
		} else {
			throw InternalException("State was not defined in this HTTP File Handle");
		}
	}

	auto current_cache = TryGetMetadataCache(opener, hfs);

	bool should_write_cache = false;
	if (!http_params.force_download && current_cache && !(flags & FileFlags::FILE_FLAGS_WRITE)) {

		HTTPMetadataCacheEntry value;
		bool found = current_cache->Find(path, value);

		if (found) {
			last_modified = value.last_modified;
			length = value.length;

			if (flags & FileFlags::FILE_FLAGS_READ) {
				read_buffer = duckdb::unique_ptr<data_t[]>(new data_t[READ_BUFFER_LEN]);
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
	string range_length;

	if (res->code != 200) {
		if ((flags & FileFlags::FILE_FLAGS_WRITE) && res->code == 404) {
			if (!(flags & FileFlags::FILE_FLAGS_FILE_CREATE) && !(flags & FileFlags::FILE_FLAGS_FILE_CREATE_NEW)) {
				throw IOException("Unable to open URL \"" + path +
				                  "\" for writing: file does not exist and CREATE flag is not set");
			}
			length = 0;
			return;
		} else {
			// HEAD request fail, use Range request for another try (read only one byte)
			if ((flags & FileFlags::FILE_FLAGS_READ) && res->code != 404) {
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
	if (flags & FileFlags::FILE_FLAGS_READ) {
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
			// Mark the file as initialized, unlocking it and allowing parallel reads
			cached_file_handle->SetInitialized();
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

void HTTPFileHandle::InitializeClient() {
	string path_out, proto_host_port;
	HTTPFileSystem::ParseUrl(path, path_out, proto_host_port);
	http_client = HTTPFileSystem::GetClient(this->http_params, proto_host_port.c_str());
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
