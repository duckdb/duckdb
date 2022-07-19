#include "httpfs.hpp"

#include "duckdb/common/atomic.hpp"
#include "duckdb/common/file_opener.hpp"
#include "duckdb/common/thread.hpp"
#include "duckdb/function/scalar/strftime.hpp"

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
	uint64_t timeout;
	Value value;

	if (opener->TryGetCurrentSetting("http_timeout", value)) {
		timeout = value.GetValue<uint64_t>();
	} else {
		timeout = DEFAULT_TIMEOUT;
	}

	return {timeout};
}

void HTTPFileSystem::ParseUrl(string &url, string &path_out, string &proto_host_port_out) {
	if (url.rfind("http://", 0) != 0 && url.rfind("https://", 0) != 0) {
		throw std::runtime_error("URL needs to start with http:// or https://");
	}
	auto slash_pos = url.find('/', 8);
	if (slash_pos == std::string::npos) {
		throw std::runtime_error("URL needs to contain a '/' after the host");
	}
	proto_host_port_out = url.substr(0, slash_pos);

	path_out = url.substr(slash_pos);

	if (path_out.empty()) {
		throw std::runtime_error("URL needs to contain a path");
	}
}

unique_ptr<ResponseWrapper> HTTPFileSystem::PostRequest(FileHandle &handle, string url, HeaderMap header_map,
                                                        unique_ptr<char[]> &buffer_out, idx_t &buffer_out_len,
                                                        char *buffer_in, idx_t buffer_in_len) {
	auto &hfs = (HTTPFileHandle &)handle;
	string path, proto_host_port;
	ParseUrl(url, path, proto_host_port);
	auto headers = initialize_http_headers(header_map);
	auto client = GetClient(hfs.http_params, proto_host_port.c_str()); // POST requests use fresh connection

	// We use a custom Request method here, because there is no Post call with a contentreceiver in httplib
	idx_t out_offset = 0;
	duckdb_httplib_openssl::Request req;
	req.method = "POST";
	req.path = path;
	req.headers = *headers;
	req.headers.emplace("Content-Type", "application/octet-stream");
	req.content_receiver = [&](const char *data, size_t data_length, uint64_t /*offset*/, uint64_t /*total_length*/) {
		if (out_offset + data_length > buffer_out_len) {
			// Buffer too small, increase its size by at least 2x to fit the new value
			auto new_size = MaxValue<idx_t>(out_offset + data_length, buffer_out_len * 2);
			auto tmp = unique_ptr<char[]> {new char[new_size]};
			memcpy(tmp.get(), buffer_out.get(), buffer_out_len);
			buffer_out = move(tmp);
			buffer_out_len = new_size;
		}
		memcpy(buffer_out.get() + out_offset, data, data_length);
		out_offset += data_length;
		return true;
	};
	req.body.assign(buffer_in, buffer_in_len);
	auto res = client->send(req);
	if (res.error() != duckdb_httplib_openssl::Error::Success) {
		throw std::runtime_error("HTTP POST error on '" + url + "' (Error code " + std::to_string((int)res.error()) +
		                         ")");
	}
	return make_unique<ResponseWrapper>(res.value());
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
	auto client =
	    GetClient(hfs.http_params, proto_host_port.c_str()); // Put requests use fresh connection for parallel uploads
	auto headers = initialize_http_headers(header_map);

	auto res = client->Put(path.c_str(), *headers, buffer_in, buffer_in_len, "application/octet-stream");
	if (res.error() != duckdb_httplib_openssl::Error::Success) {
		throw std::runtime_error("HTTP PUT error on '" + url + "' (Error code " + std::to_string((int)res.error()) +
		                         ")");
	}
	return make_unique<ResponseWrapper>(res.value());
}

unique_ptr<ResponseWrapper> HTTPFileSystem::HeadRequest(FileHandle &handle, string url, HeaderMap header_map) {
	auto &hfs = (HTTPFileHandle &)handle;
	string path, proto_host_port;
	ParseUrl(url, path, proto_host_port);
	auto headers = initialize_http_headers(header_map);

	auto res = hfs.http_client->Head(path.c_str(), *headers);
	if (res.error() != duckdb_httplib_openssl::Error::Success) {
		throw std::runtime_error("HTTP HEAD error on '" + url + "' (Error code " + std::to_string((int)res.error()) +
		                         ")");
	}
	return make_unique<ResponseWrapper>(res.value());
}

unique_ptr<ResponseWrapper> HTTPFileSystem::GetRangeRequest(FileHandle &handle, string url, HeaderMap header_map,
                                                            idx_t file_offset, char *buffer_out, idx_t buffer_out_len) {
	auto &hfs = (HTTPFileHandle &)handle;
	string path, proto_host_port;
	ParseUrl(url, path, proto_host_port);
	auto headers = initialize_http_headers(header_map);

	// send the Range header to read only subset of file
	std::string range_expr =
	    "bytes=" + std::to_string(file_offset) + "-" + std::to_string(file_offset + buffer_out_len - 1);
	headers->insert(std::pair<std::string, std::string>("Range", range_expr));

	idx_t out_offset = 0;
	idx_t tries = 0;
	idx_t max_tries = 2;

	while (true) {
		auto res = hfs.http_client->Get(
		    path.c_str(), *headers,
		    [&](const duckdb_httplib_openssl::Response &response) {
			    if (response.status >= 400) {
				    throw std::runtime_error("HTTP GET error on '" + url + "' (HTTP " +
				                             std::to_string(response.status) + ")");
			    }
			    if (response.status < 300) { // done redirecting
				    out_offset = 0;
				    auto content_length = std::stoll(response.get_header_value("Content-Length", 0));
				    if ((idx_t)content_length != buffer_out_len) {
					    throw std::runtime_error("offset error");
				    }
			    }
			    return true;
		    },
		    [&](const char *data, size_t data_length) {
			    memcpy(buffer_out + out_offset, data, data_length);
			    out_offset += data_length;
			    return true;
		    });

		if (res.error() == duckdb_httplib_openssl::Error::Success) {
			return make_unique<ResponseWrapper>(res.value());
		} else {
			// Retry mechanism for the keep-alive connection: sometimes the connection times out and the request will
			// fail with a Read error. Refreshing the connection will solve this.

			if (res.error() == duckdb_httplib_openssl::Error::Read) {
				tries += 1;
				hfs.http_client = GetClient(hfs.http_params, proto_host_port.c_str());
			}

			if (tries >= max_tries) {
				throw std::runtime_error("HTTP GET error on '" + url + "' (Error code " +
				                         std::to_string((int)res.error()) + ")");
			}
		}
	}
}

HTTPFileHandle::HTTPFileHandle(FileSystem &fs, std::string path, uint8_t flags, const HTTPParams &http_params)
    : FileHandle(fs, path), http_params(http_params), flags(flags), length(0), buffer_available(0), buffer_idx(0),
      file_offset(0), buffer_start(0), buffer_end(0) {
}

std::unique_ptr<HTTPFileHandle> HTTPFileSystem::CreateHandle(const string &path, uint8_t flags, FileLockType lock,
                                                             FileCompressionType compression, FileOpener *opener) {
	D_ASSERT(compression == FileCompressionType::UNCOMPRESSED);
	return duckdb::make_unique<HTTPFileHandle>(*this, path, flags,
	                                           opener ? HTTPParams::ReadFrom(opener) : HTTPParams());
}

std::unique_ptr<FileHandle> HTTPFileSystem::OpenFile(const string &path, uint8_t flags, FileLockType lock,
                                                     FileCompressionType compression, FileOpener *opener) {
	D_ASSERT(compression == FileCompressionType::UNCOMPRESSED);
	auto handle = CreateHandle(path, flags, lock, compression, opener);
	handle->Initialize();
	return move(handle);
}

// Buffered read from http file.
// Note that buffering is disabled when FileFlags::FILE_FLAGS_DIRECT_IO is set
void HTTPFileSystem::Read(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) {
	auto &hfh = (HTTPFileHandle &)handle;
	idx_t to_read = nr_bytes;
	idx_t buffer_offset = 0;
	if (location + nr_bytes > hfh.length) {
		throw std::runtime_error("out of file");
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
		auto &sfh = (HTTPFileHandle &)handle;
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

unique_ptr<ResponseWrapper> HTTPFileHandle::Initialize() {
	InitializeClient();

	auto &hfs = (HTTPFileSystem &)file_system;
	auto res = hfs.HeadRequest(*this, path, {});

	if (res->code != 200) {
		if ((flags & FileFlags::FILE_FLAGS_WRITE) && res->code == 404) {
			if (!(flags & FileFlags::FILE_FLAGS_FILE_CREATE) && !(flags & FileFlags::FILE_FLAGS_FILE_CREATE_NEW)) {
				throw std::runtime_error("Unable to open URL \"" + path +
				                         "\" for writing: file does not exists and CREATE flag is not set");
			}
			length = 0;
			return res;
		} else {
			throw std::runtime_error("Unable to connect to URL \"" + path + "\": " + std::to_string(res->code) + " (" +
			                         res->error + ")");
		}
	}

	// Initialize the read buffer now that we know the file exists
	if (flags & FileFlags::FILE_FLAGS_READ) {
		read_buffer = std::unique_ptr<data_t[]>(new data_t[READ_BUFFER_LEN]);
	}

	length = std::atoll(res->headers["Content-Length"].c_str());

	auto last_modified = res->headers["Last-Modified"];
	if (last_modified.empty()) {
		return res;
	}
	auto result = StrpTimeFormat::Parse("%a, %d %h %Y %T %Z", last_modified);

	struct tm tm {};
	tm.tm_year = result.data[0] - 1900;
	tm.tm_mon = result.data[1] - 1;
	tm.tm_mday = result.data[2];
	tm.tm_hour = result.data[3];
	tm.tm_min = result.data[4];
	tm.tm_sec = result.data[5];
	tm.tm_isdst = 0;
	last_modified = std::mktime(&tm);
	return res;
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
