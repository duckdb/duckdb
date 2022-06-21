#include "s3fs.hpp"

#include "crypto.hpp"
#include "duckdb.hpp"
#ifndef DUCKDB_AMALGAMATION
#include "duckdb/common/thread.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/function/scalar/strftime.hpp"
#endif

#define CPPHTTPLIB_OPENSSL_SUPPORT
#include "httplib.hpp"

#include <chrono>
#include <duckdb/function/scalar/string_functions.hpp>
#include <duckdb/storage/buffer_manager.hpp>
#include <iostream>
#include <thread>

namespace duckdb {

static HeaderMap create_s3_header(std::string url, std::string query, std::string host, std::string service,
                                  std::string method, const S3AuthParams &auth_params, std::string date_now = "",
                                  std::string datetime_now = "", std::string payload_hash = "",
                                  std::string content_type = "") {

	HeaderMap res;
	res["Host"] = host;

	// If access key is not set, we don't set the headers at all to allow accessing public files through s3 urls
	if (auth_params.secret_access_key.empty() && auth_params.secret_access_key.empty()) {
		return res;
	}

	if (payload_hash == "") {
		payload_hash = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"; // Empty payload hash
	}

	// we can pass date/time but this is mostly useful in testing. normally we just get the current datetime here.
	if (datetime_now.empty()) {
		auto timestamp = Timestamp::GetCurrentTimestamp();
		date_now = StrfTimeFormat::Format(timestamp, "%Y%m%d");
		datetime_now = StrfTimeFormat::Format(timestamp, "%Y%m%dT%H%M%SZ");
	}

	res["x-amz-date"] = datetime_now;
	res["x-amz-content-sha256"] = payload_hash;
	if (auth_params.session_token.length() > 0) {
		res["x-amz-security-token"] = auth_params.session_token;
	}

	// construct string to sign
	hash_bytes canonical_request_hash;
	hash_str canonical_request_hash_str;
	std::string signed_headers = "";
	if (content_type.length() > 0) {
		signed_headers += "content-type;";
	}
	signed_headers += "host;x-amz-content-sha256;x-amz-date";
	if (auth_params.session_token.length() > 0) {
		signed_headers += ";x-amz-security-token";
	}
	auto canonical_request = method + "\n" + S3FileSystem::UrlEncode(url) + "\n" + query;
	if (content_type.length() > 0) {
		canonical_request += "\ncontent-type:" + content_type;
	}
	canonical_request += "\nhost:" + host + "\nx-amz-content-sha256:" + payload_hash + "\nx-amz-date:" + datetime_now;
	if (auth_params.session_token.length() > 0) {
		canonical_request += "\nx-amz-security-token:" + auth_params.session_token;
	}

	canonical_request += "\n\n" + signed_headers + "\n" + payload_hash;
	sha256(canonical_request.c_str(), canonical_request.length(), canonical_request_hash);
	hex256(canonical_request_hash, canonical_request_hash_str);
	auto string_to_sign = "AWS4-HMAC-SHA256\n" + datetime_now + "\n" + date_now + "/" + auth_params.region + "/" +
	                      service + "/aws4_request\n" +
	                      std::string((char *)canonical_request_hash_str, sizeof(hash_str));
	// compute signature
	hash_bytes k_date, k_region, k_service, signing_key, signature;
	hash_str signature_str;
	auto sign_key = "AWS4" + auth_params.secret_access_key;
	hmac256(date_now, sign_key.c_str(), sign_key.length(), k_date);
	hmac256(auth_params.region, k_date, k_region);
	hmac256(service, k_region, k_service);
	hmac256("aws4_request", k_service, signing_key);
	hmac256(string_to_sign, signing_key, signature);
	hex256(signature, signature_str);

	res["Authorization"] = "AWS4-HMAC-SHA256 Credential=" + auth_params.access_key_id + "/" + date_now + "/" +
	                       auth_params.region + "/" + service + "/aws4_request, SignedHeaders=" + signed_headers +
	                       ", Signature=" + std::string((char *)signature_str, sizeof(hash_str));

	return res;
}

static unique_ptr<duckdb_httplib_openssl::Headers> initialize_http_headers(HeaderMap &header_map) {
	auto headers = make_unique<duckdb_httplib_openssl::Headers>();
	for (auto &entry : header_map) {
		headers->insert(entry);
	}
	return headers;
}

std::string S3FileSystem::UrlDecode(std::string input) {
	std::string result;
	result.reserve(input.size());
	char ch;
	std::replace(input.begin(), input.end(), '+', ' ');
	for (idx_t i = 0; i < input.length(); i++) {
		if (int(input[i]) == 37) {
			int ii;
			sscanf(input.substr(i + 1, 2).c_str(), "%x", &ii);
			ch = static_cast<char>(ii);
			result += ch;
			i += 2;
		} else {
			result += input[i];
		}
	}
	return result;
}

std::string S3FileSystem::UrlEncode(const std::string &input, bool encode_slash) {
	// https://docs.aws.amazon.com/AmazonS3/latest/API/sigv4-query-string-auth.html
	static const char *hex_digit = "0123456789ABCDEF";
	std::string result;
	result.reserve(input.size());
	for (idx_t i = 0; i < input.length(); i++) {
		char ch = input[i];
		if ((ch >= 'A' && ch <= 'Z') || (ch >= 'a' && ch <= 'z') || (ch >= '0' && ch <= '9') || ch == '_' ||
		    ch == '-' || ch == '~' || ch == '.') {
			result += ch;
		} else if (ch == '/') {
			if (encode_slash) {
				result += std::string("%2F");
			} else {
				result += ch;
			}
		} else {
			result += std::string("%");
			result += hex_digit[static_cast<unsigned char>(ch) >> 4];
			result += hex_digit[static_cast<unsigned char>(ch) & 15];
		}
	}
	return result;
}

S3AuthParams S3AuthParams::ReadFrom(FileOpener *opener) {
	std::string region;
	std::string access_key_id;
	std::string secret_access_key;
	std::string session_token;
	std::string endpoint;
	std::string url_style;
	bool use_ssl;
	Value value;

	if (opener->TryGetCurrentSetting("s3_region", value)) {
		region = value.ToString();
	}

	if (opener->TryGetCurrentSetting("s3_access_key_id", value)) {
		access_key_id = value.ToString();
	}

	if (opener->TryGetCurrentSetting("s3_secret_access_key", value)) {
		secret_access_key = value.ToString();
	}

	if (opener->TryGetCurrentSetting("s3_session_token", value)) {
		session_token = value.ToString();
	}

	if (opener->TryGetCurrentSetting("s3_endpoint", value)) {
		endpoint = value.ToString();
	} else {
		endpoint = "s3.amazonaws.com";
	}

	if (opener->TryGetCurrentSetting("s3_url_style", value)) {
		auto val_str = value.ToString();
		if (!(val_str == "vhost" || val_str != "path" || val_str != "")) {
			throw std::runtime_error(
			    "Incorrect setting found for s3_url_style, allowed values are: 'path' and 'vhost'");
		}
		url_style = val_str;
	} else {
		url_style = "vhost";
	}

	if (opener->TryGetCurrentSetting("s3_use_ssl", value)) {
		use_ssl = value.GetValue<bool>();
	} else {
		use_ssl = true;
	}

	return {region, access_key_id, secret_access_key, session_token, endpoint, url_style, use_ssl};
}

S3ConfigParams S3ConfigParams::ReadFrom(FileOpener *opener) {
	uint64_t uploader_max_filesize;
	uint64_t max_parts_per_file;
	uint64_t max_upload_threads;
	Value value;

	if (opener->TryGetCurrentSetting("s3_uploader_max_filesize", value)) {
		uploader_max_filesize = DBConfig::ParseMemoryLimit(value.GetValue<string>());
	} else {
		uploader_max_filesize = S3ConfigParams::DEFAULT_MAX_FILESIZE;
	}

	if (opener->TryGetCurrentSetting("s3_uploader_max_parts_per_file", value)) {
		max_parts_per_file = value.GetValue<uint64_t>();
	} else {
		max_parts_per_file = S3ConfigParams::DEFAULT_MAX_PARTS_PER_FILE; // AWS Default
	}

	if (opener->TryGetCurrentSetting("s3_uploader_thread_limit", value)) {
		max_upload_threads = value.GetValue<uint64_t>();
	} else {
		max_upload_threads = S3ConfigParams::DEFAULT_MAX_UPLOAD_THREADS;
	}

	return {uploader_max_filesize, max_parts_per_file, max_upload_threads};
}

void S3FileHandle::Close() {
	auto &s3fs = (S3FileSystem &)file_system;
	if ((flags & FileFlags::FILE_FLAGS_WRITE) && !upload_finalized) {
		s3fs.FlushAllBuffers(*this);
		s3fs.FinalizeMultipartUpload(*this);
	}
}

void S3FileHandle::InitializeClient() {
	auto parsed_url = S3FileSystem::S3UrlParse(path, this->auth_params);

	string proto_host_port = parsed_url.http_proto + parsed_url.host;
	http_client = HTTPFileSystem::GetClient(this->http_params, proto_host_port.c_str());
}

// Opens the multipart upload and returns the ID
string S3FileSystem::InitializeMultipartUpload(S3FileHandle &file_handle) {
	auto &s3fs = (S3FileSystem &)file_handle.file_system;

	// AWS response is around 300~ chars in docs so this should be enough to not need a resize
	idx_t response_buffer_len = 1000;
	auto response_buffer = unique_ptr<char[]> {new char[response_buffer_len]};

	string query_param = "?" + UrlEncode("uploads") + "=";
	auto res = s3fs.PostRequest(file_handle, file_handle.path + query_param, {}, response_buffer, response_buffer_len,
	                            nullptr, 0);
	string result(response_buffer.get(), response_buffer_len);

	auto open_tag_pos = result.find("<UploadId>", 0);
	auto close_tag_pos = result.find("</UploadId>", open_tag_pos);

	if (open_tag_pos == string::npos || close_tag_pos == string::npos) {
		throw std::runtime_error("Unexpected response while initializing S3 multipart upload");
	}

	open_tag_pos += 10; // Skip open tag

	return result.substr(open_tag_pos, close_tag_pos - open_tag_pos);
}

void S3FileSystem::UploadBuffer(S3FileHandle &file_handle, shared_ptr<S3WriteBuffer> write_buffer) {
	auto &s3fs = (S3FileSystem &)file_handle.file_system;

	string query_param = S3FileSystem::UrlEncode("partNumber") + "=" + to_string(write_buffer->part_no + 1) + "&" +
	                     S3FileSystem::UrlEncode("uploadId") + "=" +
	                     S3FileSystem::UrlEncode(file_handle.multipart_upload_id, true);
	unique_ptr<ResponseWrapper> res;

	bool success = false;
	string last_error = "";
	auto time_at_start = duration_cast<std::chrono::milliseconds>(system_clock::now().time_since_epoch()).count();

	// Retry loop to make large uploads resilient to brief connection issues
	while (true) {
		try {
			res = s3fs.PutRequest(file_handle, file_handle.path + "?" + query_param, {}, (char *)write_buffer->Ptr(),
			                      write_buffer->idx);
			if (res->code == 200) {
				success = true;
				break;
			} else {
				last_error = res->error + " (HTTP code " + std::to_string(res->code) + ")";
			}
		} catch (std::runtime_error &e) {
			if (strncmp(e.what(), "HTTP PUT error", 14) != 0) {
				throw e;
			}
			last_error = e.what();
		}

		// If there are no parts uploaded yet, failing immediately makes more sense than waiting for the time-out
		if (file_handle.parts_uploaded.load() == 0) {
			break;
		}

		auto current_time = duration_cast<std::chrono::milliseconds>(system_clock::now().time_since_epoch()).count();
		if ((uint64_t)(current_time - time_at_start) > file_handle.http_params.timeout) {
			break;
		}

		std::this_thread::sleep_for(std::chrono::milliseconds(MULTIPART_UPLOAD_WAIT_BETWEEN_RETRIES_MS + 1));
	}

	if (!success) {
		throw std::runtime_error("Unable to connect to URL \"" + file_handle.path + "\"(last attempt failed with: \"" +
		                         last_error + "\")");
	}

	auto etag_lookup = res->headers.find("ETag");
	if (etag_lookup == res->headers.end()) {
		throw std::runtime_error("Unexpected reponse when uploading part to S3");
	}

	// Insert etag
	file_handle.part_etags_lock.lock();
	file_handle.part_etags.insert(std::pair<uint16_t, string>(write_buffer->part_no, etag_lookup->second));
	file_handle.part_etags_lock.unlock();

	file_handle.parts_uploaded++;

	// Free up space for another thread to acquire an S3WriteBuffer
	write_buffer.reset();
	s3fs.buffers_available++;
	s3fs.buffers_available_cv.notify_one();

	// Update uploads in progress
	file_handle.uploads_in_progress--;
	file_handle.uploads_in_progress_cv.notify_one();
}

void S3FileSystem::FlushBuffer(S3FileHandle &file_handle, std::shared_ptr<S3WriteBuffer> write_buffer) {
	if (write_buffer->idx == 0) {
		return;
	}

	auto uploading = write_buffer->uploading.load();
	if (uploading) {
		return;
	}
	bool can_upload = write_buffer->uploading.compare_exchange_strong(uploading, true);
	if (!can_upload) {
		return;
	}

	file_handle.write_buffers_lock.lock();
	file_handle.write_buffers.erase(write_buffer->part_no);
	file_handle.write_buffers_lock.unlock();
	file_handle.uploads_in_progress++;

	thread upload_thread(UploadBuffer, std::ref(file_handle), write_buffer);
	upload_thread.detach();
}

// Note that FlushAll currently does not allow to continue writing afterwards. Therefore, FinalizeMultipartUpload should
// be called right after it!
// TODO: we can fix this by keeping the last partially written buffer in memory and allow reuploading it with new data.
void S3FileSystem::FlushAllBuffers(S3FileHandle &file_handle) {
	//  Collect references to all buffers to check
	std::vector<std::shared_ptr<S3WriteBuffer>> to_flush;
	file_handle.write_buffers_lock.lock();
	for (auto &item : file_handle.write_buffers) {
		to_flush.push_back(item.second);
	}
	file_handle.write_buffers_lock.unlock();

	// Flush all buffers that aren't already uploading
	for (auto &write_buffer : to_flush) {
		if (!write_buffer->uploading) {
			FlushBuffer(file_handle, write_buffer);
		}
	}
	std::unique_lock<std::mutex> lck(file_handle.uploads_in_progress_lock);
	file_handle.uploads_in_progress_cv.wait(lck,
	                                        [&file_handle] { return file_handle.uploads_in_progress.load() == 0; });
}

void S3FileSystem::FinalizeMultipartUpload(S3FileHandle &file_handle) {
	auto &s3fs = (S3FileSystem &)file_handle.file_system;

	std::stringstream ss;
	ss << "<CompleteMultipartUpload xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">";

	auto parts = file_handle.parts_uploaded.load();
	for (auto i = 0; i < parts; i++) {
		auto etag_lookup = file_handle.part_etags.find(i);
		if (etag_lookup == file_handle.part_etags.end()) {
			throw std::runtime_error("Unknown part number");
		}
		ss << "<Part><ETag>" << etag_lookup->second << "</ETag><PartNumber>" << i + 1 << "</PartNumber></Part>";
	}
	ss << "</CompleteMultipartUpload>";
	string body = ss.str();

	// Response is around ~400 in AWS docs so this should be enough to not need a resize
	idx_t response_buffer_len = 1000;
	auto response_buffer = unique_ptr<char[]> {new char[response_buffer_len]};

	string query_param = "?" + UrlEncode("uploadId") + "=" + file_handle.multipart_upload_id;
	auto res = s3fs.PostRequest(file_handle, file_handle.path + query_param, {}, response_buffer, response_buffer_len,
	                            (char *)body.c_str(), body.length());
	string result(response_buffer.get(), response_buffer_len);

	auto open_tag_pos = result.find("<CompleteMultipartUploadResult", 0);
	if (open_tag_pos == string::npos) {
		throw std::runtime_error("Unexpected response during S3 multipart upload finalization");
	}
	file_handle.upload_finalized = true;
}

std::shared_ptr<S3WriteBuffer> S3FileSystem::GetBuffer(S3FileHandle &file_handle, uint16_t write_buffer_idx) {
	auto &s3fs = (S3FileSystem &)file_handle.file_system;
	// Check if write buffer already exists
	{
		std::unique_lock<std::mutex> lck(file_handle.write_buffers_lock);
		auto lookup_result = file_handle.write_buffers.find(write_buffer_idx);
		if (lookup_result != file_handle.write_buffers.end()) {
			std::shared_ptr<S3WriteBuffer> buffer = lookup_result->second;
			return buffer;
		}
	}

	// Wait for a buffer to become available
	{
		std::unique_lock<std::mutex> lck(s3fs.buffers_available_lock);
		s3fs.buffers_available_cv.wait(lck, [&s3fs] { return s3fs.buffers_available > 0; });
		s3fs.buffers_available--;
	}

	// Try to allocate a buffer from the buffer manager
	unique_ptr<BufferHandle> duckdb_buffer;
	bool set_waiting_for_memory = false;

	while (true) {
		try {
			duckdb_buffer = buffer_manager.Allocate(file_handle.part_size);

			if (set_waiting_for_memory) {
				threads_waiting_for_memory--;
			}
			break;
		} catch (OutOfMemoryException &e) {
			if (!set_waiting_for_memory) {
				threads_waiting_for_memory++;
				set_waiting_for_memory = true;
			}
			auto buffers_available = s3fs.buffers_available.load();

			if (buffers_available >= file_handle.config_params.max_upload_threads - threads_waiting_for_memory) {
				// There exist no upload write buffers that can release more memory. We really ran out of memory here.
				throw e;
			} else {

				// Wait for more buffers to become available before trying again
				{
					std::unique_lock<std::mutex> lck(s3fs.buffers_available_lock);
					s3fs.buffers_available_cv.wait(
					    lck, [&s3fs, &buffers_available] { return s3fs.buffers_available > buffers_available; });
				}
			}
		}
	}

	auto new_write_buffer =
	    make_shared<S3WriteBuffer>(write_buffer_idx * file_handle.part_size, file_handle.part_size, duckdb_buffer);

	{
		std::unique_lock<std::mutex> lck(file_handle.write_buffers_lock);
		auto lookup_result = file_handle.write_buffers.find(write_buffer_idx);

		// Check if other thread has created the same buffer, if so we return theirs and drop ours.
		if (lookup_result != file_handle.write_buffers.end()) {
			// write_buffer_idx << std::endl;
			std::shared_ptr<S3WriteBuffer> write_buffer = lookup_result->second;
			file_handle.write_buffers_lock.unlock();
			return write_buffer;
		}
		file_handle.write_buffers.insert(
		    std::pair<uint16_t, std::shared_ptr<S3WriteBuffer>>(write_buffer_idx, new_write_buffer));
	}

	return new_write_buffer;
}

ParsedS3Url S3FileSystem::S3UrlParse(string url, const S3AuthParams &params) {
	string http_proto, host, bucket, path, query_param;

	// some URI parsing woo
	if (url.rfind("s3://", 0) != 0) {
		throw std::runtime_error("URL needs to start with s3://");
	}
	auto slash_pos = url.find('/', 5);
	if (slash_pos == std::string::npos) {
		throw std::runtime_error("URL needs to contain a '/' after the host");
	}
	bucket = url.substr(5, slash_pos - 5);
	if (bucket.empty()) {
		throw std::runtime_error("URL needs to contain a bucket name");
	}
	auto question_pos = url.find('?', 5);

	// See https://docs.aws.amazon.com/AmazonS3/latest/userguide/VirtualHosting.html
	if (params.url_style == "path") {
		path = "/" + bucket;
	} else {
		path = "";
	}

	if (question_pos == std::string::npos) {
		path += url.substr(slash_pos);
		query_param = "";
	} else {
		path += url.substr(slash_pos, question_pos - slash_pos);
		query_param = url.substr(question_pos + 1);
	}
	if (path.empty()) {
		throw std::runtime_error("URL needs to contain key");
	}

	if (params.url_style == "vhost" || params.url_style == "") {
		host = bucket + "." + params.endpoint;
	} else {
		host = params.endpoint;
	}

	http_proto = params.use_ssl ? "https://" : "http://";

	return {http_proto, host, bucket, path, query_param};
}

string S3FileSystem::GetPayloadHash(char *buffer, idx_t buffer_len) {
	if (buffer_len > 0) {
		hash_bytes payload_hash_bytes;
		hash_str payload_hash_str;
		sha256(buffer, buffer_len, payload_hash_bytes);
		hex256(payload_hash_bytes, payload_hash_str);
		return std::string((char *)payload_hash_str, sizeof(payload_hash_str));
	} else {
		return "";
	}
}

static string get_full_s3_url(S3AuthParams &auth_params, ParsedS3Url parsed_url) {
	string full_url = parsed_url.http_proto + parsed_url.host + parsed_url.path;

	if (!parsed_url.query_param.empty()) {
		full_url += "?" + parsed_url.query_param;
	}

	return full_url;
}

unique_ptr<ResponseWrapper> S3FileSystem::PostRequest(FileHandle &handle, string url, HeaderMap header_map,
                                                      unique_ptr<char[]> &buffer_out, idx_t &buffer_out_len,
                                                      char *buffer_in, idx_t buffer_in_len) {
	auto auth_params = static_cast<S3FileHandle &>(handle).auth_params;
	auto parsed_url = S3UrlParse(url, auth_params);

	string full_url = get_full_s3_url(auth_params, parsed_url);
	string post_url;
	auto payload_hash = GetPayloadHash(buffer_in, buffer_in_len);

	auto headers = create_s3_header(parsed_url.path, parsed_url.query_param, parsed_url.host, "s3", "POST", auth_params,
	                                "", "", payload_hash, "application/octet-stream");

	return HTTPFileSystem::PostRequest(handle, full_url, headers, buffer_out, buffer_out_len, buffer_in, buffer_in_len);
}

unique_ptr<ResponseWrapper> S3FileSystem::PutRequest(FileHandle &handle, string url, HeaderMap header_map,
                                                     char *buffer_in, idx_t buffer_in_len) {
	auto auth_params = static_cast<S3FileHandle &>(handle).auth_params;
	auto parsed_url = S3UrlParse(url, auth_params);
	string full_url = get_full_s3_url(auth_params, parsed_url);
	auto content_type = "application/octet-stream";
	auto payload_hash = GetPayloadHash(buffer_in, buffer_in_len);

	auto headers = create_s3_header(parsed_url.path, parsed_url.query_param, parsed_url.host, "s3", "PUT", auth_params,
	                                "", "", payload_hash, content_type);
	return HTTPFileSystem::PutRequest(handle, full_url, headers, buffer_in, buffer_in_len);
}

unique_ptr<ResponseWrapper> S3FileSystem::HeadRequest(FileHandle &handle, string url, HeaderMap header_map) {
	auto auth_params = static_cast<S3FileHandle &>(handle).auth_params;
	auto parsed_url = S3UrlParse(url, auth_params);
	string full_url = get_full_s3_url(auth_params, parsed_url);
	auto headers = create_s3_header(parsed_url.path, parsed_url.query_param, parsed_url.host, "s3", "HEAD", auth_params,
	                                "", "", "", "");
	return HTTPFileSystem::HeadRequest(handle, full_url, headers);
}

unique_ptr<ResponseWrapper> S3FileSystem::GetRangeRequest(FileHandle &handle, string url, HeaderMap header_map,
                                                          idx_t file_offset, char *buffer_out, idx_t buffer_out_len) {
	auto auth_params = static_cast<S3FileHandle &>(handle).auth_params;
	auto parsed_url = S3UrlParse(url, auth_params);
	string full_url = get_full_s3_url(auth_params, parsed_url);
	auto headers = create_s3_header(parsed_url.path, parsed_url.query_param, parsed_url.host, "s3", "GET", auth_params,
	                                "", "", "", "");
	return HTTPFileSystem::GetRangeRequest(handle, full_url, headers, file_offset, buffer_out, buffer_out_len);
}

std::unique_ptr<HTTPFileHandle> S3FileSystem::CreateHandle(const string &path, uint8_t flags, FileLockType lock,
                                                           FileCompressionType compression, FileOpener *opener) {
	if (!opener) {
		throw std::runtime_error("CreateHandle called on S3FileSystem without FileOpener");
	}
	return duckdb::make_unique<S3FileHandle>(*this, path, flags, opener ? HTTPParams::ReadFrom(opener) : HTTPParams(),
	                                         S3AuthParams::ReadFrom(opener), S3ConfigParams::ReadFrom(opener));
}

// this computes the signature from https://czak.pl/2015/09/15/s3-rest-api-with-curl.html
void S3FileSystem::Verify() {

	S3AuthParams auth_params = {
	    "us-east-1", "AKIAIOSFODNN7EXAMPLE", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY", "", "", "", true};
	auto test_header = create_s3_header("/", "", "my-precious-bucket.s3.amazonaws.com", "s3", "GET", auth_params,
	                                    "20150915", "20150915T124500Z");
	if (test_header["Authorization"] !=
	    "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20150915/us-east-1/s3/aws4_request, "
	    "SignedHeaders=host;x-amz-content-sha256;x-amz-date, "
	    "Signature=182072eb53d85c36b2d791a1fa46a12d23454ec1e921b02075c23aee40166d5a") {
		throw std::runtime_error("test fail");
	}

	if (UrlEncode("/category=Books/") != "/category%3DBooks/") {
		throw std::runtime_error("test fail");
	}
	if (UrlEncode("/?category=Books&title=Ducks Retreat/") != "/%3Fcategory%3DBooks%26title%3DDucks%20Retreat/") {
		throw std::runtime_error("test fail");
	}
	if (UrlEncode("/?category=Books&title=Ducks Retreat/", true) !=
	    "%2F%3Fcategory%3DBooks%26title%3DDucks%20Retreat%2F") {
		throw std::runtime_error("test fail");
	}
	// AWS_SECRET_ACCESS_KEY="vs1BZPxSL2qVARBSg5vCMKJsavCoEPlo/HSHRaVe" AWS_ACCESS_KEY_ID="ASIAYSPIOYDTHTBIITVC"
	// AWS_SESSION_TOKEN="IQoJb3JpZ2luX2VjENX//////////wEaCWV1LXdlc3QtMSJHMEUCIQDfjzs9BYHrEXDMU/NR+PHV1uSTr7CSVSQdjKSfiPRLdgIgCCztF0VMbi9+uHHAfBVKhV4t9MlUrQg3VAOIsLxrWyoqlAIIHRAAGgw1ODk0MzQ4OTY2MTQiDOGl2DsYxENcKCbh+irxARe91faI+hwUhT60sMGRFg0GWefKnPclH4uRFzczrDOcJlAAaQRJ7KOsT8BrJlrY1jSgjkO7PkVjPp92vi6lJX77bg99MkUTJActiOKmd84XvAE5bFc/jFbqechtBjXzopAPkKsGuaqAhCenXnFt6cwq+LZikv/NJGVw7TRphLV+Aq9PSL9XwdzIgsW2qXwe1c3rxDNj53yStRZHVggdxJ0OgHx5v040c98gFphzSULHyg0OY6wmCMTYcswpb4kO2IIi6AiD9cY25TlwPKRKPi5CdBsTPnyTeW62u7PvwK0fTSy4ZuJUuGKQnH2cKmCXquEwoOHEiQY6nQH9fzY/EDGHMRxWWhxu0HiqIfsuFqC7GS0p0ToKQE+pzNsvVwMjZc+KILIDDQpdCWRIwu53I5PZy2Cvk+3y4XLvdZKQCsAKqeOc4c94UAS4NmUT7mCDOuRV0cLBVM8F0JYBGrUxyI+YoIvHhQWmnRLuKgTb5PkF7ZWrXBHFWG5/tZDOvBbbaCWTlRCL9b0Vpg5+BM/81xd8jChP4w83"
	// aws --region eu-west-1 --debug s3 ls my-precious-bucket 2>&1 | less
	std::string canonical_query_string = "delimiter=%2F&encoding-type=url&list-type=2&prefix="; // aws s3 ls <bucket>

	S3AuthParams auth_params2 = {
	    "eu-west-1",
	    "ASIAYSPIOYDTHTBIITVC",
	    "vs1BZPxSL2qVARBSg5vCMKJsavCoEPlo/HSHRaVe",
	    "IQoJb3JpZ2luX2VjENX//////////wEaCWV1LXdlc3QtMSJHMEUCIQDfjzs9BYHrEXDMU/"
	    "NR+PHV1uSTr7CSVSQdjKSfiPRLdgIgCCztF0VMbi9+"
	    "uHHAfBVKhV4t9MlUrQg3VAOIsLxrWyoqlAIIHRAAGgw1ODk0MzQ4OTY2MTQiDOGl2DsYxENcKCbh+irxARe91faI+"
	    "hwUhT60sMGRFg0GWefKnPclH4uRFzczrDOcJlAAaQRJ7KOsT8BrJlrY1jSgjkO7PkVjPp92vi6lJX77bg99MkUTJA"
	    "ctiOKmd84XvAE5bFc/jFbqechtBjXzopAPkKsGuaqAhCenXnFt6cwq+LZikv/"
	    "NJGVw7TRphLV+"
	    "Aq9PSL9XwdzIgsW2qXwe1c3rxDNj53yStRZHVggdxJ0OgHx5v040c98gFphzSULHyg0OY6wmCMTYcswpb4kO2IIi6"
	    "AiD9cY25TlwPKRKPi5CdBsTPnyTeW62u7PvwK0fTSy4ZuJUuGKQnH2cKmCXquEwoOHEiQY6nQH9fzY/"
	    "EDGHMRxWWhxu0HiqIfsuFqC7GS0p0ToKQE+pzNsvVwMjZc+KILIDDQpdCWRIwu53I5PZy2Cvk+"
	    "3y4XLvdZKQCsAKqeOc4c94UAS4NmUT7mCDOuRV0cLBVM8F0JYBGrUxyI+"
	    "YoIvHhQWmnRLuKgTb5PkF7ZWrXBHFWG5/tZDOvBbbaCWTlRCL9b0Vpg5+BM/81xd8jChP4w83",
	    "",
	    "",
	    true};
	auto test_header2 = create_s3_header("/", canonical_query_string, "my-precious-bucket.s3.eu-west-1.amazonaws.com",
	                                     "s3", "GET", auth_params2, "20210904", "20210904T121746Z");
	if (test_header2["Authorization"] !=
	    "AWS4-HMAC-SHA256 Credential=ASIAYSPIOYDTHTBIITVC/20210904/eu-west-1/s3/aws4_request, "
	    "SignedHeaders=host;x-amz-content-sha256;x-amz-date;x-amz-security-token, "
	    "Signature=4d9d6b59d7836b6485f6ad822de97be40287da30347d83042ea7fbed530dc4c0") {
		throw std::runtime_error("test fail");
	}

	S3AuthParams auth_params3 = {"eu-west-1", "S3RVER", "S3RVER", "", "", "", true};
	auto test_header3 =
	    create_s3_header("/correct_auth_test.csv", "", "test-bucket-ceiveran.s3.amazonaws.com", "s3", "PUT",
	                     auth_params3, "20220121", "20220121T141452Z",
	                     "28a0cf6ac5c4cb73793091fe6ecc6a68bf90855ac9186158748158f50241bb0c", "text/data;charset=utf-8");
	if (test_header3["Authorization"] != "AWS4-HMAC-SHA256 Credential=S3RVER/20220121/eu-west-1/s3/aws4_request, "
	                                     "SignedHeaders=content-type;host;x-amz-content-sha256;x-amz-date, "
	                                     "Signature=5d9a6cbfaa78a6d0f2ab7df0445e2f1cc9c80cd3655ac7de9e7219c036f23f02") {
		throw std::runtime_error("test3 fail");
	}

	if (UrlEncode("/category=Books/") != "/category%3DBooks/") {
		throw std::runtime_error("test fail");
	}
	if (UrlEncode("/?category=Books&title=Ducks Retreat/") != "/%3Fcategory%3DBooks%26title%3DDucks%20Retreat/") {
		throw std::runtime_error("test fail");
	}
	if (UrlEncode("/?category=Books&title=Ducks Retreat/", true) !=
	    "%2F%3Fcategory%3DBooks%26title%3DDucks%20Retreat%2F") {
		throw std::runtime_error("test fail");
	}

	// TODO add a test that checks the signing for path-style
}

unique_ptr<ResponseWrapper> S3FileHandle::Initialize() {
	auto res = HTTPFileHandle::Initialize();

	auto &s3fs = (S3FileSystem &)file_system;

	if (flags & FileFlags::FILE_FLAGS_WRITE) {
		auto aws_minimum_part_size = 5242880; // 5 MiB https://docs.aws.amazon.com/AmazonS3/latest/userguide/qfacts.html
		auto max_part_count = config_params.max_parts_per_file;
		auto required_part_size = config_params.max_file_size / max_part_count;
		auto minimum_part_size = MaxValue<idx_t>(aws_minimum_part_size, required_part_size);

		// Round part size up to multiple of BLOCK_SIZE
		part_size = ((minimum_part_size + Storage::BLOCK_SIZE - 1) / Storage::BLOCK_SIZE) * Storage::BLOCK_SIZE;
		D_ASSERT(part_size * max_part_count >= config_params.max_file_size);

		multipart_upload_id = s3fs.InitializeMultipartUpload(*this);

		// Threads are limited by limiting the amount of write buffers available, since each
		s3fs.buffers_available = config_params.max_upload_threads;
		uploads_in_progress = 0;
		parts_uploaded = 0;
		upload_finalized = false;
	}

	return res;
}

bool S3FileSystem::CanHandleFile(const string &fpath) {
	return fpath.rfind("s3://", 0) == 0;
}

void S3FileSystem::FileSync(FileHandle &handle) {
	auto &s3fh = (S3FileHandle &)handle;
	if (!s3fh.upload_finalized) {
		FlushAllBuffers(s3fh);
		FinalizeMultipartUpload(s3fh);
	}
}

void S3FileSystem::Write(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) {
	auto &s3fh = (S3FileHandle &)handle;
	if (!(s3fh.flags & FileFlags::FILE_FLAGS_WRITE)) {
		throw InternalException("Write called on file not opened in write mode");
	}
	int64_t bytes_written = 0;

	while (bytes_written < nr_bytes) {
		auto curr_location = location + bytes_written;

		if (curr_location != s3fh.file_offset) {
			throw InternalException("Non-sequential write not supported!");
		}

		// Find buffer for writing
		auto write_buffer_idx = curr_location / s3fh.part_size;

		// Get write buffer, may block until buffer is available
		auto write_buffer = GetBuffer(s3fh, write_buffer_idx);

		// Writing to buffer
		auto idx_to_write = curr_location - write_buffer->buffer_start;
		auto bytes_to_write = MinValue<idx_t>(nr_bytes - bytes_written, s3fh.part_size - idx_to_write);
		memcpy((char *)write_buffer->Ptr() + idx_to_write, (char *)buffer + bytes_written, bytes_to_write);
		write_buffer->idx += bytes_to_write;

		// Flush to HTTP if full
		if (write_buffer->idx >= s3fh.part_size) {
			FlushBuffer(s3fh, write_buffer);
		}
		s3fh.file_offset += bytes_to_write;
		bytes_written += bytes_to_write;
	}
}

vector<string> S3FileSystem::Glob(const string &glob_pattern, FileOpener *opener) {
	if (opener == nullptr) {
		throw InternalException("Cannot S3 Glob without FileOpener");
	}
	// AWS matches on prefix, not glob pattern so we take a substring until the first wildcard char for the aws calls
	auto first_wildcard_pos = glob_pattern.find_first_of("*?[\\");
	if (first_wildcard_pos == string::npos) {
		return {glob_pattern};
	}
	string shared_path = glob_pattern.substr(0, first_wildcard_pos);

	auto s3_auth_params = S3AuthParams::ReadFrom(opener);
	auto http_params = HTTPParams::ReadFrom(opener);

	// Parse pattern
	auto parsed_url = S3UrlParse(glob_pattern, s3_auth_params);

	// Do main listobjectsv2 request
	vector<string> s3_keys;
	string main_continuation_token = "";

	// Main paging loop
	do {
		// main listobject call, may
		string response_str =
		    AWSListObjectV2::Request(shared_path, http_params, s3_auth_params, main_continuation_token);
		main_continuation_token = AWSListObjectV2::ParseContinuationToken(response_str);
		AWSListObjectV2::ParseKey(response_str, s3_keys);

		// Repeat requests until the keys of all common prefixes are parsed.
		auto common_prefixes = AWSListObjectV2::ParseCommonPrefix(response_str);
		while (!common_prefixes.empty()) {
			auto prefix_path = "s3://" + parsed_url.bucket + '/' + common_prefixes.back();
			common_prefixes.pop_back();

			// TODO we could optimize here by doing a match on the prefix, if it doesn't match we can skip this prefix
			// Paging loop for common prefix requests
			string common_prefix_continuation_token = "";
			do {
				auto prefix_res = AWSListObjectV2::Request(prefix_path, http_params, s3_auth_params,
				                                           common_prefix_continuation_token);
				AWSListObjectV2::ParseKey(prefix_res, s3_keys);
				auto more_prefixes = AWSListObjectV2::ParseCommonPrefix(prefix_res);
				common_prefixes.insert(common_prefixes.end(), more_prefixes.begin(), more_prefixes.end());
				common_prefix_continuation_token = AWSListObjectV2::ParseContinuationToken(prefix_res);
			} while (!common_prefix_continuation_token.empty());
		}
	} while (!main_continuation_token.empty());

	auto pattern_trimmed = parsed_url.path.substr(1);

	// Trim the bucket prefix for path-style urls
	if (s3_auth_params.url_style == "path") {
		pattern_trimmed = pattern_trimmed.substr(parsed_url.bucket.length() + 1);
	}

	// if a ? char was present, we re-add it here as the url parsing will have trimmed it.
	if (parsed_url.query_param != "") {
		pattern_trimmed += '?' + parsed_url.query_param;
	}

	vector<string> result;
	for (const auto &s3_key : s3_keys) {

		auto is_match = LikeFun::Glob(s3_key.data(), s3_key.length(), pattern_trimmed.data(), pattern_trimmed.length());

		if (is_match) {
			auto result_full_url = "s3://" + parsed_url.bucket + "/" + s3_key;
			result.push_back(result_full_url);
		}
	}
	return result;
}

string AWSListObjectV2::Request(string &path, HTTPParams &http_params, S3AuthParams &s3_auth_params,
                                string &continuation_token, bool use_delimiter) {
	auto parsed_url = S3FileSystem::S3UrlParse(path, s3_auth_params);

	// Construct the ListObjectsV2 call
	string req_path;
	if (s3_auth_params.url_style == "path") {
		req_path = "/" + parsed_url.bucket + "/";
	} else {
		req_path = "/";
	}

	string prefix = parsed_url.path.substr(1);

	// Trim the bucket prefix for path-style urls
	if (s3_auth_params.url_style == "path") {
		prefix = prefix.substr(parsed_url.bucket.length() + 1);
	}

	string req_params = "";
	if (!continuation_token.empty()) {
		req_params += "continuation-token=" + S3FileSystem::UrlEncode(continuation_token);
		req_params += "&";
	}
	req_params += "encoding-type=url&list-type=2";
	req_params += "&prefix=" + S3FileSystem::UrlEncode(prefix, true);

	if (use_delimiter) {
		req_params += "&delimiter=%2F";
	}

	string listobjectv2_url = parsed_url.http_proto + parsed_url.host + req_path + "?" + req_params;

	auto header_map =
	    create_s3_header(req_path, req_params, parsed_url.host, "s3", "GET", s3_auth_params, "", "", "", "");
	auto headers = initialize_http_headers(header_map);

	auto client = S3FileSystem::GetClient(
	    http_params, (parsed_url.http_proto + parsed_url.host).c_str()); // Get requests use fresh connection
	std::stringstream response;
	auto res = client->Get(
	    listobjectv2_url.c_str(), *headers,
	    [&](const duckdb_httplib_openssl::Response &response) {
		    if (response.status >= 400) {
			    std::cout << response.reason << std::endl;
			    throw std::runtime_error("HTTP GET error on '" + listobjectv2_url + "' (HTTP " +
			                             std::to_string(response.status) + ")");
		    }
		    return true;
	    },
	    [&](const char *data, size_t data_length) {
		    response << string(data, data_length);
		    return true;
	    });
	if (res.error() != duckdb_httplib_openssl::Error::Success) {
		throw std::runtime_error("HTTP GET error on '" + listobjectv2_url + "' (Error code " +
		                         std::to_string((int)res.error()) + ")");
	}

	return response.str();
}

void AWSListObjectV2::ParseKey(string &aws_response, vector<string> &result) {
	idx_t cur_pos = 0;
	while (true) {
		auto next_open_tag_pos = aws_response.find("<Key>", cur_pos);
		if (next_open_tag_pos == string::npos) {
			break;
		} else {
			auto next_close_tag_pos = aws_response.find("</Key>", next_open_tag_pos + 5);
			if (next_close_tag_pos == string::npos) {
				throw InternalException("Failed to parse S3 result");
			}
			auto parsed_path = S3FileSystem::UrlDecode(
			    aws_response.substr(next_open_tag_pos + 5, next_close_tag_pos - next_open_tag_pos - 5));
			if (parsed_path.back() != '/') {
				result.push_back(parsed_path);
			}
			cur_pos = next_close_tag_pos + 6;
		}
	}
}

string AWSListObjectV2::ParseContinuationToken(string &aws_response) {

	auto open_tag_pos = aws_response.find("<NextContinuationToken>");
	if (open_tag_pos == string::npos) {
		return "";
	} else {
		auto close_tag_pos = aws_response.find("</NextContinuationToken>", open_tag_pos + 23);
		if (close_tag_pos == string::npos) {
			throw InternalException("Failed to parse S3 result");
		}
		return aws_response.substr(open_tag_pos + 23, close_tag_pos - open_tag_pos - 23);
	}
}

vector<string> AWSListObjectV2::ParseCommonPrefix(string &aws_response) {
	vector<string> s3_prefixes;
	idx_t cur_pos = 0;
	while (true) {
		cur_pos = aws_response.find("<CommonPrefixes>", cur_pos);
		if (cur_pos == string::npos) {
			break;
		}
		auto next_open_tag_pos = aws_response.find("<Prefix>", cur_pos);
		if (next_open_tag_pos == string::npos) {
			throw InternalException("Parsing error while parsing s3 listobject result");
		} else {
			auto next_close_tag_pos = aws_response.find("</Prefix>", next_open_tag_pos + 8);
			if (next_close_tag_pos == string::npos) {
				throw InternalException("Failed to parse S3 result");
			}
			auto parsed_path = aws_response.substr(next_open_tag_pos + 8, next_close_tag_pos - next_open_tag_pos - 8);
			s3_prefixes.push_back(parsed_path);
			cur_pos = next_close_tag_pos + 6;
		}
	}
	return s3_prefixes;
}

} // namespace duckdb
