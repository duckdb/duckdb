#include "s3fs.hpp"
#include "crypto.hpp"
#include "duckdb.hpp"
#ifndef DUCKDB_AMALGAMATION
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/thread.hpp"
#include "duckdb/function/scalar/strftime.hpp"
#endif

#include <iostream>

namespace duckdb {

static std::string uri_encode(const std::string &input, bool encode_slash = false) {
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

static HeaderMap create_s3_get_header(std::string url, std::string query, std::string host, std::string region,
                                      std::string service, std::string method, std::string access_key_id,
                                      std::string secret_access_key, std::string session_token,
                                      std::string date_now = "", std::string datetime_now = "",
                                      std::string payload_hash = "", std::string content_type = "") {
	// this is the sha256 of the empty string, its useful since we have no payload for GET requests

	if (payload_hash == "") {
		payload_hash = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"; // Empty payload hash
	}

	// we can pass date/time but this is mostly useful in testing. normally we just get the current datetime here.
	if (datetime_now.empty()) {
		auto timestamp = Timestamp::GetCurrentTimestamp();
		date_now = StrfTimeFormat::Format(timestamp, "%Y%m%d");
		datetime_now = StrfTimeFormat::Format(timestamp, "%Y%m%dT%H%M%SZ");
	}

	HeaderMap res;
	res["Host"] = host;
	res["x-amz-date"] = datetime_now;
	res["x-amz-content-sha256"] = payload_hash;
	if (session_token.length() > 0) {
		res["x-amz-security-token"] = session_token;
	}

	// construct string to sign
	hash_bytes canonical_request_hash;
	hash_str canonical_request_hash_str;
	std::string signed_headers = "";
	if (content_type.length() > 0) {
		signed_headers += "content-type;";
	}
	signed_headers += "host;x-amz-content-sha256;x-amz-date";
	if (session_token.length() > 0) {
		signed_headers += ";x-amz-security-token";
	}
	auto canonical_request = method + "\n" + uri_encode(url) + "\n" + query;
	if (content_type.length() > 0) {
		canonical_request += "\ncontent-type:" + content_type;
	}
	canonical_request += "\nhost:" + host + "\nx-amz-content-sha256:" + payload_hash + "\nx-amz-date:" + datetime_now;
	if (session_token.length() > 0) {
		canonical_request += "\nx-amz-security-token:" + session_token;
	}

	canonical_request += "\n\n" + signed_headers + "\n" + payload_hash;
	sha256(canonical_request.c_str(), canonical_request.length(), canonical_request_hash);
	hex256(canonical_request_hash, canonical_request_hash_str);
	auto string_to_sign = "AWS4-HMAC-SHA256\n" + datetime_now + "\n" + date_now + "/" + region + "/" + service +
	                      "/aws4_request\n" + std::string((char *)canonical_request_hash_str, sizeof(hash_str));
	// compute signature
	hash_bytes k_date, k_region, k_service, signing_key, signature;
	hash_str signature_str;
	auto sign_key = "AWS4" + secret_access_key;
	hmac256(date_now, sign_key.c_str(), sign_key.length(), k_date);
	hmac256(region, k_date, k_region);
	hmac256(service, k_region, k_service);
	hmac256("aws4_request", k_service, signing_key);
	hmac256(string_to_sign, signing_key, signature);
	hex256(signature, signature_str);

	res["Authorization"] = "AWS4-HMAC-SHA256 Credential=" + access_key_id + "/" + date_now + "/" + region + "/" +
	                       service + "/aws4_request, SignedHeaders=" + signed_headers +
	                       ", Signature=" + std::string((char *)signature_str, sizeof(hash_str));

	return res;
}

S3AuthParams S3AuthParams::ReadFrom(FileOpener *opener) {
	std::string region;
	std::string access_key_id;
	std::string secret_access_key;
	std::string session_token;
	std::string endpoint;
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

	return {region, access_key_id, secret_access_key, session_token, endpoint};
}

void S3FileHandle::Close() {
	std::cout << "Closing handle" << "\n";
	auto &s3fs = (S3FileSystem &)file_system;
	if ((flags & FileFlags::FILE_FLAGS_WRITE) && !upload_finalized) {
		s3fs.FlushAllBuffers(*this);
		s3fs.FinalizeMultipartUpload(*this);
	}
}

// Opens the multipart upload and returns the ID
string S3FileSystem::InitializeMultipartUpload(S3FileHandle &file_handle) {
	auto &hfs = (HTTPFileSystem &)file_handle.file_system;
	char response_buffer[1000]; // TODO clean up

	string query_param = "?" + uri_encode("uploads") + "=";
	auto res = hfs.PostRequest(file_handle, file_handle.path + query_param, {}, response_buffer, 1000, nullptr, 0);
	string result(response_buffer, 1000);

	auto open_tag_pos = result.find("<UploadId>", 0);
	auto close_tag_pos = result.find("</UploadId>", open_tag_pos);

	if (open_tag_pos == string::npos || close_tag_pos == string::npos) {
		throw std::runtime_error("Unexpected response while initializing S3 multipart upload");
	}

	open_tag_pos += 10; // Skip open tag

	return result.substr(open_tag_pos, close_tag_pos - open_tag_pos);
}

void S3FileSystem::UploadBuffer(S3FileHandle &file_handle, shared_ptr<S3WriteBuffer> write_buffer) {
	auto &hfs = (HTTPFileSystem &)file_handle.file_system;

	string query_param = uri_encode("partNumber") + "=" + to_string(write_buffer->part_no + 1) + "&" +
	                     uri_encode("uploadId") + "=" + uri_encode(file_handle.multipart_upload_id, true);
	auto res = hfs.PutRequest(file_handle, file_handle.path + "?" + query_param, {},
	                          (char *)write_buffer->buffer.get(), write_buffer->idx);

	if (res->code != 200) {
		throw std::runtime_error("Unable to connect to URL \"" + file_handle.path +
		                         "\": " + std::to_string(res->code) + " (" + res->error + ")");
	}

	auto etag_lookup = res->headers.find("ETag");

	if (etag_lookup == res->headers.end()) {
		throw std::runtime_error("Unexpected reponse when uploading part to S3");
	}

	// Insert etag
	file_handle.part_etags_mutex.lock();
	file_handle.part_etags.insert(std::pair<uint16_t, string>(write_buffer->part_no, etag_lookup->second));
	file_handle.part_etags_mutex.unlock();

	// For debugging
	file_handle.parts_uploaded++;

	// Free up space for another thread to acquire a S3WriteBuffer
	file_handle.buffers_available++;
	// if the write_buffer.use_count() would be higher here, we would exceed our allowed memory briefly.
	// TODO: if this never hits, can we use unique_ptrs instead?
//	D_ASSERT(write_buffer.use_count() == 1);
	write_buffer.reset();
	file_handle.buffers_available_cv.notify_one();

	// Update uploads in progress
	file_handle.uploads_in_progress--;
	std::cout << "[END] buffers_available: " << file_handle.buffers_available.load()
	          << " parts_uploaded: " << file_handle.parts_uploaded.load()
	          << " uploads in progress: " << file_handle.uploads_in_progress.load() << std::endl;
	file_handle.uploads_in_progress_cv.notify_one();
}

void S3FileSystem::FlushBuffer(S3FileHandle &file_handle, std::shared_ptr<S3WriteBuffer> write_buffer) {
	if (write_buffer->idx == 0) {
		return; // Nothing to write
	}

	// If another thread is uploading this buffer, we can safely return.
	auto uploading = write_buffer->uploading.load();
	if (uploading) {
		return;
	}
	bool can_upload = write_buffer->uploading.compare_exchange_strong(uploading, true);
	if (!can_upload) {
		return;
	}

	file_handle.write_buffers_mutex.lock();
	file_handle.write_buffers.erase(write_buffer->part_no);
	file_handle.write_buffers_mutex.unlock();

	// note that this must be increased synchronously because the caller of this function should be able to use the
	// uploads_in_progress function to wait for all uploads to finish
	file_handle.uploads_in_progress++;

	thread upload_thread(UploadBuffer, std::ref(file_handle), write_buffer);
	upload_thread.detach();
}

// Note that FlushAll currently does not allow to continue writing afterwards. Therefore, FinalizeMultipartUpload should
// be called right after it!
// TODO we can fix this be keeping the last partially written buffer in memory and allow "restarting" it we continueing
// to write to it and overwriting the part in S3. Note that we should update the ETag!
void S3FileSystem::FlushAllBuffers(S3FileHandle &file_handle) {
	std::cout << "Flushing all:" << std::endl;
	// Collect references to all buffers to check
	std::vector<std::shared_ptr<S3WriteBuffer>> to_flush;
	file_handle.write_buffers_mutex.lock();
	for (auto &item : file_handle.write_buffers) {
		to_flush.push_back(item.second);
	}
	file_handle.write_buffers_mutex.unlock();

	// Flush all buffers that aren't already uploading
	for (auto &write_buffer : to_flush) {
		if (!write_buffer->uploading) {
			std::cout << " -> no. " << write_buffer->part_no << std::endl;
			FlushBuffer(file_handle, write_buffer);
		}
	}
	std::cout << "Waiting for uploads to finish" << std::endl;
	std::unique_lock<std::mutex> lck(file_handle.uploads_in_progress_mutex);
	file_handle.uploads_in_progress_cv.wait(lck, [&file_handle] { return file_handle.uploads_in_progress == 0; });
	std::cout << "Uploads finished" << std::endl;
}

void S3FileSystem::FinalizeMultipartUpload(S3FileHandle& file_handle) {
	std::cout << "Finalizing multipart upload" << std::endl;
	auto &s3fs = (S3FileSystem &)file_handle.file_system;

	std::stringstream ss;
	ss << "<CompleteMultipartUpload xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">";

	auto parts = file_handle.parts_uploaded.load();
	for (auto i = 0; i < parts; i++) {
		auto etag_lookup = file_handle.part_etags.find(i);
		if (etag_lookup == file_handle.part_etags.end()) {
			throw std::runtime_error("Unknown part number");
		}
		ss << "<Part><ETag>" << etag_lookup->second << "</ETag><PartNumber>" << i+1 << "</PartNumber></Part>";
	}
	ss << "</CompleteMultipartUpload>";
	string body = ss.str();

	char response_buffer[1000]; // TODO clean up

	string query_param = "?" + uri_encode("uploadId") + "=" + file_handle.multipart_upload_id;
	auto res = s3fs.PostRequest(file_handle, file_handle.path + query_param, {}, response_buffer, 1000, (char*)body.c_str(), body.length());
	string result(response_buffer, 1000);

	auto open_tag_pos = result.find("<CompleteMultipartUploadResult", 0);
	if (open_tag_pos == string::npos) {
		throw std::runtime_error("Unexpected response during S3 multipart upload finalization");
	}
	std::cout << "Finalizing multipart upload complete!" << std::endl;
	file_handle.upload_finalized = true;
}

std::shared_ptr<S3WriteBuffer> S3FileSystem::GetBuffer(S3FileHandle& file_handle, uint16_t write_buffer_idx) {
	// Do lookup of write buffer we need
	file_handle.write_buffers_mutex.lock();
	auto lookup_result = file_handle.write_buffers.find(write_buffer_idx);
	if (lookup_result != file_handle.write_buffers.end()) {
		std::shared_ptr<S3WriteBuffer> buffer = lookup_result->second;
		file_handle.write_buffers_mutex.unlock();
		return buffer;
	}
	file_handle.write_buffers_mutex.unlock();

	// Wait for buffer creation rights
	std::cout << "Waiting for buffer allocation for part " << write_buffer_idx << std::endl;
	std::cout << "Buffers: available: " << file_handle.buffers_available.load() << std::endl;
	{
		std::unique_lock<std::mutex> lck(file_handle.buffers_available_mutex);
		file_handle.buffers_available_cv.wait(lck, [&file_handle] { return file_handle.buffers_available > 0; });
		file_handle.buffers_available--;
	}

	// We're now allowed to allocate a buffer on the idx we want
	auto new_buffer = make_shared<S3WriteBuffer>(write_buffer_idx * S3WriteBuffer::BUFFER_LEN);
	file_handle.write_buffers_mutex.lock();
	lookup_result = file_handle.write_buffers.find(write_buffer_idx);

	// check whether other thread has created the same buffer, if so we return theirs and drop ours.
	if (lookup_result != file_handle.write_buffers.end()) {
		std::cout << "Found same buffer being created, returning found buffer and discarded ours " << write_buffer_idx
		          << std::endl;
		std::shared_ptr<S3WriteBuffer> buffer = lookup_result->second;
		file_handle.write_buffers_mutex.unlock();
		return buffer;
	}

	file_handle.write_buffers.insert(std::pair<uint16_t, std::shared_ptr<S3WriteBuffer>>(write_buffer_idx, new_buffer));
	file_handle.write_buffers_mutex.unlock();
	std::cout << "Allocated buffer for part " << write_buffer_idx << std::endl;
	return new_buffer;
}

void S3FileSystem::S3UrlParse(FileHandle &handle, string url, string &host_out, string &http_host_out, string &path_out,
                              string &query_param) {
	// some URI parsing woo
	if (url.rfind("s3://", 0) != 0) {
		throw std::runtime_error("URL needs to start with s3://");
	}
	auto slash_pos = url.find('/', 5);
	if (slash_pos == std::string::npos) {
		throw std::runtime_error("URL needs to contain a '/' after the host");
	}
	auto bucket = url.substr(5, slash_pos - 5);
	if (bucket.empty()) {
		throw std::runtime_error("URL needs to contain a bucket name");
	}
	auto question_pos = url.find('?', 5);

	if (question_pos == std::string::npos) {
		path_out = url.substr(slash_pos);
		query_param = "";
	} else {
		path_out = url.substr(slash_pos, question_pos - slash_pos);
		query_param = url.substr(question_pos + 1);
	}
	if (path_out.empty()) {
		throw std::runtime_error("URL needs to contain key");
	}

	// actual request
	host_out = bucket + "." + static_cast<S3FileHandle &>(handle).auth_params.endpoint;
	http_host_out = "http://" + host_out; // TODO: restore https
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

unique_ptr<ResponseWrapper> S3FileSystem::PostRequest(FileHandle &handle, string url, HeaderMap header_map,
                                                      char *buffer_out, idx_t buffer_out_len, char *buffer_in,
                                                      idx_t buffer_in_len) {
	string host, http_host, path, query_param;
	S3UrlParse(handle, url, host, http_host, path, query_param);

	auto content_type = "application/octet-stream";
	string query_append = "?" + query_param;
	auto payload_hash = GetPayloadHash(buffer_in, buffer_in_len);

	const auto &auth_params = static_cast<S3FileHandle &>(handle).auth_params;
	auto headers = create_s3_get_header(path, query_param, host, auth_params.region, "s3", "POST",
	                                    auth_params.access_key_id, auth_params.secret_access_key,
	                                    auth_params.session_token, "", "", payload_hash, content_type);
	return HTTPFileSystem::PostRequest(handle, http_host + path + query_append, headers, buffer_out, buffer_out_len,
	                                   buffer_in, buffer_in_len);
}

unique_ptr<ResponseWrapper> S3FileSystem::PutRequest(FileHandle &handle, string url, HeaderMap header_map,
                                                     char *buffer_in, idx_t buffer_in_len) {
	string host, http_host, path, query_param;
	S3UrlParse(handle, url, host, http_host, path, query_param);

	auto content_type = "application/octet-stream";
	string query_append = "?" + query_param;
	auto payload_hash = GetPayloadHash(buffer_in, buffer_in_len);

	const auto &auth_params = static_cast<S3FileHandle &>(handle).auth_params;
	auto headers = create_s3_get_header(path, query_param, host, auth_params.region, "s3", "PUT",
	                                    auth_params.access_key_id, auth_params.secret_access_key,
	                                    auth_params.session_token, "", "", payload_hash, content_type);
	return HTTPFileSystem::PutRequest(handle, http_host + path + query_append, headers, buffer_in, buffer_in_len);
}

unique_ptr<ResponseWrapper> S3FileSystem::HeadRequest(FileHandle &handle, string url, HeaderMap header_map) {
	string host, http_host, path, query_param;
	S3UrlParse(handle, url, host, http_host, path, query_param);

	const auto &auth_params = static_cast<S3FileHandle &>(handle).auth_params;
	auto headers =
	    create_s3_get_header(path, query_param, host, auth_params.region, "s3", "HEAD", auth_params.access_key_id,
	                         auth_params.secret_access_key, auth_params.session_token, "", "", "", "");
	return HTTPFileSystem::HeadRequest(handle, http_host + path, headers);
}

unique_ptr<ResponseWrapper> S3FileSystem::GetRangeRequest(FileHandle &handle, string url, HeaderMap header_map,
                                                          idx_t file_offset, char *buffer_out, idx_t buffer_out_len) {
	string host, http_host, path, query_param;
	S3UrlParse(handle, url, host, http_host, path, query_param);

	const auto &auth_params = static_cast<S3FileHandle &>(handle).auth_params;
	auto headers =
	    create_s3_get_header(path, query_param, host, auth_params.region, "s3", "GET", auth_params.access_key_id,
	                         auth_params.secret_access_key, auth_params.session_token, "", "", "", "");
	return HTTPFileSystem::GetRangeRequest(handle, http_host + path, headers, file_offset, buffer_out, buffer_out_len);
}

std::unique_ptr<HTTPFileHandle> S3FileSystem::CreateHandle(const string &path, uint8_t flags, FileLockType lock,
                                                           FileCompressionType compression, FileOpener *opener) {
	return duckdb::make_unique<S3FileHandle>(*this, path, flags,
	                                         opener ? S3AuthParams::ReadFrom(opener) : S3AuthParams());
}

// this computes the signature from https://czak.pl/2015/09/15/s3-rest-api-with-curl.html
void S3FileSystem::Verify() {
	auto test_header = create_s3_get_header("/", "", "my-precious-bucket.s3.amazonaws.com", "us-east-1", "s3", "GET",
	                                        "AKIAIOSFODNN7EXAMPLE", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY", "",
	                                        "20150915", "20150915T124500Z");
	if (test_header["Authorization"] !=
	    "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20150915/us-east-1/s3/aws4_request, "
	    "SignedHeaders=host;x-amz-content-sha256;x-amz-date, "
	    "Signature=182072eb53d85c36b2d791a1fa46a12d23454ec1e921b02075c23aee40166d5a") {
		throw std::runtime_error("test fail");
	}

	if (uri_encode("/category=Books/") != "/category%3DBooks/") {
		throw std::runtime_error("test fail");
	}
	if (uri_encode("/?category=Books&title=Ducks Retreat/") != "/%3Fcategory%3DBooks%26title%3DDucks%20Retreat/") {
		throw std::runtime_error("test fail");
	}
	if (uri_encode("/?category=Books&title=Ducks Retreat/", true) !=
	    "%2F%3Fcategory%3DBooks%26title%3DDucks%20Retreat%2F") {
		throw std::runtime_error("test fail");
	}
	// AWS_SECRET_ACCESS_KEY="vs1BZPxSL2qVARBSg5vCMKJsavCoEPlo/HSHRaVe" AWS_ACCESS_KEY_ID="ASIAYSPIOYDTHTBIITVC"
	// AWS_SESSION_TOKEN="IQoJb3JpZ2luX2VjENX//////////wEaCWV1LXdlc3QtMSJHMEUCIQDfjzs9BYHrEXDMU/NR+PHV1uSTr7CSVSQdjKSfiPRLdgIgCCztF0VMbi9+uHHAfBVKhV4t9MlUrQg3VAOIsLxrWyoqlAIIHRAAGgw1ODk0MzQ4OTY2MTQiDOGl2DsYxENcKCbh+irxARe91faI+hwUhT60sMGRFg0GWefKnPclH4uRFzczrDOcJlAAaQRJ7KOsT8BrJlrY1jSgjkO7PkVjPp92vi6lJX77bg99MkUTJActiOKmd84XvAE5bFc/jFbqechtBjXzopAPkKsGuaqAhCenXnFt6cwq+LZikv/NJGVw7TRphLV+Aq9PSL9XwdzIgsW2qXwe1c3rxDNj53yStRZHVggdxJ0OgHx5v040c98gFphzSULHyg0OY6wmCMTYcswpb4kO2IIi6AiD9cY25TlwPKRKPi5CdBsTPnyTeW62u7PvwK0fTSy4ZuJUuGKQnH2cKmCXquEwoOHEiQY6nQH9fzY/EDGHMRxWWhxu0HiqIfsuFqC7GS0p0ToKQE+pzNsvVwMjZc+KILIDDQpdCWRIwu53I5PZy2Cvk+3y4XLvdZKQCsAKqeOc4c94UAS4NmUT7mCDOuRV0cLBVM8F0JYBGrUxyI+YoIvHhQWmnRLuKgTb5PkF7ZWrXBHFWG5/tZDOvBbbaCWTlRCL9b0Vpg5+BM/81xd8jChP4w83"
	// aws --region eu-west-1 --debug s3 ls my-precious-bucket 2>&1 | less
	std::string canonical_query_string = "delimiter=%2F&encoding-type=url&list-type=2&prefix="; // aws s3 ls <bucket>
	auto test_header2 =
	    create_s3_get_header("/", canonical_query_string, "my-precious-bucket.s3.eu-west-1.amazonaws.com", "eu-west-1",
	                         "s3", "GET", "ASIAYSPIOYDTHTBIITVC", "vs1BZPxSL2qVARBSg5vCMKJsavCoEPlo/HSHRaVe",
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
	                         "20210904", "20210904T121746Z");
	if (test_header2["Authorization"] !=
	    "AWS4-HMAC-SHA256 Credential=ASIAYSPIOYDTHTBIITVC/20210904/eu-west-1/s3/aws4_request, "
	    "SignedHeaders=host;x-amz-content-sha256;x-amz-date;x-amz-security-token, "
	    "Signature=4d9d6b59d7836b6485f6ad822de97be40287da30347d83042ea7fbed530dc4c0") {
		throw std::runtime_error("test fail");
	}

	auto test_header3 = create_s3_get_header(
	    "/correct_auth_test.csv", "", "test-bucket-ceiveran.s3.amazonaws.com", "eu-west-1", "s3", "PUT", "S3RVER",
	    "S3RVER", "", "20220121", "20220121T141452Z",
	    "28a0cf6ac5c4cb73793091fe6ecc6a68bf90855ac9186158748158f50241bb0c", "text/data;charset=utf-8");
	if (test_header3["Authorization"] != "AWS4-HMAC-SHA256 Credential=S3RVER/20220121/eu-west-1/s3/aws4_request, SignedHeaders=content-type;host;x-amz-content-sha256;x-amz-date, Signature=5d9a6cbfaa78a6d0f2ab7df0445e2f1cc9c80cd3655ac7de9e7219c036f23f02") {
		throw std::runtime_error("test3 fail");
	}

	if (uri_encode("/category=Books/") != "/category%3DBooks/") {
		throw std::runtime_error("test fail");
	}
	if (uri_encode("/?category=Books&title=Ducks Retreat/") != "/%3Fcategory%3DBooks%26title%3DDucks%20Retreat/") {
		throw std::runtime_error("test fail");
	}
	if (uri_encode("/?category=Books&title=Ducks Retreat/", true) !=
	    "%2F%3Fcategory%3DBooks%26title%3DDucks%20Retreat%2F") {
		throw std::runtime_error("test fail");
	}
}

unique_ptr<ResponseWrapper> S3FileHandle::Initialize() {
	auto res = HTTPFileHandle::Initialize();

	auto &s3fs = (S3FileSystem &)file_system;

	if (flags & FileFlags::FILE_FLAGS_WRITE) {
		// TODO pre-allocated upload buffers?
		multipart_upload_id = s3fs.InitializeMultipartUpload(*this);
		buffers_available = WRITE_MAX_UPLOADS;
		uploads_in_progress =  0;
		parts_uploaded = 0;
		upload_finalized = false;
	}

	return res;
}

bool S3FileSystem::CanHandleFile(const string &fpath) {
	return fpath.rfind("s3://", 0) == 0;
}

void S3FileSystem::FileSync(FileHandle &handle) {
	std::cout << "FileSync" << "\n";
	auto &s3fh = (S3FileHandle &)handle;
	if (!s3fh.upload_finalized){
		FlushAllBuffers(s3fh);
		FinalizeMultipartUpload(s3fh);
	}
}

void S3FileSystem::Write(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) {
	auto &s3fh = (S3FileHandle &)handle;
	if (!(s3fh.flags & FileFlags::FILE_FLAGS_WRITE)){
		throw InternalException("Write called on file not opened in write mode");
	}

	// TODO: What will happen when we block on writing beyond curr_offset + MAX_MEMORY_USAGE?
	// TODO: also non-sequential writes? At least we should ensure we fail nicely?
	int64_t bytes_written = 0;

	while(bytes_written < nr_bytes) {
		auto curr_location = location + bytes_written;
		D_ASSERT(curr_location == s3fh.file_offset); // TODO non-sequential writes?
		// Find buffer for writing
		auto write_buffer_idx = curr_location / S3WriteBuffer::BUFFER_LEN;

		// Get write buffer, may block until buffer is available
		auto write_buffer = GetBuffer(s3fh, write_buffer_idx);

		// Writing to buffer
		auto idx_to_write = curr_location - write_buffer->buffer_start;
		if (idx_to_write != write_buffer->idx) {
			throw InternalException("Non-sequential write not supported!");
		}
		auto bytes_to_write = MinValue<idx_t>(nr_bytes - bytes_written, S3WriteBuffer::BUFFER_LEN - idx_to_write);
		memcpy(write_buffer->buffer.get() + idx_to_write, (char*)buffer + bytes_written, bytes_to_write);
		write_buffer->idx += bytes_to_write;

		// Flush to HTTP if full
		if (write_buffer->idx >= S3WriteBuffer::BUFFER_LEN){
			std::cout << "Buffer full: " << write_buffer->part_no << std::endl;
			FlushBuffer(s3fh, write_buffer);
		}
		s3fh.file_offset += bytes_to_write;
		bytes_written += bytes_to_write;
	}
}

}