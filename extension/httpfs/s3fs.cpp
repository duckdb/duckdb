#include "s3fs.hpp"
#include "crypto.hpp"
#include "duckdb.hpp"
#ifndef DUCKDB_AMALGAMATION
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/thread.hpp"
#include "duckdb/function/scalar/strftime.hpp"
#endif

#include <chrono>
#include <duckdb/storage/buffer_manager.hpp>
#include <iostream>
#include <thread>

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

static HeaderMap create_s3_get_header(std::string url, std::string query, std::string host, std::string service,
                                      std::string method, const S3AuthParams &auth_params, std::string date_now = "",
                                      std::string datetime_now = "", std::string payload_hash = "",
                                      std::string content_type = "") {

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
	auto canonical_request = method + "\n" + uri_encode(url) + "\n" + query;
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

S3ConfigParams S3ConfigParams::ReadFrom(FileOpener *opener) {
	uint64_t uploader_max_filesize;
	uint64_t max_parts_per_file;
	uint64_t part_upload_timeout;
	uint64_t max_upload_threads;
	Value value;

	if (opener->TryGetCurrentSetting("s3_uploader_max_filesize", value)) {
		uploader_max_filesize = DBConfig::ParseMemoryLimit(value.GetValue<string>());
	} else {
		uploader_max_filesize = 400000000000; // 400GB
	}

	if (opener->TryGetCurrentSetting("s3_uploader_max_parts_per_file", value)) {
		max_parts_per_file = value.GetValue<uint64_t>();
	} else {
		max_parts_per_file = 10000; // AWS Default
	}

	if (opener->TryGetCurrentSetting("s3_uploader_timeout", value)) {
		part_upload_timeout = value.GetValue<uint64_t>();
	} else {
		part_upload_timeout = 30000; // 30 seconds
	}

	if (opener->TryGetCurrentSetting("s3_uploader_thread_limit", value)) {
		max_upload_threads = value.GetValue<uint64_t>();
	} else {
		max_upload_threads = 100;
	}

	return {uploader_max_filesize, max_parts_per_file, part_upload_timeout, max_upload_threads};
}

void S3FileHandle::Close() {
	// std::cout << "Closing handle" << "\n";
	auto &s3fs = (S3FileSystem &)file_system;
	if ((flags & FileFlags::FILE_FLAGS_WRITE) && !upload_finalized) {
		s3fs.FlushAllBuffers(*this);
		s3fs.FinalizeMultipartUpload(*this);
	}
}

// Opens the multipart upload and returns the ID
string S3FileSystem::InitializeMultipartUpload(S3FileHandle &file_handle) {
	auto &s3fs = (S3FileSystem &)file_handle.file_system;

	// AWS response is around 300~ chars in docs so this should be enough to not need a resize
	idx_t response_buffer_len = 1000;
	auto response_buffer = unique_ptr<char[]> {new char[response_buffer_len]};

	string query_param = "?" + uri_encode("uploads") + "=";
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

	string query_param = uri_encode("partNumber") + "=" + to_string(write_buffer->part_no + 1) + "&" +
	                     uri_encode("uploadId") + "=" + uri_encode(file_handle.multipart_upload_id, true);
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
		if ((uint64_t)(current_time - time_at_start) > file_handle.config_params.part_upload_timeout) {
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
	// std::cout << "[END] buffers_available: " << s3fs.buffers_available.load() << " parts_uploaded: " <<
	// file_handle.parts_uploaded.load() << " uploads in progress: " << file_handle.uploads_in_progress.load() <<
	// std::endl;
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
// be called right after it! TODO: we can fix this be keeping the last partially written buffer in memory and allow
//  "restarting" it we continueing
void S3FileSystem::FlushAllBuffers(S3FileHandle &file_handle) {
	// std::cout << "Flushing all:" << std::endl;
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
			// std::cout << " -> no. " << write_buffer->part_no << std::endl;
			FlushBuffer(file_handle, write_buffer);
		}
	}
	// std::cout << "Waiting for uploads to finish" << std::endl;
	std::unique_lock<std::mutex> lck(file_handle.uploads_in_progress_lock);
	file_handle.uploads_in_progress_cv.wait(lck,
	                                        [&file_handle] { return file_handle.uploads_in_progress.load() == 0; });
	// std::cout << "Uploads finished" << std::endl;
}

void S3FileSystem::FinalizeMultipartUpload(S3FileHandle &file_handle) {
	// std::cout << "Finalizing multipart upload" << std::endl;
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

	// std::cout << body << "\n";

	// Response is around ~400 in AWS docs so this should be enough to not need a resize
	idx_t response_buffer_len = 1000;
	auto response_buffer = unique_ptr<char[]> {new char[response_buffer_len]};

	string query_param = "?" + uri_encode("uploadId") + "=" + file_handle.multipart_upload_id;
	auto res = s3fs.PostRequest(file_handle, file_handle.path + query_param, {}, response_buffer, response_buffer_len,
	                            (char *)body.c_str(), body.length());
	string result(response_buffer.get(), response_buffer_len);

	auto open_tag_pos = result.find("<CompleteMultipartUploadResult", 0);
	if (open_tag_pos == string::npos) {
		throw std::runtime_error("Unexpected response during S3 multipart upload finalization");
	}
	// std::cout << "Finalizing multipart upload complete!" << std::endl;
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
	// std::cout << "Waiting for buffer allocation for part " << write_buffer_idx << std::endl;
	// std::cout << "Buffers: available: " << s3fs.buffers_available.load() << std::endl;
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
			//			//std::cout << "Allocating duckdb memory...\n";
			duckdb_buffer = buffer_manager.Allocate(file_handle.part_size);
			// std::cout << "Allocating duckdb memory complete\n";

			if (set_waiting_for_memory) {
				threads_waiting_for_memory--;
			}
			break;
		} catch (OutOfMemoryException &e) {
			if (!set_waiting_for_memory) {
				threads_waiting_for_memory++;
				set_waiting_for_memory = true;
			}
			//			//std::cout << "DuckDB out of memory, we're waiting for buffers to become available\n";
			auto buffers_available = s3fs.buffers_available.load();

			if (buffers_available >= file_handle.config_params.max_upload_threads - threads_waiting_for_memory) {
				//				//std::cout << "avail: " << buffers_available << " total: " <<
				//file_handle.config_params.max_upload_threads << " waiting " <<  threads_waiting_for_memory <<
				//std::endl;
				// There exist no upload write buffers that can release more memory. We really ran out of memory here.
				throw e;
			} else {

				// Wait for more buffers to become available before trying again
				{
					std::unique_lock<std::mutex> lck(s3fs.buffers_available_lock);
					s3fs.buffers_available_cv.wait(
					    lck, [&s3fs, &buffers_available] { return s3fs.buffers_available > buffers_available; });
					//					//std::cout << "A buffer just became available, rechecking!\n";
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
			// std::cout << "Found same buffer being created, returning found buffer and discarded ours " <<
			// write_buffer_idx << std::endl;
			std::shared_ptr<S3WriteBuffer> write_buffer = lookup_result->second;
			file_handle.write_buffers_lock.unlock();
			return write_buffer;
		}
		file_handle.write_buffers.insert(
		    std::pair<uint16_t, std::shared_ptr<S3WriteBuffer>>(write_buffer_idx, new_write_buffer));
	}

	// std::cout << "Allocated buffer for part " << write_buffer_idx << std::endl;
	return new_write_buffer;
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

	auto endpoint = static_cast<S3FileHandle &>(handle).auth_params.endpoint;

	// Endpoint can be speficied as full url in which case we switch to: {endpoint}/{bucket} for host
	// This is mostly usefull in testing to specify a localhost address
	if (endpoint.rfind("http://", 0) == 0 || endpoint.rfind("https://", 0) == 0) {
		http_host_out = endpoint + "/" + bucket;
		auto url_start = http_host_out.rfind("://") + 3;
		host_out = http_host_out.substr(url_start);
	} else {
		// Endpoint is not a full url and the regular https://{bucket}.{domain} format will be used
		// actual request
		host_out = bucket + "." + endpoint;
		http_host_out = "http://" + host_out;
	}
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
                                                      unique_ptr<char[]> &buffer_out, idx_t &buffer_out_len,
                                                      char *buffer_in, idx_t buffer_in_len) {
	string host, http_host, path, query_param;
	S3UrlParse(handle, url, host, http_host, path, query_param);
	string full_url = http_host + path + "?" + query_param;

	auto payload_hash = GetPayloadHash(buffer_in, buffer_in_len);

	auto headers =
	    create_s3_get_header(path, query_param, host, "s3", "POST", static_cast<S3FileHandle &>(handle).auth_params, "",
	                         "", payload_hash, "application/octet-stream");

	return HTTPFileSystem::PostRequest(handle, full_url, headers, buffer_out, buffer_out_len, buffer_in, buffer_in_len);
}

unique_ptr<ResponseWrapper> S3FileSystem::PutRequest(FileHandle &handle, string url, HeaderMap header_map,
                                                     char *buffer_in, idx_t buffer_in_len) {
	string host, http_host, path, query_param;
	S3UrlParse(handle, url, host, http_host, path, query_param);

	auto content_type = "application/octet-stream";
	string query_append = "?" + query_param;
	auto payload_hash = GetPayloadHash(buffer_in, buffer_in_len);

	auto headers =
	    create_s3_get_header(path, query_param, host, "s3", "PUT", static_cast<S3FileHandle &>(handle).auth_params, "",
	                         "", payload_hash, content_type);
	return HTTPFileSystem::PutRequest(handle, http_host + path + query_append, headers, buffer_in, buffer_in_len);
}

unique_ptr<ResponseWrapper> S3FileSystem::HeadRequest(FileHandle &handle, string url, HeaderMap header_map) {
	string host, http_host, path, query_param;
	S3UrlParse(handle, url, host, http_host, path, query_param);
	auto headers = create_s3_get_header(path, query_param, host, "s3", "HEAD",
	                                    static_cast<S3FileHandle &>(handle).auth_params, "", "", "", "");
	return HTTPFileSystem::HeadRequest(handle, http_host + path, headers);
}

unique_ptr<ResponseWrapper> S3FileSystem::GetRangeRequest(FileHandle &handle, string url, HeaderMap header_map,
                                                          idx_t file_offset, char *buffer_out, idx_t buffer_out_len) {
	string host, http_host, path, query_param;
	S3UrlParse(handle, url, host, http_host, path, query_param);
	auto headers = create_s3_get_header(path, query_param, host, "s3", "GET",
	                                    static_cast<S3FileHandle &>(handle).auth_params, "", "", "", "");
	return HTTPFileSystem::GetRangeRequest(handle, http_host + path, headers, file_offset, buffer_out, buffer_out_len);
}

std::unique_ptr<HTTPFileHandle> S3FileSystem::CreateHandle(const string &path, uint8_t flags, FileLockType lock,
                                                           FileCompressionType compression, FileOpener *opener) {
	return duckdb::make_unique<S3FileHandle>(*this, path, flags,
	                                         opener ? S3AuthParams::ReadFrom(opener) : S3AuthParams(),
	                                         opener ? S3ConfigParams::ReadFrom(opener) : S3ConfigParams());
}

// this computes the signature from https://czak.pl/2015/09/15/s3-rest-api-with-curl.html
void S3FileSystem::Verify() {

	S3AuthParams auth_params = {"us-east-1", "AKIAIOSFODNN7EXAMPLE", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY", "",
	                            ""};
	auto test_header = create_s3_get_header("/", "", "my-precious-bucket.s3.amazonaws.com", "s3", "GET", auth_params,
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

	S3AuthParams auth_params2 = {
	    "eu-west-1", "ASIAYSPIOYDTHTBIITVC", "vs1BZPxSL2qVARBSg5vCMKJsavCoEPlo/HSHRaVe",
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
	    ""};
	auto test_header2 =
	    create_s3_get_header("/", canonical_query_string, "my-precious-bucket.s3.eu-west-1.amazonaws.com", "s3", "GET",
	                         auth_params2, "20210904", "20210904T121746Z");
	if (test_header2["Authorization"] !=
	    "AWS4-HMAC-SHA256 Credential=ASIAYSPIOYDTHTBIITVC/20210904/eu-west-1/s3/aws4_request, "
	    "SignedHeaders=host;x-amz-content-sha256;x-amz-date;x-amz-security-token, "
	    "Signature=4d9d6b59d7836b6485f6ad822de97be40287da30347d83042ea7fbed530dc4c0") {
		throw std::runtime_error("test fail");
	}

	S3AuthParams auth_params3 = {"eu-west-1", "S3RVER", "S3RVER", "", ""};
	auto test_header3 = create_s3_get_header("/correct_auth_test.csv", "", "test-bucket-ceiveran.s3.amazonaws.com",
	                                         "s3", "PUT", auth_params3, "20220121", "20220121T141452Z",
	                                         "28a0cf6ac5c4cb73793091fe6ecc6a68bf90855ac9186158748158f50241bb0c",
	                                         "text/data;charset=utf-8");
	if (test_header3["Authorization"] != "AWS4-HMAC-SHA256 Credential=S3RVER/20220121/eu-west-1/s3/aws4_request, "
	                                     "SignedHeaders=content-type;host;x-amz-content-sha256;x-amz-date, "
	                                     "Signature=5d9a6cbfaa78a6d0f2ab7df0445e2f1cc9c80cd3655ac7de9e7219c036f23f02") {
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

		// std::cout << "require part size: " << required_part_size << "\n";
		// std::cout << "chosen part size: " << part_size << "\n";
		// std::cout << "max_uploads: " << config_params.max_upload_threads << "\n";
		// std::cout << "max_buffers: " << s3fs.buffers_available << "\n";
	}

	return res;
}

bool S3FileSystem::CanHandleFile(const string &fpath) {
	return fpath.rfind("s3://", 0) == 0;
}

void S3FileSystem::FileSync(FileHandle &handle) {
	// std::cout << "FileSync" << "\n";
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
			// std::cout << "Buffer full: " << write_buffer->part_no << std::endl;
			FlushBuffer(s3fh, write_buffer);
		}
		s3fh.file_offset += bytes_to_write;
		bytes_written += bytes_to_write;
	}
}

} // namespace duckdb