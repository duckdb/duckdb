#include "s3fs.hpp"

#include "crypto.hpp"
#include "duckdb.hpp"
#ifndef DUCKDB_AMALGAMATION
#include "duckdb/common/http_state.hpp"
#include "duckdb/common/thread.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/function/scalar/strftime_format.hpp"
#endif

#include <duckdb/function/scalar/string_functions.hpp>
#include <duckdb/storage/buffer_manager.hpp>
#include <iostream>
#include <thread>

namespace duckdb {

static HeaderMap create_s3_header(string url, string query, string host, string service, string method,
                                  const S3AuthParams &auth_params, string date_now = "", string datetime_now = "",
                                  string payload_hash = "", string content_type = "") {

	HeaderMap res;
	res["Host"] = host;
	// If access key is not set, we don't set the headers at all to allow accessing public files through s3 urls
	if (auth_params.secret_access_key.empty() && auth_params.access_key_id.empty()) {
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

	string signed_headers = "";
	hash_bytes canonical_request_hash;
	hash_str canonical_request_hash_str;
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
	                      service + "/aws4_request\n" + string((char *)canonical_request_hash_str, sizeof(hash_str));
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
	                       ", Signature=" + string((char *)signature_str, sizeof(hash_str));

	return res;
}

static duckdb::unique_ptr<duckdb_httplib_openssl::Headers> initialize_http_headers(HeaderMap &header_map) {
	auto headers = make_uniq<duckdb_httplib_openssl::Headers>();
	for (auto &entry : header_map) {
		headers->insert(entry);
	}
	return headers;
}

string S3FileSystem::UrlDecode(string input) {
	string result;
	result.reserve(input.size());
	char ch;
	replace(input.begin(), input.end(), '+', ' ');
	for (idx_t i = 0; i < input.length(); i++) {
		if (int(input[i]) == 37) {
			unsigned int ii;
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

string S3FileSystem::UrlEncode(const string &input, bool encode_slash) {
	// https://docs.aws.amazon.com/AmazonS3/latest/API/sigv4-query-string-auth.html
	static const char *hex_digit = "0123456789ABCDEF";
	string result;
	result.reserve(input.size());
	for (idx_t i = 0; i < input.length(); i++) {
		char ch = input[i];
		if ((ch >= 'A' && ch <= 'Z') || (ch >= 'a' && ch <= 'z') || (ch >= '0' && ch <= '9') || ch == '_' ||
		    ch == '-' || ch == '~' || ch == '.') {
			result += ch;
		} else if (ch == '/') {
			if (encode_slash) {
				result += string("%2F");
			} else {
				result += ch;
			}
		} else {
			result += string("%");
			result += hex_digit[static_cast<unsigned char>(ch) >> 4];
			result += hex_digit[static_cast<unsigned char>(ch) & 15];
		}
	}
	return result;
}

void AWSEnvironmentCredentialsProvider::SetExtensionOptionValue(string key, const char *env_var_name) {
	static char *evar;

	if ((evar = std::getenv(env_var_name)) != NULL) {
		if (StringUtil::Lower(evar) == "false") {
			this->config.SetOption(key, Value(false));
		} else if (StringUtil::Lower(evar) == "true") {
			this->config.SetOption(key, Value(true));
		} else {
			this->config.SetOption(key, Value(evar));
		}
	}
}

void AWSEnvironmentCredentialsProvider::SetAll() {
	this->SetExtensionOptionValue("s3_region", this->DEFAULT_REGION_ENV_VAR);
	this->SetExtensionOptionValue("s3_region", this->REGION_ENV_VAR);
	this->SetExtensionOptionValue("s3_access_key_id", this->ACCESS_KEY_ENV_VAR);
	this->SetExtensionOptionValue("s3_secret_access_key", this->SECRET_KEY_ENV_VAR);
	this->SetExtensionOptionValue("s3_session_token", this->SESSION_TOKEN_ENV_VAR);
	this->SetExtensionOptionValue("s3_endpoint", this->DUCKDB_ENDPOINT_ENV_VAR);
	this->SetExtensionOptionValue("s3_use_ssl", this->DUCKDB_USE_SSL_ENV_VAR);
}

S3AuthParams S3AuthParams::ReadFrom(FileOpener *opener, FileOpenerInfo &info) {
	string region;
	string access_key_id;
	string secret_access_key;
	string session_token;
	string endpoint;
	string url_style;
	bool s3_url_compatibility_mode;
	bool use_ssl;
	Value value;

	if (FileOpener::TryGetCurrentSetting(opener, "s3_region", value, info)) {
		region = value.ToString();
	}

	if (FileOpener::TryGetCurrentSetting(opener, "s3_access_key_id", value, info)) {
		access_key_id = value.ToString();
	}

	if (FileOpener::TryGetCurrentSetting(opener, "s3_secret_access_key", value, info)) {
		secret_access_key = value.ToString();
	}

	if (FileOpener::TryGetCurrentSetting(opener, "s3_session_token", value, info)) {
		session_token = value.ToString();
	}

	if (FileOpener::TryGetCurrentSetting(opener, "s3_endpoint", value, info)) {
		endpoint = value.ToString();
	} else {
		endpoint = "s3.amazonaws.com";
	}

	if (FileOpener::TryGetCurrentSetting(opener, "s3_url_style", value, info)) {
		auto val_str = value.ToString();
		if (!(val_str == "vhost" || val_str != "path" || val_str != "")) {
			throw std::runtime_error(
			    "Incorrect setting found for s3_url_style, allowed values are: 'path' and 'vhost'");
		}
		url_style = val_str;
	} else {
		url_style = "vhost";
	}

	if (FileOpener::TryGetCurrentSetting(opener, "s3_use_ssl", value, info)) {
		use_ssl = value.GetValue<bool>();
	} else {
		use_ssl = true;
	}

	if (FileOpener::TryGetCurrentSetting(opener, "s3_url_compatibility_mode", value, info)) {
		s3_url_compatibility_mode = value.GetValue<bool>();
	} else {
		s3_url_compatibility_mode = true;
	}

	return {region,   access_key_id, secret_access_key, session_token,
	        endpoint, url_style,     use_ssl,           s3_url_compatibility_mode};
}

S3ConfigParams S3ConfigParams::ReadFrom(FileOpener *opener) {
	uint64_t uploader_max_filesize;
	uint64_t max_parts_per_file;
	uint64_t max_upload_threads;
	Value value;

	if (FileOpener::TryGetCurrentSetting(opener, "s3_uploader_max_filesize", value)) {
		uploader_max_filesize = DBConfig::ParseMemoryLimit(value.GetValue<string>());
	} else {
		uploader_max_filesize = S3ConfigParams::DEFAULT_MAX_FILESIZE;
	}

	if (FileOpener::TryGetCurrentSetting(opener, "s3_uploader_max_parts_per_file", value)) {
		max_parts_per_file = value.GetValue<uint64_t>();
	} else {
		max_parts_per_file = S3ConfigParams::DEFAULT_MAX_PARTS_PER_FILE; // AWS Default
	}

	if (FileOpener::TryGetCurrentSetting(opener, "s3_uploader_thread_limit", value)) {
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
	auto response_buffer = duckdb::unique_ptr<char[]> {new char[response_buffer_len]};

	string query_param = "uploads=";
	auto res = s3fs.PostRequest(file_handle, file_handle.path, {}, response_buffer, response_buffer_len, nullptr, 0,
	                            query_param);
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

	string query_param = "partNumber=" + to_string(write_buffer->part_no + 1) + "&" +
	                     "uploadId=" + S3FileSystem::UrlEncode(file_handle.multipart_upload_id, true);
	unique_ptr<ResponseWrapper> res;
	case_insensitive_map_t<string>::iterator etag_lookup;

	try {
		res = s3fs.PutRequest(file_handle, file_handle.path, {}, (char *)write_buffer->Ptr(), write_buffer->idx,
		                      query_param);

		if (res->code != 200) {
			throw HTTPException(*res, "Unable to connect to URL %s %s (HTTP code %s)", res->http_url, res->error,
			                    to_string(res->code));
		}

		etag_lookup = res->headers.find("ETag");
		if (etag_lookup == res->headers.end()) {
			throw IOException("Unexpected response when uploading part to S3");
		}

	} catch (IOException &ex) {
		// Ensure only one thread sets the exception
		bool f = false;
		auto exchanged = file_handle.uploader_has_error.compare_exchange_strong(f, true);
		if (exchanged) {
			file_handle.upload_exception = std::current_exception();
		}

		{
			unique_lock<mutex> lck(file_handle.uploads_in_progress_lock);
			file_handle.uploads_in_progress--;
		}
		file_handle.uploads_in_progress_cv.notify_one();

		return;
	}

	// Insert etag
	{
		unique_lock<mutex> lck(file_handle.part_etags_lock);
		file_handle.part_etags.insert(std::pair<uint16_t, string>(write_buffer->part_no, etag_lookup->second));
	}

	file_handle.parts_uploaded++;

	// Free up space for another thread to acquire an S3WriteBuffer
	write_buffer.reset();

	// Signal a buffer has become available
	{
		unique_lock<mutex> lck(s3fs.buffers_available_lock);
		s3fs.buffers_in_use--;
	}
	s3fs.buffers_available_cv.notify_one();

	// Signal a thread has finished
	{
		unique_lock<mutex> lck(file_handle.uploads_in_progress_lock);
		file_handle.uploads_in_progress--;
	}
	file_handle.uploads_in_progress_cv.notify_one();
}

void S3FileSystem::FlushBuffer(S3FileHandle &file_handle, shared_ptr<S3WriteBuffer> write_buffer) {

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

	file_handle.RethrowIOError();

	{
		unique_lock<mutex> lck(file_handle.write_buffers_lock);
		file_handle.write_buffers.erase(write_buffer->part_no);
	}

	{
		unique_lock<mutex> lck(file_handle.uploads_in_progress_lock);
		file_handle.uploads_in_progress++;
	}

	thread upload_thread(UploadBuffer, std::ref(file_handle), write_buffer);
	upload_thread.detach();
}

// Note that FlushAll currently does not allow to continue writing afterwards. Therefore, FinalizeMultipartUpload should
// be called right after it!
// TODO: we can fix this by keeping the last partially written buffer in memory and allow reuploading it with new data.
void S3FileSystem::FlushAllBuffers(S3FileHandle &file_handle) {
	//  Collect references to all buffers to check
	vector<shared_ptr<S3WriteBuffer>> to_flush;
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
	unique_lock<mutex> lck(file_handle.uploads_in_progress_lock);
	file_handle.uploads_in_progress_cv.wait(lck, [&file_handle] { return file_handle.uploads_in_progress == 0; });

	file_handle.RethrowIOError();
}

void S3FileSystem::FinalizeMultipartUpload(S3FileHandle &file_handle) {
	auto &s3fs = (S3FileSystem &)file_handle.file_system;

	std::stringstream ss;
	ss << "<CompleteMultipartUpload xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">";

	auto parts = file_handle.parts_uploaded.load();
	for (auto i = 0; i < parts; i++) {
		auto etag_lookup = file_handle.part_etags.find(i);
		if (etag_lookup == file_handle.part_etags.end()) {
			throw IOException("Unknown part number");
		}
		ss << "<Part><ETag>" << etag_lookup->second << "</ETag><PartNumber>" << i + 1 << "</PartNumber></Part>";
	}
	ss << "</CompleteMultipartUpload>";
	string body = ss.str();

	// Response is around ~400 in AWS docs so this should be enough to not need a resize
	idx_t response_buffer_len = 1000;
	auto response_buffer = duckdb::unique_ptr<char[]> {new char[response_buffer_len]};

	string query_param = "uploadId=" + S3FileSystem::UrlEncode(file_handle.multipart_upload_id, true);
	auto res = s3fs.PostRequest(file_handle, file_handle.path, {}, response_buffer, response_buffer_len,
	                            (char *)body.c_str(), body.length(), query_param);
	string result(response_buffer.get(), response_buffer_len);

	auto open_tag_pos = result.find("<CompleteMultipartUploadResult", 0);
	if (open_tag_pos == string::npos) {
		throw HTTPException(*res, "Unexpected response during S3 multipart upload finalization: %d", res->code);
	}
	file_handle.upload_finalized = true;
}

// Wrapper around the BufferManager::Allocate to that allows limiting the number of buffers that will be handed out
BufferHandle S3FileSystem::Allocate(idx_t part_size, uint16_t max_threads) {
	unique_lock<mutex> lck(buffers_available_lock);

	// Wait for a buffer to become available
	if (buffers_in_use + threads_waiting_for_memory >= max_threads) {
		buffers_available_cv.wait(lck, [&] { return buffers_in_use + threads_waiting_for_memory < max_threads; });
	}
	buffers_in_use++;

	// Try to allocate a buffer from the buffer manager
	BufferHandle duckdb_buffer;
	bool set_waiting_for_memory = false;

	while (true) {
		try {
			duckdb_buffer = buffer_manager.Allocate(part_size);

			if (set_waiting_for_memory) {
				threads_waiting_for_memory--;
			}
			break;
		} catch (OutOfMemoryException &e) {
			if (!set_waiting_for_memory) {
				threads_waiting_for_memory++;
				set_waiting_for_memory = true;
			}

			auto currently_in_use = buffers_in_use;
			if (currently_in_use == 0) {
				// There exist no upload write buffers that can release more memory. We really ran out of memory here.
				throw;
			} else {
				// Wait for more buffers to become available before trying again
				buffers_available_cv.wait(lck, [&] { return buffers_in_use < currently_in_use; });
			}
		}
	}

	return duckdb_buffer;
}

shared_ptr<S3WriteBuffer> S3FileHandle::GetBuffer(uint16_t write_buffer_idx) {
	auto &s3fs = (S3FileSystem &)file_system;

	// Check if write buffer already exists
	{
		unique_lock<mutex> lck(write_buffers_lock);
		auto lookup_result = write_buffers.find(write_buffer_idx);
		if (lookup_result != write_buffers.end()) {
			shared_ptr<S3WriteBuffer> buffer = lookup_result->second;
			return buffer;
		}
	}

	auto buffer_handle = s3fs.Allocate(part_size, config_params.max_upload_threads);
	auto new_write_buffer =
	    make_shared<S3WriteBuffer>(write_buffer_idx * part_size, part_size, std::move(buffer_handle));
	{
		unique_lock<mutex> lck(write_buffers_lock);
		auto lookup_result = write_buffers.find(write_buffer_idx);

		// Check if other thread has created the same buffer, if so we return theirs and drop ours.
		if (lookup_result != write_buffers.end()) {
			// write_buffer_idx << std::endl;
			shared_ptr<S3WriteBuffer> write_buffer = lookup_result->second;
			return write_buffer;
		}
		write_buffers.insert(pair<uint16_t, shared_ptr<S3WriteBuffer>>(write_buffer_idx, new_write_buffer));
	}

	return new_write_buffer;
}

void S3FileSystem::GetQueryParam(const string &key, string &param, duckdb_httplib_openssl::Params &query_params) {
	auto found_param = query_params.find(key);
	if (found_param != query_params.end()) {
		param = found_param->second;
		query_params.erase(found_param);
	}
}

void S3FileSystem::ReadQueryParams(const string &url_query_param, S3AuthParams &params) {
	if (url_query_param.empty()) {
		return;
	}

	duckdb_httplib_openssl::Params query_params;
	duckdb_httplib_openssl::detail::parse_query_text(url_query_param, query_params);

	GetQueryParam("s3_region", params.region, query_params);
	GetQueryParam("s3_access_key_id", params.access_key_id, query_params);
	GetQueryParam("s3_secret_access_key", params.secret_access_key, query_params);
	GetQueryParam("s3_session_token", params.session_token, query_params);
	GetQueryParam("s3_endpoint", params.endpoint, query_params);
	GetQueryParam("s3_url_style", params.url_style, query_params);
	auto found_param = query_params.find("s3_use_ssl");
	if (found_param != query_params.end()) {
		if (found_param->second == "true") {
			params.use_ssl = true;
		} else if (found_param->second == "false") {
			params.use_ssl = false;
		} else {
			throw IOException("Incorrect setting found for s3_use_ssl, allowed values are: 'true' or 'false'");
		}
		query_params.erase(found_param);
	}
	if (!query_params.empty()) {
		throw IOException("Invalid query parameters found. Supported parameters are:\n's3_region', 's3_access_key_id', "
		                  "'s3_secret_access_key', 's3_session_token',\n's3_endpoint', 's3_url_style', 's3_use_ssl'");
	}
}

ParsedS3Url S3FileSystem::S3UrlParse(string url, S3AuthParams &params) {
	string http_proto, host, bucket, path, query_param, trimmed_s3_url;

	if (url.rfind("s3://", 0) != 0) {
		throw IOException("URL needs to start with s3://");
	}
	auto slash_pos = url.find('/', 5);
	if (slash_pos == string::npos) {
		throw IOException("URL needs to contain a '/' after the host");
	}
	bucket = url.substr(5, slash_pos - 5);
	if (bucket.empty()) {
		throw IOException("URL needs to contain a bucket name");
	}

	// See https://docs.aws.amazon.com/AmazonS3/latest/userguide/VirtualHosting.html
	if (params.url_style == "path") {
		path = "/" + bucket;
	} else {
		path = "";
	}

	if (params.s3_url_compatibility_mode) {
		// In url compatibility mode, we will ignore any special chars, so query param strings are disabled
		trimmed_s3_url = url;
		path += url.substr(slash_pos);
	} else {
		// Parse query parameters
		auto question_pos = url.find_first_of('?');
		if (question_pos != string::npos) {
			query_param = url.substr(question_pos + 1);
			trimmed_s3_url = url.substr(0, question_pos);
		} else {
			trimmed_s3_url = url;
		}

		if (!query_param.empty()) {
			path += url.substr(slash_pos, question_pos - slash_pos);
		} else {
			path += url.substr(slash_pos);
		}
	}

	if (path.empty()) {
		throw IOException("URL needs to contain key");
	}

	if (params.url_style == "vhost" || params.url_style == "") {
		host = bucket + "." + params.endpoint;
	} else {
		host = params.endpoint;
	}

	http_proto = params.use_ssl ? "https://" : "http://";

	return {http_proto, host, bucket, path, query_param, trimmed_s3_url};
}

string S3FileSystem::GetPayloadHash(char *buffer, idx_t buffer_len) {
	if (buffer_len > 0) {
		hash_bytes payload_hash_bytes;
		hash_str payload_hash_str;
		sha256(buffer, buffer_len, payload_hash_bytes);
		hex256(payload_hash_bytes, payload_hash_str);
		return string((char *)payload_hash_str, sizeof(payload_hash_str));
	} else {
		return "";
	}
}

string ParsedS3Url::GetHTTPUrl(S3AuthParams &auth_params, string http_query_string) {
	string full_url = http_proto + host + S3FileSystem::UrlEncode(path);

	if (!http_query_string.empty()) {
		full_url += "?" + http_query_string;
	}
	return full_url;
}

unique_ptr<ResponseWrapper> S3FileSystem::PostRequest(FileHandle &handle, string url, HeaderMap header_map,
                                                      duckdb::unique_ptr<char[]> &buffer_out, idx_t &buffer_out_len,
                                                      char *buffer_in, idx_t buffer_in_len, string http_params) {
	auto auth_params = static_cast<S3FileHandle &>(handle).auth_params;
	auto parsed_s3_url = S3UrlParse(url, auth_params);
	string http_url = parsed_s3_url.GetHTTPUrl(auth_params, http_params);
	auto payload_hash = GetPayloadHash(buffer_in, buffer_in_len);
	auto headers = create_s3_header(parsed_s3_url.path, http_params, parsed_s3_url.host, "s3", "POST", auth_params, "",
	                                "", payload_hash, "application/octet-stream");

	return HTTPFileSystem::PostRequest(handle, http_url, headers, buffer_out, buffer_out_len, buffer_in, buffer_in_len);
}

unique_ptr<ResponseWrapper> S3FileSystem::PutRequest(FileHandle &handle, string url, HeaderMap header_map,
                                                     char *buffer_in, idx_t buffer_in_len, string http_params) {
	auto auth_params = static_cast<S3FileHandle &>(handle).auth_params;
	auto parsed_s3_url = S3UrlParse(url, auth_params);
	string http_url = parsed_s3_url.GetHTTPUrl(auth_params, http_params);
	auto content_type = "application/octet-stream";
	auto payload_hash = GetPayloadHash(buffer_in, buffer_in_len);

	auto headers = create_s3_header(parsed_s3_url.path, http_params, parsed_s3_url.host, "s3", "PUT", auth_params, "",
	                                "", payload_hash, content_type);
	return HTTPFileSystem::PutRequest(handle, http_url, headers, buffer_in, buffer_in_len);
}

unique_ptr<ResponseWrapper> S3FileSystem::HeadRequest(FileHandle &handle, string s3_url, HeaderMap header_map) {
	auto auth_params = static_cast<S3FileHandle &>(handle).auth_params;
	auto parsed_s3_url = S3UrlParse(s3_url, auth_params);
	string http_url = parsed_s3_url.GetHTTPUrl(auth_params);
	auto headers =
	    create_s3_header(parsed_s3_url.path, "", parsed_s3_url.host, "s3", "HEAD", auth_params, "", "", "", "");
	return HTTPFileSystem::HeadRequest(handle, http_url, headers);
}

unique_ptr<ResponseWrapper> S3FileSystem::GetRequest(FileHandle &handle, string s3_url, HeaderMap header_map) {
	auto auth_params = static_cast<S3FileHandle &>(handle).auth_params;
	auto parsed_s3_url = S3UrlParse(s3_url, auth_params);
	string http_url = parsed_s3_url.GetHTTPUrl(auth_params);
	auto headers =
	    create_s3_header(parsed_s3_url.path, "", parsed_s3_url.host, "s3", "GET", auth_params, "", "", "", "");
	return HTTPFileSystem::GetRequest(handle, http_url, headers);
}

unique_ptr<ResponseWrapper> S3FileSystem::GetRangeRequest(FileHandle &handle, string s3_url, HeaderMap header_map,
                                                          idx_t file_offset, char *buffer_out, idx_t buffer_out_len) {
	auto auth_params = static_cast<S3FileHandle &>(handle).auth_params;
	auto parsed_s3_url = S3UrlParse(s3_url, auth_params);
	string http_url = parsed_s3_url.GetHTTPUrl(auth_params);
	auto headers =
	    create_s3_header(parsed_s3_url.path, "", parsed_s3_url.host, "s3", "GET", auth_params, "", "", "", "");
	return HTTPFileSystem::GetRangeRequest(handle, http_url, headers, file_offset, buffer_out, buffer_out_len);
}

unique_ptr<HTTPFileHandle> S3FileSystem::CreateHandle(const string &path, uint8_t flags, FileLockType lock,
                                                      FileCompressionType compression, FileOpener *opener) {
	FileOpenerInfo info = {path};
	auto auth_params = S3AuthParams::ReadFrom(opener, info);

	// Scan the query string for any s3 authentication parameters
	auto parsed_s3_url = S3UrlParse(path, auth_params);
	ReadQueryParams(parsed_s3_url.query_param, auth_params);

	return duckdb::make_uniq<S3FileHandle>(*this, path, flags, HTTPParams::ReadFrom(opener), auth_params,
	                                       S3ConfigParams::ReadFrom(opener));
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
	string canonical_query_string = "delimiter=%2F&encoding-type=url&list-type=2&prefix="; // aws s3 ls <bucket>

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

	// bug #4082
	S3AuthParams auth_params4 = {
	    "auto", "asdf", "asdfasdfasdfasdfasdfasdfasdfasdfasdfasdfasdfasdfasdfasdfasdfasdfasdf", "", "", "", true};
	create_s3_header("/", "", "exampple.com", "s3", "GET", auth_params4);

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

void S3FileHandle::Initialize(FileOpener *opener) {
	HTTPFileHandle::Initialize(opener);

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

		uploads_in_progress = 0;
		parts_uploaded = 0;
		upload_finalized = false;
	}
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
		auto write_buffer = s3fh.GetBuffer(write_buffer_idx);

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

static bool Match(vector<string>::const_iterator key, vector<string>::const_iterator key_end,
                  vector<string>::const_iterator pattern, vector<string>::const_iterator pattern_end) {

	while (key != key_end && pattern != pattern_end) {
		if (*pattern == "**") {
			if (std::next(pattern) == pattern_end) {
				return true;
			}
			while (key != key_end) {
				if (Match(key, key_end, std::next(pattern), pattern_end)) {
					return true;
				}
				key++;
			}
			return false;
		}
		if (!LikeFun::Glob(key->data(), key->length(), pattern->data(), pattern->length())) {
			return false;
		}
		key++;
		pattern++;
	}
	return key == key_end && pattern == pattern_end;
}

vector<string> S3FileSystem::Glob(const string &glob_pattern, FileOpener *opener) {
	if (opener == nullptr) {
		throw InternalException("Cannot S3 Glob without FileOpener");
	}

	FileOpenerInfo info = {glob_pattern};

	// Trim any query parameters from the string
	auto s3_auth_params = S3AuthParams::ReadFrom(opener, info);

	// In url compatibility mode, we ignore globs allowing users to query files with the glob chars
	if (s3_auth_params.s3_url_compatibility_mode) {
		return {glob_pattern};
	}

	auto parsed_s3_url = S3UrlParse(glob_pattern, s3_auth_params);
	auto parsed_glob_url = parsed_s3_url.trimmed_s3_url;

	// AWS matches on prefix, not glob pattern, so we take a substring until the first wildcard char for the aws calls
	auto first_wildcard_pos = parsed_glob_url.find_first_of("*[\\");
	if (first_wildcard_pos == string::npos) {
		return {glob_pattern};
	}

	string shared_path = parsed_glob_url.substr(0, first_wildcard_pos);
	auto http_params = HTTPParams::ReadFrom(opener);

	ReadQueryParams(parsed_s3_url.query_param, s3_auth_params);

	// Do main listobjectsv2 request
	vector<string> s3_keys;
	string main_continuation_token = "";

	// Main paging loop
	do {
		// main listobject call, may
		string response_str = AWSListObjectV2::Request(shared_path, http_params, s3_auth_params,
		                                               main_continuation_token, HTTPState::TryGetState(opener).get());
		main_continuation_token = AWSListObjectV2::ParseContinuationToken(response_str);
		AWSListObjectV2::ParseKey(response_str, s3_keys);

		// Repeat requests until the keys of all common prefixes are parsed.
		auto common_prefixes = AWSListObjectV2::ParseCommonPrefix(response_str);
		while (!common_prefixes.empty()) {
			auto prefix_path = "s3://" + parsed_s3_url.bucket + '/' + common_prefixes.back();
			common_prefixes.pop_back();

			// TODO we could optimize here by doing a match on the prefix, if it doesn't match we can skip this prefix
			// Paging loop for common prefix requests
			string common_prefix_continuation_token = "";
			do {
				auto prefix_res =
				    AWSListObjectV2::Request(prefix_path, http_params, s3_auth_params, common_prefix_continuation_token,
				                             HTTPState::TryGetState(opener).get());
				AWSListObjectV2::ParseKey(prefix_res, s3_keys);
				auto more_prefixes = AWSListObjectV2::ParseCommonPrefix(prefix_res);
				common_prefixes.insert(common_prefixes.end(), more_prefixes.begin(), more_prefixes.end());
				common_prefix_continuation_token = AWSListObjectV2::ParseContinuationToken(prefix_res);
			} while (!common_prefix_continuation_token.empty());
		}
	} while (!main_continuation_token.empty());

	auto pattern_trimmed = parsed_s3_url.path.substr(1);

	// Trim the bucket prefix for path-style urls
	if (s3_auth_params.url_style == "path") {
		pattern_trimmed = pattern_trimmed.substr(parsed_s3_url.bucket.length() + 1);
	}

	vector<string> pattern_splits = StringUtil::Split(pattern_trimmed, "/");
	vector<string> result;
	for (const auto &s3_key : s3_keys) {

		vector<string> key_splits = StringUtil::Split(s3_key, "/");
		bool is_match = Match(key_splits.begin(), key_splits.end(), pattern_splits.begin(), pattern_splits.end());

		if (is_match) {
			auto result_full_url = "s3://" + parsed_s3_url.bucket + "/" + s3_key;
			// if a ? char was present, we re-add it here as the url parsing will have trimmed it.
			if (!parsed_s3_url.query_param.empty()) {
				result_full_url += '?' + parsed_s3_url.query_param;
			}
			result.push_back(result_full_url);
		}
	}
	return result;
}

string S3FileSystem::GetName() const {
	return "S3FileSystem";
}

bool S3FileSystem::ListFiles(const string &directory, const std::function<void(const string &, bool)> &callback,
                             FileOpener *opener) {
	string trimmed_dir = directory;
	StringUtil::RTrim(trimmed_dir, PathSeparator(trimmed_dir));
	auto glob_res = Glob(JoinPath(trimmed_dir, "**"), opener);

	if (glob_res.empty()) {
		return false;
	}

	for (const auto &file : glob_res) {
		callback(file, false);
	}

	return true;
}

string AWSListObjectV2::Request(string &path, HTTPParams &http_params, S3AuthParams &s3_auth_params,
                                string &continuation_token, optional_ptr<HTTPState> state, bool use_delimiter) {
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
		req_params += "continuation-token=" + S3FileSystem::UrlEncode(continuation_token, true);
		req_params += "&";
	}
	req_params += "encoding-type=url&list-type=2";
	req_params += "&prefix=" + S3FileSystem::UrlEncode(prefix, true);

	if (use_delimiter) {
		req_params += "&delimiter=%2F";
	}

	string listobjectv2_url = req_path + "?" + req_params;

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
			    throw HTTPException(response, "HTTP GET error on '%s' (HTTP %d)", listobjectv2_url, response.status);
		    }
		    return true;
	    },
	    [&](const char *data, size_t data_length) {
		    if (state) {
			    state->total_bytes_received += data_length;
		    }
		    response << string(data, data_length);
		    return true;
	    });
	if (state) {
		state->get_count++;
	}
	if (res.error() != duckdb_httplib_openssl::Error::Success) {
		throw IOException(to_string(res.error()) + " error for HTTP GET to '" + listobjectv2_url + "'");
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
