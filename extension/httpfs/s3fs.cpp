#include "s3fs.hpp"
#include "crypto.hpp"
#include "httplib.hpp"
#include "duckdb.hpp"

#include <map>

using namespace duckdb;

static std::map<std::string, std::string> create_s3_get_header(std::string url, std::string host, std::string region,
                                                               std::string service, std::string method,
                                                               std::string access_key_id, std::string secret_access_key,
                                                               std::string date_now = "",
                                                               std::string datetime_now = "") {
	// this is the sha256 of the empty string, its useful since we have no payload for GET requests
	auto empty_payload_hash = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

	// we can pass date/time but this is mostly useful in testing. normally we just get the current datetime here.
	if (datetime_now.empty()) {
		auto t = std::time(NULL);
		auto tmp = std::gmtime(&t);
		date_now.resize(8);
		datetime_now.resize(16);

		strftime((char *)date_now.c_str(), date_now.size(), "%Y%m%d", tmp);
		strftime((char *)datetime_now.c_str(), datetime_now.size(), "%Y%m%dT%H%M%SZ", tmp);
	}

	std::map<std::string, std::string> res;
	res["Host"] = host;
	res["x-amz-date"] = datetime_now;
	res["x-amz-content-sha256"] = empty_payload_hash;

	// construct string to sign
	hash_bytes canonical_request_hash;
	hash_str canonical_request_hash_str;
	auto canonical_request = method + "\n" + url + "\n\nhost:" + host + "\nx-amz-content-sha256:" + empty_payload_hash +
	                         "\nx-amz-date:" + datetime_now + "\n\nhost;x-amz-content-sha256;x-amz-date\n" +
	                         empty_payload_hash;
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
	                       service + "/aws4_request, SignedHeaders=host;x-amz-content-sha256;x-amz-date, Signature=" +
	                       std::string((char *)signature_str, sizeof(hash_str));

	return res;
}

static httplib::Result s3_get(std::string s3_url, std::string region, std::string method, std::string access_key_id,
                              std::string secret_access_key, idx_t file_offset = 0, char *buffer_out = nullptr,
                              idx_t buffer_len = 0) {
	// some URI parsing woo
	if (s3_url.rfind("s3://", 0) != 0) {
		throw std::runtime_error("URL needs to start with s3://");
	}
	auto slash_pos = s3_url.find('/', 5);
	if (slash_pos == std::string::npos) {
		throw std::runtime_error("URL needs to contain a '/' after the host");
	}
	auto bucket = s3_url.substr(5, slash_pos - 5);
	if (bucket.empty()) {
		throw std::runtime_error("URL needs to contain a bucket name");
	}
	auto path = s3_url.substr(slash_pos);
	if (path.empty()) {
		throw std::runtime_error("URL needs to contain a bucket name");
	}
	auto host = bucket + ".s3.amazonaws.com";
	// actual request
	httplib::Client cli(std::string("http://" + host).c_str());
	auto auth_headers = create_s3_get_header(path, host, region, "s3", method, access_key_id, secret_access_key);
	httplib::Headers headers;
	for (auto &entry : auth_headers) {
		headers.insert(entry);
	}

	if (method == "HEAD") {
		return cli.Head(path.c_str(), headers);
	}
	std::string range_expr =
	    "bytes=" + std::to_string(file_offset) + "-" + std::to_string(file_offset + buffer_len - 1);
	// printf("%s(%llu, %llu)\n", method.c_str(), file_offset, buffer_len);

	// send the Range header to read only subset of file
	headers.insert(std::pair<std::string, std::string>("Range", range_expr));

	idx_t out_offset = 0;
	return cli.Get(
	    path.c_str(), headers,
	    [&](const httplib::Response &response) {
		    if (response.status >= 300) {
			    throw std::runtime_error("HTTP error");
		    }
		    auto content_length = std::stol(response.get_header_value("Content-Length", 0));
		    if (content_length != buffer_len) {
			    throw std::runtime_error("offset error");
		    }
		    return true; // return 'false' if you want to cancel the request.
	    },
	    [&](const char *data, size_t data_length) {
		    memcpy(buffer_out + out_offset, data, data_length);
		    out_offset += data_length;
		    return true; // return 'false' if you want to cancel the request.
	    });
}

std::unique_ptr<FileHandle> S3FileSystem::OpenFile(const char *path, uint8_t flags, FileLockType lock) {
	return duckdb::make_unique<S3FileHandle>(*this, path);
}

void S3FileSystem::Read(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) {
	auto &sfh = (S3FileHandle &)handle;
	idx_t to_read = nr_bytes;
	idx_t buffer_offset = 0;
	if (location + nr_bytes > sfh.length) {
		throw std::runtime_error("out of file");
	}

	// TODO we need to check if location is within the cached buffer and update or invalidate
	// for now just invalidate
	if (location != sfh.file_offset) {
		sfh.buffer_available = 0;
		sfh.buffer_idx = 0;
		sfh.file_offset = location;
	}

	while (to_read > 0) {
		auto buffer_read_len = MinValue<idx_t>(sfh.buffer_available, to_read);
		memcpy((char *)buffer + buffer_offset, sfh.buffer.get() + sfh.buffer_idx, buffer_read_len);

		buffer_offset += buffer_read_len;
		to_read -= buffer_read_len;

		sfh.buffer_idx += buffer_read_len;
		sfh.buffer_available -= buffer_read_len;
		sfh.file_offset += buffer_read_len;

		if (to_read > 0 && sfh.buffer_available == 0) {
			auto new_buffer_available = MinValue<idx_t>(sfh.BUFFER_LEN, sfh.length - sfh.file_offset);
			auto res = s3_get(sfh.path, region, "GET", access_key_id, secret_access_key, sfh.file_offset,
			                  (char *)sfh.buffer.get(), new_buffer_available);
			sfh.buffer_available = new_buffer_available;
			sfh.buffer_idx = 0;
		}
	}
}

void S3FileHandle::IntializeMetadata() {
	// get length using HEAD
	auto &sfs = (S3FileSystem &)file_system;
	auto res = s3_get(path, sfs.region, "HEAD", sfs.access_key_id, sfs.secret_access_key);
	length = std::atoll(res->get_header_value("Content-Length").c_str());

	struct tm tm;
	strptime(res->get_header_value("Last-Modified").c_str(), "%a, %d %h %Y %T %Z", &tm);
	last_modified = std::mktime(&tm);
}

// this computes the signature from https://czak.pl/2015/09/15/s3-rest-api-with-curl.html
void S3FileSystem::Verify() {
	auto test_header = create_s3_get_header("/", "my-precious-bucket.s3.amazonaws.com", "us-east-1", "s3", "GET",
	                                        "AKIAIOSFODNN7EXAMPLE", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
	                                        "20150915", "20150915T124500Z");
	if (test_header["Authorization"] !=
	    "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20150915/us-east-1/s3/aws4_request, "
	    "SignedHeaders=host;x-amz-content-sha256;x-amz-date, "
	    "Signature=182072eb53d85c36b2d791a1fa46a12d23454ec1e921b02075c23aee40166d5a") {
		throw std::runtime_error("test fail");
	}
}
