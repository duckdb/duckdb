#include "s3fs.hpp"
#include "crypto.hpp"
#include "duckdb.hpp"
#ifndef DUCKDB_AMALGAMATION
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/function/scalar/strftime.hpp"
#endif

using namespace duckdb;

static std::string uri_encode(std::string  input, bool encodeSlash = false) {
	// https://docs.aws.amazon.com/AmazonS3/latest/API/sigv4-query-string-auth.html
	static auto hex_digt = "0123456789ABCDEF";
	std::string result = "";
	for (int i = 0; i < input.length(); i++) {
		char ch = input[i];
		if ((ch >= 'A' && ch <= 'Z') || (ch >= 'a' && ch <= 'z') || (ch >= '0' && ch <= '9') || ch == '_' ||
		    ch == '-' || ch == '~' || ch == '.') {
			result += ch;
		} else if (ch == '/') {
			if (encodeSlash) {
				result += std::string("%2F");
			} else {
				result += ch;
			}
		} else {
			result += std::string("%") + hex_digt[static_cast<unsigned char>(ch) >> 4] +
			          hex_digt[static_cast<unsigned char>(ch) & 15];
		}
	}
	return result;
}

static HeaderMap create_s3_get_header(std::string url, std::string host, std::string region, std::string service,
                                      std::string method, std::string access_key_id, std::string secret_access_key,
                                      std::string date_now = "", std::string datetime_now = "") {
	// this is the sha256 of the empty string, its useful since we have no payload for GET requests
	auto empty_payload_hash = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

	// we can pass date/time but this is mostly useful in testing. normally we just get the current datetime here.
	if (datetime_now.empty()) {
		auto timestamp = Timestamp::GetCurrentTimestamp();
		date_now = StrfTimeFormat::Format(timestamp, "%Y%m%d");
		datetime_now = StrfTimeFormat::Format(timestamp, "%Y%m%dT%H%M%SZ");
	}

	HeaderMap res;
	res["Host"] = host;
	res["x-amz-date"] = datetime_now;
	res["x-amz-content-sha256"] = empty_payload_hash;

	// construct string to sign
	hash_bytes canonical_request_hash;
	hash_str canonical_request_hash_str;
	auto canonical_request = method + "\n" + uri_encode(url) + "\n\nhost:" + host +
	                         "\nx-amz-content-sha256:" + empty_payload_hash + "\nx-amz-date:" + datetime_now +
	                         "\n\nhost;x-amz-content-sha256;x-amz-date\n" + empty_payload_hash;
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

unique_ptr<ResponseWrapper> S3FileSystem::Request(FileHandle &handle, string url, string method, HeaderMap header_map,
                                                  idx_t file_offset, char *buffer_out, idx_t buffer_len) {
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
	auto path = url.substr(slash_pos);
	if (path.empty()) {
		throw std::runtime_error("URL needs to contain key");
	}
	auto host = bucket + ".s3.amazonaws.com";
	auto http_host = "https://" + host;
	// actual request

	return HTTPFileSystem::Request(handle, http_host + path, method, CreateAuthHeaders(host, path, method), file_offset,
	                               buffer_out, buffer_len);
}

HeaderMap S3FileSystem::CreateAuthHeaders(string host, string path, string method) {
	auto region = database_instance.config.set_variables["s3_region"].str_value;
	auto access_key_id = database_instance.config.set_variables["s3_access_key_id"].str_value;
	auto secret_access_key = database_instance.config.set_variables["s3_secret_access_key"].str_value;

	return create_s3_get_header(path, host, region, "s3", method, access_key_id, secret_access_key);
}

std::unique_ptr<FileHandle> S3FileSystem::OpenFile(const string &path, uint8_t flags, FileLockType lock,
                                                   FileCompressionType compression) {
	D_ASSERT(compression == FileCompressionType::UNCOMPRESSED);
	return duckdb::make_unique<HTTPFileHandle>(*this, path);
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

bool S3FileSystem::CanHandleFile(const string &fpath) {
	return fpath.rfind("s3://", 0) == 0;
}
