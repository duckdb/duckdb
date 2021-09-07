#include "s3fs.hpp"
#include "crypto.hpp"
#include "duckdb.hpp"
#ifndef DUCKDB_AMALGAMATION
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/function/scalar/strftime.hpp"
#endif

using namespace duckdb;

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
	if (session_token.length() > 0) {
		res["x-amz-security-token"] = session_token;
	}

	// construct string to sign
	hash_bytes canonical_request_hash;
	hash_str canonical_request_hash_str;
	std::string signed_headers = "host;x-amz-content-sha256;x-amz-date";
	if (session_token.length() > 0) {
		signed_headers += ";x-amz-security-token";
	}
	auto canonical_request = method + "\n" + uri_encode(url) + "\n" + query + "\nhost:" + host +
	                         "\nx-amz-content-sha256:" + empty_payload_hash + "\nx-amz-date:" + datetime_now;
	if (access_token.length() > 0) {
		canonical_request += "\nx-amz-security-token:" + access_token;
	}
	canonical_request += "\n\n" + signed_headers + "\n" + empty_payload_hash;
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
	// With STS credentials the session token must be provided
	//   See: https://docs.aws.amazon.com/AmazonS3/latest/API/sigv4-query-string-auth.html
	auto session_token = database_instance.config.set_variables["s3_session_token"].str_value;

	return create_s3_get_header(path, "", host, region, "s3", method, access_key_id, secret_access_key, session_token);
}

std::unique_ptr<FileHandle> S3FileSystem::OpenFile(const string &path, uint8_t flags, FileLockType lock,
                                                   FileCompressionType compression) {
	D_ASSERT(compression == FileCompressionType::UNCOMPRESSED);
	return duckdb::make_unique<HTTPFileHandle>(*this, path);
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

bool S3FileSystem::CanHandleFile(const string &fpath) {
	return fpath.rfind("s3://", 0) == 0;
}
