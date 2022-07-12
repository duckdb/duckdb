#include "catch.hpp"
#include "mbedtls_wrapper.hpp"

#include <chrono>
#include <thread>
#include <fstream>
#include <sstream>

using namespace duckdb_mbedtls;
using namespace std;

static string file_to_string(string filename) {
	std::ifstream stream(filename, ios_base::binary);
	std::stringstream buffer;
	buffer << stream.rdbuf();
	return buffer.str();
}

TEST_CASE("Test that we can verify a signature", "[mbedtls]") {
	// those files are created with the create_files.sh script
	auto file_content = file_to_string("test/mbedtls/dummy_file");
	auto signature = file_to_string("test/mbedtls/dummy_file.signature");
	auto pubkey = file_to_string("test/mbedtls/public.pem");

	auto hash = MbedTlsWrapper::ComputeSha256Hash(file_content);
	REQUIRE(MbedTlsWrapper::IsValidSha256Signature(pubkey, signature, hash));
	string empty_string = "";

	auto borked_pubkey = pubkey;
	borked_pubkey[10]++;

	// a borked public key is an exception, this should never happen
	REQUIRE_THROWS(MbedTlsWrapper::IsValidSha256Signature(borked_pubkey, signature, hash));
	REQUIRE_THROWS(MbedTlsWrapper::IsValidSha256Signature(empty_string, signature, hash));

	// wrong-length signatures or hashes should never happen either
	REQUIRE_THROWS(MbedTlsWrapper::IsValidSha256Signature(pubkey, empty_string, hash));
	REQUIRE_THROWS(MbedTlsWrapper::IsValidSha256Signature(pubkey, signature, empty_string));

	// lets flip some bits in the file, it should not validate
	auto borked_file = file_content;
	borked_file[10]++;
	auto hash2 = MbedTlsWrapper::ComputeSha256Hash(borked_file);
	REQUIRE(!MbedTlsWrapper::IsValidSha256Signature(pubkey, signature, hash2));

	auto borked_signature = signature;
	borked_signature[10]++;
	REQUIRE(!MbedTlsWrapper::IsValidSha256Signature(pubkey, borked_signature, hash));

	auto borked_hash = hash;
	borked_hash[10]++;
	auto hash3 = MbedTlsWrapper::ComputeSha256Hash(empty_string);
	REQUIRE(!MbedTlsWrapper::IsValidSha256Signature(pubkey, signature, hash3));
	REQUIRE(!MbedTlsWrapper::IsValidSha256Signature(pubkey, signature, borked_hash));

	// seems all right!
	REQUIRE(MbedTlsWrapper::IsValidSha256Signature(pubkey, signature, hash));
}
