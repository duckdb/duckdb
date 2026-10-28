#include "catch.hpp"
#include "duckdb/common/local_file_system.hpp"
#include "duckdb/common/random_engine.hpp"
#include "duckdb/main/extension.hpp"
#include "duckdb/main/extension_helper.hpp"
#include "duckdb/main/settings.hpp"
#include "mbedtls/pk.h"
#include "mbedtls_wrapper.hpp"
#include "test_helpers.hpp"

#include <fstream>
#include <sstream>

using namespace duckdb;

// Builds an unsigned stub extension: body + metadata block (fields written in reverse)
static string MakeUnsignedStub(const string &marker, idx_t body_size = 1000) {
	string fields[8];
	fields[0] = "4";
	fields[1] = DuckDB::Platform();
	fields[2] = ExtensionHelper::GetVersionDirectoryName();
	fields[3] = "v0.0.1";
	fields[4] = "CPP";
	fields[6] = marker;
	string result(body_size, 'x');
	for (idx_t i = 8; i > 0; i--) {
		auto field = fields[i - 1];
		field.resize(32, '\0');
		result += field;
	}
	return result;
}

// Builds a stub extension with a placeholder trailing signature
static string MakeStubExtension(idx_t signature_size, const string &marker, idx_t body_size = 1000) {
	return MakeUnsignedStub(marker, body_size) + string(signature_size, 's');
}

static ParsedExtensionMetaData ParseFooter(const string &file) {
	return ExtensionHelper::ParseExtensionFooter(file.data(), file.size());
}

static void RequireValidFooter(const string &file, idx_t expected_size) {
	auto parsed = ParseFooter(file);
	REQUIRE(parsed.AppearsValid());
	REQUIRE(parsed.metadata_error.empty());
	REQUIRE(parsed.GetInvalidMetadataError().empty());
	REQUIRE(parsed.signature_size == expected_size);
	REQUIRE(parsed.extension_version == "v0.0.1");
}

static void RequireFooterError(const string &file, const string &expected_error) {
	auto parsed = ParseFooter(file);
	REQUIRE(parsed.AppearsValid());
	REQUIRE(StringUtil::Contains(parsed.GetInvalidMetadataError(), expected_error));
}

static string WriteStubFile(const string &name, const string &content) {
	auto path = TestCreatePath(name + ".duckdb_extension");
	std::ofstream out(path, std::ios::binary);
	out.write(content.data(), NumericCast<std::streamsize>(content.size()));
	return path;
}

TEST_CASE("Extension footer parses every supported signature size", "[extension_footer]") {
	// legacy footers, with and without an explicit marker
	RequireValidFooter(MakeStubExtension(256, ""), 256);
	RequireValidFooter(MakeStubExtension(256, "256"), 256);
	RequireValidFooter(MakeStubExtension(384, "384"), 384);
	RequireValidFooter(MakeStubExtension(512, "512"), 512);
	// footers without a body
	RequireValidFooter(MakeStubExtension(256, "", 0), 256);
	RequireValidFooter(MakeStubExtension(512, "512", 0), 512);

	// a legacy footer parses exactly as the plain metadata parser at the legacy offset
	auto legacy = MakeStubExtension(256, "");
	auto direct =
	    ExtensionHelper::ParseExtensionMetaData(legacy.data() + legacy.size() - ParsedExtensionMetaData::FOOTER_SIZE);
	auto probed = ParseFooter(legacy);
	REQUIRE(probed.magic_value == direct.magic_value);
	REQUIRE(probed.platform == direct.platform);
	REQUIRE(probed.duckdb_version == direct.duckdb_version);
	REQUIRE(probed.extension_version == direct.extension_version);
	REQUIRE(probed.signature == direct.signature);
	REQUIRE(probed.signature_size == direct.signature_size);
}

TEST_CASE("Extension footer rejects a marker that does not match the layout", "[extension_footer]") {
	const string mismatch = "does not match the footer layout";
	RequireFooterError(MakeStubExtension(256, "384"), mismatch);
	RequireFooterError(MakeStubExtension(256, "512"), mismatch);
	RequireFooterError(MakeStubExtension(384, ""), mismatch);
	RequireFooterError(MakeStubExtension(384, "256"), mismatch);
	RequireFooterError(MakeStubExtension(384, "512"), mismatch);
	RequireFooterError(MakeStubExtension(512, "384"), mismatch);
	// the size used downstream is the layout's, never the disagreeing marker
	REQUIRE(ParseFooter(MakeStubExtension(256, "384")).signature_size == 256);
}

TEST_CASE("Extension footer rejects non-canonical or unsupported markers", "[extension_footer]") {
	const string unrecognized = "unrecognized signature-size marker";
	for (auto &marker :
	     {"300", "0384", "abc", " 384", "+384", "384x", "38\0014", "0", "0256", "1024", "256  ", "384 "}) {
		INFO(marker);
		RequireFooterError(MakeStubExtension(384, marker), unrecognized);
		RequireFooterError(MakeStubExtension(256, marker), unrecognized);
	}
}

TEST_CASE("Extension footer rejects truncated files", "[extension_footer]") {
	auto legacy = MakeStubExtension(256, "", 0);
	for (idx_t size : {idx_t(0), idx_t(1), idx_t(255), idx_t(256), idx_t(511)}) {
		auto tail = legacy.substr(legacy.size() - size);
		REQUIRE(!ExtensionHelper::ParseExtensionFooter(tail.data(), tail.size()).AppearsValid());
	}
	// only the legacy layout fits, so a 512-byte-signature footer missing its metadata is not found
	auto large = MakeStubExtension(512, "512", 0);
	auto tail = large.substr(large.size() - 600);
	REQUIRE(!ExtensionHelper::ParseExtensionFooter(tail.data(), tail.size()).AppearsValid());

	DuckDB db(nullptr);
	REQUIRE(!ExtensionHelper::CheckExtensionBufferSignature(*db.instance, tail.data(), 100,
	                                                        ExtensionRepositoryType::CORE, ""));
	Connection con(db);
	auto path = WriteStubFile("footer_truncated", legacy.substr(0, 511));
	auto result = con.Query(StringUtil::Format("LOAD '%s'", path));
	REQUIRE(result->HasError());
	REQUIRE(StringUtil::Contains(result->GetError(), "must be at least"));
}

TEST_CASE("Extension install and load use the probing footer parser", "[extension_footer]") {
	DBConfig config;
	config.SetOptionByName("extension_directory", TestCreatePath("footer_ext_dir"));
	config.SetOptionByName("allow_unsigned_extensions", true);
	DuckDB db(nullptr, &config);
	Connection con(db);

	for (idx_t size : {idx_t(256), idx_t(384), idx_t(512)}) {
		auto name = "footer_ok_" + std::to_string(size);
		auto path = WriteStubFile(name, MakeStubExtension(size, std::to_string(size)));
		REQUIRE_NO_FAIL(con.Query(StringUtil::Format("INSTALL '%s'", path)));
		auto version = con.Query(
		    StringUtil::Format("SELECT extension_version FROM duckdb_extensions() WHERE extension_name = '%s'", name));
		REQUIRE(CHECK_COLUMN(version, 0, {"v0.0.1"}));
	}

	auto mismatch_path = WriteStubFile("footer_mismatch", MakeStubExtension(256, "384"));
	auto result = con.Query(StringUtil::Format("INSTALL '%s'", mismatch_path));
	REQUIRE(result->HasError());
	REQUIRE(StringUtil::Contains(result->GetError(), "does not match the footer layout"));

	result = con.Query(StringUtil::Format("LOAD '%s'", mismatch_path));
	REQUIRE(result->HasError());
	REQUIRE(StringUtil::Contains(result->GetError(), "does not match the footer layout"));

	auto unknown_path = WriteStubFile("footer_unknown", MakeStubExtension(384, "0384"));
	result = con.Query(StringUtil::Format("LOAD '%s'", unknown_path));
	REQUIRE(result->HasError());
	REQUIRE(StringUtil::Contains(result->GetError(), "unrecognized signature-size marker"));

	// a malformed footer is never overridable, unlike a version or platform mismatch
	auto foreign = MakeStubExtension(256, "256");
	foreign[foreign.size() - ParsedExtensionMetaData::FOOTER_SIZE + 6 * 32] = 'Z';
	auto foreign_path = WriteStubFile("footer_foreign", foreign);
	REQUIRE_FAIL(con.Query(StringUtil::Format("INSTALL '%s'", foreign_path)));
	REQUIRE_NO_FAIL(con.Query("SET allow_extensions_metadata_mismatch=true"));
	REQUIRE_NO_FAIL(con.Query(StringUtil::Format("INSTALL '%s'", foreign_path)));
	for (auto &statement : {"FORCE INSTALL '%s'", "LOAD '%s'"}) {
		result = con.Query(StringUtil::Format(statement, mismatch_path));
		REQUIRE(result->HasError());
		REQUIRE(StringUtil::Contains(result->GetError(), "does not match the footer layout"));
	}
}

TEST_CASE("Signed extension loading rejects stubs of every footer size", "[extension_footer]") {
	DuckDB db(nullptr);
	Connection con(db);
	for (idx_t size : {idx_t(256), idx_t(384), idx_t(512)}) {
		auto path =
		    WriteStubFile("footer_unsigned_" + std::to_string(size), MakeStubExtension(size, std::to_string(size)));
		auto result = con.Query(StringUtil::Format("LOAD '%s'", path));
		REQUIRE(result->HasError());
		REQUIRE(StringUtil::Contains(result->GetError(), "signature is either missing or invalid"));
	}
}

struct TestSigningKey {
	idx_t signature_size;
	const char *private_key_path;
	const char *public_key_path;
};

static const TestSigningKey TEST_SIGNING_KEYS[] = {
    {256, "test/mbedtls/private.pem", "test/mbedtls/public.pem"},
    {384, "test/mbedtls/private_3072.pem", "test/mbedtls/public_3072.pem"},
    {512, "test/mbedtls/private_4096.pem", "test/mbedtls/public_4096.pem"}};

static string ReadTestFile(const string &path) {
	std::ifstream in(path, std::ios::binary);
	std::stringstream buffer;
	buffer << in.rdbuf();
	return buffer.str();
}

// Same scheme as the engine: SHA-256 of the concatenated SHA-256 hashes of 1 MiB chunks
static string ComputeExtensionHash(const string &signed_region) {
	const idx_t chunk_size = 1024ULL * 1024ULL;
	string chunk_hashes;
	for (idx_t start = 0; start < signed_region.size(); start += chunk_size) {
		chunk_hashes += duckdb_mbedtls::MbedTlsWrapper::ComputeSha256Hash(signed_region.substr(start, chunk_size));
	}
	return duckdb_mbedtls::MbedTlsWrapper::ComputeSha256Hash(chunk_hashes);
}

static int TestRandom(void *rng, unsigned char *output, size_t length) {
	auto &engine = *static_cast<RandomEngine *>(rng);
	for (size_t i = 0; i < length; i++) {
		output[i] = static_cast<unsigned char>(engine.NextRandomInteger());
	}
	return 0;
}

// RSA PKCS#1 v1.5 SHA-256 signature of the hash
static string SignHash(const string &private_key_pem, const string &hash) {
	RandomEngine engine(42);
	mbedtls_pk_context pk;
	mbedtls_pk_init(&pk);
	string signature(MBEDTLS_PK_SIGNATURE_MAX_SIZE, '\0');
	size_t signature_length = 0;
	auto key = reinterpret_cast<const unsigned char *>(private_key_pem.c_str());
	auto hash_data = reinterpret_cast<const unsigned char *>(hash.data());
	bool success = mbedtls_pk_parse_key(&pk, key, private_key_pem.size() + 1, nullptr, 0, TestRandom, &engine) == 0 &&
	               mbedtls_pk_sign(&pk, MBEDTLS_MD_SHA256, hash_data, hash.size(),
	                               reinterpret_cast<unsigned char *>(&signature[0]), signature.size(),
	                               &signature_length, TestRandom, &engine) == 0;
	mbedtls_pk_free(&pk);
	REQUIRE(success);
	signature.resize(signature_length);
	return signature;
}

static string MakeSignedStub(const TestSigningKey &key) {
	auto stub = MakeUnsignedStub(std::to_string(key.signature_size));
	auto signature = SignHash(ReadTestFile(key.private_key_path), ComputeExtensionHash(stub));
	REQUIRE(signature.size() == key.signature_size);
	return stub + signature;
}

static void WriteBinaryFile(const string &path, const string &content) {
	std::ofstream out(path, std::ios::binary);
	out.write(content.data(), NumericCast<std::streamsize>(content.size()));
}

TEST_CASE("Signed extensions of every footer size verify against a trusted repository key", "[extension_footer]") {
	DBConfig config;
	config.SetOptionByName("extension_directory", TestCreatePath("footer_signed_ext_dir"));
	config.SetOptionByName("extension_repository_directory", TestCreatePath("footer_signed_repositories"));
	config.SetOptionByName("allow_extension_repositories", "allowed");
	DuckDB db(nullptr, &config);
	Connection con(db);
	auto &instance = *db.instance;
	LocalFileSystem fs;
	REQUIRE(!Settings::Get<AllowUnsignedExtensionsSetting>(instance));

	vector<string> prefixes;
	vector<string> stubs;
	for (auto &key : TEST_SIGNING_KEYS) {
		auto size = std::to_string(key.signature_size);
		INFO(size);
		auto repository = "signed" + size;
		auto name = "footer_signed_" + size;
		auto prefix = TestCreatePath("footer_signed_repo_" + size);
		auto extension_dir =
		    fs.JoinPath(fs.JoinPath(prefix, ExtensionHelper::GetVersionDirectoryName()), DuckDB::Platform());
		fs.CreateDirectoriesRecursive(extension_dir);
		auto extension_path = fs.JoinPath(extension_dir, name + ".duckdb_extension");
		auto stub = MakeSignedStub(key);
		WriteBinaryFile(extension_path, stub);
		REQUIRE_NO_FAIL(
		    con.Query(StringUtil::Format("CREATE EXTENSION REPOSITORY %s WITH PREFIX '%s' USING PUBLIC KEY '%s'",
		                                 repository, prefix, ReadTestFile(key.public_key_path))));

		REQUIRE(ExtensionHelper::CheckExtensionBufferSignature(instance, stub.data(), stub.size(),
		                                                       ExtensionRepositoryType::USER_PROVIDED, repository));
		REQUIRE_NO_FAIL(con.Query(StringUtil::Format("INSTALL %s FROM %s", name, repository)));
		// the stub is not a shared library, so a load that passed signature verification fails opening it
		auto result = con.Query(StringUtil::Format("LOAD %s FROM %s", name, repository));
		REQUIRE(result->HasError());
		REQUIRE(StringUtil::Contains(result->GetError(), "could not be loaded: "));
		REQUIRE(!StringUtil::Contains(result->GetError(), "signature"));

		auto tampered_body = stub;
		tampered_body[10]++;
		REQUIRE(!ExtensionHelper::CheckExtensionBufferSignature(instance, tampered_body.data(), tampered_body.size(),
		                                                        ExtensionRepositoryType::USER_PROVIDED, repository));
		auto tampered_marker = stub;
		tampered_marker[stub.size() - key.signature_size - ParsedExtensionMetaData::METADATA_SIZE + 32]++;
		REQUIRE(!ExtensionHelper::CheckExtensionBufferSignature(instance, tampered_marker.data(),
		                                                        tampered_marker.size(),
		                                                        ExtensionRepositoryType::USER_PROVIDED, repository));

		WriteBinaryFile(extension_path, tampered_body);
		result = con.Query(StringUtil::Format("FORCE INSTALL %s FROM %s", name, repository));
		REQUIRE(result->HasError());
		REQUIRE(StringUtil::Contains(result->GetError(), "doesn't have a valid signature"));

		prefixes.push_back(prefix);
		stubs.push_back(stub);
	}

	// a 384-byte signature never verifies against a 2048-bit key
	REQUIRE(!ExtensionHelper::CheckExtensionBufferSignature(instance, stubs[1].data(), stubs[1].size(),
	                                                        ExtensionRepositoryType::USER_PROVIDED, "signed256"));
	auto extension_dir =
	    fs.JoinPath(fs.JoinPath(prefixes[0], ExtensionHelper::GetVersionDirectoryName()), DuckDB::Platform());
	WriteBinaryFile(fs.JoinPath(extension_dir, "footer_wrong_key.duckdb_extension"), stubs[1]);
	auto result = con.Query("INSTALL footer_wrong_key FROM signed256");
	REQUIRE(result->HasError());
	REQUIRE(StringUtil::Contains(result->GetError(), "doesn't have a valid signature"));
}
