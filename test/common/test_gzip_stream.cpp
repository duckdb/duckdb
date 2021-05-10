// #include "catch.hpp"
// #include "duckdb/common/file_system.hpp"
// #include "duckdb/common/fstream_util.hpp"
// #include "duckdb/common/gzip_stream.hpp"
// #include "test_helpers.hpp"

// using namespace duckdb;
// using namespace std;

// unsigned char test_txt_gz[] = {0x1f, 0x8b, 0x08, 0x08, 0x9a, 0x57, 0xc8, 0x5c, 0x00, 0x03, 0x74, 0x65, 0x73, 0x74,
//                                0x2e, 0x74, 0x78, 0x74, 0x00, 0xf3, 0x48, 0xcd, 0xc9, 0xc9, 0xd7, 0x51, 0x08, 0xcf,
//                                0x2f, 0xca, 0x49, 0xe1, 0x02, 0x00, 0x90, 0x3a, 0xf6, 0x40, 0x0d, 0x00, 0x00, 0x00};

// unsigned int test_txt_gz_len = 42;

// TEST_CASE("Test basic stream read from GZIP files", "[gzip_stream]") {
// 	string gzip_file_path = TestCreatePath("test.txt.gz");

// 	ofstream ofp(gzip_file_path, ios::out | ios::binary);
// 	ofp.write((const char *)test_txt_gz, test_txt_gz_len);
// 	ofp.close();

// 	GzipStream gz(gzip_file_path);
// 	std::string s(istreambuf_iterator<char>(gz), {});
// 	REQUIRE(s == "Hello, World\n");

// 	std::ofstream ofp2(gzip_file_path, ios::out | ios::binary);
// 	ofp2.write((const char *)test_txt_gz, 5); // header too short
// 	ofp2.close();

// 	GzipStream gz2(gzip_file_path);
// 	REQUIRE_THROWS(s = string(std::istreambuf_iterator<char>(gz2), {}));

// 	GzipStream gz3("XXX_THIS_DOES_NOT_EXIST");
// 	REQUIRE_THROWS(s = string(std::istreambuf_iterator<char>(gz3), {}));
// }
