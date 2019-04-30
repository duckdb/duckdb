#include "catch.hpp"
#include "common/fstream_util.hpp"
#include "common/gzip_stream.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test stream read from GZIP files", "[gzip_stream]") {
	GzipStream gz("/Users/hannes/Desktop/test2.txt.gz");
	std::string s(std::istreambuf_iterator<char>(gz), {});
	cout << s;
}
