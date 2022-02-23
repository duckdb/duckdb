#include "duckdb/parser/parser.hpp"

extern "C" int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size) {
	std::string input(reinterpret_cast<const char *>(data), size);
	duckdb::Parser parser;
	try {
		parser.ParseQuery(input);
	} catch (std::exception &e) {
	}
	return 0;
}
