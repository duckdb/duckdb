#include "duckdb/common/enums/file_compression_type.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/exception/parser_exception.hpp"

namespace duckdb {

FileCompressionType FileCompressionTypeFromString(const string &input) {
	auto parameter = StringUtil::Lower(input);
	if (parameter == "infer" || parameter == "auto") {
		return FileCompressionType::AUTO_DETECT;
	} else if (parameter == "gzip") {
		return FileCompressionType::GZIP;
	} else if (parameter == "zstd") {
		return FileCompressionType::ZSTD;
	} else if (parameter == "uncompressed" || parameter == "none" || parameter.empty()) {
		return FileCompressionType::UNCOMPRESSED;
	} else {
		throw ParserException("Unrecognized file compression type \"%s\"", input);
	}
}

string CompressionExtensionFromType(const FileCompressionType type) {
	switch (type) {
	case FileCompressionType::GZIP:
		return ".gz";
	case FileCompressionType::ZSTD:
		return ".zst";
	default:
		throw NotImplementedException("Compression Extension of file compression type is not implemented");
	}
}

} // namespace duckdb
