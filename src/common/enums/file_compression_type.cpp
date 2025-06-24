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

bool IsFileCompressed(string path, FileCompressionType type) {
	auto extension = CompressionExtensionFromType(type);
	std::size_t question_mark_pos = std::string::npos;
	if (!StringUtil::StartsWith(path, "\\\\?\\")) {
		question_mark_pos = path.find('?');
	}
	path = path.substr(0, question_mark_pos);
	if (StringUtil::EndsWith(path, extension)) {
		return true;
	}
	return false;
}

} // namespace duckdb
