#include "duckdb/common/enums/file_compression_type.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/exception.hpp"

namespace duckdb {

const FileCompressionType FileCompressionType::AUTO_DETECT = FileCompressionType("auto_detect");
const FileCompressionType FileCompressionType::UNCOMPRESSED = FileCompressionType("uncompressed");
const FileCompressionType FileCompressionType::GZIP = FileCompressionType("gzip");
const FileCompressionType FileCompressionType::ZSTD = FileCompressionType("zstd");

FileCompressionType::FileCompressionType(string compression_p) : compression(StringUtil::Lower(compression_p)) {
	if (compression == "infer" || compression == "auto") {
		compression = "auto_detect";
	} else if (compression == "none" || compression.empty()) {
		compression = "uncompressed";
	}
}

FileCompressionType FileCompressionTypeFromString(const string &input) {
	return FileCompressionType(input);
}

string CompressionExtensionFromType(const FileCompressionType &type) {
	if (type == FileCompressionType::GZIP) {
		return ".gz";
	}
	if (type == FileCompressionType::ZSTD) {
		return ".zst";
	}
	throw NotImplementedException("Compression Extension of file compression type is not implemented");
}

bool IsFileCompressed(string path, const FileCompressionType &type) {
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
