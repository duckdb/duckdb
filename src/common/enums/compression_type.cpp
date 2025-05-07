#include "duckdb/common/enums/compression_type.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/storage/storage_manager.hpp"

namespace duckdb {

// LCOV_EXCL_START

vector<string> ListCompressionTypes(void) {
	vector<string> compression_types;
	uint8_t amount_of_compression_options = (uint8_t)CompressionType::COMPRESSION_COUNT;
	compression_types.reserve(amount_of_compression_options);
	for (uint8_t i = 0; i < amount_of_compression_options; i++) {
		compression_types.push_back(CompressionTypeToString((CompressionType)i));
	}
	return compression_types;
}

bool CompressionTypeIsDeprecated(CompressionType compression_type, optional_ptr<StorageManager> storage_manager) {
	vector<CompressionType> types({CompressionType::COMPRESSION_PATAS, CompressionType::COMPRESSION_CHIMP});
	if (storage_manager) {
		if (storage_manager->GetStorageVersion() >= 5) {
			//! NOTE: storage_manager is an optional_ptr because it's called from ForceCompressionSetting, which doesn't
			//! have guaranteed access to a StorageManager The introduction of DICT_FSST deprecates Dictionary and FSST
			//! compression methods
			types.emplace_back(CompressionType::COMPRESSION_DICTIONARY);
			types.emplace_back(CompressionType::COMPRESSION_FSST);
		} else {
			types.emplace_back(CompressionType::COMPRESSION_DICT_FSST);
		}
	}
	for (auto &type : types) {
		if (type == compression_type) {
			return true;
		}
	}
	return false;
}

CompressionType CompressionTypeFromString(const string &str) {
	auto compression = StringUtil::Lower(str);
	//! NOTE: this explicitly does not include 'constant' and 'empty validity', these are internal compression functions
	//! not general purpose
	if (compression == "uncompressed") {
		return CompressionType::COMPRESSION_UNCOMPRESSED;
	} else if (compression == "rle") {
		return CompressionType::COMPRESSION_RLE;
	} else if (compression == "dictionary") {
		return CompressionType::COMPRESSION_DICTIONARY;
	} else if (compression == "pfor") {
		return CompressionType::COMPRESSION_PFOR_DELTA;
	} else if (compression == "bitpacking") {
		return CompressionType::COMPRESSION_BITPACKING;
	} else if (compression == "fsst") {
		return CompressionType::COMPRESSION_FSST;
	} else if (compression == "chimp") {
		return CompressionType::COMPRESSION_CHIMP;
	} else if (compression == "patas") {
		return CompressionType::COMPRESSION_PATAS;
	} else if (compression == "zstd") {
		return CompressionType::COMPRESSION_ZSTD;
	} else if (compression == "alp") {
		return CompressionType::COMPRESSION_ALP;
	} else if (compression == "alprd") {
		return CompressionType::COMPRESSION_ALPRD;
	} else if (compression == "roaring") {
		return CompressionType::COMPRESSION_ROARING;
	} else if (compression == "dict_fsst") {
		return CompressionType::COMPRESSION_DICT_FSST;
	} else {
		return CompressionType::COMPRESSION_AUTO;
	}
}

string CompressionTypeToString(CompressionType type) {
	switch (type) {
	case CompressionType::COMPRESSION_AUTO:
		return "Auto";
	case CompressionType::COMPRESSION_UNCOMPRESSED:
		return "Uncompressed";
	case CompressionType::COMPRESSION_CONSTANT:
		return "Constant";
	case CompressionType::COMPRESSION_RLE:
		return "RLE";
	case CompressionType::COMPRESSION_DICTIONARY:
		return "Dictionary";
	case CompressionType::COMPRESSION_PFOR_DELTA:
		return "PFOR";
	case CompressionType::COMPRESSION_BITPACKING:
		return "BitPacking";
	case CompressionType::COMPRESSION_FSST:
		return "FSST";
	case CompressionType::COMPRESSION_CHIMP:
		return "Chimp";
	case CompressionType::COMPRESSION_PATAS:
		return "Patas";
	case CompressionType::COMPRESSION_ZSTD:
		return "ZSTD";
	case CompressionType::COMPRESSION_ALP:
		return "ALP";
	case CompressionType::COMPRESSION_ALPRD:
		return "ALPRD";
	case CompressionType::COMPRESSION_ROARING:
		return "Roaring";
	case CompressionType::COMPRESSION_DICT_FSST:
		return "DICT_FSST";
	case CompressionType::COMPRESSION_EMPTY:
		return "Empty Validity";
	default:
		throw InternalException("Unrecognized compression type!");
	}
}
// LCOV_EXCL_STOP

} // namespace duckdb
