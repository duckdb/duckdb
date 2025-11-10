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

namespace {
struct CompressionMethodRequirements {
	CompressionType type;
	optional_idx minimum_storage_version;
	optional_idx maximum_storage_version;
};
} // namespace

CompressionAvailabilityResult CompressionTypeIsAvailable(CompressionType compression_type,
                                                         optional_ptr<StorageManager> storage_manager) {
	//! Max storage compatibility
	vector<CompressionMethodRequirements> candidates({{CompressionType::COMPRESSION_PATAS, optional_idx(), 0},
	                                                  {CompressionType::COMPRESSION_CHIMP, optional_idx(), 0},
	                                                  {CompressionType::COMPRESSION_DICTIONARY, 0, 4},
	                                                  {CompressionType::COMPRESSION_FSST, 0, 4},
	                                                  {CompressionType::COMPRESSION_DICT_FSST, 5, optional_idx()}});

	optional_idx current_storage_version;
	if (storage_manager && storage_manager->HasStorageVersion()) {
		current_storage_version = storage_manager->GetStorageVersion();
	}
	for (auto &candidate : candidates) {
		auto &type = candidate.type;
		if (type != compression_type) {
			continue;
		}
		auto &min = candidate.minimum_storage_version;
		auto &max = candidate.maximum_storage_version;

		if (!min.IsValid()) {
			//! Used to signal: always deprecated
			return CompressionAvailabilityResult::Deprecated();
		}

		if (!current_storage_version.IsValid()) {
			//! Can't determine in this call whether it's available or not, default to available
			return CompressionAvailabilityResult();
		}

		auto current_version = current_storage_version.GetIndex();
		D_ASSERT(min.IsValid());
		if (min.GetIndex() > current_version) {
			//! Minimum required storage version is higher than the current storage version, this method isn't available
			//! yet
			return CompressionAvailabilityResult::NotAvailableYet();
		}
		if (max.IsValid() && max.GetIndex() < current_version) {
			//! Maximum supported storage version is lower than the current storage version, this method is no longer
			//! available
			return CompressionAvailabilityResult::Deprecated();
		}
		return CompressionAvailabilityResult();
	}
	return CompressionAvailabilityResult();
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
