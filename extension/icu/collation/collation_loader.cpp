#include "collation_data.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "zstd.h"

namespace duckdb {
namespace collation {

//! Reads the arrays of a decompressed unit. The unit starts with the number of elements of
//! every array, followed by the arrays themselves, largest element type first, so that
//! every array is aligned.
class UnitReader {
public:
	UnitReader(const uint8_t *data, uint32_t size, uint32_t array_count)
	    : counts(reinterpret_cast<const uint32_t *>(data)), position(data), end(data + size), array(0) {
		auto header = array_count * sizeof(uint32_t);
		header += (8 - header % 8) % 8;
		position += header;
	}

	//! The next array, count_p is set to the number of elements
	template <class T>
	const T *Read(uint32_t &count_p) {
		count_p = counts[array++];
		auto result = reinterpret_cast<const T *>(position);
		position += count_p * sizeof(T);
		if (position > end) {
			throw InternalException("Collation data is corrupt");
		}
		return result;
	}

	template <class T>
	const T *Read() {
		uint32_t count_p;
		return Read<T>(count_p);
	}

	//! An array of structs of 32-bit fields
	template <class T>
	const T *ReadStruct(uint32_t &count_p) {
		count_p = counts[array++];
		auto result = reinterpret_cast<const T *>(position);
		position += count_p * sizeof(T);
		if (position > end) {
			throw InternalException("Collation data is corrupt");
		}
		return result;
	}

	template <class T>
	const T *ReadStruct() {
		uint32_t count_p;
		return ReadStruct<T>(count_p);
	}

private:
	const uint32_t *counts;
	const uint8_t *position;
	const uint8_t *end;
	uint32_t array;
};

//! Keeps the units that have been decompressed, they are used until the process ends
class UnitCache {
public:
	static UnitCache &Get() {
		static UnitCache cache;
		return cache;
	}

	//! Decompresses a unit, or returns it when it was decompressed before
	const uint8_t *Load(uint32_t unit) {
		lock_guard<mutex> guard(lock);
		auto entry = units.find(unit);
		if (entry != units.end()) {
			return entry->second.get();
		}
		auto &compressed = collation_units[unit];
		auto data = unique_ptr<uint8_t[]>(new uint8_t[compressed.size]);
		auto size = duckdb_zstd::ZSTD_decompress_usingDDict(GetContext(), data.get(), compressed.size, compressed.data,
		                                                    compressed.compressed_size, GetDict());
		if (duckdb_zstd::ZSTD_isError(size) || size != compressed.size) {
			throw InternalException("Failed to decompress the collation data");
		}
		auto result = data.get();
		units[unit] = std::move(data);
		return result;
	}

private:
	//! The dictionary all the units are compressed against
	duckdb_zstd::ZSTD_DDict *GetDict() {
		if (!dict) {
			dict = duckdb_zstd::ZSTD_createDDict(collation_dictionary, collation_dictionary_size);
			if (!dict) {
				throw InternalException("Failed to load the collation dictionary");
			}
		}
		return dict;
	}
	duckdb_zstd::ZSTD_DCtx *GetContext() {
		if (!context) {
			context = duckdb_zstd::ZSTD_createDCtx();
			if (!context) {
				throw InternalException("Failed to create a decompression context");
			}
		}
		return context;
	}

private:
	mutex lock;
	unordered_map<uint32_t, unique_ptr<uint8_t[]>> units;
	duckdb_zstd::ZSTD_DDict *dict = nullptr;
	duckdb_zstd::ZSTD_DCtx *context = nullptr;
};

static constexpr uint32_t ROOT_ARRAY_COUNT = 13;
static constexpr uint32_t NORMALIZATION_ARRAY_COUNT = 10;
static constexpr uint32_t TAILORING_ARRAY_COUNT = 8;

const CollationRoot &GetCollationRoot() {
	// the root collation is read once, the initialization of a local static is thread safe
	static const CollationRoot root = []() {
		auto &unit = collation_units[COLLATION_ROOT_UNIT];
		UnitReader reader(UnitCache::Get().Load(COLLATION_ROOT_UNIT), unit.size, ROOT_ARRAY_COUNT);
		CollationRoot result {};
		result.table.ces = reader.Read<collation_element_t>();
		result.table.trie_stage1 = reader.Read<uint32_t>();
		result.table.trie_stage2 = reader.Read<uint32_t>();
		result.table.expansions = reader.ReadStruct<CollationExpansion>();
		result.table.entries = reader.ReadStruct<CollationEntry>();
		result.table.contexts = reader.ReadStruct<CollationContext>();
		result.table.context_chars = reader.Read<uint32_t>();
		result.han_block_lower = reader.Read<uint32_t>();
		result.han_block_upper = reader.Read<uint32_t>();
		result.han_range_start = reader.Read<uint32_t>();
		result.han_range_index = reader.Read<uint32_t>();
		result.han_range_length = reader.Read<uint16_t>();
		result.compressible_lead_byte = reader.Read<uint8_t>();
		return result;
	}();
	return root;
}

const CollationNormalization &GetCollationNormalization() {
	static const CollationNormalization normalization = []() {
		auto &unit = collation_units[COLLATION_NORMALIZATION_UNIT];
		UnitReader reader(UnitCache::Get().Load(COLLATION_NORMALIZATION_UNIT), unit.size, NORMALIZATION_ARRAY_COUNT);
		CollationNormalization result {};
		result.decomposition_chars = reader.Read<uint32_t>();
		result.decomposition_codepoint = reader.Read<uint32_t>(result.decomposition_count);
		result.decomposition_offset = reader.Read<uint32_t>();
		result.combining_class_start = reader.Read<uint32_t>(result.combining_class_count);
		result.combining_class_end = reader.Read<uint32_t>();
		result.fcd_start = reader.Read<uint32_t>(result.fcd_count);
		result.fcd_end = reader.Read<uint32_t>();
		result.fcd_value = reader.Read<uint16_t>();
		result.decomposition_length = reader.Read<uint8_t>();
		result.combining_class_value = reader.Read<uint8_t>();
		return result;
	}();
	return normalization;
}

const CollationTailoring &GetCollationTailoring(uint32_t unit) {
	static mutex lock;
	static unordered_map<uint32_t, unique_ptr<CollationTailoring>> tailorings;

	lock_guard<mutex> guard(lock);
	auto entry = tailorings.find(unit);
	if (entry != tailorings.end()) {
		return *entry->second;
	}
	UnitReader reader(UnitCache::Get().Load(unit), collation_units[unit].size, TAILORING_ARRAY_COUNT);
	auto tailoring = make_uniq<CollationTailoring>();
	tailoring->table.trie_stage1 = nullptr;
	tailoring->table.trie_stage2 = nullptr;
	tailoring->table.ces = reader.Read<collation_element_t>();
	tailoring->table.expansions = reader.ReadStruct<CollationExpansion>();
	tailoring->table.entries = reader.ReadStruct<CollationEntry>();
	tailoring->table.contexts = reader.ReadStruct<CollationContext>();
	tailoring->table.context_chars = reader.Read<uint32_t>();
	tailoring->codepoints = reader.Read<uint32_t>(tailoring->count);
	tailoring->values = reader.Read<uint32_t>();
	uint32_t reorder_count;
	auto reorder = reader.Read<uint8_t>(reorder_count);
	tailoring->reorder_table = reorder_count ? reorder : nullptr;

	auto &result = *tailoring;
	tailorings[unit] = std::move(tailoring);
	return result;
}

} // namespace collation
} // namespace duckdb
