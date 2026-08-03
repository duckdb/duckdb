#include "tz_data.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "zstd.h"

namespace duckdb {
namespace datetime {

//! The layout of a decompressed unit: the counts and the scalars, then the arrays with the
//! widest element first, which leaves every one of them aligned without padding between them
struct TZUnitHeader {
	uint32_t transition_count;
	uint32_t type_count;
	uint32_t link_count;
	int32_t final_raw;
	int32_t final_year;
	int32_t final_rule;
};

//! Keeps the zones that have been decompressed, which are used until the process ends
class ZoneCache {
public:
	static ZoneCache &Get() {
		static ZoneCache cache;
		return cache;
	}

	const TZZoneData &Load(uint16_t data_index) {
		lock_guard<mutex> guard(lock);
		auto entry = zones.find(data_index);
		if (entry != zones.end()) {
			return *entry->second;
		}

		auto &unit = TZ_UNITS[data_index];
		auto buffer = unique_ptr<uint8_t[]>(new uint8_t[unit.size]);
		const auto size = duckdb_zstd::ZSTD_decompress_usingDDict(GetContext(), buffer.get(), unit.size, unit.data,
		                                                          unit.compressed_size, GetDict());
		if (duckdb_zstd::ZSTD_isError(size) || size != unit.size) {
			throw InternalException("Failed to decompress the time zone data");
		}

		auto data = make_uniq<TZZoneData>();
		auto position = buffer.get();
		const auto header = reinterpret_cast<const TZUnitHeader *>(position);
		position += sizeof(TZUnitHeader);

		data->transition_count = header->transition_count;
		data->type_count = uint16_t(header->type_count);
		data->link_count = uint16_t(header->link_count);
		data->final_raw = header->final_raw;
		data->final_year = header->final_year;
		data->final_rule = int16_t(header->final_rule);

		data->transitions = reinterpret_cast<const int64_t *>(position);
		position += header->transition_count * sizeof(int64_t);
		data->type_offsets = reinterpret_cast<const TZTypeOffset *>(position);
		position += header->type_count * sizeof(TZTypeOffset);
		data->links = reinterpret_cast<const uint16_t *>(position);
		position += header->link_count * sizeof(uint16_t);
		data->type_map = position;
		position += header->transition_count;
		if (position != buffer.get() + unit.size) {
			throw InternalException("The time zone data is corrupt");
		}

		auto &result = *data;
		buffers[data_index] = std::move(buffer);
		zones[data_index] = std::move(data);
		return result;
	}

private:
	//! The dictionary that all the units are compressed against
	duckdb_zstd::ZSTD_DDict *GetDict() {
		if (!dict) {
			dict = duckdb_zstd::ZSTD_createDDict(TZ_DICTIONARY, TZ_DICTIONARY_SIZE);
			if (!dict) {
				throw InternalException("Failed to load the time zone dictionary");
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
	//! The decompressed bytes, which the pointers of the zone data point into
	unordered_map<uint16_t, unique_ptr<uint8_t[]>> buffers;
	unordered_map<uint16_t, unique_ptr<TZZoneData>> zones;
	duckdb_zstd::ZSTD_DDict *dict = nullptr;
	duckdb_zstd::ZSTD_DCtx *context = nullptr;
};

const TZZoneData &GetZoneData(uint16_t data_index) {
	return ZoneCache::Get().Load(data_index);
}

} // namespace datetime
} // namespace duckdb
