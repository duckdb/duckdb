//===----------------------------------------------------------------------===//
//                         DuckDB
//
// tz_data.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"

namespace duckdb {
namespace datetime {

//! A pair of offsets (in seconds) that is in effect over some interval of a zone
struct TZTypeOffset {
	int32_t raw_offset;
	int32_t dst_offset;
};

//! A recurring daylight savings rule that applies to a zone after its last explicit transition.
//! Days are encoded the same way as the IANA "ON" field: a positive day of the week selects the
//! n-th such day of the month, a negative day of the week selects the first (positive day) or
//! last (negative day) such day relative to the day of the month.
struct TZRule {
	int8_t start_month;
	int8_t start_day;
	int8_t start_day_of_week;
	int32_t start_time;
	int8_t start_time_mode;
	int8_t end_month;
	int8_t end_day;
	int8_t end_day_of_week;
	int32_t end_time;
	int8_t end_time_mode;
	int32_t dst_savings;
};

//! The transition data of a single zone. Zones that are aliases of one another share their data.
struct TZZoneData {
	//! Transition times in seconds since the epoch, in ascending order
	const int64_t *transitions;
	uint32_t transition_count;
	//! The offsets in effect between the transitions - the first entry is the initial offset
	const TZTypeOffset *type_offsets;
	uint16_t type_count;
	//! For every transition, the index of the offsets that take effect at that transition
	const uint8_t *type_map;
	//! The indexes of all zones sharing this data, including the zone that owns it
	const uint16_t *links;
	uint16_t link_count;
	//! The recurring rule that applies from final_year onwards, or -1 if the data is complete
	int16_t final_rule;
	int32_t final_raw;
	int32_t final_year;
};

//! A single time zone identifier. Multiple identifiers can refer to the same data.
struct TZZone {
	const char *name;
	uint16_t data_index;
};

//! The zone identifiers, sorted lexicographically by name
extern const TZZone TZ_ZONES[];
extern const idx_t TZ_ZONE_COUNT;
//! The zone data, indexed by TZZone::data_index
extern const TZZoneData TZ_ZONE_DATA[];
//! The recurring rules, indexed by TZZoneData::final_rule
extern const TZRule TZ_RULES[];
//! The IANA release that the data was generated from
extern const char *const TZ_VERSION;

} // namespace datetime
} // namespace duckdb
