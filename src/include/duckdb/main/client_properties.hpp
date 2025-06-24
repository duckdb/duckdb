//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/client_properties.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/string.hpp"
#include "duckdb/common/types.hpp"

namespace duckdb {

enum class ArrowOffsetSize : uint8_t { REGULAR, LARGE };

enum ArrowFormatVersion : uint8_t {
	//! Base Version
	V1_0 = 10,
	//! Added 256-bit Decimal type.
	V1_1 = 11,
	//! Added MonthDayNano interval type.
	V1_2 = 12,
	//! Added Run-End Encoded Layout.
	V1_3 = 13,
	//! Added Variable-size Binary View Layout and the associated BinaryView and Utf8View types.
	//! Added ListView Layout and the associated ListView and LargeListView types. Added Variadic buffers.
	V1_4 = 14,
	//! Expanded Decimal type bit widths to allow 32-bit and 64-bit types.
	V1_5 = 15
};

//! A set of properties from the client context that can be used to interpret the query result
struct ClientProperties {
	ClientProperties(string time_zone_p, const ArrowOffsetSize arrow_offset_size_p, const bool arrow_use_list_view_p,
	                 const bool produce_arrow_string_view_p, const bool lossless_conversion,
	                 const ArrowFormatVersion arrow_output_version, const optional_ptr<ClientContext> client_context)
	    : time_zone(std::move(time_zone_p)), arrow_offset_size(arrow_offset_size_p),
	      arrow_use_list_view(arrow_use_list_view_p), produce_arrow_string_view(produce_arrow_string_view_p),
	      arrow_lossless_conversion(lossless_conversion), arrow_output_version(arrow_output_version),
	      client_context(client_context) {
	}

	string time_zone = "UTC";
	ArrowOffsetSize arrow_offset_size = ArrowOffsetSize::REGULAR;
	bool arrow_use_list_view = false;
	bool produce_arrow_string_view = false;
	bool arrow_lossless_conversion = false;
	ArrowFormatVersion arrow_output_version = V1_0;
	optional_ptr<ClientContext> client_context;
};
} // namespace duckdb
