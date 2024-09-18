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

//! A set of properties from the client context that can be used to interpret the query result
struct ClientProperties {
	ClientProperties(string time_zone_p, ArrowOffsetSize arrow_offset_size_p, bool arrow_use_list_view_p,
	                 bool produce_arrow_string_view_p, bool lossless_conversion)
	    : time_zone(std::move(time_zone_p)), arrow_offset_size(arrow_offset_size_p),
	      arrow_use_list_view(arrow_use_list_view_p), arrow_lossless_conversion(lossless_conversion) {
	}
	ClientProperties() {};
	string time_zone = "UTC";
	ArrowOffsetSize arrow_offset_size = ArrowOffsetSize::REGULAR;
	bool arrow_use_list_view = false;
	bool produce_arrow_string_view = false;
	bool arrow_lossless_conversion = false;
};
} // namespace duckdb
