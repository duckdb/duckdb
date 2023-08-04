//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/client_properties.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include <string>

namespace duckdb {
enum ArrowOffsetSize { REGULAR, LARGE };

//! A set of properties from the client context that can be used to interpret the query result
struct ClientProperties {
	ClientProperties(string time_zone_p, ArrowOffsetSize arrow_offset_size_p)
	    : time_zone(std::move(time_zone_p)), arrow_offset_size(arrow_offset_size_p) {
	}
	ClientProperties() {};
	string time_zone = "UTC";
	ArrowOffsetSize arrow_offset_size = ArrowOffsetSize::REGULAR;
};
} // namespace duckdb
