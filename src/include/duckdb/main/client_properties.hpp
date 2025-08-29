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
#include "duckdb/common/enums/arrow_format_version.hpp"

namespace duckdb {

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
	ClientProperties() {};

	string time_zone = "UTC";
	ArrowOffsetSize arrow_offset_size = ArrowOffsetSize::REGULAR;
	bool arrow_use_list_view = false;
	bool produce_arrow_string_view = false;
	bool arrow_lossless_conversion = false;
	ArrowFormatVersion arrow_output_version = ArrowFormatVersion::V1_0;
	optional_ptr<ClientContext> client_context;
};
} // namespace duckdb
