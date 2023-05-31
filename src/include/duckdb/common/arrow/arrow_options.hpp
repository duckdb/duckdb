//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/arrow/arrow_options.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

namespace duckdb {

enum ArrowOffsetSize { REGULAR, LARGE };

struct ArrowOptions {
	explicit ArrowOptions(ArrowOffsetSize offset_size_p) : offset_size(offset_size_p) {
	}
	ArrowOptions(ArrowOffsetSize offset_size_p, string timezone_p) : offset_size(offset_size_p), timezone(timezone_p) {
	}
	ArrowOptions() {
	}
	ArrowOffsetSize offset_size = ArrowOffsetSize::REGULAR;
	string timezone = "UTC";
};
} // namespace duckdb
