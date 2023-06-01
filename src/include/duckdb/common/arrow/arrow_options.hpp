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
	ArrowOffsetSize offset_size = ArrowOffsetSize::LARGE;
};
} // namespace duckdb
