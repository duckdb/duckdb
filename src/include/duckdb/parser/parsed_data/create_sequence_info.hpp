//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/parsed_data/create_sequence_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/parsed_data/create_info.hpp"

#include <limits>

namespace duckdb {

struct CreateSequenceInfo : public CreateInfo {
	CreateSequenceInfo()
	    : CreateInfo(CatalogType::SEQUENCE), name(string()), usage_count(0), increment(1), min_value(1),
	      max_value(std::numeric_limits<int64_t>::max()), start_value(1), cycle(false) {
	}

	//! Sequence name to create
	string name;
	//! Usage count of the sequence
	uint64_t usage_count;
	//! The increment value
	int64_t increment;
	//! The minimum value of the sequence
	int64_t min_value;
	//! The maximum value of the sequence
	int64_t max_value;
	//! The start value of the sequence
	int64_t start_value;
	//! Whether or not the sequence cycles
	bool cycle;
};

} // namespace duckdb
