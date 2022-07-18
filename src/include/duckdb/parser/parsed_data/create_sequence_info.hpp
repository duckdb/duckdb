//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/parsed_data/create_sequence_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/parsed_data/create_info.hpp"
#include "duckdb/common/limits.hpp"

namespace duckdb {

enum class SequenceInfo : uint8_t {
	// Sequence start
	SEQ_START,
	// Sequence increment
	SEQ_INC,
	// Sequence minimum value
	SEQ_MIN,
	// Sequence maximum value
	SEQ_MAX,
	// Sequence cycle option
	SEQ_CYCLE,
	// Sequence owner table
	SEQ_OWN
};

struct CreateSequenceInfo : public CreateInfo {
	CreateSequenceInfo()
	    : CreateInfo(CatalogType::SEQUENCE_ENTRY, INVALID_SCHEMA), name(string()), usage_count(0), increment(1),
	      min_value(1), max_value(NumericLimits<int64_t>::Maximum()), start_value(1), cycle(false) {
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

public:
	unique_ptr<CreateInfo> Copy() const override {
		auto result = make_unique<CreateSequenceInfo>();
		CopyProperties(*result);
		result->name = name;
		result->schema = schema;
		result->usage_count = usage_count;
		result->increment = increment;
		result->min_value = min_value;
		result->max_value = max_value;
		result->start_value = start_value;
		result->cycle = cycle;
		return move(result);
	}
};

} // namespace duckdb
