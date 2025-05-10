//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/parsed_data/sequence_value_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/parsed_data/parse_info.hpp"

namespace duckdb {

struct SequenceValueInfo : public ParseInfo {
	static constexpr const ParseInfoType TYPE = ParseInfoType::SEQUENCE_VALUE_INFO;

public:
	SequenceValueInfo() : ParseInfo(TYPE) {};

	//! Schema name
	string schema;
	//! Sequence name
	string name;
	//! Usage count
	uint64_t usage_count;
	//! Counter value
	int64_t counter;

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<ParseInfo> Deserialize(Deserializer &deserializer);
};

} // namespace duckdb
