//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/parsed_data/sample_options.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/parser/parsed_expression.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/types/value.hpp"

namespace duckdb {

enum class SampleMethod : uint8_t { SYSTEM_SAMPLE = 0, BERNOULLI_SAMPLE = 1, RESERVOIR_SAMPLE = 2 };

// **DEPRECATED**: Use EnumUtil directly instead.
string SampleMethodToString(SampleMethod method);

struct SampleOptions {
	Value sample_size;
	bool is_percentage;
	SampleMethod method;
	int64_t seed = -1;

	unique_ptr<SampleOptions> Copy();
	static bool Equals(SampleOptions *a, SampleOptions *b);
	void Serialize(Serializer &serializer) const;
	static unique_ptr<SampleOptions> Deserialize(Deserializer &deserializer);
};

} // namespace duckdb
