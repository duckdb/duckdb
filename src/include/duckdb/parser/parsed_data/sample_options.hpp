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

enum class SampleMethod : uint8_t { INVALID = 0, SYSTEM_SAMPLE = 1, BERNOULLI_SAMPLE = 2, RESERVOIR_SAMPLE = 3 };

// **DEPRECATED**: Use EnumUtil directly instead.
string SampleMethodToString(SampleMethod method);

class SampleOptions {

public:
	explicit SampleOptions(int64_t seed_ = -1);

	Value sample_size;
	bool is_percentage;
	SampleMethod method;
	optional_idx seed = optional_idx::Invalid();

	unique_ptr<SampleOptions> Copy();
	void SetSeed(idx_t new_seed);
	static bool Equals(SampleOptions *a, SampleOptions *b);
	void Serialize(Serializer &serializer) const;
	static unique_ptr<SampleOptions> Deserialize(Deserializer &deserializer);
	int64_t GetSeed() const;
};

} // namespace duckdb
