#include "duckdb/parser/parsed_data/sample_options.hpp"
#include "duckdb/common/enum_util.hpp"
#include "duckdb/common/to_string.hpp"

namespace duckdb {

// **DEPRECATED**: Use EnumUtil directly instead.
string SampleMethodToString(SampleMethod method) {
	return EnumUtil::ToString(method);
}

SampleOptions::SampleOptions(int64_t seed_) {
	repeatable = false;
	if (seed_ >= 0) {
		seed = static_cast<idx_t>(seed_);
	}
	sample_size = 0;
	is_percentage = false;
	method = SampleMethod::INVALID;
}

unique_ptr<SampleOptions> SampleOptions::Copy() {
	auto result = make_uniq<SampleOptions>();
	result->sample_size = sample_size;
	result->is_percentage = is_percentage;
	result->method = method;
	result->seed = seed;
	result->repeatable = repeatable;
	return result;
}

void SampleOptions::SetSeed(idx_t new_seed) {
	seed = new_seed;
}

bool SampleOptions::Equals(SampleOptions *a, SampleOptions *b) {
	if (a == b) {
		return true;
	}
	if (!a || !b) {
		return false;
	}
	// if only one is valid, they are not equal
	if (a->seed.IsValid() != b->seed.IsValid()) {
		return false;
	}
	// if both are invalid, then they are technically the same
	if (!a->seed.IsValid() && !b->seed.IsValid()) {
		return true;
	}
	if (a->sample_size != b->sample_size || a->is_percentage != b->is_percentage || a->method != b->method ||
	    a->seed.GetIndex() != b->seed.GetIndex()) {
		return false;
	}
	return true;
}

int64_t SampleOptions::GetSeed() const {
	if (seed.IsValid()) {
		return static_cast<int64_t>(seed.GetIndex());
	}
	return -1;
}

} // namespace duckdb
