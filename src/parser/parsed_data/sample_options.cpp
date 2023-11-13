#include "duckdb/parser/parsed_data/sample_options.hpp"
#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/common/serializer/deserializer.hpp"

namespace duckdb {

// **DEPRECATED**: Use EnumUtil directly instead.
string SampleMethodToString(SampleMethod method) {
	return EnumUtil::ToString(method);
}

unique_ptr<SampleOptions> SampleOptions::Copy() {
	auto result = make_uniq<SampleOptions>();
	result->sample_size = sample_size;
	result->is_percentage = is_percentage;
	result->method = method;
	result->seed = seed;
	return result;
}

bool SampleOptions::Equals(SampleOptions *a, SampleOptions *b) {
	if (a == b) {
		return true;
	}
	if (!a || !b) {
		return false;
	}
	if (a->sample_size != b->sample_size || a->is_percentage != b->is_percentage || a->method != b->method ||
	    a->seed != b->seed) {
		return false;
	}
	return true;
}

} // namespace duckdb
