#include "duckdb/parser/parsed_data/sample_options.hpp"
#include "duckdb/common/serializer.hpp"

namespace duckdb {

string SampleMethodToString(SampleMethod method) {
	switch (method) {
	case SampleMethod::SYSTEM_SAMPLE:
		return "System";
	case SampleMethod::BERNOULLI_SAMPLE:
		return "Bernoulli";
	case SampleMethod::RESERVOIR_SAMPLE:
		return "Reservoir";
	default:
		return "Unknown";
	}
}

void SampleOptions::Serialize(Serializer &serializer) {
	sample_size.Serialize(serializer);
	serializer.Write<bool>(is_percentage);
	serializer.Write<SampleMethod>(method);
	serializer.Write<int64_t>(seed);
}

unique_ptr<SampleOptions> SampleOptions::Deserialize(Deserializer &source) {
	auto result = make_unique<SampleOptions>();
	result->sample_size = Value::Deserialize(source);
	result->is_percentage = source.Read<bool>();
	result->method = source.Read<SampleMethod>();
	result->seed = source.Read<int64_t>();
	return result;
}

unique_ptr<SampleOptions> SampleOptions::Copy() {
	auto result = make_unique<SampleOptions>();
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
