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

void SampleOptions::Serialize(Serializer &serializer) const {
	serializer.WriteProperty<Value>(100, "sample_size", sample_size);
	serializer.WritePropertyWithDefault<bool>(101, "is_percentage", is_percentage);
	serializer.WriteProperty<SampleMethod>(102, "method", method);

	if (seed.IsValid()) {
		serializer.WriteProperty<int64_t>(103, "seed", static_cast<int64_t>(seed.GetIndex()));
	} else {
		serializer.WriteProperty<int64_t>(103, "seed", -1);
	}
}

unique_ptr<SampleOptions> SampleOptions::Deserialize(Deserializer &deserializer) {
	auto result = duckdb::unique_ptr<SampleOptions>(new SampleOptions());
	deserializer.ReadProperty<Value>(100, "sample_size", result->sample_size);
	deserializer.ReadPropertyWithDefault<bool>(101, "is_percentage", result->is_percentage);
	deserializer.ReadProperty<SampleMethod>(102, "method", result->method);
	int64_t seed;
	deserializer.ReadProperty<int64_t>(103, "seed", seed);
	if (seed != -1) {
		result->seed = static_cast<idx_t>(seed);
	}
	return result;
}

} // namespace duckdb
