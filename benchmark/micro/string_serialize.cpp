#include "benchmark_runner.hpp"
#include "duckdb_benchmark_macro.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/common/serializer/binary_serializer.hpp"
#include "duckdb/common/serializer/memory_stream.hpp"
#include "duckdb/common/serializer/binary_deserializer.hpp"

using namespace duckdb;

static Vector string_vector(LogicalType::VARCHAR, STANDARD_VECTOR_SIZE);

static void InitStringVector() {
	auto string_data = FlatVector::GetData<string_t>(string_vector);
	for (idx_t i = 0; i < STANDARD_VECTOR_SIZE; i++) {
		string_data[i] = StringVector::AddString(string_vector, StringUtil::Repeat("X", i % 100));
		if (i % 10 == 0) {
			FlatVector::SetNull(string_vector, i, true);
		}
	}
}

static void RunStringSerializationBenchmark(idx_t serialization_version) {
	for (idx_t i = 0; i < 10000; i++) {
		MemoryStream stream;
		SerializationOptions options;
		options.serialization_compatibility = SerializationCompatibility::FromIndex(serialization_version);
		BinarySerializer serializer(stream, options);
		serializer.Begin();
		string_vector.Serialize(serializer, STANDARD_VECTOR_SIZE, true);
		serializer.End();

		stream.SetPosition(0);
		BinaryDeserializer deserializer(stream);
		Vector result(LogicalType::VARCHAR, STANDARD_VECTOR_SIZE);
		result.Deserialize(deserializer, STANDARD_VECTOR_SIZE);
	}
}

DUCKDB_BENCHMARK(StringSerializationNew, "[string_serializer]")
void Load(DuckDBBenchmarkState *state) override {
	InitStringVector();
}
void RunBenchmark(DuckDBBenchmarkState *state) override {
	RunStringSerializationBenchmark(8);
}
string VerifyResult(QueryResult *result) override {
	return string();
}
string BenchmarkInfo() override {
	return "Test string serialization performance - new version";
}
FINISH_BENCHMARK(StringSerializationNew)

DUCKDB_BENCHMARK(StringSerializationOld, "[string_serializer]")
void Load(DuckDBBenchmarkState *state) override {
	InitStringVector();
}
void RunBenchmark(DuckDBBenchmarkState *state) override {
	RunStringSerializationBenchmark(7);
}
string VerifyResult(QueryResult *result) override {
	return string();
}
string BenchmarkInfo() override {
	return "Test string serialization performance - old version";
}
FINISH_BENCHMARK(StringSerializationOld)
