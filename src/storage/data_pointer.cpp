#include "duckdb/storage/data_pointer.hpp"
#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/common/serializer/deserializer.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/function/compression_function.hpp"

namespace duckdb {

unique_ptr<ColumnSegmentState> ColumnSegmentState::Deserialize(Deserializer &deserializer) {
	auto compression_type = deserializer.Get<CompressionType>();
	auto &db = deserializer.Get<DatabaseInstance &>();
	auto &info = deserializer.Get<const CompressionInfo &>();

	auto compression_function = DBConfig::GetConfig(db).GetCompressionFunction(compression_type, info);
	if (!compression_function || !compression_function->deserialize_state) {
		throw SerializationException("Deserializing a ColumnSegmentState but could not find deserialize method");
	}
	return compression_function->deserialize_state(deserializer);
}

} // namespace duckdb
