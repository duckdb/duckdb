#include "duckdb/parser/parsed_data/detach_info.hpp"
#include "duckdb/common/field_writer.hpp"

namespace duckdb {

DetachInfo::DetachInfo() : ParseInfo(TYPE) {
}

unique_ptr<DetachInfo> DetachInfo::Copy() const {
	auto result = make_uniq<DetachInfo>();
	result->name = name;
	result->if_not_found = if_not_found;
	return result;
}

void DetachInfo::Serialize(Serializer &serializer) const {
	FieldWriter writer(serializer);
	writer.WriteString(name);
	writer.WriteField(if_not_found);
	writer.Finalize();
}

unique_ptr<ParseInfo> DetachInfo::Deserialize(Deserializer &deserializer) {
	auto result = make_uniq<DetachInfo>();

	FieldReader reader(deserializer);
	result->name = reader.ReadRequired<string>();
	result->if_not_found = reader.ReadRequired<OnEntryNotFound>();
	reader.Finalize();

	return std::move(result);
}

} // namespace duckdb
