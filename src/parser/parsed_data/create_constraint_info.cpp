#include "duckdb/parser/parsed_data/create_constraint_info.hpp"

#include "duckdb/common/field_writer.hpp"

namespace duckdb {

unique_ptr<CreateInfo> CreateConstraintInfo::Copy() const {
	auto result = make_uniq<CreateConstraintInfo>();
	CopyProperties(*result);

	result->constraint_name = constraint_name;
	result->table = unique_ptr_cast<TableRef, BaseTableRef>(table->Copy());

	return std::move(result);
}

void CreateConstraintInfo::SerializeInternal(Serializer &serializer) const {
	FieldWriter writer(serializer);
	writer.WriteString(constraint_name);

	writer.Finalize();
}

unique_ptr<CreateConstraintInfo> CreateConstraintInfo::Deserialize(Deserializer &deserializer) {
	auto result = make_uniq<CreateConstraintInfo>();
	result->DeserializeBase(deserializer);

	FieldReader reader(deserializer);
	result->constraint_name = reader.ReadRequired<string>();

	reader.Finalize();
	return result;
}
} // namespace duckdb
