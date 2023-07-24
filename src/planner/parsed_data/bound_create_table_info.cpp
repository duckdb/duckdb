#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/common/field_writer.hpp"
#include "duckdb/common/serializer/format_serializer.hpp"
#include "duckdb/common/serializer/format_deserializer.hpp"

namespace duckdb {

void BoundCreateTableInfo::Serialize(Serializer &serializer) const {
	serializer.WriteOptional(base);
}

unique_ptr<BoundCreateTableInfo> BoundCreateTableInfo::Deserialize(Deserializer &source,
                                                                   PlanDeserializationState &state) {
	auto info = source.ReadOptional<CreateInfo>();
	auto binder = Binder::CreateBinder(state.context);
	auto bound_info = binder->BindCreateTableInfo(std::move(info));
	return bound_info;
}

} // namespace duckdb
