#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/common/field_writer.hpp"

namespace duckdb {
void BoundCreateTableInfo::Serialize(Serializer &serializer) const {
	serializer.WriteOptional(base);
	serializer.WriteList(constraints);
	serializer.WriteList(bound_constraints);
	serializer.WriteList(bound_defaults);
	serializer.WriteOptional(query);
}

unique_ptr<BoundCreateTableInfo> BoundCreateTableInfo::Deserialize(Deserializer &source,
                                                                   PlanDeserializationState &state) {
	auto create_info_base = source.ReadOptional<CreateInfo>();
	// Get schema from the catalog to create BoundCreateTableInfo
	auto schema_name = create_info_base->schema;
	auto catalog = create_info_base->catalog;
	auto &schema_catalog_entry = Catalog::GetSchema(state.context, catalog, schema_name);

	auto result = make_uniq<BoundCreateTableInfo>(schema_catalog_entry, std::move(create_info_base));
	source.ReadList<Constraint>(result->constraints);
	source.ReadList<BoundConstraint>(result->bound_constraints);
	source.ReadList<Expression>(result->bound_defaults, state);
	result->query = source.ReadOptional<LogicalOperator>(state);
	return result;
}
} // namespace duckdb
