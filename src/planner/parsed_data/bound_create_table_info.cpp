#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/common/field_writer.hpp"

namespace duckdb {
void BoundCreateTableInfo::Serialize(Serializer &serializer) const {
	schema.Serialize(serializer);
	serializer.WriteOptional(base);

	// TODO[YLM]: Review if we want/need to serialize more of the fields.
	//! The map of column names -> column index, used during binding
	// case_insensitive_map_t<column_t> name_map;

	//! Column dependency manager of the table
	// ColumnDependencyManager column_dependency_manager;

	serializer.WriteList(constraints);
	serializer.WriteList(bound_constraints);
	serializer.WriteList(bound_defaults);

	//! Dependents of the table (in e.g. default values)
	// unordered_set<CatalogEntry *> dependencies;

	//! The existing table data on disk (if any)
	// unique_ptr<PersistentTableData> data;

	//! CREATE TABLE from QUERY
	serializer.WriteOptional(query);

	//! Indexes created by this table <Block_ID, Offset>
	// vector<BlockPointer> indexes;
}

unique_ptr<BoundCreateTableInfo> BoundCreateTableInfo::Deserialize(Deserializer &source,
                                                                   PlanDeserializationState &state) {
	auto &context = state.context;
	auto create_info = SchemaCatalogEntry::Deserialize(source);
	auto schema_name = create_info->schema;
	auto schema = Catalog::GetSchema(context, INVALID_CATALOG, schema_name);
	auto result = make_uniq<BoundCreateTableInfo>(*schema, std::move(create_info));
	result->base = source.ReadOptional<CreateInfo>();

	source.ReadList<Constraint>(result->constraints);
	source.ReadList<BoundConstraint>(result->bound_constraints);
	source.ReadList<Expression>(result->bound_defaults, state);

	result->query = source.ReadOptional<LogicalOperator>(state);
	return result;
}
} // namespace duckdb
