#include "duckdb/catalog/catalog_entry/constraint_catalog_entry.hpp"
#include "duckdb/common/field_writer.hpp"

namespace duckdb {

ConstraintCatalogEntry::ConstraintCatalogEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateConstraintInfo &info)
    : StandardEntry(CatalogType::CONSTRAINT_ENTRY, schema, catalog, info.constraint_name) {
}

void ConstraintCatalogEntry::Serialize(Serializer &serializer) const {
	// here we serialize the constraint metadata in the following order:
	// schema name, table name, constraint name

	FieldWriter writer(serializer);
	writer.WriteString(GetSchemaName());
	writer.WriteString(GetTableName());
	writer.WriteString(GetConstraintName());
	writer.Finalize();
}

unique_ptr<CreateConstraintInfo> ConstraintCatalogEntry::Deserialize(Deserializer &source, ClientContext &context) {
	// here we deserialize the constraint metadata in the following order:
	// schema name, table schema name, table name, constraint name

	auto create_constraint_info = make_uniq<CreateConstraintInfo>();

	FieldReader reader(source);

	create_constraint_info->schema = reader.ReadRequired<string>();
	create_constraint_info->table = make_uniq<BaseTableRef>();
	create_constraint_info->table->schema_name = create_constraint_info->schema;
	create_constraint_info->table->table_name = reader.ReadRequired<string>();
	create_constraint_info->constraint_name = reader.ReadRequired<string>();

	reader.Finalize();
	return create_constraint_info;
}

} // namespace duckdb
