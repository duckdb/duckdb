#include "duckdb/catalog/catalog_entry/index_catalog_entry.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/execution/index/art/art.hpp"
#include "duckdb/common/field_writer.hpp"

namespace duckdb {

IndexCatalogEntry::IndexCatalogEntry(Catalog *catalog, SchemaCatalogEntry *schema, CreateIndexInfo *info)
    : StandardEntry(CatalogType::INDEX_ENTRY, schema, catalog, info->index_name), index(nullptr), sql(info->sql) {
}

IndexCatalogEntry::~IndexCatalogEntry() {
	// remove the associated index from the info
	if (!info || !index) {
		return;
	}
	info->indexes.RemoveIndex(index);
}

string IndexCatalogEntry::ToSQL() {
	if (sql.empty()) {
		throw InternalException("Cannot convert INDEX to SQL because it was not created with a SQL statement");
	}
	if (sql[sql.size() - 1] != ';') {
		sql += ";";
	}
	return sql;
}

void IndexCatalogEntry::Serialize(duckdb::MetaBlockWriter &serializer) {
	// Here we serialize the index metadata in the following order:
	// schema name, table name, index name, sql, index type, index constraint type, expression list.
	// column_ids, unbound_expression
	FieldWriter writer(serializer);
	writer.WriteString(info->schema);
	writer.WriteString(info->table);
	writer.WriteString(name);
	writer.WriteString(sql);
	writer.WriteField(index->type);
	writer.WriteField(index->constraint_type);
	writer.WriteSerializableList(expressions);
	writer.WriteSerializableList(parsed_expressions);
	writer.WriteList<idx_t>(index->column_ids);
	writer.Finalize();
}

unique_ptr<CreateIndexInfo> IndexCatalogEntry::Deserialize(Deserializer &source) {
	// Here we deserialize the index metadata in the following order:
	// root block, root offset, schema name, table name, index name, sql, index type, index constraint type, expression
	// list.

	auto create_index_info = make_unique<CreateIndexInfo>();

	FieldReader reader(source);

	create_index_info->schema = reader.ReadRequired<string>();
	create_index_info->table = make_unique<BaseTableRef>();
	create_index_info->table->schema_name = create_index_info->schema;
	create_index_info->table->table_name = reader.ReadRequired<string>();
	create_index_info->index_name = reader.ReadRequired<string>();
	create_index_info->sql = reader.ReadRequired<string>();
	create_index_info->index_type = IndexType(reader.ReadRequired<uint8_t>());
	create_index_info->constraint_type = IndexConstraintType(reader.ReadRequired<uint8_t>());
	create_index_info->expressions = reader.ReadRequiredSerializableList<ParsedExpression>();
	create_index_info->parsed_expressions = reader.ReadRequiredSerializableList<ParsedExpression>();

	create_index_info->column_ids = reader.ReadRequiredList<idx_t>();
	reader.Finalize();
	return create_index_info;
}

} // namespace duckdb
