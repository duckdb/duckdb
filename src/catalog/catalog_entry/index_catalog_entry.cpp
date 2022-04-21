#include "duckdb/catalog/catalog_entry/index_catalog_entry.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/execution/index/art/art.hpp"

namespace duckdb {

IndexCatalogEntry::IndexCatalogEntry(Catalog *catalog, SchemaCatalogEntry *schema, CreateIndexInfo *info)
    : StandardEntry(CatalogType::INDEX_ENTRY, schema, catalog, info->index_name), index(nullptr), sql(info->sql) {
	//	for (auto& index_expressions: info->expressions){
	//		expressions.push_back(index_expressions->Copy());
	//	}
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
	return sql;
}

pair<idx_t, idx_t> IndexCatalogEntry::Serialize(duckdb::MetaBlockWriter &writer) {
	if (index->type != IndexType::ART) {
		throw NotImplementedException("The implementation of this index serialization does not exist.");
	}
	// We first do a DFS on the ART
	auto art_index = (ART *)index;
	return art_index->DepthFirstSearchCheckpoint(writer);
}

void IndexCatalogEntry::SerializeMetadata(duckdb::MetaBlockWriter &serializer) {
	// Here we serialize the index metadata in the following order:
	// schema name, table name, index name, sql, index type, index constraint type, expression list.
	FieldWriter writer(serializer);
	writer.WriteString(info->schema);
	writer.WriteString(info->table);
	writer.WriteString(name);
	writer.WriteString(sql);
	if (index->type == IndexType::ART) {
		uint8_t index_type = 0;
		writer.WriteField(index_type);
	} else {
		throw NotImplementedException("Can't serialize index type");
	}
	uint8_t constraint_type = index->constraint_type;
	writer.WriteField(constraint_type);
	writer.WriteSerializableList(expressions);
	writer.Finalize();
}

unique_ptr<CreateIndexInfo> IndexCatalogEntry::DeserializeMetadata(Deserializer &source) {
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
	auto index_type = reader.ReadRequired<uint8_t>();
	if (index_type == 0) {
		create_index_info->index_type = IndexType::ART;
	} else {
		throw NotImplementedException("Can't deserialize index type");
	}
	auto index_constraint_type = reader.ReadRequired<uint8_t>();
	if (index_constraint_type == 0) {
		create_index_info->unique = false;
	} else if (index_constraint_type == 1) {
		create_index_info->unique = true;
	} else {
		throw NotImplementedException("Can't deserialize this index constraint");
	}
	create_index_info->expressions = reader.ReadRequiredSerializableList<ParsedExpression>();
	reader.Finalize();
	return create_index_info;
}

} // namespace duckdb
