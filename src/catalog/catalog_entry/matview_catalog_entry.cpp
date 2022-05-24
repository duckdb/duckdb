#include "duckdb/catalog/catalog_entry/matview_catalog_entry.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/type_catalog_entry.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/field_writer.hpp"
#include "duckdb/common/serializer.hpp"
#include "duckdb/execution/index/art/art.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/parser/constraints/list.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/keyword_helper.hpp"
#include "duckdb/parser/parsed_data/alter_table_info.hpp"
#include "duckdb/parser/parsed_expression_iterator.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/constraints/bound_check_constraint.hpp"
#include "duckdb/planner/constraints/bound_foreign_key_constraint.hpp"
#include "duckdb/planner/constraints/bound_not_null_constraint.hpp"
#include "duckdb/planner/constraints/bound_unique_constraint.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/expression_binder/alter_binder.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"
#include "duckdb/storage/storage_manager.hpp"

#include <algorithm>
#include <sstream>

namespace duckdb {

MatViewCatalogEntry::MatViewCatalogEntry(Catalog *catalog, SchemaCatalogEntry *schema, BoundCreateTableInfo *info,
                                         std::shared_ptr<DataTable> inherited_storage)
    : TableCatalogEntry(catalog, schema, info, inherited_storage) {
	this->type = CatalogType::MATVIEW_ENTRY;
	this->sql = info->base->sql;
}

void MatViewCatalogEntry::Serialize(Serializer &serializer) {
	FieldWriter writer(serializer);
	writer.WriteString(schema->name);
	writer.WriteString(name);
	writer.WriteRegularSerializableList(columns);
	writer.WriteSerializableList(constraints);
	writer.WriteString(sql);
	writer.Finalize();
}

unique_ptr<CreateMatViewInfo> MatViewCatalogEntry::Deserialize(Deserializer &source) {
	auto info = make_unique<CreateMatViewInfo>();

	FieldReader reader(source);
	info->schema = reader.ReadRequired<string>();
	info->table = reader.ReadRequired<string>();
	info->columns = reader.ReadRequiredSerializableList<ColumnDefinition, ColumnDefinition>();
	info->constraints = reader.ReadRequiredSerializableList<Constraint>();
	info->sql = reader.ReadRequired<string>();
	reader.Finalize();

	return info;
}

string MatViewCatalogEntry::ToSQL() {
	/// TODO: It is now a direct output of the SQL executed at the time of creation,
	/// which can be formatted to output a bit more beautifully.
	return sql;
}
} // namespace duckdb
