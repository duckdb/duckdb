#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/common/field_writer.hpp"

namespace duckdb {
// TODO[YLM]: Review if we want/need to serialize more of the fields.
void BoundCreateTableInfo::Serialize(Serializer &serializer) const {
  D_ASSERT(schema);
  schema->Serialize(serializer);
  serializer.WriteOptional(base);
	serializer.WriteOptional(query);
}

unique_ptr<BoundCreateTableInfo> BoundCreateTableInfo::Deserialize(Deserializer &source, ClientContext& context) {
  auto create_info = SchemaCatalogEntry::Deserialize(source);
  auto schema_name = create_info->schema;
  auto result = make_unique<BoundCreateTableInfo>(move(create_info));
  result->schema = Catalog::GetCatalog(context).GetSchema(context, schema_name);
  result->base = source.ReadOptional<CreateInfo>();
  result->query = source.ReadOptional<LogicalOperator>(context);
  return result;
}
}