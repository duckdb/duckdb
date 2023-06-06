#include "duckdb/parser/parsed_data/create_property_graph_info.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/catalog/catalog.hpp"

namespace duckdb {

CreatePropertyGraphInfo::CreatePropertyGraphInfo() : CreateInfo(CatalogType::PROPERTY_GRAPH_ENTRY) {
}

CreatePropertyGraphInfo::CreatePropertyGraphInfo(string catalog_p, string schema_p, string name_p)
    : CreateInfo(CatalogType::PROPERTY_GRAPH_ENTRY, std::move(schema_p), std::move(catalog_p)),
      property_graph_name(std::move(name_p)) {
}

CreatePropertyGraphInfo::CreatePropertyGraphInfo(SchemaCatalogEntry &schema, string pg_name)
    : CreatePropertyGraphInfo(schema.catalog.GetName(), schema.name, std::move(pg_name)) {
}

void CreatePropertyGraphInfo::SerializeInternal(Serializer &serializer) const {
	FieldWriter writer(serializer);
	writer.WriteString(property_graph_name);
    writer.WriteSerializableList<PropertyGraphTable>(vertex_tables);

    writer.WriteSerializableList<PropertyGraphTable>(edge_tables);
	for (auto &label_entry : label_map) {
		writer.WriteString(label_entry.first);
		label_entry.second->Serialize(serializer);
	}
	writer.Finalize();
}

unique_ptr<CreateInfo> CreatePropertyGraphInfo::Copy() const {
	auto result = make_uniq<CreatePropertyGraphInfo>(catalog, schema, property_graph_name);
	CopyProperties(*result);

	for (auto &vertex_table : vertex_tables) {
		result->vertex_tables.push_back(vertex_table->Copy());
	}
	for (auto &edge_table : edge_tables) {
		result->edge_tables.push_back(edge_table->Copy());
	}
	for (auto &label_entry : label_map) {
		result->label_map[label_entry.first] = label_entry.second;
	}

	return std::move(result);
}

    unique_ptr<CreatePropertyGraphInfo> CreatePropertyGraphInfo::Deserialize(Deserializer &deserializer) {
        auto result = make_uniq<CreatePropertyGraphInfo>();
        result->DeserializeBase(deserializer);

        FieldReader reader(deserializer);
        result->property_graph_name = reader.ReadRequired<string>();
        result->vertex_tables = reader.ReadRequiredSerializableList<PropertyGraphTable>();
        result->edge_tables = reader.ReadRequiredSerializableList<PropertyGraphTable>();


        return result;
    }

} // namespace duckdb
