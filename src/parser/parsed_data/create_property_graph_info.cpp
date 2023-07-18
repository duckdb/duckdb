#include "duckdb/parser/parsed_data/create_property_graph_info.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/catalog/catalog.hpp"

namespace duckdb {

CreatePropertyGraphInfo::CreatePropertyGraphInfo() : CreateInfo(CatalogType::VIEW_ENTRY) {
}

CreatePropertyGraphInfo::CreatePropertyGraphInfo(string property_graph_name)
    : CreateInfo(CatalogType::VIEW_ENTRY), property_graph_name(std::move(property_graph_name)) {
}

void CreatePropertyGraphInfo::SerializeInternal(Serializer &serializer) const {
	FieldWriter writer(serializer);
	writer.WriteString(property_graph_name);
	writer.WriteSerializableList<PropertyGraphTable>(vertex_tables);
	writer.WriteSerializableList<PropertyGraphTable>(edge_tables);
	//    writer.WriteRegularSerializableMap<PropertyGraphTable*>(label_map);

	writer.Finalize();
}

unique_ptr<CreateInfo> CreatePropertyGraphInfo::Copy() const {
	auto result = make_uniq<CreatePropertyGraphInfo>(property_graph_name);

	for (auto &vertex_table : vertex_tables) {
		auto copied_vertex_table = vertex_table->Copy();
		for (auto &label : copied_vertex_table->sub_labels) {
			result->label_map[label] = copied_vertex_table;
		}
		result->label_map[copied_vertex_table->main_label] = copied_vertex_table;
		result->vertex_tables.push_back(std::move(copied_vertex_table));
	}
	for (auto &edge_table : edge_tables) {
		auto copied_edge_table = edge_table->Copy();
		for (auto &label : copied_edge_table->sub_labels) {
			result->label_map[label] = copied_edge_table;
		}
		result->label_map[copied_edge_table->main_label] = copied_edge_table;
		result->edge_tables.push_back(std::move(copied_edge_table));
	}
	return std::move(result);
}

unique_ptr<CreateInfo> CreatePropertyGraphInfo::Deserialize(FieldReader &reader) {
	auto result = make_uniq<CreatePropertyGraphInfo>();
	result->property_graph_name = reader.ReadRequired<string>();
	result->vertex_tables = reader.ReadRequiredSharedSerializableList<PropertyGraphTable>();
	result->edge_tables = reader.ReadRequiredSharedSerializableList<PropertyGraphTable>();
	//        result->label_map = reader.ReadRequiredSerializableMap<PropertyGraphTable>();
	reader.Finalize();
	return std::move(result);
}

} // namespace duckdb
