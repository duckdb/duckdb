#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/common/serializer/deserializer.hpp"
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

void CreatePropertyGraphInfo::Serialize(Serializer &serializer) const {
	serializer.WriteProperty<string>(100, "property_graph_name", property_graph_name);
	serializer.WriteList(101, "vertex_tables", vertex_tables.size(), [&](Serializer::List &list, idx_t i) {
		auto &entry = vertex_tables[i];
		list.WriteObject([&](Serializer &obj) { entry->Serialize(obj); });
	});
	serializer.WriteList(102, "edge_tables", edge_tables.size(), [&](Serializer::List &list, idx_t i) {
		auto &entry = edge_tables[i];
		list.WriteObject([&](Serializer &obj) { entry->Serialize(obj); });
	});
	serializer.WriteProperty(103, "label_map", label_map);
}

unique_ptr<CreateInfo> CreatePropertyGraphInfo::Deserialize(Deserializer &deserializer) {
	auto result = make_uniq<CreatePropertyGraphInfo>();
	deserializer.ReadProperty<string>(100, "property_graph_name", result->property_graph_name);

	deserializer.ReadList(101, "vertex_tables", [&](Deserializer::List &list, idx_t i) {
		list.ReadObject([&](Deserializer &obj) { result->vertex_tables[i]->Deserialize(obj); });
	});

	deserializer.ReadProperty(103, "label_map", result->label_map);
	return std::move(result);
}

} // namespace duckdb
