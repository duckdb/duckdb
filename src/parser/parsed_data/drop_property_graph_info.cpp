#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/common/serializer/deserializer.hpp"
#include "duckdb/parser/parsed_data/drop_property_graph_info.hpp"

namespace duckdb {

DropPropertyGraphInfo::DropPropertyGraphInfo() : DropInfo() {
}

DropPropertyGraphInfo::DropPropertyGraphInfo(string property_graph_name)
    : DropInfo(), property_graph_name(std::move(property_graph_name)) {
}

unique_ptr<DropInfo> DropPropertyGraphInfo::Copy() const {
	auto result = make_uniq<DropPropertyGraphInfo>(property_graph_name);
	return std::move(result);
}

string DropPropertyGraphInfo::ToString() const {
	string result = "-DROP PROPERTY GRAPH " + property_graph_name;
	return result;
}

void DropPropertyGraphInfo::Serialize(Serializer &serializer) const {
	serializer.WriteProperty<string>(100, "property_graph_name", property_graph_name);
}

unique_ptr<DropInfo> DropPropertyGraphInfo::Deserialize(Deserializer &deserializer) {
	auto result = make_uniq<DropPropertyGraphInfo>();
	deserializer.ReadProperty<string>(100, "property_graph_name", result->property_graph_name);
	return std::move(result);
}

} // namespace duckdb
