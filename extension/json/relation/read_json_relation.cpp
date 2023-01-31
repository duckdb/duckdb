#include "relation/read_json_relation.hpp"
#include "duckdb/parser/column_definition.hpp"

namespace duckdb {

ReadJSONRelation::ReadJSONRelation(const shared_ptr<ClientContext> &context, string json_file_p,
                                   vector<ColumnDefinition> columns_p, string alias_p)
    : TableFunctionRelation(context, "read_json", {Value(json_file_p)}, nullptr, false),
      json_file(std::move(json_file_p)), alias(std::move(alias_p)) {

	if (alias.empty()) {
		alias = StringUtil::Split(json_file, ".")[0];
	}
	columns = move(columns_p);

	child_list_t<Value> column_names;
	for (idx_t i = 0; i < columns.size(); i++) {
		column_names.push_back(make_pair(columns[i].Name(), Value(columns[i].Type().ToString())));
	}

	AddNamedParameter("columns", Value::STRUCT(std::move(column_names)));
}

string ReadJSONRelation::GetAlias() {
	return alias;
}

} // namespace duckdb
