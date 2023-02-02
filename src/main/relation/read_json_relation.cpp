#include "duckdb/main/relation/read_json_relation.hpp"
#include "duckdb/parser/column_definition.hpp"
#if defined(BUILD_JSON_EXTENSION) && !defined(DISABLE_BUILTIN_EXTENSIONS)
#include "json_scan.hpp"
#endif
namespace duckdb {

shared_ptr<Relation> ReadJSONRelation::CreateRelation(const shared_ptr<ClientContext> &context, string json_file,
                                                      vector<ColumnDefinition> columns, named_parameter_map_t options,
                                                      string alias) {
#if defined(BUILD_JSON_EXTENSION) && !defined(DISABLE_BUILTIN_EXTENSIONS)
	return make_shared<ReadJSONRelation>(connection->context, name, move(column_definitions), move(options), alias);
#else
	return nullptr;
#endif
}

ReadJSONRelation::ReadJSONRelation(const shared_ptr<ClientContext> &context, string json_file_p,
                                   vector<ColumnDefinition> columns_p, named_parameter_map_t options, string alias_p)
    : TableFunctionRelation(context, "read_json", {Value(json_file_p)}, move(options), nullptr, false),
      json_file(std::move(json_file_p)), alias(std::move(alias_p)) {

#if defined(BUILD_JSON_EXTENSION) && !defined(DISABLE_BUILTIN_EXTENSIONS)
	if (alias.empty()) {
		alias = StringUtil::Split(json_file, ".")[0];
	}

	if (columns_p.empty()) {
		// No columns were supplied, have to auto_detect
		vector<LogicalType> types;
		vector<string> names;

		JSONScanData bind_data;

		// Resolve the extra options that could be relevant to AutoDetect
		JSONScan::InitializeBindData(*context, bind_data, options, names, types);
		D_ASSERT(names.empty());
		D_ASSERT(types.empty());

		// Now detect the types of the JSON file, so we can create ColumnDefinitions for the relation
		JSONScan::AutoDetect(*context, bind_data, types, names);

		D_ASSERT(!names.empty());
		D_ASSERT(names.size() == types.size());
		for (idx_t i = 0; i < names.size(); i++) {
			auto &name = names[i];
			auto &type = types[i];

			columns.push_back(ColumnDefinition(name, type));
		}
		AddNamedParameter("auto_detect", Value::BOOLEAN(true));
	} else {
		columns = move(columns_p);
		child_list_t<Value> column_names;
		for (idx_t i = 0; i < columns.size(); i++) {
			column_names.push_back(make_pair(columns[i].Name(), Value(columns[i].Type().ToString())));
		}

		AddNamedParameter("columns", Value::STRUCT(std::move(column_names)));
	}
#endif
}

string ReadJSONRelation::GetAlias() {
	return alias;
}

} // namespace duckdb
