#pragma once

#include "duckdb/main/relation/table_function_relation.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/common/named_parameter_map.hpp"
#include "duckdb/parser/column_definition.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"

namespace duckdb {

class ReadJSONRelation : public TableFunctionRelation {
public:
	static shared_ptr<Relation> CreateRelation(const shared_ptr<ClientContext> &context, string json_file,
	                                           vector<ColumnDefinition> columns, named_parameter_map_t options,
	                                           string alias = "");
	string json_file;
	string alias;

public:
	string GetAlias() override;

private:
	ReadJSONRelation(const shared_ptr<ClientContext> &context, string json_file, vector<ColumnDefinition> columns,
	                 named_parameter_map_t options, string alias = "");
};

} // namespace duckdb
