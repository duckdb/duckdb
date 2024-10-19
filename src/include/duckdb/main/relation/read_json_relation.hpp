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
	ReadJSONRelation(const shared_ptr<ClientContext> &context, string json_file, named_parameter_map_t options,
	                 bool auto_detect, string alias = "");
	ReadJSONRelation(const shared_ptr<ClientContext> &context, vector<string> &json_file, named_parameter_map_t options,
	                 bool auto_detect, string alias = "");
	~ReadJSONRelation() override;
	string json_file;
	string alias;

public:
	string GetAlias() override;

private:
	void InitializeAlias(const vector<string> &input);
};

} // namespace duckdb
