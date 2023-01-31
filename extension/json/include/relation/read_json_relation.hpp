#include "duckdb/main/relation/table_function_relation.hpp"

namespace duckdb {

class ReadJSONRelation : public TableFunctionRelation {
public:
	ReadJSONRelation(const shared_ptr<ClientContext> &context, string json_file, vector<ColumnDefinition> columns,
	                 string alias = "");
	string json_file;
	string alias;

public:
	string GetAlias() override;
};

} // namespace duckdb
