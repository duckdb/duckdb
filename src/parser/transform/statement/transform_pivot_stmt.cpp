#include "duckdb/parser/transformer.hpp"
#include "duckdb/parser/statement/pragma_statement.hpp"

namespace duckdb {

unique_ptr<PragmaStatement> Transformer::TransformPivotStatement(duckdb_libpgquery::PGNode *stmt) {
	auto pivot = (duckdb_libpgquery::PGPivotStmt *) stmt;
	auto result = make_unique<PragmaStatement>();
	result->info->name = "pivot_statement";
	auto source = TransformTableRefNode(pivot->source);
	auto aggregate = TransformExpression(pivot->aggr);
	auto columns = TransformStringList(pivot->columns);
	result->info->parameters.emplace_back(Value(source->ToString()));
	result->info->parameters.emplace_back(Value(aggregate->ToString()));
	vector<Value> column_values;
	for(auto &c : columns) {
		column_values.emplace_back(c);
	}
	result->info->parameters.emplace_back(Value::LIST(LogicalType::VARCHAR, std::move(column_values)));
	return result;
}

} // namespace duckdb