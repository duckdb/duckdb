#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/statement/call_statement.hpp"
#include "duckdb/parser/peg/transformer/peg_transformer.hpp"

namespace duckdb {

unique_ptr<SQLStatement> PEGTransformerFactory::TransformCheckpointStatement(PEGTransformer &transformer,
                                                                             const bool &checkpoint_force,
                                                                             const string &catalog_name) {
	auto checkpoint_name = checkpoint_force ? "force_checkpoint" : "checkpoint";
	auto result = make_uniq<CallStatement>();
	vector<unique_ptr<ParsedExpression>> children;
	auto function = make_uniq<FunctionExpression>(checkpoint_name, std::move(children));
	function->catalog = SYSTEM_CATALOG;
	function->schema = DEFAULT_SCHEMA;
	if (!catalog_name.empty()) {
		function->children.push_back(make_uniq<ConstantExpression>(catalog_name));
	}
	result->function = std::move(function);
	return std::move(result);
}
bool PEGTransformerFactory::TransformCheckpointForce(PEGTransformer &transformer) {
	return true;
}

} // namespace duckdb
