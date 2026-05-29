#include "duckdb/function/function.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/statement/call_statement.hpp"
#include "duckdb/parser/peg/transformer/peg_transformer.hpp"

namespace duckdb {

unique_ptr<SQLStatement>
PEGTransformerFactory::TransformCallStatement(PEGTransformer &transformer,
                                              const QualifiedName &qualified_table_function,
                                              vector<unique_ptr<ParsedExpression>> table_function_arguments) {
	auto result = make_uniq<CallStatement>();
	auto function_expression =
	    make_uniq<FunctionExpression>(qualified_table_function.catalog, qualified_table_function.schema,
	                                  qualified_table_function.name, std::move(table_function_arguments));
	result->function = std::move(function_expression);
	return std::move(result);
}

} // namespace duckdb
