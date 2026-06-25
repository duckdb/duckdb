#include "duckdb/function/function.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/statement/call_statement.hpp"
#include "duckdb/parser/peg/transformer/peg_transformer.hpp"

namespace duckdb {

unique_ptr<SQLStatement>
PEGTransformerFactory::TransformCallStatement(PEGTransformer &transformer,
                                              const QualifiedName &qualified_table_function,
                                              vector<FunctionArgument> table_function_arguments) {
	auto result = make_uniq<CallStatement>();
	auto function_expression =
	    make_uniq<FunctionExpression>(qualified_table_function.Catalog(), qualified_table_function.Schema(),
	                                  qualified_table_function.Name(), std::move(table_function_arguments));
	result->function = std::move(function_expression);
	return std::move(result);
}

} // namespace duckdb
