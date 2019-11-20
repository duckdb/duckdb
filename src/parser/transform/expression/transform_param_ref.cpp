#include "duckdb/parser/expression/parameter_expression.hpp"
#include "duckdb/parser/transformer.hpp"

using namespace duckdb;
using namespace std;

unique_ptr<ParsedExpression> Transformer::TransformParamRef(PGParamRef *node) {
	if (!node) {
		return nullptr;
	}
	auto expr = make_unique<ParameterExpression>();
	if (node->number == 0) {
		expr->parameter_nr = prepared_statement_parameter_index + 1;
	} else {
		expr->parameter_nr = node->number;
	}
	prepared_statement_parameter_index = max(prepared_statement_parameter_index, expr->parameter_nr);
	return move(expr);
}
