#include "parser/expression/parameter_expression.hpp"
#include "parser/transformer.hpp"

using namespace duckdb;
using namespace postgres;
using namespace std;

unique_ptr<Expression> Transformer::TransformParamRef(ParamRef *node) {
	if (!node) {
		return nullptr;
	}
	auto expr = make_unique<ParameterExpression>();

	return move(expr);
}
