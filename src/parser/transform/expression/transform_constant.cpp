#include "parser/expression/constant_expression.hpp"
#include "parser/transformer.hpp"

using namespace duckdb;
using namespace postgres;
using namespace std;

unique_ptr<Expression> Transformer::TransformValue(postgres::Value val) {
	switch (val.type) {
	case T_Integer:
		return make_unique<ConstantExpression>(Value::INTEGER(val.val.ival));
	case T_BitString: // FIXME: this should actually convert to BLOB
	case T_String:
		return make_unique<ConstantExpression>(Value(string(val.val.str)));
	case T_Float:
		return make_unique<ConstantExpression>(Value(stod(string(val.val.str))));
	case T_Null:
		return make_unique<ConstantExpression>();
	default:
		throw NotImplementedException("Value not implemented!");
	}
}

unique_ptr<Expression> Transformer::TransformConstant(A_Const *c) {
	return TransformValue(c->val);
}
