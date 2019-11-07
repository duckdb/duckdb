#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/transformer.hpp"
#include "duckdb/common/operator/cast_operators.hpp"

using namespace duckdb;
using namespace std;

unique_ptr<ParsedExpression> Transformer::TransformValue(postgres::Value val) {
	switch (val.type) {
	case postgres::T_Integer:
		assert(val.val.ival <= numeric_limits<int32_t>::max());
		return make_unique<ConstantExpression>(SQLType::INTEGER, Value::INTEGER((int32_t)val.val.ival));
	case postgres::T_BitString: // FIXME: this should actually convert to BLOB
	case postgres::T_String:
		return make_unique<ConstantExpression>(SQLType::VARCHAR, Value(string(val.val.str)));
	case postgres::T_Float: {
		bool cast_as_double = false;
		for (auto ptr = val.val.str; *ptr; ptr++) {
			if (*ptr == '.') {
				// found decimal point, cast as double
				cast_as_double = true;
				break;
			}
		}
		int64_t value;
		if (!cast_as_double && TryCast::Operation<const char *, int64_t>(val.val.str, value)) {
			// successfully cast to bigint: bigint value
			return make_unique<ConstantExpression>(SQLType::BIGINT, Value::BIGINT(value));
		} else {
			// could not cast to bigint: cast to double
			double dbl_value = Cast::Operation<const char *, double>(val.val.str);
			return make_unique<ConstantExpression>(SQLType::DOUBLE, Value::DOUBLE(dbl_value));
		}
	}
	case postgres::T_Null:
		return make_unique<ConstantExpression>(SQLType::SQLNULL, Value());
	default:
		throw NotImplementedException("Value not implemented!");
	}
}

unique_ptr<ParsedExpression> Transformer::TransformConstant(postgres::A_Const *c) {
	return TransformValue(c->val);
}
