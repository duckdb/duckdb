#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/transformer.hpp"
#include "duckdb/common/operator/cast_operators.hpp"

using namespace duckdb;
using namespace std;

unique_ptr<ParsedExpression> Transformer::TransformValue(PGValue val) {
	switch (val.type) {
	case T_PGInteger:
		assert(val.val.ival <= numeric_limits<int32_t>::max());
		return make_unique<ConstantExpression>(SQLType::INTEGER, Value::INTEGER((int32_t)val.val.ival));
	case T_PGBitString: // FIXME: this should actually convert to BLOB
	case T_PGString:
		return make_unique<ConstantExpression>(SQLType::VARCHAR, Value(string(val.val.str)));
	case T_PGFloat: {
		bool cast_as_double = false;
		for (auto ptr = val.val.str; *ptr; ptr++) {
			if (*ptr == '.') {
				// found decimal point, cast as double
				cast_as_double = true;
				break;
			}
		}
		int64_t value;
		if (!cast_as_double && TryCast::Operation<string_t, int64_t>(string_t(val.val.str), value)) {
			// successfully cast to bigint: bigint value
			return make_unique<ConstantExpression>(SQLType::BIGINT, Value::BIGINT(value));
		} else {
			// could not cast to bigint: cast to double
			double dbl_value = Cast::Operation<string_t, double>(string_t(val.val.str));
			if (!Value::DoubleIsValid(dbl_value)) {
				throw ParserException("Double value \"%s\" is out of range!", val.val.str);
			}
			return make_unique<ConstantExpression>(SQLType::DOUBLE, Value::DOUBLE(dbl_value));
		}
	}
	case T_PGNull:
		return make_unique<ConstantExpression>(SQLType::SQLNULL, Value());
	default:
		throw NotImplementedException("Value not implemented!");
	}
}

unique_ptr<ParsedExpression> Transformer::TransformConstant(PGAConst *c) {
	return TransformValue(c->val);
}
