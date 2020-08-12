#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/transformer.hpp"
#include "duckdb/common/operator/cast_operators.hpp"
#include "duckdb/common/limits.hpp"

namespace duckdb {
using namespace std;

unique_ptr<ConstantExpression> Transformer::TransformValue(PGValue val) {
	switch (val.type) {
	case T_PGInteger:
		assert(val.val.ival <= NumericLimits<int32_t>::Maximum());
		return make_unique<ConstantExpression>(LogicalType::INTEGER, Value::INTEGER((int32_t)val.val.ival));
	case T_PGBitString: // FIXME: this should actually convert to BLOB
	case T_PGString:
		return make_unique<ConstantExpression>(LogicalType::VARCHAR, Value(string(val.val.str)));
	case T_PGFloat: {
		bool cast_as_double = false;
		for (auto ptr = val.val.str; *ptr; ptr++) {
			if (*ptr == '.' || *ptr == 'e') {
				// found decimal point or exponent, cast as double
				cast_as_double = true;
				break;
			}
		}
		if (!cast_as_double) {
			int64_t bigint_value;
			// try to cast as bigint first
			if (TryCast::Operation<string_t, int64_t>(string_t(val.val.str), bigint_value)) {
				// successfully cast to bigint: bigint value
				return make_unique<ConstantExpression>(LogicalType::BIGINT, Value::BIGINT(bigint_value));
			}
			hugeint_t hugeint_value;
			// if that is not successful; try to cast as hugeint
			if (TryCast::Operation<string_t, hugeint_t>(string_t(val.val.str), hugeint_value)) {
				// successfully cast to bigint: bigint value
				return make_unique<ConstantExpression>(LogicalType::HUGEINT, Value::HUGEINT(hugeint_value));
			}
		}
		// if there is a decimal or the value is too big to cast as either hugeint or bigint
		double dbl_value = Cast::Operation<string_t, double>(string_t(val.val.str));
		if (!Value::DoubleIsValid(dbl_value)) {
			throw ParserException("Double value \"%s\" is out of range!", val.val.str);
		}
		return make_unique<ConstantExpression>(LogicalType::DOUBLE, Value::DOUBLE(dbl_value));
	}
	case T_PGNull:
		return make_unique<ConstantExpression>(LogicalType::SQLNULL, Value());
	default:
		throw NotImplementedException("Value not implemented!");
	}
}

unique_ptr<ParsedExpression> Transformer::TransformConstant(PGAConst *c) {
	return TransformValue(c->val);
}

} // namespace duckdb
