#include "duckdb/common/operator/cast_operators.hpp"
#include "duckdb/common/types/decimal.hpp"
#include "transformer/peg_transformer.hpp"

namespace duckdb {

// NumberLiteral <- < [+-]?[0-9]*([.][0-9]*)? >
unique_ptr<ParsedExpression> PEGTransformerFactory::TransformNumberLiteral(PEGTransformer &transformer,
                                                                           optional_ptr<ParseResult> parse_result) {
	auto literal_pr = parse_result->Cast<NumberParseResult>();
	string_t str_val(literal_pr.number);
	bool try_cast_as_integer = true;
	bool try_cast_as_decimal = true;
	optional_idx decimal_position = optional_idx::Invalid();
	idx_t num_underscores = 0;
	idx_t num_integer_underscores = 0;
	for (idx_t i = 0; i < str_val.GetSize(); i++) {
		if (literal_pr.number[i] == '.') {
			// decimal point: cast as either decimal or double
			try_cast_as_integer = false;
			decimal_position = i;
		}
		if (literal_pr.number[i] == 'e' || literal_pr.number[i] == 'E') {
			// found exponent, cast as double
			try_cast_as_integer = false;
			try_cast_as_decimal = false;
		}
		if (literal_pr.number[i] == '_') {
			num_underscores++;
			if (!decimal_position.IsValid()) {
				num_integer_underscores++;
			}
		}
	}
	if (try_cast_as_integer) {
		int64_t bigint_value;
		// try to cast as bigint first
		if (TryCast::Operation<string_t, int64_t>(str_val, bigint_value)) {
			// successfully cast to bigint: bigint value
			return make_uniq<ConstantExpression>(Value::BIGINT(bigint_value));
		}
		hugeint_t hugeint_value;
		// if that is not successful; try to cast as hugeint
		if (TryCast::Operation<string_t, hugeint_t>(str_val, hugeint_value)) {
			// successfully cast to bigint: bigint value
			return make_uniq<ConstantExpression>(Value::HUGEINT(hugeint_value));
		}
		uhugeint_t uhugeint_value;
		// if that is not successful; try to cast as uhugeint
		if (TryCast::Operation<string_t, uhugeint_t>(str_val, uhugeint_value)) {
			// successfully cast to bigint: bigint value
			return make_uniq<ConstantExpression>(Value::UHUGEINT(uhugeint_value));
		}
	}
	idx_t decimal_offset = literal_pr.number[0] == '-' ? 3 : 2;
	if (try_cast_as_decimal && decimal_position.IsValid() &&
	    str_val.GetSize() - num_underscores < Decimal::MAX_WIDTH_DECIMAL + decimal_offset) {
		// figure out the width/scale based on the decimal position
		auto width = NumericCast<uint8_t>(str_val.GetSize() - 1 - num_underscores);
		auto scale = NumericCast<uint8_t>(width - decimal_position.GetIndex() + num_integer_underscores);
		if (literal_pr.number[0] == '-') {
			width--;
		}
		if (width <= Decimal::MAX_WIDTH_DECIMAL) {
			// we can cast the value as a decimal
			Value val = Value(str_val);
			val = val.DefaultCastAs(LogicalType::DECIMAL(width, scale));
			return make_uniq<ConstantExpression>(std::move(val));
		}
	}
	// if there is a decimal or the value is too big to cast as either hugeint or bigint
	double dbl_value = Cast::Operation<string_t, double>(str_val);
	return make_uniq<ConstantExpression>(Value::DOUBLE(dbl_value));
}

// StringLiteral <- '\'' [^\']* '\''
string PEGTransformerFactory::TransformStringLiteral(PEGTransformer &transformer,
                                                     optional_ptr<ParseResult> parse_result) {
	auto &string_literal_pr = parse_result->Cast<StringLiteralParseResult>();
	return string_literal_pr.result;
}
} // namespace duckdb
