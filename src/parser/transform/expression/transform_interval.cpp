#include "duckdb/parser/expression/cast_expression.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/transformer.hpp"
#include "duckdb/common/operator/cast_operators.hpp"

namespace duckdb {

unique_ptr<ParsedExpression> Transformer::TransformInterval(duckdb_libpgquery::PGIntervalConstant &node) {
	// handle post-fix notation of INTERVAL

	// three scenarios
	// interval (expr) year
	// interval 'string' year
	// interval int year
	unique_ptr<ParsedExpression> expr;
	switch (node.val_type) {
	case duckdb_libpgquery::T_PGAExpr:
		expr = TransformExpression(node.eval);
		break;
	case duckdb_libpgquery::T_PGString:
		expr = make_uniq<ConstantExpression>(Value(node.sval));
		break;
	case duckdb_libpgquery::T_PGInteger:
		expr = make_uniq<ConstantExpression>(Value(node.ival));
		break;
	default:
		throw InternalException("Unsupported interval transformation");
	}

	if (!node.typmods) {
		return make_uniq<CastExpression>(LogicalType::INTERVAL, std::move(expr));
	}

	int32_t mask = NumericCast<int32_t>(
	    PGPointerCast<duckdb_libpgquery::PGAConst>(node.typmods->head->data.ptr_value)->val.val.ival);
	// these seemingly random constants are from libpg_query/include/utils/datetime.hpp
	// they are copied here to avoid having to include this header
	// the bitshift is from the function INTERVAL_MASK in the parser
	constexpr int32_t MONTH_MASK = 1 << 1;
	constexpr int32_t YEAR_MASK = 1 << 2;
	constexpr int32_t DAY_MASK = 1 << 3;
	constexpr int32_t HOUR_MASK = 1 << 10;
	constexpr int32_t MINUTE_MASK = 1 << 11;
	constexpr int32_t SECOND_MASK = 1 << 12;
	constexpr int32_t MILLISECOND_MASK = 1 << 13;
	constexpr int32_t MICROSECOND_MASK = 1 << 14;
	constexpr int32_t WEEK_MASK = 1 << 24;
	constexpr int32_t DECADE_MASK = 1 << 25;
	constexpr int32_t CENTURY_MASK = 1 << 26;
	constexpr int32_t MILLENNIUM_MASK = 1 << 27;
	constexpr int32_t QUARTER_MASK = 1 << 29;

	// we need to check certain combinations
	// because certain interval masks (e.g. INTERVAL '10' HOURS TO DAYS) set multiple bits
	// for now we don't support all of the combined ones
	// (we might add support if someone complains about it)

	string fname;
	LogicalType parse_type = LogicalType::DOUBLE;
	LogicalType target_type;
	if (mask & YEAR_MASK && mask & MONTH_MASK) {
		// DAY TO HOUR
		throw ParserException("YEAR TO MONTH is not supported");
	} else if (mask & DAY_MASK && mask & HOUR_MASK) {
		// DAY TO HOUR
		throw ParserException("DAY TO HOUR is not supported");
	} else if (mask & DAY_MASK && mask & MINUTE_MASK) {
		// DAY TO MINUTE
		throw ParserException("DAY TO MINUTE is not supported");
	} else if (mask & DAY_MASK && mask & SECOND_MASK) {
		// DAY TO SECOND
		throw ParserException("DAY TO SECOND is not supported");
	} else if (mask & HOUR_MASK && mask & MINUTE_MASK) {
		// DAY TO SECOND
		throw ParserException("HOUR TO MINUTE is not supported");
	} else if (mask & HOUR_MASK && mask & SECOND_MASK) {
		// DAY TO SECOND
		throw ParserException("HOUR TO SECOND is not supported");
	} else if (mask & MINUTE_MASK && mask & SECOND_MASK) {
		// DAY TO SECOND
		throw ParserException("MINUTE TO SECOND is not supported");
	} else if (mask & YEAR_MASK) {
		// YEAR
		fname = "to_years";
		target_type = LogicalType::INTEGER;
	} else if (mask & MONTH_MASK) {
		// MONTH
		fname = "to_months";
		target_type = LogicalType::INTEGER;
	} else if (mask & DAY_MASK) {
		// DAY
		fname = "to_days";
		target_type = LogicalType::INTEGER;
	} else if (mask & HOUR_MASK) {
		// HOUR
		fname = "to_hours";
		target_type = LogicalType::BIGINT;
	} else if (mask & MINUTE_MASK) {
		// MINUTE
		fname = "to_minutes";
		target_type = LogicalType::BIGINT;
	} else if (mask & SECOND_MASK) {
		// SECOND
		fname = "to_seconds";
		target_type = LogicalType::DOUBLE;
	} else if (mask & MILLISECOND_MASK) {
		// MILLISECOND
		fname = "to_milliseconds";
		target_type = LogicalType::DOUBLE;
	} else if (mask & MICROSECOND_MASK) {
		// MICROSECOND
		fname = "to_microseconds";
		target_type = LogicalType::BIGINT;
	} else if (mask & WEEK_MASK) {
		// WEEK
		fname = "to_weeks";
		target_type = LogicalType::INTEGER;
	} else if (mask & QUARTER_MASK) {
		// QUARTER
		fname = "to_quarters";
		target_type = LogicalType::INTEGER;
	} else if (mask & DECADE_MASK) {
		// DECADE
		fname = "to_decades";
		target_type = LogicalType::INTEGER;
	} else if (mask & CENTURY_MASK) {
		// CENTURY
		fname = "to_centuries";
		target_type = LogicalType::INTEGER;
	} else if (mask & MILLENNIUM_MASK) {
		// MILLENNIUM
		fname = "to_millennia";
		target_type = LogicalType::INTEGER;
	} else {
		throw InternalException("Unsupported interval post-fix");
	}
	// first push a cast to the parse type
	expr = make_uniq<CastExpression>(parse_type, std::move(expr));

	// next, truncate it if the target type doesn't match the parse type
	if (target_type != parse_type) {
		vector<unique_ptr<ParsedExpression>> children;
		children.push_back(std::move(expr));
		expr = make_uniq<FunctionExpression>("trunc", std::move(children));
		expr = make_uniq<CastExpression>(target_type, std::move(expr));
	}
	// now push the operation
	vector<unique_ptr<ParsedExpression>> children;
	children.push_back(std::move(expr));
	return make_uniq<FunctionExpression>(fname, std::move(children));
}

} // namespace duckdb
