#include "duckdb/parser/expression/cast_expression.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/transformer.hpp"
#include "duckdb/common/operator/cast_operators.hpp"

namespace duckdb {

unique_ptr<ParsedExpression> Transformer::TransformInterval(duckdb_libpgquery::PGIntervalConstant *node) {
	// handle post-fix notation of INTERVAL

	// three scenarios
	// interval (expr) year
	// interval 'string' year
	// interval int year
	unique_ptr<ParsedExpression> expr;
	switch (node->val_type) {
	case duckdb_libpgquery::T_PGAExpr:
		expr = TransformExpression(node->eval);
		break;
	case duckdb_libpgquery::T_PGString:
		expr = make_unique<ConstantExpression>(Value(node->sval));
		break;
	case duckdb_libpgquery::T_PGInteger:
		expr = make_unique<ConstantExpression>(Value(node->ival));
		break;
	default:
		throw ParserException("Unsupported interval transformation");
	}

	if (!node->typmods) {
		return make_unique<CastExpression>(LogicalType::INTERVAL, move(expr));
	}

	int32_t mask = ((duckdb_libpgquery::PGAConst *)node->typmods->head->data.ptr_value)->val.val.ival;
	// these seemingly random constants are from datetime.hpp
	// they are copied here to avoid having to include this header
	// the bitshift is from the function INTERVAL_MASK in the parser
	constexpr int32_t month_mask = 1 << 1;
	constexpr int32_t year_mask = 1 << 2;
	constexpr int32_t day_mask = 1 << 3;
	constexpr int32_t hour_mask = 1 << 10;
	constexpr int32_t minute_mask = 1 << 11;
	constexpr int32_t second_mask = 1 << 12;
	constexpr int32_t millisecond_mask = 1 << 13;
	constexpr int32_t microsecond_mask = 1 << 14;

	// we need to check certain combinations
	// because certain interval masks (e.g. INTERVAL '10' HOURS TO DAYS) set multiple bits
	// for now we don't support all of the combined ones
	// (we might add support if someone complains about it)

	string fname;
	LogicalType target_type;
	if (mask & year_mask && mask & month_mask) {
		// DAY TO HOUR
		throw ParserException("YEAR TO MONTH is not supported");
	} else if (mask & day_mask && mask & hour_mask) {
		// DAY TO HOUR
		throw ParserException("DAY TO HOUR is not supported");
	} else if (mask & day_mask && mask & minute_mask) {
		// DAY TO MINUTE
		throw ParserException("DAY TO MINUTE is not supported");
	} else if (mask & day_mask && mask & second_mask) {
		// DAY TO SECOND
		throw ParserException("DAY TO SECOND is not supported");
	} else if (mask & hour_mask && mask & minute_mask) {
		// DAY TO SECOND
		throw ParserException("HOUR TO MINUTE is not supported");
	} else if (mask & hour_mask && mask & second_mask) {
		// DAY TO SECOND
		throw ParserException("HOUR TO SECOND is not supported");
	} else if (mask & minute_mask && mask & second_mask) {
		// DAY TO SECOND
		throw ParserException("MINUTE TO SECOND is not supported");
	} else if (mask & year_mask) {
		// YEAR
		fname = "to_years";
		target_type = LogicalType::INTEGER;
	} else if (mask & month_mask) {
		// MONTH
		fname = "to_months";
		target_type = LogicalType::INTEGER;
	} else if (mask & day_mask) {
		// DAY
		fname = "to_days";
		target_type = LogicalType::INTEGER;
	} else if (mask & hour_mask) {
		// HOUR
		fname = "to_hours";
		target_type = LogicalType::BIGINT;
	} else if (mask & minute_mask) {
		// MINUTE
		fname = "to_minutes";
		target_type = LogicalType::BIGINT;
	} else if (mask & second_mask) {
		// SECOND
		fname = "to_seconds";
		target_type = LogicalType::BIGINT;
	} else if (mask & millisecond_mask) {
		// MILLISECOND
		fname = "to_milliseconds";
		target_type = LogicalType::BIGINT;
	} else if (mask & microsecond_mask) {
		// SECOND
		fname = "to_microseconds";
		target_type = LogicalType::BIGINT;
	} else {
		throw ParserException("Unsupported interval post-fix");
	}
	// first push a cast to the target type
	expr = make_unique<CastExpression>(target_type, move(expr));
	// now push the operation
	vector<unique_ptr<ParsedExpression>> children;
	children.push_back(move(expr));
	return make_unique<FunctionExpression>(fname, children);
}

} // namespace duckdb
