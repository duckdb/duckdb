#include "duckdb/common/limits.hpp"
#include "duckdb/parser/expression/cast_expression.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/transformer.hpp"
#include "duckdb/common/operator/cast_operators.hpp"

namespace duckdb {
using namespace std;

unique_ptr<ParsedExpression> Transformer::TransformTypeCast(PGTypeCast *root) {
	if (!root) {
		return nullptr;
	}
	// get the type to cast to
	auto type_name = root->typeName;
	LogicalType target_type = TransformTypeName(type_name);

	// check for a constant BLOB value, then return ConstantExpression with BLOB
	if (target_type == LogicalType::BLOB && root->arg->type == T_PGAConst) {
		PGAConst *c = reinterpret_cast<PGAConst *>(root->arg);
		if (c->val.type == T_PGString) {
			return make_unique<ConstantExpression>(Value::BLOB(string(c->val.val.str)));
		}
	}
	if (target_type == LogicalType::INTERVAL && root->arg->type == T_PGAConst) {
		// handle post-fix notation of INTERVAL
		if (root->typeName->typmods && root->typeName->typmods->length > 0) {
			PGAConst *c = reinterpret_cast<PGAConst *>(root->arg);
			assert(c->val.type == T_PGString);

			int64_t amount;
			if (!TryCast::Operation<string_t, int64_t>(c->val.val.str, amount)) {
				throw ParserException("Interval post-fix requires a number!");
			}

			int mask = ((PGAConst *)root->typeName->typmods->head->data.ptr_value)->val.val.ival;
			// these seemingly random constants are from datetime.hpp
			// they are copied here to avoid having to include this header
			// the bitshift is from the function INTERVAL_MASK in the parser
			constexpr int MONTH_MASK = 1 << 1;
			constexpr int YEAR_MASK = 1 << 2;
			constexpr int DAY_MASK = 1 << 3;
			constexpr int HOUR_MASK = 1 << 10;
			constexpr int MINUTE_MASK = 1 << 11;
			constexpr int SECOND_MASK = 1 << 12;

			// we check that ONLY the bit specified by the mask is set
			// because certain interval masks (e.g. INTERVAL '10' HOURS TO DAYS) set multiple bits
			unique_ptr<ParsedExpression> expr;
			if (mask & DAY_MASK && mask & HOUR_MASK) {
				// DAY TO HOUR
				expr = make_unique<ConstantExpression>(Value(to_string(amount * 24) + " hours"));
			} else if (mask & DAY_MASK && mask & MINUTE_MASK) {
				// DAY TO MINUTE
				expr = make_unique<ConstantExpression>(Value(to_string(amount * 24 * 60) + " minutes"));
			} else if (mask & DAY_MASK && mask & SECOND_MASK) {
				// DAY TO SECOND
				expr = make_unique<ConstantExpression>(Value(to_string(amount * 24 * 60 * 60) + " seconds"));
			} else if (mask & HOUR_MASK) {
				// HOUR
				expr = make_unique<ConstantExpression>(Value(to_string(amount) + " hours"));
			} else if (mask & YEAR_MASK) {
				// YEAR
				expr = make_unique<ConstantExpression>(Value(to_string(amount) + " years"));
			} else if (mask & MONTH_MASK) {
				// MONTH
				expr = make_unique<ConstantExpression>(Value(to_string(amount) + " months"));
			} else if (mask & DAY_MASK) {
				// DAY
				expr = make_unique<ConstantExpression>(Value(to_string(amount) + " days"));
			} else if (mask & MINUTE_MASK) {
				// MINUTE
				expr = make_unique<ConstantExpression>(Value(to_string(amount) + " minutes"));
			} else if (mask & SECOND_MASK) {
				// SECOND
				expr = make_unique<ConstantExpression>(Value(to_string(amount) + " seconds"));
			} else {
				throw ParserException("Unsupported interval post-fix");
			}
			return make_unique<CastExpression>(target_type, move(expr));
		}
	}

	// transform the expression node
	auto expression = TransformExpression(root->arg);

	// now create a cast operation
	return make_unique<CastExpression>(target_type, move(expression));
}

} // namespace duckdb
