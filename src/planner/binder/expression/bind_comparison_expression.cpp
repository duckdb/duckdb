#include "duckdb/parser/expression/comparison_expression.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression_binder.hpp"

#include "duckdb/function/scalar/string_functions.hpp"

#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"

using namespace duckdb;
using namespace std;

unique_ptr<Expression> ExpressionBinder::PushCollation(ClientContext &context, unique_ptr<Expression> source, CollationType collation) {
	// replace default collation with system collation
	if (collation == CollationType::COLLATE_DEFAULT) {
		collation = context.db.collation;
	}
	switch(collation) {
	case CollationType::COLLATE_NONE: {
		return move(source);
	}
	case CollationType::COLLATE_NOCASE: {
		// push lower
		auto function = make_unique<BoundFunctionExpression>(TypeId::VARCHAR, LowerFun::GetFunction());
		function->children.push_back(move(source));
		function->arguments.push_back({SQLType::VARCHAR});
		function->sql_return_type = SQLType::VARCHAR;
		return move(function);
	}
	case CollationType::COLLATE_NOACCENT: {
		// push strip_accents
		auto function = make_unique<BoundFunctionExpression>(TypeId::VARCHAR, StripAccentsFun::GetFunction());
		function->children.push_back(move(source));
		function->arguments.push_back({SQLType::VARCHAR});
		function->sql_return_type = SQLType::VARCHAR;
		return move(function);
	}
	case CollationType::COLLATE_NOCASE_NOACCENT: {
		// push both NOCASE and NOACCENT
		auto expr = PushCollation(context, move(source), CollationType::COLLATE_NOCASE);
		return PushCollation(context, move(expr), CollationType::COLLATE_NOACCENT);
	}
	default:
		throw BinderException("Unsupported collation type in binder");
	}
}

BindResult ExpressionBinder::BindExpression(ComparisonExpression &expr, idx_t depth) {
	// first try to bind the children of the case expression
	string error;
	BindChild(expr.left, depth, error);
	BindChild(expr.right, depth, error);
	if (!error.empty()) {
		return BindResult(error);
	}
	// the children have been successfully resolved
	auto &left = (BoundExpression &)*expr.left;
	auto &right = (BoundExpression &)*expr.right;
	// cast the input types to the same type
	// now obtain the result type of the input types
	auto input_type = MaxSQLType(left.sql_type, right.sql_type);
	if (input_type.id == SQLTypeId::VARCHAR) {
		// for comparison with strings, we prefer to bind to the numeric types
		if (left.sql_type.IsNumeric()) {
			input_type = left.sql_type;
		} else if (right.sql_type.IsNumeric()) {
			input_type = right.sql_type;
		} else {
			// else: check if collations are compatible
			if (left.sql_type.collation != CollationType::COLLATE_DEFAULT &&
			    right.sql_type.collation != CollationType::COLLATE_DEFAULT &&
				left.sql_type.collation != right.sql_type.collation) {
				throw BinderException("Cannot combine types with different collation!");
			}
		}
	}
	if (input_type.id == SQLTypeId::UNKNOWN) {
		throw BinderException("Could not determine type of parameters: try adding explicit type casts");
	}
	// add casts (if necessary)
	left.expr = BoundCastExpression::AddCastToType(move(left.expr), left.sql_type, input_type);
	right.expr = BoundCastExpression::AddCastToType(move(right.expr), right.sql_type, input_type);
	if (input_type.id == SQLTypeId::VARCHAR && input_type.collation != CollationType::COLLATE_NONE) {
		// handle collation
		left.expr = PushCollation(context, move(left.expr), input_type.collation);
		right.expr = PushCollation(context, move(right.expr), input_type.collation);
	}
	// now create the bound comparison expression
	return BindResult(make_unique<BoundComparisonExpression>(expr.type, move(left.expr), move(right.expr)),
	                  SQLType(SQLTypeId::BOOLEAN));
}
