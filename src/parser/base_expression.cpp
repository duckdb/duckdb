#include "duckdb/parser/base_expression.hpp"

#include "duckdb/main/config.hpp"
#include "duckdb/common/printer.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/parser/expression/case_expression.hpp"
#include "duckdb/parser/expression/cast_expression.hpp"
#include "duckdb/parser/expression/collate_expression.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/operator_expression.hpp"
#include "duckdb/parser/expression/subquery_expression.hpp"
#include "duckdb/parser/expression/window_expression.hpp"
#include "duckdb/parser/query_node/select_node.hpp"

namespace duckdb {

void BaseExpression::Print() const {
	Printer::Print(ToString());
}

// Port of PostgreSQL's FigureColnameInternal.
// Returns: 0 = no name found, 1 = weak name (type cast), 2 = strong name.
// Caller uses "?column?" as fallback when strength is 0.
static int FigureColnameInternal(const BaseExpression &expr, string &name) {
	switch (expr.expression_class) {
	case ExpressionClass::COLUMN_REF: {
		auto &column_ref = expr.Cast<ColumnRefExpression>();
		name = StringUtil::Lower(column_ref.GetColumnName());
		return 2;
	}
	case ExpressionClass::FUNCTION: {
		auto &function = expr.Cast<FunctionExpression>();
		if (function.is_operator) {
			// PG: operators like +, -, *, / don't produce column names
			return 0;
		}
		name = StringUtil::Lower(function.function_name);
		if (name == "count_star") {
			name = "count";
		} else if (name == "trim") {
			name = "btrim";
		}
		return 2;
	}
	case ExpressionClass::CAST: {
		auto &cast = expr.Cast<CastExpression>();
		int strength = 0;
		if (cast.child) {
			strength = FigureColnameInternal(*cast.child, name);
		}
		if (strength <= 1) {
			name = StringUtil::Lower(cast.cast_type.ToString());
			while (name.ends_with("[]")) {
				name = name.substr(0, name.size() - 2);
			}
			if (name.starts_with('"') && name.ends_with('"')) {
				name = name.substr(1, name.size() - 2);
			}
			if (name == "varchar") {
				name = "text";
			} else if (name == "integer") {
				name = "int4";
			} else if (name == "bigint") {
				name = "int8";
			} else if (name == "float") {
				name = "float4";
			} else if (name == "double") {
				name = "float8";
			} else if (name == "boolean") {
				name = "bool";
			} else if (name.starts_with("decimal")) {
				name = "numeric";
			} else if (name == "blob") {
				name = "bytea";
			}
			return 1;
		}
		return strength;
	}
	case ExpressionClass::COLLATE: {
		auto &collate = expr.Cast<CollateExpression>();
		if (collate.child) {
			return FigureColnameInternal(*collate.child, name);
		}
		return 0;
	}
	case ExpressionClass::CASE: {
		auto &case_expr = expr.Cast<CaseExpression>();
		int strength = 0;
		if (case_expr.else_expr) {
			strength = FigureColnameInternal(*case_expr.else_expr, name);
		}
		if (strength <= 1) {
			name = "case";
			return 1;
		}
		return strength;
	}
	case ExpressionClass::SUBQUERY: {
		// PG: T_SubLink
		auto &subquery = expr.Cast<SubqueryExpression>();
		switch (subquery.subquery_type) {
		case SubqueryType::EXISTS:
		case SubqueryType::NOT_EXISTS:
			name = "exists";
			return 2;
		case SubqueryType::SCALAR:
			// PG: EXPR_SUBLINK -- get column name of the subquery's single target
			if (subquery.subquery && subquery.subquery->node &&
			    subquery.subquery->node->type == QueryNodeType::SELECT_NODE) {
				auto &select = subquery.subquery->node->Cast<SelectNode>();
				if (select.select_list.size() == 1) {
					auto &target = *select.select_list[0];
					if (!target.alias.empty()) {
						name = StringUtil::Lower(target.alias);
						return 2;
					}
					return FigureColnameInternal(target, name);
				}
			}
			return 0;
		// As with other operator-like nodes, these have no names
		default:
			return 0;
		}
	}
	case ExpressionClass::OPERATOR: {
		auto &op = expr.Cast<OperatorExpression>();
		switch (op.type) {
		// PG: T_A_Expr AEXPR_NULLIF
		case ExpressionType::OPERATOR_NULLIF:
			name = "nullif";
			return 2;
		// PG: T_CoalesceExpr
		case ExpressionType::OPERATOR_COALESCE:
			name = "coalesce";
			return 2;
		// PG: T_A_ArrayExpr
		case ExpressionType::ARRAY_CONSTRUCTOR:
			name = "array";
			return 2;
		// PG: A_Indirection with only subscripts (no field names) -> recurse
		// into the source expression.  Matches PG's FigureColnameInternal
		// which uses the last string field name (if any) or falls back to
		// the source expression's name.
		case ExpressionType::ARRAY_EXTRACT:
			if (!op.children.empty()) {
				return FigureColnameInternal(*op.children[0], name);
			}
			return 0;
		// PG: T_A_Indirection -- find last field name
		case ExpressionType::STRUCT_EXTRACT:
			if (op.children.size() == 2 && op.children[1]->expression_class == ExpressionClass::CONSTANT) {
				auto &constant = op.children[1]->Cast<ConstantExpression>();
				if (!constant.value.IsNull() && constant.value.type().id() == LogicalTypeId::VARCHAR) {
					name = StringUtil::Lower(constant.value.GetValue<string>());
					return 2;
				}
			}
			return FigureColnameInternal(*op.children[0], name);
		default:
			return 0;
		}
	}
	case ExpressionClass::WINDOW: {
		auto &window = expr.Cast<WindowExpression>();
		name = StringUtil::Lower(window.function_name);
		return 2;
	}
	default:
		return 0;
	}
}

string BaseExpression::GetName() const {
#ifdef DEBUG
	if (DBConfigOptions::debug_print_bindings) {
		return ToString();
	}
#endif
	if (!alias.empty()) {
		return alias;
	}
	// For generic expression display (e.g. EXPLAIN) fall back to the
	// expression text rather than PG's "?column?" target-list fallback.
	// PG-style target-list naming is in GetColumnName() below.
	return ToString();
}

// PG-compatible target-list column naming (FigureColname in parse_target.c).
// Used only when computing SELECT result column headers.
string BaseExpression::GetColumnName() const {
	if (!alias.empty()) {
		return alias;
	}
	string name;
	if (FigureColnameInternal(*this, name) > 0) {
		return name;
	}
	return "?column?";
}

bool BaseExpression::Equals(const BaseExpression &other) const {
	if (expression_class != other.expression_class || type != other.type) {
		return false;
	}
	return true;
}

void BaseExpression::Verify() const {
}

} // namespace duckdb
