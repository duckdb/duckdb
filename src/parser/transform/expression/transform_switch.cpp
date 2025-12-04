#include "duckdb/parser/expression/case_expression.hpp"
#include "duckdb/parser/expression/comparison_expression.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/transformer.hpp"

namespace duckdb {

unique_ptr<ParsedExpression> Transformer::TransformSwitch(duckdb_libpgquery::PGSwitchExpr &root) {
	auto case_node = make_uniq<CaseExpression>();
	auto root_arg = TransformExpression(PGPointerCast<duckdb_libpgquery::PGNode>(root.arg));
	auto expr = TransformExpression(PGPointerCast<duckdb_libpgquery::PGNode>(root.arg_map));
	if (expr->GetExpressionClass() != ExpressionClass::FUNCTION) {
		throw InternalException("Expected a map expression inside the SWITCH statement");
	}
	auto &map_expr = expr->Cast<FunctionExpression>();
	if (map_expr.function_name != "map") {
		throw InternalException("Expected a map expression inside the SWITCH statement");
	}
	Value keys;
	if (!ConstructConstantFromExpression(*map_expr.children[0], keys)) {
		throw NotImplementedException("Only constant expressions are supported for keys inside SWITCH");
	}

	Value values;
	if (!ConstructConstantFromExpression(*map_expr.children[1], values)) {
		throw ParserException("Only constant expressions are supported for values inside SWITCH");
	}

	vector<Value> keys_unpacked = ListValue::GetChildren(keys);
	vector<Value> values_unpacked = ListValue::GetChildren(values);
	if (keys_unpacked.empty()) {
		throw ParserException("No values provided for SWITCH expression");
	}
	for (idx_t i = 0; i < keys_unpacked.size(); i++) {
		CaseCheck case_check;
		unique_ptr<ConstantExpression> const_key;
		if (keys_unpacked[i].IsNull()) {
			const_key = make_uniq<ConstantExpression>(Value(LogicalType::SQLNULL));
		} else {
			const_key = make_uniq<ConstantExpression>(keys_unpacked[i]);
		}
		auto const_value = make_uniq<ConstantExpression>(values_unpacked[i]);
		if (root_arg) {
			case_check.when_expr =
			    make_uniq<ComparisonExpression>(ExpressionType::COMPARE_EQUAL, root_arg->Copy(), std::move(const_key));
		} else {
			case_check.when_expr = std::move(const_key);
		}
		case_check.then_expr = make_uniq<ConstantExpression>(values_unpacked[i]);
		case_node->case_checks.push_back(std::move(case_check));
	}
	if (root.defresult) {
		case_node->else_expr = TransformExpression(PGPointerCast<duckdb_libpgquery::PGNode>(root.defresult));
	} else {
		case_node->else_expr = make_uniq<ConstantExpression>(Value(LogicalType::SQLNULL));
	}
	SetQueryLocation(*case_node, root.location);
	Printer::Print(case_node->ToString());
	return std::move(case_node);
}

} // namespace duckdb
