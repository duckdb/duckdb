
#include "parser/transform.hpp"

#include "parser/expression/aggregate_expression.hpp"
#include "parser/expression/columnref_expression.hpp"
#include "parser/expression/comparison_expression.hpp"
#include "parser/expression/constant_expression.hpp"
#include "parser/expression/function_expression.hpp"
#include "parser/expression/operator_expression.hpp"

using namespace std;

static bool IsAggregateFunction(const std::string &fun_name) {
	if (fun_name == "min" || fun_name == "max" || fun_name == "count" ||
	    fun_name == "avg" || fun_name == "sum")
		return true;
	return false;
}

unique_ptr<AbstractExpression> TransformColumnRef(ColumnRef *root) {
	List *fields = root->fields;
	switch ((reinterpret_cast<Node *>(fields->head->data.ptr_value))->type) {
	case T_String: {
		if (fields->length < 1 || fields->length > 2) {
			throw ParserException("Unexpected field length");
		}
		string column_name = string(
		    reinterpret_cast<value *>(fields->head->data.ptr_value)->val.str);
		string table_name = fields->length == 1
		                        ? string()
		                        : string(reinterpret_cast<value *>(
		                                     fields->head->next->data.ptr_value)
		                                     ->val.str);
		return make_unique<ColumnRefExpression>(column_name, table_name);
	}
	case T_A_Star: {
		return make_unique<ColumnRefExpression>();
	}
	default:
		throw NotImplementedException("ColumnRef not implemented!");
	}
}

unique_ptr<AbstractExpression> TransformValue(value val) {
	switch (val.type) {
	case T_Integer:
		return make_unique<ConstantExpression>((int32_t)val.val.ival);
	case T_String:
		return make_unique<ConstantExpression>(string(val.val.str));
	case T_Float:
		return make_unique<ConstantExpression>(stod(string(val.val.str)));
	case T_Null:
		return make_unique<ConstantExpression>();
	default:
		throw NotImplementedException("Value not implemented!");
	}
}

unique_ptr<AbstractExpression> TransformAExpr(A_Expr *root) {
	if (!root) {
		return nullptr;
	}
	unique_ptr<AbstractExpression> result = nullptr;
	ExpressionType target_type;
	const char *name =
	    (reinterpret_cast<value *>(root->name->head->data.ptr_value))->val.str;
	if ((root->kind) != AEXPR_DISTINCT) {
		target_type = StringToExpressionType(std::string(name));
	} else {
		target_type = ExpressionType::COMPARE_DISTINCT_FROM;
		;
	}
	if (target_type == ExpressionType::INVALID) {
		return nullptr;
	}

	auto left_expr = TransformExpression(root->lexpr);
	auto right_expr = TransformExpression(root->rexpr);

	int type_id = static_cast<int>(target_type);
	if (type_id <= 6) {
		result = make_unique<OperatorExpression>(
		    target_type, TypeId::INVALID, move(left_expr), move(right_expr));
	} else if (((10 <= type_id) && (type_id <= 17)) || (type_id == 20)) {
		result = make_unique<ComparisonExpression>(target_type, move(left_expr),
		                                           move(right_expr));
	} else {
		throw NotImplementedException("A_Expr transform not implemented.");
	}
	return result;
}

unique_ptr<AbstractExpression> TransformFuncCall(FuncCall *root) {
	std::string fun_name = StringUtil::Lower(
	    (reinterpret_cast<value *>(root->funcname->head->data.ptr_value))
	        ->val.str);

	if (!IsAggregateFunction(fun_name)) {
		// Normal functions (i.e. built-in functions or UDFs)
		fun_name =
		    (reinterpret_cast<value *>(root->funcname->tail->data.ptr_value))
		        ->val.str;
		vector<unique_ptr<AbstractExpression>> children;
		if (root->args != nullptr) {
			for (auto node = root->args->head; node != nullptr;
			     node = node->next) {
				auto child_expr =
				    TransformExpression((Node *)node->data.ptr_value);
				children.push_back(move(child_expr));
			}
		}
		return make_unique<FunctionExpression>(fun_name.c_str(), children);
	} else {
		// Aggregate function
		auto agg_fun_type = StringToExpressionType("AGGREGATE_" + fun_name);
		if (root->agg_star) {
			return make_unique<AggregateExpression>(
			    agg_fun_type, false, make_unique<ColumnRefExpression>());
		} else {
			if (root->args->length < 2) {
				auto child = TransformExpression(
				    (Node *)root->args->head->data.ptr_value);
				return make_unique<AggregateExpression>(
				    agg_fun_type, root->agg_distinct, move(child));
			} else {
				throw NotImplementedException(
				    "Aggregation over multiple columns not supported yet...\n");
			}
		}
	}
}

unique_ptr<AbstractExpression> TransformExpression(Node *node) {
	if (!node) {
		return nullptr;
	}

	switch (node->type) {
	case T_ColumnRef:
		return TransformColumnRef(reinterpret_cast<ColumnRef *>(node));
	case T_A_Const:
		return TransformValue(reinterpret_cast<A_Const *>(node)->val);
	case T_A_Expr:
		return TransformAExpr(reinterpret_cast<A_Expr *>(node));
	case T_FuncCall:
		return TransformFuncCall(reinterpret_cast<FuncCall *>(node));
	case T_ParamRef:
	case T_BoolExpr:
	case T_CaseExpr:
	case T_SubLink:
	case T_NullTest:
	case T_TypeCast:
	default:
		throw NotImplementedException("Expr of type %d not implemented\n",
		                              (int)node->type);
	}
}

bool TransformExpressionList(List *list,
                             vector<unique_ptr<AbstractExpression>> &result) {
	if (!list) {
		return false;
	}
	for (auto node = list->head; node != nullptr; node = node->next) {
		auto target = reinterpret_cast<ResTarget *>(node->data.ptr_value);
		auto expr = TransformExpression(target->val);
		if (!target) {
			return false;
		}
		result.push_back(move(expr));
	}
	return true;
}
