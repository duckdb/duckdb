
#include "parser/transform.hpp"

#include "parser/expression/aggregate_expression.hpp"
#include "parser/expression/basetableref_expression.hpp"
#include "parser/expression/columnref_expression.hpp"
#include "parser/expression/comparison_expression.hpp"
#include "parser/expression/conjunction_expression.hpp"
#include "parser/expression/constant_expression.hpp"
#include "parser/expression/crossproduct_expression.hpp"
#include "parser/expression/function_expression.hpp"
#include "parser/expression/join_expression.hpp"
#include "parser/expression/operator_expression.hpp"
#include "parser/expression/subquery_expression.hpp"

using namespace duckdb;
using namespace std;

static bool IsAggregateFunction(const std::string &fun_name) {
	if (fun_name == "min" || fun_name == "max" || fun_name == "count" ||
	    fun_name == "avg" || fun_name == "sum")
		return true;
	return false;
}

std::string TransformAlias(Alias *root) {
	if (!root) {
		return "";
	}
	return root->aliasname;
}

static TypeId TransformStringToTypeId(char *str) {
	std::string lower_str = StringUtil::Lower(std::string(str));
	// Transform column type
	if (lower_str == "int" || lower_str == "int4") {
		return TypeId::INTEGER;
	} else if (lower_str == "varchar" || lower_str == "bpchar" ||
	           lower_str == "text") {
		return TypeId::VARCHAR;
	} else if (lower_str == "int8") {
		return TypeId::BIGINT;
	} else if (lower_str == "int2") {
		return TypeId::SMALLINT;
	} else if (lower_str == "timestamp") {
		return TypeId::TIMESTAMP;
	} else if (lower_str == "bool") {
		return TypeId::BOOLEAN;
	} else if (lower_str == "double" || lower_str == "float8" ||
	           lower_str == "real" || lower_str == "float4" ||
	           lower_str == "numeric") {
		return TypeId::DECIMAL;
	} else if (lower_str == "tinyint") {
		return TypeId::TINYINT;
	} else if (lower_str == "varbinary") {
		return TypeId::VARBINARY;
	} else if (lower_str == "date") {
		return TypeId::DATE;
	} else {
		throw NotImplementedException("DataType %s not supported yet...\n",
		                              str);
	}
}

unique_ptr<AbstractExpression> TransformTypeCast(TypeCast *root) {
	if (!root) {
		return nullptr;
	}
	switch (root->arg->type) {
	case T_A_Const: { // cast a constant value
		// get the original constant value
		auto constant =
		    TransformConstant(reinterpret_cast<A_Const *>(root->arg));
		Value &source_value =
		    reinterpret_cast<ConstantExpression *>(constant.get())->value;
		// get the type to cast to
		TypeName *type_name = root->typeName;
		char *name =
		    (reinterpret_cast<value *>(type_name->names->tail->data.ptr_value)
		         ->val.str);
		// perform the cast and substitute the expression
		Value new_value = source_value.CastAs(TransformStringToTypeId(name));
		throw NotImplementedException(
		    "TypeCast Source of type %d not supported yet...\n",
		    root->arg->type);

		return make_unique<ConstantExpression>(new_value);
	}
	default:
		throw NotImplementedException(
		    "TypeCast Source of type %d not supported yet...\n",
		    root->arg->type);
	}
}

unique_ptr<AbstractExpression> TransformBoolExpr(BoolExpr *root) {
	unique_ptr<AbstractExpression> result;
	for (auto node = root->args->head; node != nullptr; node = node->next) {
		auto next =
		    TransformExpression(reinterpret_cast<Node *>(node->data.ptr_value));

		switch (root->boolop) {
		case AND_EXPR: {
			if (!result) {
				result = move(next);
			} else {
				result = make_unique<ConjunctionExpression>(
				    ExpressionType::CONJUNCTION_AND, move(result), move(next));
			}
			break;
		}
		case OR_EXPR: {
			if (!result) {
				result = move(next);
			} else {
				result = make_unique<ConjunctionExpression>(
				    ExpressionType::CONJUNCTION_OR, move(result), move(next));
			}
			break;
		}
		case NOT_EXPR: {
			result = make_unique<OperatorExpression>(
			    ExpressionType::OPERATOR_NOT, TypeId::INVALID, move(next),
			    nullptr);
			break;
		}
		}
	}
	return result;
}

unique_ptr<AbstractExpression> TransformRangeVar(RangeVar *root) {
	auto result = make_unique<BaseTableRefExpression>();

	result->alias = TransformAlias(root->alias);
	if (root->relname)
		result->table_name = root->relname;
	if (root->schemaname)
		result->schema_name = root->schemaname;
	if (root->catalogname)
		result->database_name = root->catalogname;
	return move(result);
}

unique_ptr<AbstractExpression> TransformRangeSubselect(RangeSubselect *root) {
	auto result = make_unique<SubqueryExpression>();
	result->alias = TransformAlias(root->alias);
	result->subquery = move(TransformSelect(root->subquery));
	if (!result->subquery) {
		return nullptr;
	}

	return move(result);
}

unique_ptr<AbstractExpression> TransformJoin(JoinExpr *root) {
	auto result = make_unique<JoinExpression>();
	switch (root->jointype) {
	case JOIN_INNER: {
		result->type = duckdb::JoinType::INNER;
		break;
	}
	case JOIN_LEFT: {
		result->type = duckdb::JoinType::LEFT;
		break;
	}
	case JOIN_FULL: {
		result->type = duckdb::JoinType::OUTER;
		break;
	}
	case JOIN_RIGHT: {
		result->type = duckdb::JoinType::RIGHT;
		break;
	}
	case JOIN_SEMI: {
		result->type = duckdb::JoinType::SEMI;
		break;
	}
	default: {
		throw NotImplementedException("Join type %d not supported yet...\n",
		                              root->jointype);
	}
	}

	// Check the type of left arg and right arg before transform
	if (root->larg->type == T_RangeVar) {
		result->left =
		    move(TransformRangeVar(reinterpret_cast<RangeVar *>(root->larg)));
	} else if (root->larg->type == T_RangeSubselect) {
		result->left = move(TransformRangeSubselect(
		    reinterpret_cast<RangeSubselect *>(root->larg)));
	} else if (root->larg->type == T_JoinExpr) {
		result->left =
		    move(TransformJoin(reinterpret_cast<JoinExpr *>(root->larg)));
	} else {
		throw NotImplementedException("Join arg type %d not supported yet...\n",
		                              root->larg->type);
	}

	if (root->rarg->type == T_RangeVar) {
		result->right =
		    move(TransformRangeVar(reinterpret_cast<RangeVar *>(root->rarg)));
	} else if (root->rarg->type == T_RangeSubselect) {
		result->right = move(TransformRangeSubselect(
		    reinterpret_cast<RangeSubselect *>(root->rarg)));
	} else if (root->rarg->type == T_JoinExpr) {
		result->right =
		    move(TransformJoin(reinterpret_cast<JoinExpr *>(root->rarg)));
	} else {
		throw NotImplementedException("Join arg type %d not supported yet...\n",
		                              root->larg->type);
	}

	// transform the quals, depends on AExprTranform and BoolExprTransform
	switch (root->quals->type) {
	case T_A_Expr: {
		result->condition =
		    move(TransformAExpr(reinterpret_cast<A_Expr *>(root->quals)));
		break;
	}
	case T_BoolExpr: {
		result->condition =
		    move(TransformBoolExpr(reinterpret_cast<BoolExpr *>(root->quals)));
		break;
	}
	default: {
		throw NotImplementedException(
		    "Join quals type %d not supported yet...\n", root->larg->type);
	}
	}
	return move(result);
}

unique_ptr<AbstractExpression> TransformFrom(List *root) {
	if (!root) {
		return nullptr;
	}

	if (root->length > 1) {
		// Cross Product
		auto result = make_unique<CrossProductExpression>();
		for (auto node = root->head; node != nullptr; node = node->next) {
			Node *n = reinterpret_cast<Node *>(node->data.ptr_value);
			switch (n->type) {
			case T_RangeVar: {
				result->children.push_back(
				    move(TransformRangeVar(reinterpret_cast<RangeVar *>(n))));
				break;
			}
			case T_RangeSubselect: {
				result->children.push_back(move(TransformRangeSubselect(
				    reinterpret_cast<RangeSubselect *>(n))));
				break;
			}
			default: {
				throw NotImplementedException(
				    "From Type %d not supported yet...", n->type);
			}
			}
		}
		return move(result);
	}

	Node *n = reinterpret_cast<Node *>(root->head->data.ptr_value);
	switch (n->type) {
	case T_RangeVar: {
		return TransformRangeVar(reinterpret_cast<RangeVar *>(n));
	}
	case T_JoinExpr: {
		return TransformJoin(reinterpret_cast<JoinExpr *>(n));
	}
	case T_RangeSubselect: {
		return TransformRangeSubselect(reinterpret_cast<RangeSubselect *>(n));
	}
	default: {
		throw NotImplementedException("From Type %d not supported yet...",
		                              n->type);
	}
	}
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

unique_ptr<AbstractExpression> TransformConstant(A_Const *c) {
	return TransformValue(c->val);
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
		return TransformConstant(reinterpret_cast<A_Const *>(node));
	case T_A_Expr:
		return TransformAExpr(reinterpret_cast<A_Expr *>(node));
	case T_FuncCall:
		return TransformFuncCall(reinterpret_cast<FuncCall *>(node));
	case T_BoolExpr:
		return TransformBoolExpr(reinterpret_cast<BoolExpr *>(node));
	case T_TypeCast:
		return TransformTypeCast(reinterpret_cast<TypeCast *>(node));
	case T_ParamRef:
	case T_CaseExpr:
	case T_SubLink:
	case T_NullTest:
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
