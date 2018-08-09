
#include "parser/transform.hpp"

#include "parser/expression/expression_list.hpp"
#include "parser/tableref/tableref_list.hpp"

using namespace std;

namespace duckdb {

static bool IsAggregateFunction(const string &fun_name) {
	if (fun_name == "min" || fun_name == "max" || fun_name == "count" ||
	    fun_name == "avg" || fun_name == "sum")
		return true;
	return false;
}

string TransformAlias(Alias *root) {
	if (!root) {
		return "";
	}
	return root->aliasname;
}

TypeId TransformStringToTypeId(char *str) {
	string lower_str = StringUtil::Lower(string(str));
	// Transform column type
	if (lower_str == "int" || lower_str == "int4") {
		return TypeId::INTEGER;
	} else if (lower_str == "varchar" || lower_str == "bpchar" ||
	           lower_str == "text" || lower_str == "string") {
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
	// get the type to cast to
	TypeName *type_name = root->typeName;
	char *name =
	    (reinterpret_cast<value *>(type_name->names->tail->data.ptr_value)
	         ->val.str);
	TypeId target_type = TransformStringToTypeId(name);

	if (root->arg->type == T_A_Const) {
		// cast a constant value
		// get the original constant value
		auto constant =
		    TransformConstant(reinterpret_cast<A_Const *>(root->arg));
		Value &source_value =
		    reinterpret_cast<ConstantExpression *>(constant.get())->value;

		if (TypeIsIntegral(source_value.type) && TypeIsIntegral(target_type)) {
			// properly handle numeric overflows
			target_type = std::max(MinimalType(source_value.GetNumericValue()),
			                       target_type);
		}

		// perform the cast and substitute the expression
		Value new_value = source_value.CastAs(target_type);

		return make_unique<ConstantExpression>(new_value);
	} else {
		// transform the expression node
		auto expression = TransformExpression(root->arg);
		// now create a cast operation
		return make_unique<CastExpression>(target_type, move(expression));
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

unique_ptr<TableRef> TransformRangeVar(RangeVar *root) {
	auto result = make_unique<BaseTableRef>();

	result->alias = TransformAlias(root->alias);
	if (root->relname)
		result->table_name = root->relname;
	if (root->schemaname)
		result->schema_name = root->schemaname;
	if (root->catalogname)
		result->database_name = root->catalogname;
	return move(result);
}

unique_ptr<TableRef> TransformRangeSubselect(RangeSubselect *root) {
	auto result = make_unique<SubqueryRef>();
	result->alias = TransformAlias(root->alias);
	result->subquery = move(TransformSelect(root->subquery));
	if (!result->subquery) {
		return nullptr;
	}

	return move(result);
}

unique_ptr<TableRef> TransformJoin(JoinExpr *root) {
	auto result = make_unique<JoinRef>();
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

unique_ptr<TableRef> TransformFrom(List *root) {
	if (!root) {
		return nullptr;
	}

	if (root->length > 1) {
		// Cross Product
		auto result = make_unique<CrossProductRef>();
		for (auto node = root->head; node != nullptr; node = node->next) {
			if (result->left) {
				auto new_result = make_unique<CrossProductRef>();
				new_result->right = move(result);
				result = move(new_result);
			}

			Node *n = reinterpret_cast<Node *>(node->data.ptr_value);
			switch (n->type) {
			case T_RangeVar:
				result->left =
				    move(TransformRangeVar(reinterpret_cast<RangeVar *>(n)));
				break;
			case T_RangeSubselect:
				result->left = move(TransformRangeSubselect(
				    reinterpret_cast<RangeSubselect *>(n)));
				break;
			default:
				throw NotImplementedException(
				    "From Type %d not supported yet...", n->type);
			}
			if (!result->right) {
				result->right = move(result->left);
				result->left = nullptr;
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
		string column_name, table_name;
		if (fields->length == 1) {
			column_name =
			    string(reinterpret_cast<value *>(fields->head->data.ptr_value)
			               ->val.str);
		} else {
			table_name =
			    string(reinterpret_cast<value *>(fields->head->data.ptr_value)
			               ->val.str);
			column_name = string(
			    reinterpret_cast<value *>(fields->head->next->data.ptr_value)
			        ->val.str);
		}
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
		target_type = StringToExpressionType(string(name));
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
	string fun_name = StringUtil::Lower(
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
			if (!root->args) {
				throw NotImplementedException(
				    "Aggregation over zero columns not supported!");
			} else if (root->args->length < 2) {
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

unique_ptr<AbstractExpression> TransformCase(CaseExpr *root) {
	if (!root) {
		return nullptr;
	}
	// CASE expression WHEN value THEN result [WHEN ...] ELSE result uses this,
	// but we rewrite to CASE WHEN expression = value THEN result ... to only
	// have to handle one case downstream.
	auto arg = TransformExpression(reinterpret_cast<Node *>(root->arg));

	unique_ptr<AbstractExpression> def_res;
	if (root->defresult) {
		def_res = move(
		    TransformExpression(reinterpret_cast<Node *>(root->defresult)));
	} else {
		Value null = Value(1);
		null.is_null = true;
		def_res = unique_ptr<AbstractExpression>(new ConstantExpression(null));
	}
	// def_res will be the else part of the innermost case expression

	// CASE WHEN e1 THEN r1 WHEN w2 THEN r2 ELSE r3 is rewritten to
	// CASE WHEN e1 THEN r1 ELSE CASE WHEN e2 THEN r2 ELSE r3

	auto exp_root = unique_ptr<AbstractExpression>(new CaseExpression());
	AbstractExpression *cur_root = exp_root.get();
	AbstractExpression *next_root = nullptr;

	for (auto cell = root->args->head; cell != nullptr; cell = cell->next) {
		CaseWhen *w = reinterpret_cast<CaseWhen *>(cell->data.ptr_value);

		auto test_raw = TransformExpression(reinterpret_cast<Node *>(w->expr));
		unique_ptr<AbstractExpression> test;
		if (arg.get()) {
			test = unique_ptr<AbstractExpression>(new ComparisonExpression(
			    ExpressionType::COMPARE_EQUAL, move(arg), move(test_raw)));
		} else {
			test = move(test_raw);
		}

		auto res_true =
		    TransformExpression(reinterpret_cast<Node *>(w->result));

		unique_ptr<AbstractExpression> res_false;
		if (cell->next == nullptr) {
			res_false = move(def_res);
		} else {
			res_false = unique_ptr<AbstractExpression>(new CaseExpression());
			next_root = res_false.get();
		}

		cur_root->AddChild(move(test));
		cur_root->AddChild(move(res_true));
		cur_root->AddChild(move(res_false));

		cur_root = next_root;
	}

	return move(exp_root);
}

unique_ptr<AbstractExpression> TransformSubquery(SubLink *root) {
	if (!root) {
		return nullptr;
	}
	auto result = make_unique<SubqueryExpression>();
	result->subquery = move(TransformSelect(root->subselect));
	if (!result->subquery) {
		return nullptr;
	}

	switch (root->subLinkType) {
	// TODO: add IN/EXISTS subqueries
	//    case ANY_SUBLINK: {
	//
	//      auto col_expr = TransformExpression(root->testexpr);
	//      expr = new
	//      expression::ComparisonExpression(ExpressionType::COMPARE_IN,
	//                                                  col_expr,
	//                                                  subquery_expr);
	//      break;
	//    }
	//    case EXISTS_SUBLINK: {
	//      return new
	//      expression::OperatorExpression(ExpressionType::OPERATOR_EXISTS,
	//                                                type::TypeId::BOOLEAN,
	//                                                subquery_expr,
	//                                                nullptr);
	//      break;
	//    }
	case EXPR_SUBLINK: {
		return result;
	}
	default: {
		throw NotImplementedException("Subquery of type %d not implemented\n",
		                              (int)root->subLinkType);
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
	case T_CaseExpr:
		return TransformCase(reinterpret_cast<CaseExpr *>(node));
	case T_SubLink:
		return TransformSubquery(reinterpret_cast<SubLink *>(node));

	case T_ParamRef:
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
		if (!target) {
			return false;
		}
		auto expr = TransformExpression(target->val);
		if (target->name) {
			expr->alias = string(target->name);
		}
		result.push_back(move(expr));
	}
	return true;
}

unique_ptr<AbstractExpression> TransformListValue(Expr *node) {
	if (!node) {
		return nullptr;
	}

	switch (node->type) {
	case T_A_Const:
		return TransformConstant((A_Const *)node);
	case T_ParamRef:
	case T_TypeCast:
	case T_SetToDefault:

	default:
		throw NotImplementedException("Expr of type %d not implemented\n",
		                              (int)node->type);
	}
}

bool TransformValueList(List *list,
                        vector<unique_ptr<AbstractExpression>> &result) {
	if (!list) {
		return false;
	}
	for (auto value_list = list->head; value_list != NULL;
	     value_list = value_list->next) {
		List *target = (List *)(value_list->data.ptr_value);

		for (auto node = target->head; node != nullptr; node = node->next) {
			auto val = reinterpret_cast<Expr *>(node->data.ptr_value);

			if (!val) {
				return false;
			}
			auto expr = TransformListValue(val);
			result.push_back(move(expr));
		}
	}
	return true;
}
} // namespace duckdb
