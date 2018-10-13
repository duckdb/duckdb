
#include "parser/transform.hpp"

#include "parser/expression/list.hpp"
#include "parser/tableref/tableref_list.hpp"

using namespace postgres;
using namespace std;

namespace duckdb {

static bool IsAggregateFunction(const string &fun_name) {
	if (fun_name == "min" || fun_name == "max" || fun_name == "count" ||
	    fun_name == "avg" || fun_name == "sum" || fun_name == "first")
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
	if (lower_str == "int" || lower_str == "int4" || lower_str == "signed") {
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

unique_ptr<Expression> TransformTypeCast(TypeCast *root) {
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

		if (!source_value.is_null && TypeIsIntegral(source_value.type) &&
		    TypeIsIntegral(target_type)) {
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

unique_ptr<Expression> TransformBoolExpr(BoolExpr *root) {
	unique_ptr<Expression> result;
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
			    ExpressionType::OPERATOR_NOT, TypeId::BOOLEAN, move(next),
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
	result->subquery = TransformSelect(root->subquery);
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
		    TransformRangeVar(reinterpret_cast<RangeVar *>(root->larg));
	} else if (root->larg->type == T_RangeSubselect) {
		result->left = TransformRangeSubselect(
		    reinterpret_cast<RangeSubselect *>(root->larg));
	} else if (root->larg->type == T_JoinExpr) {
		result->left = TransformJoin(reinterpret_cast<JoinExpr *>(root->larg));
	} else {
		throw NotImplementedException("Join arg type %d not supported yet...\n",
		                              root->larg->type);
	}

	if (root->rarg->type == T_RangeVar) {
		result->right =
		    TransformRangeVar(reinterpret_cast<RangeVar *>(root->rarg));
	} else if (root->rarg->type == T_RangeSubselect) {
		result->right = TransformRangeSubselect(
		    reinterpret_cast<RangeSubselect *>(root->rarg));
	} else if (root->rarg->type == T_JoinExpr) {
		result->right = TransformJoin(reinterpret_cast<JoinExpr *>(root->rarg));
	} else {
		throw NotImplementedException("Join arg type %d not supported yet...\n",
		                              root->larg->type);
	}

	if (!root->quals) { // CROSS JOIN
		auto cross = make_unique<CrossProductRef>();
		cross->left = move(result->left);
		cross->right = move(result->right);
		return move(cross);
	}

	// transform the quals, depends on AExprTranform and BoolExprTransform
	switch (root->quals->type) {
	case T_A_Expr: {
		result->condition =
		    TransformAExpr(reinterpret_cast<A_Expr *>(root->quals));
		break;
	}
	case T_BoolExpr: {
		result->condition =
		    TransformBoolExpr(reinterpret_cast<BoolExpr *>(root->quals));
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
		CrossProductRef *cur_root = result.get();
		for (auto node = root->head; node != nullptr; node = node->next) {
			unique_ptr<TableRef> next;
			Node *n = reinterpret_cast<Node *>(node->data.ptr_value);
			switch (n->type) {
			case T_RangeVar:
				next = TransformRangeVar(reinterpret_cast<RangeVar *>(n));
				break;
			case T_RangeSubselect:
				next = TransformRangeSubselect(
				    reinterpret_cast<RangeSubselect *>(n));
				break;
			case T_JoinExpr:
				next = TransformJoin(reinterpret_cast<JoinExpr *>(n));
				break;
			default:
				throw NotImplementedException(
				    "From Type %d not supported yet...", n->type);
			}
			if (!cur_root->left) {
				cur_root->left = move(next);
			} else if (!cur_root->right) {
				cur_root->right = move(next);
			} else {
				auto old_res = move(result);
				result = make_unique<CrossProductRef>();
				result->left = move(old_res);
				result->right = move(next);
				cur_root = result.get();
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

unique_ptr<Expression> TransformColumnRef(ColumnRef *root) {
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
		return make_unique<StarExpression>();
	}
	default:
		throw NotImplementedException("ColumnRef not implemented!");
	}
}

unique_ptr<Expression> TransformValue(value val) {
	switch (val.type) {
	case T_Integer:
		return make_unique<ConstantExpression>(Value::INTEGER(val.val.ival));
	case T_BitString: // FIXME: this should actually convert to BLOB
	case T_String:
		return make_unique<ConstantExpression>(Value(string(val.val.str)));
	case T_Float:
		return make_unique<ConstantExpression>(
		    Value(stod(string(val.val.str))));
	case T_Null:
		return make_unique<ConstantExpression>();
	default:
		throw NotImplementedException("Value not implemented!");
	}
}

unique_ptr<Expression> TransformConstant(A_Const *c) {
	return TransformValue(c->val);
}

// COALESCE(a,b,c) returns the first argument that is NOT NULL, so
// rewrite into CASE(a IS NOT NULL, a, CASE(b IS NOT NULL, b, c))
unique_ptr<Expression> TransformCoalesce(A_Expr *root) {
	if (!root) {
		return nullptr;
	}
	auto coalesce_args = reinterpret_cast<List *>(root->lexpr);
	// TODO: this is somewhat duplicated from the CASE rewrite below, perhaps
	// they can be merged
	auto exp_root = unique_ptr<Expression>(new CaseExpression());
	Expression *cur_root = exp_root.get();
	Expression *next_root = nullptr;

	for (auto cell = coalesce_args->head; cell && cell->next;
	     cell = cell->next) {
		// we need this twice
		auto value_expr =
		    TransformExpression(reinterpret_cast<Node *>(cell->data.ptr_value));
		auto res_true =
		    TransformExpression(reinterpret_cast<Node *>(cell->data.ptr_value));

		auto test = unique_ptr<Expression>(
		    new OperatorExpression(ExpressionType::OPERATOR_IS_NOT_NULL,
		                           TypeId::BOOLEAN, move(value_expr)));

		// the last argument does not need its own CASE because if we get there
		// we might as well return it directly
		unique_ptr<Expression> res_false;
		if (cell->next->next == nullptr) {
			res_false = TransformExpression(
			    reinterpret_cast<Node *>(cell->next->data.ptr_value));
		} else {
			res_false = unique_ptr<Expression>(new CaseExpression());
			next_root = res_false.get();
		}
		cur_root->AddChild(move(test));
		cur_root->AddChild(move(res_true));
		cur_root->AddChild(move(res_false));
		cur_root = next_root;
	}
	return exp_root;
}

unique_ptr<Expression> TransformNullTest(NullTest *root) {
	if (!root) {
		return nullptr;
	}
	auto arg = TransformExpression(reinterpret_cast<Node *>(root->arg));
	if (root->argisrow) {
		throw NotImplementedException("IS NULL argisrow");
	}
	ExpressionType expr_type = (root->nulltesttype == IS_NULL)
	                               ? ExpressionType::OPERATOR_IS_NULL
	                               : ExpressionType::OPERATOR_IS_NOT_NULL;

	return unique_ptr<Expression>(
	    new OperatorExpression(expr_type, TypeId::BOOLEAN, move(arg)));
}

unique_ptr<Expression> TransformAExpr(A_Expr *root) {
	if (!root) {
		return nullptr;
	}
	ExpressionType target_type;
	auto name = string(
	    (reinterpret_cast<value *>(root->name->head->data.ptr_value))->val.str);

	switch (root->kind) {
	case AEXPR_DISTINCT:
		target_type = ExpressionType::COMPARE_DISTINCT_FROM;
		break;
	case AEXPR_IN: {
		auto left_expr = TransformExpression(root->lexpr);
		auto result = make_unique<OperatorExpression>(
		    ExpressionType::COMPARE_IN, TypeId::BOOLEAN, move(left_expr));
		TransformExpressionList((List *)root->rexpr, result->children);
		// this looks very odd, but seems to be the way to find out its NOT IN
		if (name == "<>") {
			return make_unique<OperatorExpression>(ExpressionType::OPERATOR_NOT,
			                                       TypeId::BOOLEAN,
			                                       move(result), nullptr);
		} else {
			return move(result);
		}
	} break;
	// rewrite NULLIF(a, b) into CASE WHEN a=b THEN NULL ELSE a END
	case AEXPR_NULLIF: {
		auto case_expr = unique_ptr<Expression>(new CaseExpression());
		auto test_expr = unique_ptr<Expression>(new ComparisonExpression(
		    ExpressionType::COMPARE_EQUAL, TransformExpression(root->lexpr),
		    TransformExpression(root->rexpr)));
		case_expr->AddChild(move(test_expr));
		auto null_expr =
		    unique_ptr<Expression>(new ConstantExpression(Value()));
		case_expr->AddChild(move(null_expr));
		case_expr->AddChild(TransformExpression(root->lexpr));
		return case_expr;
	} break;
	// rewrite (NOT) X BETWEEN A AND B into (NOT) AND(GREATERTHANOREQUALTO(X,
	// A), LESSTHANOREQUALTO(X, B))
	case AEXPR_BETWEEN:
	case AEXPR_NOT_BETWEEN: {
		auto between_args = reinterpret_cast<List *>(root->rexpr);

		if (between_args->length != 2 || !between_args->head->data.ptr_value ||
		    !between_args->tail->data.ptr_value) {
			throw Exception("(NOT) BETWEEN needs two args");
		}

		auto between_left = TransformExpression(
		    reinterpret_cast<Node *>(between_args->head->data.ptr_value));
		auto between_right = TransformExpression(
		    reinterpret_cast<Node *>(between_args->tail->data.ptr_value));

		auto compare_left = make_unique<ComparisonExpression>(
		    ExpressionType::COMPARE_GREATERTHANOREQUALTO,
		    TransformExpression(root->lexpr), move(between_left));
		auto compare_right = make_unique<ComparisonExpression>(
		    ExpressionType::COMPARE_LESSTHANOREQUALTO,
		    TransformExpression(root->lexpr), move(between_right));
		auto compare_between = make_unique<ConjunctionExpression>(
		    ExpressionType::CONJUNCTION_AND, move(compare_left),
		    move(compare_right));
		if (root->kind == AEXPR_BETWEEN) {
			return move(compare_between);
		} else {
			return make_unique<OperatorExpression>(
			    ExpressionType::OPERATOR_NOT, TypeId::BOOLEAN,
			    move(compare_between), nullptr);
		}
	} break;
	default: {
		target_type = StringToExpressionType(name);
		if (target_type == ExpressionType::INVALID) {
			throw NotImplementedException(
			    "A_Expr transform not implemented %s.", name.c_str());
		}
	}
	}

	// continuing default case
	auto left_expr = TransformExpression(root->lexpr);
	auto right_expr = TransformExpression(root->rexpr);
	if (!left_expr) {
		switch (target_type) {
		case ExpressionType::OPERATOR_ADD:
			return right_expr;
		case ExpressionType::OPERATOR_SUBTRACT:
			target_type = ExpressionType::OPERATOR_MULTIPLY;
			left_expr = make_unique<ConstantExpression>(Value(-1));
			break;
		default:
			throw Exception("Unknown unary operator");
		}
	}

	unique_ptr<Expression> result = nullptr;
	int type_id = static_cast<int>(target_type);
	if (type_id >= static_cast<int>(ExpressionType::BINOP_BOUNDARY_START) &&
	    type_id <= static_cast<int>(ExpressionType::BINOP_BOUNDARY_END)) {
		// binary operator
		result = make_unique<OperatorExpression>(
		    target_type, TypeId::INVALID, move(left_expr), move(right_expr));
	} else if (type_id >=
	               static_cast<int>(ExpressionType::COMPARE_BOUNDARY_START) &&
	           type_id <=
	               static_cast<int>(ExpressionType::COMPARE_BOUNDARY_END)) {
		result = make_unique<ComparisonExpression>(target_type, move(left_expr),
		                                           move(right_expr));
	} else {
		throw NotImplementedException("A_Expr transform not implemented.");
	}
	return result;
}

unique_ptr<Expression> TransformFuncCall(FuncCall *root) {
	string fun_name = StringUtil::Lower(
	    (reinterpret_cast<value *>(root->funcname->head->data.ptr_value))
	        ->val.str);

	if (!IsAggregateFunction(fun_name)) {
		// Normal functions (i.e. built-in functions or UDFs)
		fun_name =
		    (reinterpret_cast<value *>(root->funcname->tail->data.ptr_value))
		        ->val.str;
		vector<unique_ptr<Expression>> children;
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
			    agg_fun_type, false, make_unique<StarExpression>());
		} else {
			if (!root->args) {
				throw NotImplementedException(
				    "Aggregation over zero columns not supported!");
			} else if (root->args->length < 2) {

				if (agg_fun_type == ExpressionType::AGGREGATE_AVG) {
					// rewrite AVG(a) to SUM(a) / COUNT(a)
					// first create the SUM
					auto sum = make_unique<AggregateExpression>(
					    ExpressionType::AGGREGATE_SUM, root->agg_distinct,
					    TransformExpression(
					        (Node *)root->args->head->data.ptr_value));
					// now create the count
					auto count = make_unique<AggregateExpression>(
					    ExpressionType::AGGREGATE_COUNT, root->agg_distinct,
					    TransformExpression(
					        (Node *)root->args->head->data.ptr_value));
					// cast both to decimal
					auto sum_cast =
					    make_unique<CastExpression>(TypeId::DECIMAL, move(sum));
					auto count_cast = make_unique<CastExpression>(
					    TypeId::DECIMAL, move(count));
					// create the divide operator
					return make_unique<OperatorExpression>(
					    ExpressionType::OPERATOR_DIVIDE, TypeId::DECIMAL,
					    move(sum_cast), move(count_cast));
				} else {
					auto child = TransformExpression(
					    (Node *)root->args->head->data.ptr_value);
					return make_unique<AggregateExpression>(
					    agg_fun_type, root->agg_distinct, move(child));
				}
			} else {
				throw NotImplementedException(
				    "Aggregation over multiple columns not supported yet...\n");
			}
		}
	}
}

unique_ptr<Expression> TransformCase(CaseExpr *root) {
	if (!root) {
		return nullptr;
	}
	// CASE expression WHEN value THEN result [WHEN ...] ELSE result uses this,
	// but we rewrite to CASE WHEN expression = value THEN result ... to only
	// have to handle one case downstream.

	unique_ptr<Expression> def_res;
	if (root->defresult) {
		def_res =
		    TransformExpression(reinterpret_cast<Node *>(root->defresult));
	} else {
		def_res = unique_ptr<Expression>(new ConstantExpression(Value()));
	}
	// def_res will be the else part of the innermost case expression

	// CASE WHEN e1 THEN r1 WHEN w2 THEN r2 ELSE r3 is rewritten to
	// CASE WHEN e1 THEN r1 ELSE CASE WHEN e2 THEN r2 ELSE r3

	auto exp_root = unique_ptr<Expression>(new CaseExpression());
	Expression *cur_root = exp_root.get();
	Expression *next_root = nullptr;

	for (auto cell = root->args->head; cell != nullptr; cell = cell->next) {
		CaseWhen *w = reinterpret_cast<CaseWhen *>(cell->data.ptr_value);

		auto test_raw = TransformExpression(reinterpret_cast<Node *>(w->expr));
		unique_ptr<Expression> test;
		// TODO: how do we copy those things?
		auto arg = TransformExpression(reinterpret_cast<Node *>(root->arg));

		if (arg) {
			test = unique_ptr<Expression>(new ComparisonExpression(
			    ExpressionType::COMPARE_EQUAL, move(arg), move(test_raw)));
		} else {
			test = move(test_raw);
		}

		auto res_true =
		    TransformExpression(reinterpret_cast<Node *>(w->result));

		unique_ptr<Expression> res_false;
		if (cell->next == nullptr) {
			res_false = move(def_res);
		} else {
			res_false = unique_ptr<Expression>(new CaseExpression());
			next_root = res_false.get();
		}

		cur_root->AddChild(move(test));
		cur_root->AddChild(move(res_true));
		cur_root->AddChild(move(res_false));

		cur_root = next_root;
	}

	return exp_root;
}

unique_ptr<Expression> TransformSubquery(SubLink *root) {
	if (!root) {
		return nullptr;
	}
	auto subquery_expr = make_unique<SubqueryExpression>();
	subquery_expr->subquery = TransformSelect(root->subselect);
	if (!subquery_expr->subquery) {
		return nullptr;
	}

	switch (root->subLinkType) {
	case EXISTS_SUBLINK: {
		subquery_expr->type = SubqueryType::EXISTS;
		return make_unique<OperatorExpression>(ExpressionType::OPERATOR_EXISTS,
		                                       TypeId::BOOLEAN,
		                                       move(subquery_expr));
	}
	case ANY_SUBLINK: {
		subquery_expr->type = SubqueryType::IN;
		return make_unique<OperatorExpression>(
		    ExpressionType::COMPARE_IN, TypeId::BOOLEAN,
		    TransformExpression(root->testexpr), move(subquery_expr));
	}
	case EXPR_SUBLINK: {
		return subquery_expr;
	}
	default: {
		throw NotImplementedException("Subquery of type %d not implemented\n",
		                              (int)root->subLinkType);
	}
	}
}

unique_ptr<Expression> TransformResTarget(ResTarget *root) {
	if (!root) {
		return nullptr;
	}
	auto expr = TransformExpression(root->val);
	if (!expr) {
		return nullptr;
	}
	if (root->name) {
		expr->alias = string(root->name);
	}
	return expr;
}

unique_ptr<Expression> TransformExpression(Node *node) {
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
	case T_CoalesceExpr:
		return TransformCoalesce(reinterpret_cast<A_Expr *>(node));
	case T_NullTest:
		return TransformNullTest(reinterpret_cast<NullTest *>(node));
	case T_ResTarget:
		return TransformResTarget(reinterpret_cast<ResTarget *>(node));
	case T_SetToDefault:
		return make_unique<DefaultExpression>();
	case T_ParamRef:

	default:
		throw NotImplementedException("Expr of type %d not implemented\n",
		                              (int)node->type);
	}
}

bool TransformExpressionList(List *list,
                             vector<unique_ptr<Expression>> &result) {
	if (!list) {
		return false;
	}
	for (auto node = list->head; node != nullptr; node = node->next) {
		auto target = reinterpret_cast<Node *>(node->data.ptr_value);
		if (!target) {
			return false;
		}
		auto expr = TransformExpression(target);
		if (!expr) {
			return false;
		}
		result.push_back(move(expr));
	}
	return true;
}

unique_ptr<Expression> TransformListValue(Expr *node) {
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

} // namespace duckdb
