#include "from_substrait.hpp"

#include "duckdb/common/types/value.hpp"
#include "duckdb/parser/expression/list.hpp"
#include "duckdb/main/relation/join_relation.hpp"
#include "duckdb/main/relation/cross_product_relation.hpp"

#include "duckdb/main/relation/limit_relation.hpp"
#include "duckdb/main/relation/projection_relation.hpp"
#include "duckdb/main/relation/aggregate_relation.hpp"
#include "duckdb/main/relation/filter_relation.hpp"
#include "duckdb/main/relation/order_relation.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/common/exception.hpp"

#include "substrait/plan.pb.h"

namespace duckdb {
SubstraitToDuckDB::SubstraitToDuckDB(Connection &con_p, string &serialized) : con(con_p) {
	plan.ParseFromString(serialized);
	for (auto &sext : plan.extensions()) {
		if (!sext.has_extension_function()) {
			continue;
		}
		functions_map[sext.extension_function().function_anchor()] = sext.extension_function().name();
	}
}

unique_ptr<ParsedExpression> SubstraitToDuckDB::TransformLiteralExpr(const substrait::Expression &sexpr) {
	const auto &slit = sexpr.literal();
	Value dval;
	switch (slit.literal_type_case()) {
	case substrait::Expression_Literal::LiteralTypeCase::kFp64:
		dval = Value::DOUBLE(slit.fp64());
		break;

	case substrait::Expression_Literal::LiteralTypeCase::kString:
		dval = Value(slit.string());
		break;
	case substrait::Expression_Literal::LiteralTypeCase::kDecimal: {
		const auto &substrait_decimal = slit.decimal();
		// TODO: Support hugeint?
		int64_t substrait_value = stoll(substrait_decimal.value());
		dval = Value::DECIMAL(substrait_value, substrait_decimal.precision(), substrait_decimal.scale());
		break;
	}

	case substrait::Expression_Literal::LiteralTypeCase::kBoolean: {
		dval = Value(slit.boolean());
		break;
	}
	case substrait::Expression_Literal::LiteralTypeCase::kI32:
		dval = Value::INTEGER(slit.i32());
		break;
	case substrait::Expression_Literal::LiteralTypeCase::kI64:
		dval = Value::BIGINT(slit.i64());
		break;
	default:
		throw InternalException(to_string(slit.literal_type_case()));
	}
	return make_unique<ConstantExpression>(dval);
}

unique_ptr<ParsedExpression> SubstraitToDuckDB::TransformSelectionExpr(const substrait::Expression &sexpr) {
	if (!sexpr.selection().has_direct_reference() || !sexpr.selection().direct_reference().has_struct_field()) {
		throw InternalException("Can only have direct struct references in selections");
	}
	return make_unique<PositionalReferenceExpression>(sexpr.selection().direct_reference().struct_field().field() + 1);
}

unique_ptr<ParsedExpression> SubstraitToDuckDB::TransformScalarFunctionExpr(const substrait::Expression &sexpr) {
	vector<unique_ptr<ParsedExpression>> children;
	for (auto &sarg : sexpr.scalar_function().args()) {
		children.push_back(TransformExpr(sarg));
	}
	auto function_name = FindFunction(sexpr.scalar_function().function_reference());
	// string compare galore
	// TODO simplify this
	if (function_name == "and") {
		return make_unique<ConjunctionExpression>(ExpressionType::CONJUNCTION_AND, move(children));
	} else if (function_name == "cast") {
		// TODO actually unpack the constant expression here and not rely on ToString
		auto expr = Parser::ParseExpressionList(StringUtil::Format("asdf::%s", children[1]->ToString()));
		auto &cast = (CastExpression &)*expr[0];
		return make_unique<CastExpression>(cast.cast_type, move(children[0]));
	} else if (function_name == "or") {
		return make_unique<ConjunctionExpression>(ExpressionType::CONJUNCTION_OR, move(children));
	} else if (function_name == "lessthan") {
		return make_unique<ComparisonExpression>(ExpressionType::COMPARE_LESSTHAN, move(children[0]),
		                                         move(children[1]));
	} else if (function_name == "equal") {
		return make_unique<ComparisonExpression>(ExpressionType::COMPARE_EQUAL, move(children[0]), move(children[1]));
	} else if (function_name == "notequal") {
		return make_unique<ComparisonExpression>(ExpressionType::COMPARE_NOTEQUAL, move(children[0]),
		                                         move(children[1]));
	} else if (function_name == "lessthanequal") {
		return make_unique<ComparisonExpression>(ExpressionType::COMPARE_LESSTHANOREQUALTO, move(children[0]),
		                                         move(children[1]));
	} else if (function_name == "greaterthanequal") {
		return make_unique<ComparisonExpression>(ExpressionType::COMPARE_GREATERTHANOREQUALTO, move(children[0]),
		                                         move(children[1]));
	} else if (function_name == "greaterthan") {
		return make_unique<ComparisonExpression>(ExpressionType::COMPARE_GREATERTHAN, move(children[0]),
		                                         move(children[1]));
	} else if (function_name == "is_not_null") {
		return make_unique<OperatorExpression>(ExpressionType::OPERATOR_IS_NOT_NULL, move(children[0]));
	}

	return make_unique<FunctionExpression>(function_name, move(children));
}

unique_ptr<ParsedExpression> SubstraitToDuckDB::TransformIfThenExpr(const substrait::Expression &sexpr) {
	const auto &scase = sexpr.if_then();
	auto dcase = make_unique<CaseExpression>();
	for (const auto &sif : scase.ifs()) {
		CaseCheck dif;
		dif.when_expr = TransformExpr(sif.if_());
		dif.then_expr = TransformExpr(sif.then());
		dcase->case_checks.push_back(move(dif));
	}
	dcase->else_expr = TransformExpr(scase.else_());
	return dcase;
}

unique_ptr<ParsedExpression> SubstraitToDuckDB::TransformExpr(const substrait::Expression &sexpr) {
	switch (sexpr.rex_type_case()) {
	case substrait::Expression::RexTypeCase::kLiteral:
		return TransformLiteralExpr(sexpr);
	case substrait::Expression::RexTypeCase::kSelection:
		return TransformSelectionExpr(sexpr);
	case substrait::Expression::RexTypeCase::kScalarFunction:
		return TransformScalarFunctionExpr(sexpr);
	case substrait::Expression::RexTypeCase::kIfThen:
		return TransformIfThenExpr(sexpr);
	default:
		throw InternalException("Unsupported expression type " + to_string(sexpr.rex_type_case()));
	}
}

string SubstraitToDuckDB::FindFunction(uint64_t id) {
	if (functions_map.find(id) == functions_map.end()) {
		throw InternalException("Could not find aggregate function " + to_string(id));
	}
	return functions_map[id];
}

OrderByNode SubstraitToDuckDB::TransformOrder(const substrait::SortField &sordf) {

	OrderType dordertype;
	OrderByNullType dnullorder;

	switch (sordf.direction()) {
	case substrait::SortField_SortDirection::SortField_SortDirection_SORT_DIRECTION_ASC_NULLS_FIRST:
		dordertype = OrderType::ASCENDING;
		dnullorder = OrderByNullType::NULLS_FIRST;
		break;
	case substrait::SortField_SortDirection::SortField_SortDirection_SORT_DIRECTION_ASC_NULLS_LAST:
		dordertype = OrderType::ASCENDING;
		dnullorder = OrderByNullType::NULLS_LAST;
		break;
	case substrait::SortField_SortDirection::SortField_SortDirection_SORT_DIRECTION_DESC_NULLS_FIRST:
		dordertype = OrderType::DESCENDING;
		dnullorder = OrderByNullType::NULLS_FIRST;
		break;
	case substrait::SortField_SortDirection::SortField_SortDirection_SORT_DIRECTION_DESC_NULLS_LAST:
		dordertype = OrderType::DESCENDING;
		dnullorder = OrderByNullType::NULLS_LAST;
		break;
	default:
		throw InternalException("Unsupported ordering " + to_string(sordf.direction()));
	}

	return {dordertype, dnullorder, TransformExpr(sordf.expr())};
}

shared_ptr<Relation> SubstraitToDuckDB::TransformJoinOp(const substrait::Rel &sop) {
	auto &sjoin = sop.join();

	JoinType djointype;
	switch (sjoin.type()) {
	case substrait::JoinRel::JoinType::JoinRel_JoinType_JOIN_TYPE_INNER:
		djointype = JoinType::INNER;
		break;
	case substrait::JoinRel::JoinType::JoinRel_JoinType_JOIN_TYPE_LEFT:
		djointype = JoinType::LEFT;
		break;
	case substrait::JoinRel::JoinType::JoinRel_JoinType_JOIN_TYPE_RIGHT:
		djointype = JoinType::RIGHT;
		break;
		//		case substrait::JoinRel::JoinType::JoinRel_JoinType_JOIN_TYPE_MARK:
		//			djointype = JoinType::MARK;
		//			break;
		//		case substrait::JoinRel::JoinType::JoinRel_JoinType_JOIN_TYPE_SINGLE:
		//			djointype = JoinType::SINGLE;
		//			break;
	case substrait::JoinRel::JoinType::JoinRel_JoinType_JOIN_TYPE_SEMI:
		djointype = JoinType::SEMI;
		break;
	default:
		throw InternalException("Unsupported join type");
	}
	vector<unique_ptr<ParsedExpression>> expressions;
	//		for (auto &sexpr : sjoin.duplicate_eliminated_columns()) {
	//			expressions.push_back(TransformExpr(sexpr));
	//		}
	return make_shared<JoinRelation>(TransformOp(sjoin.left())->Alias("left"),
	                                 TransformOp(sjoin.right())->Alias("right"), TransformExpr(sjoin.expression()),
	                                 djointype);
}

shared_ptr<Relation> SubstraitToDuckDB::TransformCrossProductOp(const substrait::Rel &sop) {
	auto &sub_cross = sop.cross();

	return make_shared<CrossProductRelation>(TransformOp(sub_cross.left())->Alias("left"),
	                                         TransformOp(sub_cross.right())->Alias("right"));
}

shared_ptr<Relation> SubstraitToDuckDB::TransformFetchOp(const substrait::Rel &sop) {
	auto &slimit = sop.fetch();
	return make_shared<LimitRelation>(TransformOp(slimit.input()), slimit.count(), slimit.offset());
}

shared_ptr<Relation> SubstraitToDuckDB::TransformFilterOp(const substrait::Rel &sop) {
	auto &sfilter = sop.filter();
	return make_shared<FilterRelation>(TransformOp(sfilter.input()), TransformExpr(sfilter.condition()));
}

shared_ptr<Relation> SubstraitToDuckDB::TransformProjectOp(const substrait::Rel &sop) {
	vector<unique_ptr<ParsedExpression>> expressions;
	vector<string> aliases;
	idx_t expr_idx = 1;
	for (auto &sexpr : sop.project().expressions()) {
		expressions.push_back(TransformExpr(sexpr));
		aliases.push_back("expr_" + to_string(expr_idx++));
	}
	return make_shared<ProjectionRelation>(TransformOp(sop.project().input()), move(expressions), move(aliases));
}

shared_ptr<Relation> SubstraitToDuckDB::TransformAggregateOp(const substrait::Rel &sop) {
	vector<unique_ptr<ParsedExpression>> groups, expressions;

	if (sop.aggregate().groupings_size() > 1) {
		throw InternalException("Only single grouping sets are supported for now");
	}
	if (sop.aggregate().groupings_size() > 0) {
		for (auto &sgrpexpr : sop.aggregate().groupings(0).grouping_expressions()) {
			groups.push_back(TransformExpr(sgrpexpr));
			expressions.push_back(TransformExpr(sgrpexpr));
		}
	}

	for (auto &smeas : sop.aggregate().measures()) {
		vector<unique_ptr<ParsedExpression>> children;
		for (auto &sarg : smeas.measure().args()) {
			children.push_back(TransformExpr(sarg));
		}
		expressions.push_back(
		    make_unique<FunctionExpression>(FindFunction(smeas.measure().function_reference()), move(children)));
	}

	return make_shared<AggregateRelation>(TransformOp(sop.aggregate().input()), move(expressions), move(groups));
}

shared_ptr<Relation> SubstraitToDuckDB::TransformReadOp(const substrait::Rel &sop) {
	auto &sget = sop.read();
	if (!sget.has_named_table()) {
		throw InternalException("Can only scan named tables for now");
	}

	auto scan = con.Table(sop.read().named_table().names(0));

	if (sget.has_filter()) {
		scan = make_shared<FilterRelation>(move(scan), TransformExpr(sget.filter()));
	}

	if (sget.has_projection()) {
		vector<unique_ptr<ParsedExpression>> expressions;
		vector<string> aliases;
		idx_t expr_idx = 0;
		for (auto &sproj : sget.projection().select().struct_items()) {
			// FIXME how to get actually alias?
			aliases.push_back("expr_" + to_string(expr_idx++));
			// TODO make sure nothing else is in there
			expressions.push_back(make_unique<PositionalReferenceExpression>(sproj.field() + 1));
		}

		scan = make_shared<ProjectionRelation>(move(scan), move(expressions), move(aliases));
	}

	return scan;
}

shared_ptr<Relation> SubstraitToDuckDB::TransformSortOp(const substrait::Rel &sop) {
	vector<OrderByNode> order_nodes;
	for (auto &sordf : sop.sort().sorts()) {
		order_nodes.push_back(TransformOrder(sordf));
	}
	return make_shared<OrderRelation>(TransformOp(sop.sort().input()), move(order_nodes));
}
shared_ptr<Relation> SubstraitToDuckDB::TransformOp(const substrait::Rel &sop) {
	switch (sop.rel_type_case()) {
	case substrait::Rel::RelTypeCase::kJoin:
		return TransformJoinOp(sop);
	case substrait::Rel::RelTypeCase::kCross:
		return TransformCrossProductOp(sop);
	case substrait::Rel::RelTypeCase::kFetch:
		return TransformFetchOp(sop);
	case substrait::Rel::RelTypeCase::kFilter:
		return TransformFilterOp(sop);
	case substrait::Rel::RelTypeCase::kProject:
		return TransformProjectOp(sop);
	case substrait::Rel::RelTypeCase::kAggregate:
		return TransformAggregateOp(sop);
	case substrait::Rel::RelTypeCase::kRead:
		return TransformReadOp(sop);
	case substrait::Rel::RelTypeCase::kSort:
		return TransformSortOp(sop);
	default:
		throw InternalException("Unsupported relation type " + to_string(sop.rel_type_case()));
	}
}

shared_ptr<Relation> SubstraitToDuckDB::TransformPlan() {
	return TransformOp(plan.relations(0).rel());
}
} // namespace duckdb