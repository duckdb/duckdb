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
#include "duckdb/common/types.hpp"

#include "substrait/plan.pb.h"

namespace duckdb {
SubstraitToDuckDB::SubstraitToDuckDB(Connection &con_p, string &serialized) : con(con_p) {
	if (!plan.ParseFromString(serialized)) {
		throw std::runtime_error("Was not possible to convert binary into Substrait plan");
	}
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
	case substrait::Expression_Literal::LiteralTypeCase::kI8:
		dval = Value::TINYINT(slit.i8());
		break;
	case substrait::Expression_Literal::LiteralTypeCase::kI32:
		dval = Value::INTEGER(slit.i32());
		break;
	case substrait::Expression_Literal::LiteralTypeCase::kI64:
		dval = Value::BIGINT(slit.i64());
		break;
	case substrait::Expression_Literal::LiteralTypeCase::kDate: {
		date_t date(slit.date());
		dval = Value::DATE(date);
		break;
	}
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
	auto function_name = FindFunction(sexpr.scalar_function().function_reference());
	vector<unique_ptr<ParsedExpression>> children;
	for (auto &sarg : sexpr.scalar_function().args()) {
		children.push_back(TransformExpr(sarg));
	}
	// string compare galore
	// TODO simplify this
	if (function_name == "and") {
		return make_unique<ConjunctionExpression>(ExpressionType::CONJUNCTION_AND, move(children));
	} else if (function_name == "or") {
		return make_unique<ConjunctionExpression>(ExpressionType::CONJUNCTION_OR, move(children));
	} else if (function_name == "lt") {
		return make_unique<ComparisonExpression>(ExpressionType::COMPARE_LESSTHAN, move(children[0]),
		                                         move(children[1]));
	} else if (function_name == "equal") {
		return make_unique<ComparisonExpression>(ExpressionType::COMPARE_EQUAL, move(children[0]), move(children[1]));
	} else if (function_name == "not_equal") {
		return make_unique<ComparisonExpression>(ExpressionType::COMPARE_NOTEQUAL, move(children[0]),
		                                         move(children[1]));
	} else if (function_name == "lte") {
		return make_unique<ComparisonExpression>(ExpressionType::COMPARE_LESSTHANOREQUALTO, move(children[0]),
		                                         move(children[1]));
	} else if (function_name == "gte") {
		return make_unique<ComparisonExpression>(ExpressionType::COMPARE_GREATERTHANOREQUALTO, move(children[0]),
		                                         move(children[1]));
	} else if (function_name == "gt") {
		return make_unique<ComparisonExpression>(ExpressionType::COMPARE_GREATERTHAN, move(children[0]),
		                                         move(children[1]));
	} else if (function_name == "is_not_null") {
		return make_unique<OperatorExpression>(ExpressionType::OPERATOR_IS_NOT_NULL, move(children[0]));
	} else if (function_name == "is_not_distinct_from") {
		return make_unique<ComparisonExpression>(ExpressionType::COMPARE_NOT_DISTINCT_FROM, move(children[0]),
		                                         move(children[1]));
	} else if (function_name == "between") {
		// FIXME: ADD between to substrait extension
		return make_unique<BetweenExpression>(move(children[0]), move(children[1]), move(children[2]));
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
	return move(dcase);
}
LogicalType SubstraitToDuckDB::SubstraitToDuckType(const ::substrait::Type &s_type) {

	if (s_type.has_bool_()) {
		return LogicalType(LogicalTypeId::BOOLEAN);
	} else if (s_type.has_i16()) {
		return LogicalType(LogicalTypeId::SMALLINT);
	} else if (s_type.has_i32()) {
		return LogicalType(LogicalTypeId::INTEGER);
	} else if (s_type.has_decimal()) {
		auto &s_decimal_type = s_type.decimal();
		return LogicalType::DECIMAL(s_decimal_type.precision(), s_decimal_type.scale());
	} else if (s_type.has_i64()) {
		return LogicalType(LogicalTypeId::BIGINT);
	} else if (s_type.has_date()) {
		return LogicalType(LogicalTypeId::DATE);
	} else if (s_type.has_varchar() || s_type.has_string()) {
		return LogicalType(LogicalTypeId::VARCHAR);
	} else if (s_type.has_fp64()) {
		return LogicalType(LogicalTypeId::DOUBLE);
	} else {
		throw InternalException("Substrait type not yet supported");
	}
}

unique_ptr<ParsedExpression> SubstraitToDuckDB::TransformCastExpr(const substrait::Expression &sexpr) {
	const auto &scast = sexpr.cast();
	auto cast_type = SubstraitToDuckType(scast.type());
	auto cast_child = TransformExpr(scast.input());
	return make_unique<CastExpression>(cast_type, move(cast_child));
}

unique_ptr<ParsedExpression> SubstraitToDuckDB::TransformInExpr(const substrait::Expression &sexpr) {
	const auto &substrait_in = sexpr.singular_or_list();

	vector<unique_ptr<ParsedExpression>> values;
	values.emplace_back(TransformExpr(substrait_in.value()));

	for (idx_t i = 0; i < (idx_t)substrait_in.options_size(); i++) {
		values.emplace_back(TransformExpr(substrait_in.options(i)));
	}

	return make_unique<OperatorExpression>(ExpressionType::COMPARE_IN, move(values));
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
	case substrait::Expression::RexTypeCase::kCast:
		return TransformCastExpr(sexpr);
	case substrait::Expression::RexTypeCase::kSingularOrList:
		return TransformInExpr(sexpr);
	case substrait::Expression::RexTypeCase::kSubquery:
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
	case substrait::JoinRel::JoinType::JoinRel_JoinType_JOIN_TYPE_SINGLE:
		djointype = JoinType::SINGLE;
		break;
	case substrait::JoinRel::JoinType::JoinRel_JoinType_JOIN_TYPE_SEMI:
		djointype = JoinType::SEMI;
		break;
	default:
		throw InternalException("Unsupported join type");
	}
	vector<unique_ptr<ParsedExpression>> expressions;
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
	for (auto &sexpr : sop.project().expressions()) {
		expressions.push_back(TransformExpr(sexpr));
	}

	vector<string> mock_aliases;
	for (size_t i = 0; i < expressions.size(); i++) {
		mock_aliases.push_back("expr_" + to_string(i));
	}
	return make_shared<ProjectionRelation>(TransformOp(sop.project().input()), move(expressions), move(mock_aliases));
}

shared_ptr<Relation> SubstraitToDuckDB::TransformAggregateOp(const substrait::Rel &sop) {
	vector<unique_ptr<ParsedExpression>> groups, expressions;

	if (sop.aggregate().groupings_size() > 0) {
		for (auto &sgrp : sop.aggregate().groupings()) {
			for (auto &sgrpexpr : sgrp.grouping_expressions()) {
				groups.push_back(TransformExpr(sgrpexpr));
				expressions.push_back(TransformExpr(sgrpexpr));
			}
		}
	}

	for (auto &smeas : sop.aggregate().measures()) {
		vector<unique_ptr<ParsedExpression>> children;
		for (auto &sarg : smeas.measure().args()) {
			children.push_back(TransformExpr(sarg));
		}
		auto function_name = FindFunction(smeas.measure().function_reference());
		if (function_name == "count" && children.empty()) {
			function_name = "count_star";
		}
		expressions.push_back(make_unique<FunctionExpression>(function_name, move(children)));
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

shared_ptr<Relation> SubstraitToDuckDB::TransformRootOp(const substrait::RelRoot &sop) {
	vector<string> aliases;
	auto column_names = sop.names();
	vector<unique_ptr<ParsedExpression>> expressions;
	int id = 1;
	for (auto &column_name : column_names) {
		aliases.push_back(column_name);
		expressions.push_back(make_unique<PositionalReferenceExpression>(id++));
	}
	return make_shared<ProjectionRelation>(TransformOp(sop.input()), move(expressions), aliases);
}

shared_ptr<Relation> SubstraitToDuckDB::TransformPlan() {
	D_ASSERT(!plan.relations().empty());
	auto d_plan = TransformRootOp(plan.relations(0).root());
	return d_plan;
}

} // namespace duckdb
