#include "to_substrait.hpp"

#include "duckdb/common/constants.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/planner/expression/list.hpp"
#include "duckdb/planner/table_filter.hpp"
#include "duckdb/planner/filter/conjunction_filter.hpp"
#include "duckdb/planner/filter/constant_filter.hpp"
#include "duckdb/planner/joinside.hpp"
#include "duckdb/planner/operator/list.hpp"
#include "duckdb/function/table/table_scan.hpp"
#include "duckdb/common/enums/expression_type.hpp"

#include "substrait/plan.pb.h"
#include "substrait/algebra.pb.h"

namespace duckdb {

string DuckDBToSubstrait::SerializeToString() {
	string serialized;
	if (!plan.SerializeToString(&serialized)) {
		throw InternalException("It was not possible to serialize the substrait plan");
	}
	return serialized;
}

string DuckDBToSubstrait::GetDecimalInternalString(Value &value) {
	switch (value.type().InternalType()) {
	case PhysicalType::INT8:
		return to_string(value.GetValueUnsafe<int8_t>());
	case PhysicalType::INT16:
		return to_string(value.GetValueUnsafe<int16_t>());
	case PhysicalType::INT32:
		return to_string(value.GetValueUnsafe<int32_t>());
	case PhysicalType::INT64:
		return to_string(value.GetValueUnsafe<int64_t>());
	case PhysicalType::INT128:
		return value.GetValueUnsafe<hugeint_t>().ToString();
	default:
		throw InternalException("Not accepted internal type for decimal");
	}
}

void DuckDBToSubstrait::TransformDecimal(Value &dval, substrait::Expression &sexpr) {
	auto &sval = *sexpr.mutable_literal();
	auto *allocated_decimal = new ::substrait::Expression_Literal_Decimal();
	uint8_t scale, width;
	dval.type().GetDecimalProperties(width, scale);
	allocated_decimal->set_scale(scale);
	allocated_decimal->set_precision(width);
	auto *decimal_value = new string();
	*decimal_value = GetDecimalInternalString(dval);
	allocated_decimal->set_allocated_value(decimal_value);
	sval.set_allocated_decimal(allocated_decimal);
}

void DuckDBToSubstrait::TransformInteger(Value &dval, substrait::Expression &sexpr) {
	auto &sval = *sexpr.mutable_literal();
	sval.set_i32(dval.GetValue<int32_t>());
}

void DuckDBToSubstrait::TransformDouble(Value &dval, substrait::Expression &sexpr) {
	auto &sval = *sexpr.mutable_literal();
	sval.set_fp64(dval.GetValue<double>());
}

void DuckDBToSubstrait::TransformBigInt(Value &dval, substrait::Expression &sexpr) {
	auto &sval = *sexpr.mutable_literal();
	sval.set_i64(dval.GetValue<int64_t>());
}

void DuckDBToSubstrait::TransformDate(Value &dval, substrait::Expression &sexpr) {
	// TODO how are we going to represent dates?
	auto &sval = *sexpr.mutable_literal();
	sval.set_string(dval.ToString());
}

void DuckDBToSubstrait::TransformVarchar(Value &dval, substrait::Expression &sexpr) {
	auto &sval = *sexpr.mutable_literal();
	string duck_str = dval.GetValue<string>();
	sval.set_string(dval.GetValue<string>());
}

void DuckDBToSubstrait::TransformHugeInt(Value &dval, substrait::Expression &sexpr) {
	// Must create a cast from decimal to hugeint
	auto sfun = sexpr.mutable_scalar_function();
	sfun->set_function_reference(RegisterFunction("cast"));
	auto &sval = *sfun->add_args()->mutable_literal();
	auto *allocated_decimal = new ::substrait::Expression_Literal_Decimal();
	auto hugeint_str = dval.ToString();
	allocated_decimal->set_scale(0);
	allocated_decimal->set_precision((int32_t)hugeint_str.size());

	auto *decimal_value = new string();
	*decimal_value = hugeint_str;
	allocated_decimal->set_allocated_value(decimal_value);
	sval.set_allocated_decimal(allocated_decimal);
	sfun->add_args()->mutable_literal()->set_string("HUGEINT");
}

void DuckDBToSubstrait::TransformBoolean(Value &dval, substrait::Expression &sexpr) {
	auto &sval = *sexpr.mutable_literal();
	sval.set_boolean(dval.GetValue<bool>());
}
void DuckDBToSubstrait::TransformConstant(Value &dval, substrait::Expression &sexpr) {
	auto &duckdb_type = dval.type();
	switch (duckdb_type.id()) {
	case LogicalTypeId::DECIMAL:
		TransformDecimal(dval, sexpr);
		break;
	case LogicalTypeId::INTEGER:
		TransformInteger(dval, sexpr);
		break;
	case LogicalTypeId::BIGINT:
		TransformBigInt(dval, sexpr);
		break;
	case LogicalTypeId::DATE:
		TransformDate(dval, sexpr);
		break;
	case LogicalTypeId::VARCHAR:
		TransformVarchar(dval, sexpr);
		break;
	case LogicalTypeId::HUGEINT:
		TransformHugeInt(dval, sexpr);
		break;
	case LogicalTypeId::BOOLEAN:
		TransformBoolean(dval, sexpr);
		break;
	case LogicalTypeId::DOUBLE:
		TransformDouble(dval, sexpr);
		break;
	default:
		throw InternalException(duckdb_type.ToString());
	}
}

void DuckDBToSubstrait::TransformBoundRefExpression(Expression &dexpr, substrait::Expression &sexpr,
                                                    uint64_t col_offset) {
	auto &dref = (BoundReferenceExpression &)dexpr;
	CreateFieldRef(&sexpr, dref.index + col_offset);
}

void DuckDBToSubstrait::TransformCastExpression(Expression &dexpr, substrait::Expression &sexpr, uint64_t col_offset) {
	auto &dcast = (BoundCastExpression &)dexpr;
	auto sfun = sexpr.mutable_scalar_function();
	sfun->set_function_reference(RegisterFunction("cast"));
	TransformExpr(*dcast.child, *sfun->add_args(), col_offset);
	sfun->add_args()->mutable_literal()->set_string(dcast.return_type.ToString());
}

void DuckDBToSubstrait::TransformFunctionExpression(Expression &dexpr, substrait::Expression &sexpr,
                                                    uint64_t col_offset) {
	auto &dfun = (BoundFunctionExpression &)dexpr;
	auto sfun = sexpr.mutable_scalar_function();
	sfun->set_function_reference(RegisterFunction(dfun.function.name));

	for (auto &darg : dfun.children) {
		auto sarg = sfun->add_args();
		TransformExpr(*darg, *sarg, col_offset);
	}
}

void DuckDBToSubstrait::TransformConstantExpression(Expression &dexpr, substrait::Expression &sexpr) {
	auto &dconst = (BoundConstantExpression &)dexpr;
	TransformConstant(dconst.value, sexpr);
}

void DuckDBToSubstrait::TransformComparisonExpression(Expression &dexpr, substrait::Expression &sexpr) {
	auto &dcomp = (BoundComparisonExpression &)dexpr;

	string fname;
	switch (dexpr.type) {
	case ExpressionType::COMPARE_EQUAL:
		fname = "equal";
		break;
	case ExpressionType::COMPARE_LESSTHAN:
		fname = "lessthan";
		break;
	case ExpressionType::COMPARE_LESSTHANOREQUALTO:
		fname = "lessthanequal";
		break;
	case ExpressionType::COMPARE_GREATERTHAN:
		fname = "greaterthan";
		break;
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
		fname = "greaterthanequal";
		break;
	case ExpressionType::COMPARE_NOTEQUAL:
		fname = "notequal";
		break;
	default:
		throw InternalException(ExpressionTypeToString(dexpr.type));
	}

	auto scalar_fun = sexpr.mutable_scalar_function();
	scalar_fun->set_function_reference(RegisterFunction(fname));
	TransformExpr(*dcomp.left, *scalar_fun->add_args(), 0);
	TransformExpr(*dcomp.right, *scalar_fun->add_args(), 0);
}

void DuckDBToSubstrait::TransformConjunctionExpression(Expression &dexpr, substrait::Expression &sexpr,
                                                       uint64_t col_offset) {
	auto &dconj = (BoundConjunctionExpression &)dexpr;
	string fname;
	switch (dexpr.type) {
	case ExpressionType::CONJUNCTION_AND:
		fname = "and";
		break;
	case ExpressionType::CONJUNCTION_OR:
		fname = "or";
		break;
	default:
		throw InternalException(ExpressionTypeToString(dexpr.type));
	}

	auto scalar_fun = sexpr.mutable_scalar_function();
	scalar_fun->set_function_reference(RegisterFunction(fname));
	for (auto &child : dconj.children) {
		TransformExpr(*child, *scalar_fun->add_args(), col_offset);
	}
}

void DuckDBToSubstrait::TransformNotNullExpression(Expression &dexpr, substrait::Expression &sexpr,
                                                   uint64_t col_offset) {
	auto &dop = (BoundOperatorExpression &)dexpr;
	auto scalar_fun = sexpr.mutable_scalar_function();
	scalar_fun->set_function_reference(RegisterFunction("is_not_null"));
	TransformExpr(*dop.children[0], *scalar_fun->add_args(), col_offset);
}

void DuckDBToSubstrait::TransformCaseExpression(Expression &dexpr, substrait::Expression &sexpr) {
	auto &dcase = (BoundCaseExpression &)dexpr;
	auto scase = sexpr.mutable_if_then();

	for (auto &dcheck : dcase.case_checks) {
		auto sif = scase->mutable_ifs()->Add();
		TransformExpr(*dcheck.when_expr, *sif->mutable_if_());
		TransformExpr(*dcheck.then_expr, *sif->mutable_then());
	}
	TransformExpr(*dcase.else_expr, *scase->mutable_else_());
}
void DuckDBToSubstrait::TransformExpr(Expression &dexpr, substrait::Expression &sexpr, uint64_t col_offset) {
	switch (dexpr.type) {
	case ExpressionType::BOUND_REF:
		TransformBoundRefExpression(dexpr, sexpr, col_offset);
		break;
	case ExpressionType::OPERATOR_CAST:
		TransformCastExpression(dexpr, sexpr, col_offset);
		break;
	case ExpressionType::BOUND_FUNCTION:
		TransformFunctionExpression(dexpr, sexpr, col_offset);
		break;
	case ExpressionType::VALUE_CONSTANT:
		TransformConstantExpression(dexpr, sexpr);
		break;
	case ExpressionType::COMPARE_EQUAL:
	case ExpressionType::COMPARE_LESSTHAN:
	case ExpressionType::COMPARE_LESSTHANOREQUALTO:
	case ExpressionType::COMPARE_GREATERTHAN:
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
	case ExpressionType::COMPARE_NOTEQUAL:
		TransformComparisonExpression(dexpr, sexpr);
		break;
	case ExpressionType::CONJUNCTION_AND:
	case ExpressionType::CONJUNCTION_OR:
		TransformConjunctionExpression(dexpr, sexpr, col_offset);
		break;
	case ExpressionType::OPERATOR_IS_NOT_NULL:
		TransformNotNullExpression(dexpr, sexpr, col_offset);
		break;
	case ExpressionType::CASE_EXPR:
		TransformCaseExpression(dexpr, sexpr);
		break;
	default:
		throw InternalException(ExpressionTypeToString(dexpr.type));
	}
}

uint64_t DuckDBToSubstrait::RegisterFunction(const string &name) {
	if (name.empty()) {
		throw InternalException("Missing function name");
	}
	if (functions_map.find(name) == functions_map.end()) {
		auto function_id = last_function_id++;
		auto sfun = plan.add_extensions()->mutable_extension_function();
		sfun->set_function_anchor(function_id);
		sfun->set_name(name);

		functions_map[name] = function_id;
	}
	return functions_map[name];
}

void DuckDBToSubstrait::CreateFieldRef(substrait::Expression *expr, uint64_t col_idx) {
	expr->mutable_selection()->mutable_direct_reference()->mutable_struct_field()->set_field((int32_t)col_idx);
}

substrait::Expression *DuckDBToSubstrait::TransformIsNotNullFilter(uint64_t col_idx, TableFilter &dfilter) {
	auto s_expr = new substrait::Expression();
	auto scalar_fun = s_expr->mutable_scalar_function();
	scalar_fun->set_function_reference(RegisterFunction("is_not_null"));
	CreateFieldRef(scalar_fun->add_args(), col_idx);
	return s_expr;
}

substrait::Expression *DuckDBToSubstrait::TransformConjuctionAndFilter(uint64_t col_idx, TableFilter &dfilter) {
	auto &conjunction_filter = (ConjunctionAndFilter &)dfilter;
	return CreateConjunction(conjunction_filter.child_filters,
	                         [&](unique_ptr<TableFilter> &in) { return TransformFilter(col_idx, *in); });
}

substrait::Expression *DuckDBToSubstrait::TransformConstantComparisonFilter(uint64_t col_idx, TableFilter &dfilter) {
	auto s_expr = new substrait::Expression();
	auto s_scalar = s_expr->mutable_scalar_function();
	auto &constant_filter = (ConstantFilter &)dfilter;
	CreateFieldRef(s_scalar->add_args(), col_idx);
	TransformConstant(constant_filter.constant, *s_scalar->add_args());

	uint64_t function_id;
	switch (constant_filter.comparison_type) {
	case ExpressionType::COMPARE_EQUAL:
		function_id = RegisterFunction("equal");
		break;
	case ExpressionType::COMPARE_LESSTHANOREQUALTO:
		function_id = RegisterFunction("lessthanequal");
		break;
	case ExpressionType::COMPARE_LESSTHAN:
		function_id = RegisterFunction("lessthan");
		break;
	case ExpressionType::COMPARE_GREATERTHAN:
		function_id = RegisterFunction("greaterthan");
		break;
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
		function_id = RegisterFunction("greaterthanequal");
		break;
	default:
		throw InternalException(ExpressionTypeToString(constant_filter.comparison_type));
	}
	s_scalar->set_function_reference(function_id);
	return s_expr;
}

substrait::Expression *DuckDBToSubstrait::TransformFilter(uint64_t col_idx, TableFilter &dfilter) {
	switch (dfilter.filter_type) {
	case TableFilterType::IS_NOT_NULL:
		return TransformIsNotNullFilter(col_idx, dfilter);
	case TableFilterType::CONJUNCTION_AND:
		return TransformConjuctionAndFilter(col_idx, dfilter);
	case TableFilterType::CONSTANT_COMPARISON:
		return TransformConstantComparisonFilter(col_idx, dfilter);
	default:
		throw InternalException("Unsupported table filter type");
	}
}

substrait::Expression *DuckDBToSubstrait::TransformJoinCond(JoinCondition &dcond, uint64_t left_ncol) {
	auto expr = new substrait::Expression();
	string join_comparision;
	switch (dcond.comparison) {
	case ExpressionType::COMPARE_EQUAL:
		join_comparision = "equal";
		break;
	case ExpressionType::COMPARE_GREATERTHAN:
		join_comparision = "greaterthan";
		break;
	case ExpressionType::COMPARE_NOT_DISTINCT_FROM:
		join_comparision = "notdistinctfrom";
		break;
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
		join_comparision = "greaterthanorequalto";
		break;
	case ExpressionType::COMPARE_LESSTHANOREQUALTO:
		join_comparision = "lessthanorequalto";
		break;
	default:
		throw InternalException("Unsupported join comparison");
	}
	auto scalar_fun = expr->mutable_scalar_function();
	scalar_fun->set_function_reference(RegisterFunction(join_comparision));
	TransformExpr(*dcond.left, *scalar_fun->add_args());
	TransformExpr(*dcond.right, *scalar_fun->add_args(), left_ncol);
	return expr;
}

void DuckDBToSubstrait::TransformOrder(BoundOrderByNode &dordf, substrait::SortField &sordf) {
	switch (dordf.type) {
	case OrderType::ASCENDING:
		switch (dordf.null_order) {
		case OrderByNullType::NULLS_FIRST:
			sordf.set_direction(
			    substrait::SortField_SortDirection::SortField_SortDirection_SORT_DIRECTION_ASC_NULLS_FIRST);
			break;
		case OrderByNullType::NULLS_LAST:
			sordf.set_direction(
			    substrait::SortField_SortDirection::SortField_SortDirection_SORT_DIRECTION_ASC_NULLS_LAST);

			break;
		default:
			throw InternalException("Unsupported ordering type");
		}
		break;
	case OrderType::DESCENDING:
		switch (dordf.null_order) {
		case OrderByNullType::NULLS_FIRST:
			sordf.set_direction(
			    substrait::SortField_SortDirection::SortField_SortDirection_SORT_DIRECTION_DESC_NULLS_FIRST);
			break;
		case OrderByNullType::NULLS_LAST:
			sordf.set_direction(
			    substrait::SortField_SortDirection::SortField_SortDirection_SORT_DIRECTION_DESC_NULLS_LAST);

			break;
		default:
			throw InternalException("Unsupported ordering type");
		}
		break;
	default:
		throw InternalException("Unsupported ordering type");
	}
	TransformExpr(*dordf.expression, *sordf.mutable_expr());
}

substrait::Rel *DuckDBToSubstrait::TransformFilter(LogicalOperator &dop) {

	auto &dfilter = (LogicalFilter &)dop;
	auto res = TransformOp(*dop.children[0]);

	if (!dfilter.expressions.empty()) {
		auto filter = new substrait::Rel();
		filter->mutable_filter()->set_allocated_input(res);
		filter->mutable_filter()->set_allocated_condition(
		    CreateConjunction(dfilter.expressions, [&](unique_ptr<Expression> &in) {
			    auto expr = new substrait::Expression();
			    TransformExpr(*in, *expr);
			    return expr;
		    }));
		res = filter;
	}

	if (!dfilter.projection_map.empty()) {
		auto projection = new substrait::Rel();
		projection->mutable_project()->set_allocated_input(res);
		for (auto col_idx : dfilter.projection_map) {
			CreateFieldRef(projection->mutable_project()->add_expressions(), col_idx);
		}
		res = projection;
	}
	return res;
}

substrait::Rel *DuckDBToSubstrait::TransformProjection(LogicalOperator &dop) {
	auto res = new substrait::Rel();
	auto &dproj = (LogicalProjection &)dop;
	auto sproj = res->mutable_project();
	sproj->set_allocated_input(TransformOp(*dop.children[0]));

	for (auto &dexpr : dproj.expressions) {
		TransformExpr(*dexpr, *sproj->add_expressions());
	}
	return res;
}

substrait::Rel *DuckDBToSubstrait::TransformTopN(LogicalOperator &dop) {
	auto &dtopn = (LogicalTopN &)dop;
	auto res = new substrait::Rel();
	auto stopn = res->mutable_fetch();

	auto sord_rel = new substrait::Rel();
	auto sord = sord_rel->mutable_sort();
	sord->set_allocated_input(TransformOp(*dop.children[0]));

	for (auto &dordf : dtopn.orders) {
		TransformOrder(dordf, *sord->add_sorts());
	}

	stopn->set_allocated_input(sord_rel);
	stopn->set_offset(dtopn.offset);
	stopn->set_count(dtopn.limit);
	return res;
}

substrait::Rel *DuckDBToSubstrait::TransformLimit(LogicalOperator &dop) {
	auto &dlimit = (LogicalLimit &)dop;
	auto res = new substrait::Rel();
	auto stopn = res->mutable_fetch();
	stopn->set_allocated_input(TransformOp(*dop.children[0]));

	stopn->set_offset(dlimit.offset_val);
	stopn->set_count(dlimit.limit_val);
	return res;
}

substrait::Rel *DuckDBToSubstrait::TransformOrderBy(LogicalOperator &dop) {
	auto res = new substrait::Rel();
	auto &dord = (LogicalOrder &)dop;
	auto sord = res->mutable_sort();

	sord->set_allocated_input(TransformOp(*dop.children[0]));

	for (auto &dordf : dord.orders) {
		TransformOrder(dordf, *sord->add_sorts());
	}
	return res;
}

substrait::Rel *DuckDBToSubstrait::TransformComparisonJoin(LogicalOperator &dop) {
	auto res = new substrait::Rel();
	auto sjoin = res->mutable_join();
	//		sjoin->set_delim_join(false);
	auto &djoin = (LogicalComparisonJoin &)dop;

	sjoin->set_allocated_left(TransformOp(*dop.children[0]));
	sjoin->set_allocated_right(TransformOp(*dop.children[1]));

	auto left_col_count = dop.children[0]->types.size();
	if (dop.children[0]->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
		auto child_join = (LogicalComparisonJoin *)dop.children[0].get();
		left_col_count = child_join->left_projection_map.size() + child_join->right_projection_map.size();
	}
	sjoin->set_allocated_expression(
	    CreateConjunction(djoin.conditions, [&](JoinCondition &in) { return TransformJoinCond(in, left_col_count); }));

	switch (djoin.join_type) {
	case JoinType::INNER:
		sjoin->set_type(substrait::JoinRel::JoinType::JoinRel_JoinType_JOIN_TYPE_INNER);
		break;
	case JoinType::LEFT:
		sjoin->set_type(substrait::JoinRel::JoinType::JoinRel_JoinType_JOIN_TYPE_LEFT);
		break;
	case JoinType::RIGHT:
		sjoin->set_type(substrait::JoinRel::JoinType::JoinRel_JoinType_JOIN_TYPE_RIGHT);
		break;
	case JoinType::SINGLE:
		sjoin->set_type(substrait::JoinRel::JoinType::JoinRel_JoinType_JOIN_TYPE_SINGLE);
		break;
	case JoinType::SEMI:
		sjoin->set_type(substrait::JoinRel::JoinType::JoinRel_JoinType_JOIN_TYPE_SEMI);
		break;
	default:
		throw InternalException("Unsupported join type " + JoinTypeToString(djoin.join_type));
	}

	// somewhat odd semantics on our side
	if (djoin.left_projection_map.empty()) {
		for (uint64_t i = 0; i < dop.children[0]->types.size(); i++) {
			djoin.left_projection_map.push_back(i);
		}
	}
	if (djoin.right_projection_map.empty()) {
		for (uint64_t i = 0; i < dop.children[1]->types.size(); i++) {
			djoin.right_projection_map.push_back(i);
		}
	}
	auto proj_rel = new substrait::Rel();
	auto projection = proj_rel->mutable_project();
	for (auto left_idx : djoin.left_projection_map) {
		CreateFieldRef(projection->add_expressions(), left_idx);
	}

	for (auto right_idx : djoin.right_projection_map) {
		CreateFieldRef(projection->add_expressions(), right_idx + left_col_count);
	}
	projection->set_allocated_input(res);
	return proj_rel;
}

substrait::Rel *DuckDBToSubstrait::TransformAggregateGroup(LogicalOperator &dop) {
	auto res = new substrait::Rel();
	auto &daggr = (LogicalAggregate &)dop;
	auto saggr = res->mutable_aggregate();
	saggr->set_allocated_input(TransformOp(*dop.children[0]));
	// we only do a single grouping set for now
	auto sgrp = saggr->add_groupings();
	for (auto &dgrp : daggr.groups) {
		if (dgrp->type != ExpressionType::BOUND_REF) {
			// TODO push projection or push substrait to allow expressions here
			throw InternalException("No expressions in groupings yet");
		}
		TransformExpr(*dgrp, *sgrp->add_grouping_expressions());
	}
	for (auto &dmeas : daggr.expressions) {
		auto smeas = saggr->add_measures()->mutable_measure();
		if (dmeas->type != ExpressionType::BOUND_AGGREGATE) {
			// TODO push projection or push substrait, too
			throw InternalException("No non-aggregate expressions in measures yet");
		}
		auto &daexpr = (BoundAggregateExpression &)*dmeas;
		smeas->set_function_reference(RegisterFunction(daexpr.function.name));

		for (auto &darg : daexpr.children) {
			TransformExpr(*darg, *smeas->add_args());
		}
	}
	return res;
}

substrait::Rel *DuckDBToSubstrait::TransformGet(LogicalOperator &dop) {
	auto get_rel = new substrait::Rel();
	substrait::Rel *rel = get_rel;
	auto &dget = (LogicalGet &)dop;
	auto &table_scan_bind_data = (TableScanBindData &)*dget.bind_data;
	auto sget = get_rel->mutable_read();

	// Turn Filter pushdown into Filter
	if (!dget.table_filters.filters.empty()) {
		auto filter = new substrait::Rel();
		filter->mutable_filter()->set_allocated_input(get_rel);

		filter->mutable_filter()->set_allocated_condition(
		    CreateConjunction(dget.table_filters.filters, [&](std::pair<const idx_t, unique_ptr<TableFilter>> &in) {
			    auto col_idx = in.first;
			    auto &filter = *in.second;
			    return TransformFilter(col_idx, filter);
		    }));
		rel = filter;
	}

	// Turn Projection Pushdown into Projection
	if (!dget.column_ids.empty()) {
		auto projection_rel = new substrait::Rel();
		projection_rel->mutable_project()->set_allocated_input(rel);
		for (auto col_idx : dget.column_ids) {
			CreateFieldRef(projection_rel->mutable_project()->add_expressions(), col_idx);
		}
		rel = projection_rel;
	}

	// TODO add schema
	sget->mutable_named_table()->add_names(table_scan_bind_data.table->name);

	return rel;
}

substrait::Rel *DuckDBToSubstrait::TransformCrossProduct(LogicalOperator &dop) {
	auto rel = new substrait::Rel();
	auto sub_cross_prod = rel->mutable_cross();
	auto &djoin = (LogicalCrossProduct &)dop;
	sub_cross_prod->set_allocated_left(TransformOp(*dop.children[0]));
	sub_cross_prod->set_allocated_right(TransformOp(*dop.children[1]));
	auto bindings = djoin.GetColumnBindings();
	return rel;
}

substrait::Rel *DuckDBToSubstrait::TransformOp(LogicalOperator &dop) {
	switch (dop.type) {
	case LogicalOperatorType::LOGICAL_FILTER:
		return TransformFilter(dop);
	case LogicalOperatorType::LOGICAL_TOP_N:
		return TransformTopN(dop);
	case LogicalOperatorType::LOGICAL_LIMIT:
		return TransformLimit(dop);
	case LogicalOperatorType::LOGICAL_ORDER_BY:
		return TransformOrderBy(dop);
	case LogicalOperatorType::LOGICAL_PROJECTION:
		return TransformProjection(dop);
	case LogicalOperatorType::LOGICAL_COMPARISON_JOIN:
		return TransformComparisonJoin(dop);
	case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY:
		return TransformAggregateGroup(dop);
	case LogicalOperatorType::LOGICAL_GET:
		return TransformGet(dop);
	case LogicalOperatorType::LOGICAL_CROSS_PRODUCT:
		return TransformCrossProduct(dop);

	default:
		throw InternalException(LogicalOperatorToString(dop.type));
	}
}

substrait::RelRoot *DuckDBToSubstrait::TransformRootOp(LogicalOperator &dop) {
	auto root_rel = new substrait::RelRoot();
	LogicalOperator *current_op = &dop;
	bool weird_scenario = current_op->type == LogicalOperatorType::LOGICAL_PROJECTION &&
	                      current_op->children[0]->type == LogicalOperatorType::LOGICAL_TOP_N;
	if (weird_scenario) {
		// This is a weird scenario where a projection is put on top of a top-k but the actual aliases are on the
		// projection below the top-k still.
		current_op = current_op->children[0].get();
	}
	// If the root operator is not a projection, we must go down until we find the first projection to get the aliases
	while (current_op->type != LogicalOperatorType::LOGICAL_PROJECTION) {
		if (current_op->children.size() != 1) {
			throw InternalException("Root node has more than 1, or 0 children up to reaching a projection node");
		}
		current_op = current_op->children[0].get();
	}
	root_rel->set_allocated_input(TransformOp(dop));
	auto &dproj = (LogicalProjection &)*current_op;
	if (!weird_scenario) {
		for (auto &expression : dproj.expressions) {
			root_rel->add_names(expression->GetName());
		}
	} else {
		for (auto &expression : dop.expressions) {
			D_ASSERT(expression->type == ExpressionType::BOUND_REF);
			auto b_expr = (BoundReferenceExpression *)expression.get();
			root_rel->add_names(dproj.expressions[b_expr->index]->GetName());
		}
	}

	return root_rel;
}

void DuckDBToSubstrait::TransformPlan(LogicalOperator &dop) {
	plan.add_relations()->set_allocated_root(TransformRootOp(dop));
}
} // namespace duckdb