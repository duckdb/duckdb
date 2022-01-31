#include "to_substrait.hpp"

#include "duckdb/common/constants.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/planner/expression.hpp"
#include "duckdb/planner/expression/list.hpp"
#include "duckdb/planner/table_filter.hpp"
#include "duckdb/planner/filter/null_filter.hpp"
#include "duckdb/planner/filter/conjunction_filter.hpp"
#include "duckdb/planner/filter/constant_filter.hpp"
#include "duckdb/planner/joinside.hpp"
#include "duckdb/planner/operator/list.hpp"

#include "duckdb/function/table/table_scan.hpp"

#include "substrait/plan.pb.h"
#include "substrait/expression.pb.h"
namespace duckdb {
using namespace std;

string GetDecimalInternalString(duckdb::Value &value) {
	switch (value.type().InternalType()) {
	case duckdb::PhysicalType::INT8:
		return to_string(value.GetValueUnsafe<int8_t>());
	case duckdb::PhysicalType::INT16:
		return to_string(value.GetValueUnsafe<int16_t>());
	case duckdb::PhysicalType::INT32:
		return to_string(value.GetValueUnsafe<int32_t>());
	case duckdb::PhysicalType::INT64:
		return to_string(value.GetValueUnsafe<int64_t>());
	case duckdb::PhysicalType::INT128:
		return value.GetValueUnsafe<duckdb::hugeint_t>().ToString();
	default:
		throw runtime_error("Not accepted internal type for decimal");
	}
}
void DuckDBToSubstrait::TransformConstant(duckdb::Value &dval, substrait::Expression &sexpr) {
	auto &duckdb_type = dval.type();
	switch (duckdb_type.id()) {
	case duckdb::LogicalTypeId::DECIMAL: {
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
		break;
	}
	case duckdb::LogicalTypeId::INTEGER: {
		auto &sval = *sexpr.mutable_literal();
		sval.set_i32(dval.GetValue<int32_t>());
		break;
	}
	case duckdb::LogicalTypeId::BIGINT: {
		auto &sval = *sexpr.mutable_literal();
		sval.set_i64(dval.GetValue<int64_t>());
		break;
	}
	case duckdb::LogicalTypeId::DATE: {
		// TODO how are we going to represent dates?
		auto &sval = *sexpr.mutable_literal();
		sval.set_string(dval.ToString());
		break;
	}
	case duckdb::LogicalTypeId::VARCHAR: {
		auto &sval = *sexpr.mutable_literal();
		sval.set_string(dval.GetValue<string>());
		break;
	}

	case duckdb::LogicalTypeId::HUGEINT: {
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
		break;
	}
	case duckdb::LogicalTypeId::BOOLEAN: {
		auto &sval = *sexpr.mutable_literal();
		sval.set_boolean(dval.GetValue<bool>());
		break;
	}
	default:
		throw runtime_error(duckdb_type.ToString());
	}
}

void DuckDBToSubstrait::TransformExpr(duckdb::Expression &dexpr, substrait::Expression &sexpr, uint64_t col_offset) {
	switch (dexpr.type) {
	case duckdb::ExpressionType::BOUND_REF: {
		auto &dref = (duckdb::BoundReferenceExpression &)dexpr;
		CreateFieldRef(&sexpr, dref.index + col_offset);
		return;
	}
	case duckdb::ExpressionType::OPERATOR_CAST: {
		auto &dcast = (duckdb::BoundCastExpression &)dexpr;
		auto sfun = sexpr.mutable_scalar_function();
		sfun->set_function_reference(RegisterFunction("cast"));
		TransformExpr(*dcast.child, *sfun->add_args(), col_offset);
		sfun->add_args()->mutable_literal()->set_string(dcast.return_type.ToString());
		return;
	}
	case duckdb::ExpressionType::BOUND_FUNCTION: {
		auto &dfun = (duckdb::BoundFunctionExpression &)dexpr;
		auto sfun = sexpr.mutable_scalar_function();
		sfun->set_function_reference(RegisterFunction(dfun.function.name));

		for (auto &darg : dfun.children) {
			auto sarg = sfun->add_args();
			TransformExpr(*darg, *sarg, col_offset);
		}

		return;
	}
	case duckdb::ExpressionType::VALUE_CONSTANT: {
		auto &dconst = (duckdb::BoundConstantExpression &)dexpr;
		TransformConstant(dconst.value, sexpr);
		return;
	}
	case duckdb::ExpressionType::COMPARE_EQUAL:
	case duckdb::ExpressionType::COMPARE_LESSTHAN:
	case duckdb::ExpressionType::COMPARE_LESSTHANOREQUALTO:
	case duckdb::ExpressionType::COMPARE_GREATERTHAN:
	case duckdb::ExpressionType::COMPARE_GREATERTHANOREQUALTO:
	case duckdb::ExpressionType::COMPARE_NOTEQUAL:

	{
		auto &dcomp = (duckdb::BoundComparisonExpression &)dexpr;

		string fname;
		switch (dexpr.type) {
		case duckdb::ExpressionType::COMPARE_EQUAL:
			fname = "equal";
			break;
		case duckdb::ExpressionType::COMPARE_LESSTHAN:
			fname = "lessthan";
			break;
		case duckdb::ExpressionType::COMPARE_LESSTHANOREQUALTO:
			fname = "lessthanequal";
			break;
		case duckdb::ExpressionType::COMPARE_GREATERTHAN:
			fname = "greaterthan";
			break;
		case duckdb::ExpressionType::COMPARE_GREATERTHANOREQUALTO:
			fname = "greaterthanequal";
			break;
		case duckdb::ExpressionType::COMPARE_NOTEQUAL:
			fname = "notequal";
			break;
		default:
			throw runtime_error(duckdb::ExpressionTypeToString(dexpr.type));
		}

		auto scalar_fun = sexpr.mutable_scalar_function();
		scalar_fun->set_function_reference(RegisterFunction(fname));
		TransformExpr(*dcomp.left, *scalar_fun->add_args(), 0);
		TransformExpr(*dcomp.right, *scalar_fun->add_args(), 0);

		return;
	}
	case duckdb::ExpressionType::CONJUNCTION_AND:
	case duckdb::ExpressionType::CONJUNCTION_OR: {
		auto &dconj = (duckdb::BoundConjunctionExpression &)dexpr;
		string fname;
		switch (dexpr.type) {
		case duckdb::ExpressionType::CONJUNCTION_AND:
			fname = "and";
			break;
		case duckdb::ExpressionType::CONJUNCTION_OR:
			fname = "or";
			break;
		default:
			throw runtime_error(duckdb::ExpressionTypeToString(dexpr.type));
		}

		auto scalar_fun = sexpr.mutable_scalar_function();
		scalar_fun->set_function_reference(RegisterFunction(fname));
		for (auto &child : dconj.children) {
			TransformExpr(*child, *scalar_fun->add_args(), col_offset);
		}
		return;
	}
	case duckdb::ExpressionType::OPERATOR_IS_NOT_NULL: {
		auto &dop = (duckdb::BoundOperatorExpression &)dexpr;

		auto scalar_fun = sexpr.mutable_scalar_function();
		scalar_fun->set_function_reference(RegisterFunction("is_not_null"));
		TransformExpr(*dop.children[0], *scalar_fun->add_args(), col_offset);

		return;
	}
	case duckdb::ExpressionType::CASE_EXPR: {
		auto &dcase = (duckdb::BoundCaseExpression &)dexpr;
		auto scase = sexpr.mutable_if_then();

		for (auto &dcheck : dcase.case_checks) {
			auto sif = scase->mutable_ifs(2);
			TransformExpr(*dcheck.when_expr, *sif->mutable_if_());
			TransformExpr(*dcheck.then_expr, *sif->mutable_then());
		}
		TransformExpr(*dcase.else_expr, *scase->mutable_else_());
		return;
	}

	default:
		throw runtime_error(duckdb::ExpressionTypeToString(dexpr.type));
	}
}

uint64_t DuckDBToSubstrait::RegisterFunction(string name) {
	if (name.empty()) {
		throw runtime_error("empty function name bad");
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

void DuckDBToSubstrait::TransformFilter(uint64_t col_idx, duckdb::TableFilter &dfilter, substrait::Expression &sfilter,
                                        bool recursive) {
	switch (dfilter.filter_type) {
	case duckdb::TableFilterType::IS_NOT_NULL: {
		auto scalar_fun = sfilter.mutable_scalar_function();
		scalar_fun->set_function_reference(RegisterFunction("is_not_null"));
		CreateFieldRef(scalar_fun->add_args(), col_idx);
		return;
	}

	case duckdb::TableFilterType::CONJUNCTION_AND: {
		auto &conjunction_filter = (duckdb::ConjunctionAndFilter &)dfilter;

		auto sfilter_conj = CreateConjunction(
		    conjunction_filter.child_filters,
		    [&](unique_ptr<duckdb::TableFilter> &in, substrait::Expression *out, bool recursive_p) {
			    TransformFilter(col_idx, *in, *out, recursive);
		    },
		    recursive);
		sfilter = *sfilter_conj;

		return;
	}
	case duckdb::TableFilterType::CONSTANT_COMPARISON: {
		auto &constant_filter = (duckdb::ConstantFilter &)dfilter;
		CreateFieldRef(sfilter.mutable_scalar_function()->add_args(), col_idx);

		TransformConstant(constant_filter.constant, *sfilter.mutable_scalar_function()->add_args());

		uint64_t function_id;
		switch (constant_filter.comparison_type) {
		case duckdb::ExpressionType::COMPARE_EQUAL:
			function_id = RegisterFunction("equal");
			break;
		case duckdb::ExpressionType::COMPARE_LESSTHANOREQUALTO:
			function_id = RegisterFunction("lessthanequal");
			break;
		case duckdb::ExpressionType::COMPARE_LESSTHAN:
			function_id = RegisterFunction("lessthan");
			break;
		case duckdb::ExpressionType::COMPARE_GREATERTHAN:
			function_id = RegisterFunction("greaterthan");
			break;
		case duckdb::ExpressionType::COMPARE_GREATERTHANOREQUALTO:
			function_id = RegisterFunction("greaterthanequal");
			break;
		default:
			throw runtime_error(duckdb::ExpressionTypeToString(constant_filter.comparison_type));
		}

		sfilter.mutable_scalar_function()->set_function_reference(function_id);
		return;
	}
	default:
		throw runtime_error("Unsupported table filter type");
	}
}

void DuckDBToSubstrait::TransformJoinCond(duckdb::JoinCondition &dcond, substrait::Expression &scond,
                                          uint64_t left_ncol, bool recursive) {
	string join_comparision;
	switch (dcond.comparison) {
	case duckdb::ExpressionType::COMPARE_EQUAL:
		join_comparision = "equal";
		break;
	case duckdb::ExpressionType::COMPARE_GREATERTHAN:
		join_comparision = "greaterthan";
		break;
	default:
		throw runtime_error("Unsupported join comparision");
	}
	auto scalar_fun = scond.mutable_scalar_function();
	scalar_fun->set_function_reference(RegisterFunction(join_comparision));
	TransformExpr(*dcond.left, *scalar_fun->add_args());
	TransformExpr(*dcond.right, *scalar_fun->add_args(), left_ncol);
}

void DuckDBToSubstrait::TransformOrder(duckdb::BoundOrderByNode &dordf, substrait::SortField &sordf) {
	switch (dordf.type) {
	case duckdb::OrderType::ASCENDING:
		switch (dordf.null_order) {
		case duckdb::OrderByNullType::NULLS_FIRST:
			sordf.set_direction(
			    substrait::SortField_SortDirection::SortField_SortDirection_SORT_DIRECTION_ASC_NULLS_FIRST);
			break;
		case duckdb::OrderByNullType::NULLS_LAST:
			sordf.set_direction(
			    substrait::SortField_SortDirection::SortField_SortDirection_SORT_DIRECTION_ASC_NULLS_LAST);

			break;
		default:
			throw runtime_error("Unsupported ordering type");
		}
		break;
	case duckdb::OrderType::DESCENDING:
		switch (dordf.null_order) {
		case duckdb::OrderByNullType::NULLS_FIRST:
			sordf.set_direction(
			    substrait::SortField_SortDirection::SortField_SortDirection_SORT_DIRECTION_DESC_NULLS_FIRST);
			break;
		case duckdb::OrderByNullType::NULLS_LAST:
			sordf.set_direction(
			    substrait::SortField_SortDirection::SortField_SortDirection_SORT_DIRECTION_DESC_NULLS_LAST);

			break;
		default:
			throw runtime_error("Unsupported ordering type");
		}
		break;
	default:
		throw runtime_error("Unsupported ordering type");
	}
	TransformExpr(*dordf.expression, *sordf.mutable_expr());
}

void DuckDBToSubstrait::ComparisonJoinTransform(duckdb::LogicalOperator &dop, substrait::Rel &sop,
                                                ::substrait::JoinRel *sjoin, substrait::Rel *sjoin_rel) {
	auto &djoin = (duckdb::LogicalComparisonJoin &)dop;

	TransformOp(*dop.children[0], *sjoin->mutable_left());
	TransformOp(*dop.children[1], *sjoin->mutable_right());

	auto left_col_count = dop.children[0]->types.size();

	sjoin->set_allocated_expression(CreateConjunction(
	    djoin.conditions,
	    [&](duckdb::JoinCondition &in, substrait::Expression *out, bool recursive) {
		    TransformJoinCond(in, *out, left_col_count, recursive);
	    },
	    false));

	switch (djoin.join_type) {
	case duckdb::JoinType::INNER:
		sjoin->set_type(substrait::JoinRel::JoinType::JoinRel_JoinType_JOIN_TYPE_INNER);
		break;
	case duckdb::JoinType::LEFT:
		sjoin->set_type(substrait::JoinRel::JoinType::JoinRel_JoinType_JOIN_TYPE_LEFT);
		break;
	case duckdb::JoinType::RIGHT:
		sjoin->set_type(substrait::JoinRel::JoinType::JoinRel_JoinType_JOIN_TYPE_RIGHT);
		break;
		//	case duckdb::JoinType::SINGLE:
		//		sjoin->set_type(substrait::JoinRel::JoinType::JoinRel_JoinType_JOIN_TYPE_SINGLE);
		//		break;
	case duckdb::JoinType::SEMI:
		sjoin->set_type(substrait::JoinRel::JoinType::JoinRel_JoinType_JOIN_TYPE_SEMI);
		break;
		//	case duckdb::JoinType::MARK:
		//		sjoin->set_type(substrait::JoinRel::JoinType::JoinRel_JoinType_JOIN_TYPE_MARK);
		//		sjoin->set_mark_index(djoin.mark_index);
		//		break;
	default:
		throw runtime_error("Unsupported join type");
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

	for (auto left_idx : djoin.left_projection_map) {
		CreateFieldRef(sop.mutable_project()->add_expressions(), left_idx);
	}

	for (auto right_idx : djoin.right_projection_map) {
		CreateFieldRef(sop.mutable_project()->add_expressions(), right_idx + left_col_count);
	}
	sop.mutable_project()->set_allocated_input(sjoin_rel);
}

void DuckDBToSubstrait::TransformOp(duckdb::LogicalOperator &dop, substrait::Rel &sop) {
	switch (dop.type) {

	case duckdb::LogicalOperatorType::LOGICAL_FILTER: {
		auto &dfilter = (duckdb::LogicalFilter &)dop;
		auto res = new substrait::Rel();

		TransformOp(*dop.children[0], *res);

		if (!dfilter.expressions.empty()) {
			auto filter = new substrait::Rel();
			filter->mutable_filter()->set_allocated_input(res);
			filter->mutable_filter()->set_allocated_condition(CreateConjunction(
			    dfilter.expressions,
			    [&](unique_ptr<duckdb::Expression> &in, substrait::Expression *out, bool recursive) {
				    TransformExpr(*in, *out);
			    },
			    false));
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
		sop = *res;

		return;
	}
	case duckdb::LogicalOperatorType::LOGICAL_TOP_N: {
		auto &dtopn = (duckdb::LogicalTopN &)dop;
		auto stopn = sop.mutable_fetch();

		auto sord_rel = new substrait::Rel();
		auto sord = sord_rel->mutable_sort();
		TransformOp(*dop.children[0], *sord->mutable_input());

		for (auto &dordf : dtopn.orders) {
			TransformOrder(dordf, *sord->add_sorts());
		}

		stopn->set_allocated_input(sord_rel);
		stopn->set_offset(dtopn.offset);
		stopn->set_count(dtopn.limit);
		return;
	}

	case duckdb::LogicalOperatorType::LOGICAL_LIMIT: {
		auto &dtopn = (duckdb::LogicalLimit &)dop;
		auto stopn = sop.mutable_fetch();

		TransformOp(*dop.children[0], *stopn->mutable_input());

		stopn->set_offset(dtopn.offset_val);
		stopn->set_count(dtopn.limit_val);
		return;
	}

	case duckdb::LogicalOperatorType::LOGICAL_ORDER_BY: {
		auto &dord = (duckdb::LogicalOrder &)dop;
		auto sord = sop.mutable_sort();

		TransformOp(*dop.children[0], *sord->mutable_input());

		for (auto &dordf : dord.orders) {
			TransformOrder(dordf, *sord->add_sorts());
		}
		return;
	}

	case duckdb::LogicalOperatorType::LOGICAL_PROJECTION: {
		auto &dproj = (duckdb::LogicalProjection &)dop;
		auto sproj = sop.mutable_project();

		TransformOp(*dop.children[0], *sproj->mutable_input());

		for (auto &dexpr : dproj.expressions) {
			TransformExpr(*dexpr, *sproj->add_expressions());
			//			sproj->add_aliases(dexpr->GetName());
		}
		return;
	}

	case duckdb::LogicalOperatorType::LOGICAL_COMPARISON_JOIN: {
		auto sjoin_rel = new substrait::Rel();
		auto sjoin = sjoin_rel->mutable_join();
		//		sjoin->set_delim_join(false);
		ComparisonJoinTransform(dop, sop, sjoin, sjoin_rel);
		return;
	}
		//	case duckdb::LogicalOperatorType::LOGICAL_DELIM_JOIN: {
		//		auto &djoin = (duckdb::LogicalDelimJoin &)dop;
		//		auto sjoin_rel = new substrait::Rel();
		//		auto sjoin = sjoin_rel->mutable_join();
		//		sjoin->set_delim_join(true);
		//		for (auto &dexpr : djoin.duplicate_eliminated_columns) {
		//			TransformExpr(*dexpr, *sjoin->add_duplicate_eliminated_columns());
		//		}
		//		ComparisonJoinTransform(dop, sop, sjoin, sjoin_rel);
		//		return;
		//	}
	case duckdb::LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY: {
		auto &daggr = (duckdb::LogicalAggregate &)dop;
		auto saggr = sop.mutable_aggregate();
		TransformOp(*dop.children[0], *saggr->mutable_input());

		// we only do a single grouping set for now
		auto sgrp = saggr->add_groupings();
		for (auto &dgrp : daggr.groups) {
			if (dgrp->type != duckdb::ExpressionType::BOUND_REF) {
				// TODO push projection or push substrait to allow expressions here
				throw runtime_error("No expressions in groupings yet");
			}
			TransformExpr(*dgrp, *sgrp->add_grouping_expressions());
		}
		for (auto &dmeas : daggr.expressions) {
			auto smeas = saggr->add_measures()->mutable_measure();
			if (dmeas->type != duckdb::ExpressionType::BOUND_AGGREGATE) {
				// TODO push projection or push substrait, too
				throw runtime_error("No non-aggregate expressions in measures yet");
			}
			auto &daexpr = (duckdb::BoundAggregateExpression &)*dmeas;
			smeas->set_function_reference(RegisterFunction(daexpr.function.name));

			for (auto &darg : daexpr.children) {
				TransformExpr(*darg, *smeas->add_args());
			}
		}
		return;
	}

	case duckdb::LogicalOperatorType::LOGICAL_GET: {
		auto &dget = (duckdb::LogicalGet &)dop;
		auto &table_scan_bind_data = (duckdb::TableScanBindData &)*dget.bind_data;
		auto sget = sop.mutable_read();

		if (!dget.table_filters.filters.empty()) {
			sget->unsafe_arena_set_allocated_filter(CreateConjunction(
			    dget.table_filters.filters,
			    [&](pair<const duckdb::idx_t, unique_ptr<duckdb::TableFilter>> &in, substrait::Expression *out,
			        bool recursive) {
				    auto col_idx = in.first;
				    auto &filter = *in.second;
				    TransformFilter(col_idx, filter, *out, recursive);
			    },
			    false));
		}

		for (auto column_index : dget.column_ids) {
			sget->mutable_projection()->mutable_select()->add_struct_items()->set_field((int32_t)column_index);
		}

		// TODO add schema
		sget->mutable_named_table()->add_names(table_scan_bind_data.table->name);
		sget->mutable_common()->mutable_direct();

		return;
	}

		//	case duckdb::LogicalOperatorType::LOGICAL_CROSS_PRODUCT: {
		//		auto sub_cross_rel = new substrait::Rel();
		//		auto sub_cross_prod = sub_cross_rel->mutable_cross();
		//		auto &djoin = (duckdb::LogicalCrossProduct &)dop;
		//		TransformOp(*dop.children[0], *sub_cross_prod->mutable_left());
		//		TransformOp(*dop.children[1], *sub_cross_prod->mutable_right());
		//		auto bindings = djoin.GetColumnBindings();
		//
		//		for (uint32_t idx = 0; idx < bindings.size(); idx++) {
		//			CreateFieldRef(sop.mutable_project()->add_expressions(), idx);
		//		}
		//
		//		sop.mutable_project()->set_allocated_input(sub_cross_rel);
		//		return;
		//	}

	default:
		throw runtime_error(duckdb::LogicalOperatorToString(dop.type));
	}
}

void DuckDBToSubstrait::TransformPlan(duckdb::LogicalOperator &dop) {
	auto sroot = plan.add_relations()->mutable_rel();
	TransformOp(dop, *sroot);
}
} // namespace duckdb