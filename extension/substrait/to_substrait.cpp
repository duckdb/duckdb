#include "to_substrait.hpp"

#include "duckdb/common/constants.hpp"
#include "duckdb/common/enums/expression_type.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/function/table/table_scan.hpp"
#include "duckdb/planner/expression/list.hpp"
#include "duckdb/planner/filter/conjunction_filter.hpp"
#include "duckdb/planner/filter/constant_filter.hpp"
#include "duckdb/planner/joinside.hpp"
#include "duckdb/planner/operator/list.hpp"
#include "duckdb/planner/table_filter.hpp"
#include "duckdb/storage/statistics/string_statistics.hpp"
#include "google/protobuf/util/json_util.h"
#include "substrait/algebra.pb.h"
#include "substrait/plan.pb.h"
#include "duckdb/parser/constraints/not_null_constraint.hpp"

namespace duckdb {

string DuckDBToSubstrait::SerializeToString() {
	string serialized;
	if (!plan.SerializeToString(&serialized)) {
		throw InternalException("It was not possible to serialize the substrait plan");
	}
	return serialized;
}

string DuckDBToSubstrait::SerializeToJson() {
	string serialized;
	auto success = google::protobuf::util::MessageToJsonString(plan, &serialized);
	if (!success.ok()) {
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

void DuckDBToSubstrait::AllocateFunctionArgument(substrait::Expression_ScalarFunction *scalar_fun,
                                                 substrait::Expression *value) {
	auto function_argument = new substrait::FunctionArgument();
	function_argument->set_allocated_value(value);
	scalar_fun->mutable_arguments()->AddAllocated(function_argument);
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
::substrait::Type DuckDBToSubstrait::DuckToSubstraitType(LogicalType &d_type) {
	::substrait::Type s_type;
	switch (d_type.id()) {
	case LogicalTypeId::HUGEINT: {
		// FIXME: Support for hugeint types?
		auto s_decimal = new substrait::Type_Decimal();
		s_decimal->set_scale(38);
		s_decimal->set_precision(0);
		s_type.set_allocated_decimal(s_decimal);
		break;
	}
	case LogicalTypeId::DECIMAL: {
		auto s_decimal = new substrait::Type_Decimal();
		s_decimal->set_scale(DecimalType::GetScale(d_type));
		s_decimal->set_precision(DecimalType::GetWidth(d_type));
		s_type.set_allocated_decimal(s_decimal);
		break;
	}
		// Substrait ppl think unsigned types are not common, so we have to upcast these beauties
		// Which completely borks the optimization they are created for
	case LogicalTypeId::UTINYINT: {
		auto s_integer = new substrait::Type_I16();
		s_type.set_allocated_i16(s_integer);
		break;
	}
	case LogicalTypeId::USMALLINT: {
		auto s_integer = new substrait::Type_I32();
		s_type.set_allocated_i32(s_integer);
		break;
	}
	case LogicalTypeId::UINTEGER: {
		auto s_integer = new substrait::Type_I64();
		s_type.set_allocated_i64(s_integer);
		break;
	}
	case LogicalTypeId::INTEGER: {
		auto s_integer = new substrait::Type_I32();
		s_type.set_allocated_i32(s_integer);
		break;
	}
	case LogicalTypeId::DOUBLE: {
		auto s_double = new substrait::Type_FP64();
		s_type.set_allocated_fp64(s_double);
		break;
	}
	case LogicalTypeId::BIGINT: {
		auto s_bigint = new substrait::Type_I64();
		s_type.set_allocated_i64(s_bigint);
		break;
	}
	case LogicalTypeId::DATE: {
		auto s_date = new substrait::Type_Date();
		s_type.set_allocated_date(s_date);
		break;
	}
	case LogicalTypeId::VARCHAR: {
		auto s_varchar = new substrait::Type_VarChar();
		s_type.set_allocated_varchar(s_varchar);
		break;
	}
	case LogicalTypeId::BOOLEAN: {
		auto s_bool = new substrait::Type_Boolean();
		s_type.set_allocated_bool_(s_bool);
		break;
	}
	default:
		throw InternalException("Type not supported: " + d_type.ToString());
	}
	return s_type;
}

void DuckDBToSubstrait::TransformBoolean(Value &dval, substrait::Expression &sexpr) {
	auto &sval = *sexpr.mutable_literal();
	sval.set_boolean(dval.GetValue<bool>());
}

void DuckDBToSubstrait::TransformHugeInt(Value &dval, substrait::Expression &sexpr) {
	auto &sval = *sexpr.mutable_literal();
	auto *allocated_decimal = new ::substrait::Expression_Literal_Decimal();
	auto hugeint_str = dval.ToString();
	allocated_decimal->set_scale(0);
	allocated_decimal->set_precision((int32_t)hugeint_str.size());

	auto *decimal_value = new string();
	*decimal_value = hugeint_str;
	allocated_decimal->set_allocated_value(decimal_value);
	sval.set_allocated_decimal(allocated_decimal);
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
	auto scast = sexpr.mutable_cast();
	TransformExpr(*dcast.child, *scast->mutable_input(), col_offset);
	*scast->mutable_type() = DuckToSubstraitType(dcast.return_type);
}

void DuckDBToSubstrait::TransformFunctionExpression(Expression &dexpr, substrait::Expression &sexpr,
                                                    uint64_t col_offset) {
	auto &dfun = (BoundFunctionExpression &)dexpr;
	auto sfun = sexpr.mutable_scalar_function();
	sfun->set_function_reference(RegisterFunction(dfun.function.name));

	for (auto &darg : dfun.children) {
		auto sarg = sfun->add_arguments();
		TransformExpr(*darg, *sarg->mutable_value(), col_offset);
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
		fname = "lt";
		break;
	case ExpressionType::COMPARE_LESSTHANOREQUALTO:
		fname = "lte";
		break;
	case ExpressionType::COMPARE_GREATERTHAN:
		fname = "gt";
		break;
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
		fname = "gte";
		break;
	case ExpressionType::COMPARE_NOTEQUAL:
		fname = "not_equal";
		break;
	default:
		throw InternalException(ExpressionTypeToString(dexpr.type));
	}

	auto scalar_fun = sexpr.mutable_scalar_function();
	scalar_fun->set_function_reference(RegisterFunction(fname));
	auto sarg = scalar_fun->add_arguments();
	TransformExpr(*dcomp.left, *sarg->mutable_value(), 0);
	sarg = scalar_fun->add_arguments();
	TransformExpr(*dcomp.right, *sarg->mutable_value(), 0);
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
		auto s_arg = scalar_fun->add_arguments();
		TransformExpr(*child, *s_arg->mutable_value(), col_offset);
	}
}

void DuckDBToSubstrait::TransformNotNullExpression(Expression &dexpr, substrait::Expression &sexpr,
                                                   uint64_t col_offset) {
	auto &dop = (BoundOperatorExpression &)dexpr;
	auto scalar_fun = sexpr.mutable_scalar_function();
	scalar_fun->set_function_reference(RegisterFunction("is_not_null"));
	auto s_arg = scalar_fun->add_arguments();
	TransformExpr(*dop.children[0], *s_arg->mutable_value(), col_offset);
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

void DuckDBToSubstrait::TransformInExpression(Expression &dexpr, substrait::Expression &sexpr) {
	auto &duck_in_op = (BoundOperatorExpression &)dexpr;
	auto subs_in_op = sexpr.mutable_singular_or_list();

	// Get the expression
	TransformExpr(*duck_in_op.children[0], *subs_in_op->mutable_value());

	// Get the values
	for (idx_t i = 1; i < duck_in_op.children.size(); i++) {
		subs_in_op->add_options();
		TransformExpr(*duck_in_op.children[i], *subs_in_op->mutable_options(i - 1));
	}
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
	case ExpressionType::COMPARE_IN:
		TransformInExpression(dexpr, sexpr);
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
	auto selection = new ::substrait::Expression_FieldReference();
	selection->mutable_direct_reference()->mutable_struct_field()->set_field((int32_t)col_idx);
	auto root_reference = new ::substrait::Expression_FieldReference_RootReference();
	selection->set_allocated_root_reference(root_reference);
	D_ASSERT(selection->root_type_case() == substrait::Expression_FieldReference::RootTypeCase::kRootReference);
	expr->set_allocated_selection(selection);
	D_ASSERT(expr->has_selection());
}

substrait::Expression *DuckDBToSubstrait::TransformIsNotNullFilter(uint64_t col_idx, TableFilter &dfilter) {
	auto s_expr = new substrait::Expression();
	auto scalar_fun = s_expr->mutable_scalar_function();
	scalar_fun->set_function_reference(RegisterFunction("is_not_null"));
	auto s_arg = scalar_fun->add_arguments();
	CreateFieldRef(s_arg->mutable_value(), col_idx);
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
	auto s_arg = s_scalar->add_arguments();
	CreateFieldRef(s_arg->mutable_value(), col_idx);
	s_arg = s_scalar->add_arguments();
	TransformConstant(constant_filter.constant, *s_arg->mutable_value());
	uint64_t function_id;
	switch (constant_filter.comparison_type) {
	case ExpressionType::COMPARE_EQUAL:
		function_id = RegisterFunction("equal");
		break;
	case ExpressionType::COMPARE_LESSTHANOREQUALTO:
		function_id = RegisterFunction("lte");
		break;
	case ExpressionType::COMPARE_LESSTHAN:
		function_id = RegisterFunction("lt");
		break;
	case ExpressionType::COMPARE_GREATERTHAN:
		function_id = RegisterFunction("gt");
		break;
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
		function_id = RegisterFunction("gte");
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
		join_comparision = "gt";
		break;
	case ExpressionType::COMPARE_NOT_DISTINCT_FROM:
		join_comparision = "is_not_distinct_from";
		break;
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
		join_comparision = "gte";
		break;
	case ExpressionType::COMPARE_LESSTHANOREQUALTO:
		join_comparision = "lte";
		break;
	default:
		throw InternalException("Unsupported join comparison");
	}
	auto scalar_fun = expr->mutable_scalar_function();
	scalar_fun->set_function_reference(RegisterFunction(join_comparision));
	auto s_arg = scalar_fun->add_arguments();
	TransformExpr(*dcond.left, *s_arg->mutable_value());
	s_arg = scalar_fun->add_arguments();
	TransformExpr(*dcond.right, *s_arg->mutable_value(), left_ncol);
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
			auto s_arg = smeas->add_arguments();
			TransformExpr(*darg, *s_arg->mutable_value());
		}
	}
	return res;
}

void DuckDBToSubstrait::TransformTypeInfo(substrait::Type_Struct *type_schema, LogicalType &type,
                                          BaseStatistics &column_statistics, bool not_null) {

	substrait::Type_Nullability type_nullability;
	if (not_null) {
		type_nullability = substrait::Type_Nullability::Type_Nullability_NULLABILITY_REQUIRED;
	} else {
		type_nullability = substrait::Type_Nullability::Type_Nullability_NULLABILITY_NULLABLE;
	}
	switch (type.id()) {
	case LogicalTypeId::BOOLEAN: {
		auto bool_type = new substrait::Type_Boolean;
		bool_type->set_nullability(type_nullability);
		type_schema->add_types()->set_allocated_bool_(bool_type);
		break;
	}
	case LogicalTypeId::TINYINT: {
		auto integral_type = new substrait::Type_I8;
		integral_type->set_nullability(type_nullability);
		type_schema->add_types()->set_allocated_i8(integral_type);
		break;
	}
	case LogicalTypeId::SMALLINT: {
		auto integral_type = new substrait::Type_I16;
		integral_type->set_nullability(type_nullability);
		type_schema->add_types()->set_allocated_i16(integral_type);
		break;
	}
	case LogicalTypeId::INTEGER: {
		auto integral_type = new substrait::Type_I32;
		integral_type->set_nullability(type_nullability);
		type_schema->add_types()->set_allocated_i32(integral_type);
		break;
	}
	case LogicalTypeId::BIGINT: {
		auto integral_type = new substrait::Type_I64;
		integral_type->set_nullability(type_nullability);
		type_schema->add_types()->set_allocated_i64(integral_type);
		break;
	}
	case LogicalTypeId::DATE: {
		auto date_type = new substrait::Type_Date;
		date_type->set_nullability(type_nullability);
		type_schema->add_types()->set_allocated_date(date_type);
		break;
	}
	case LogicalTypeId::TIME: {
		auto time_type = new substrait::Type_Time;
		time_type->set_nullability(type_nullability);
		type_schema->add_types()->set_allocated_time(time_type);
		break;
	}
	case LogicalTypeId::TIMESTAMP: {
		// FIXME: Shouldn't this have a precision?
		auto timestamp_type = new substrait::Type_Timestamp;
		timestamp_type->set_nullability(type_nullability);
		type_schema->add_types()->set_allocated_timestamp(timestamp_type);
		break;
	}
	case LogicalTypeId::FLOAT: {
		auto float_type = new substrait::Type_FP32;
		float_type->set_nullability(type_nullability);
		type_schema->add_types()->set_allocated_fp32(float_type);
		break;
	}
	case LogicalTypeId::DOUBLE: {
		auto double_type = new substrait::Type_FP64;
		double_type->set_nullability(type_nullability);
		type_schema->add_types()->set_allocated_fp64(double_type);
		break;
	}
	case LogicalTypeId::DECIMAL: {
		auto decimal_type = new substrait::Type_Decimal;
		decimal_type->set_nullability(type_nullability);
		decimal_type->set_precision(DecimalType::GetWidth(type));
		decimal_type->set_scale(DecimalType::GetScale(type));
		type_schema->add_types()->set_allocated_decimal(decimal_type);
		break;
	}
	case LogicalTypeId::VARCHAR: {
		auto varchar_type = new substrait::Type_VarChar;
		varchar_type->set_nullability(type_nullability);
		auto &string_statistics = (StringStatistics &)column_statistics;
		varchar_type->set_length(string_statistics.max_string_length);
		type_schema->add_types()->set_allocated_varchar(varchar_type);
		break;
	}
	default:
		throw NotImplementedException("Logical Type " + type.ToString() +
		                              " not implemented as Substrait Schema Result.");
	}
}

set<idx_t> GetNotNullConstraintCol(TableCatalogEntry &tbl) {
	set<idx_t> not_null;
	for (auto &constraint : tbl.constraints) {
		if (constraint->type == ConstraintType::NOT_NULL) {
			not_null.insert(((NotNullConstraint *)constraint.get())->index);
		}
	}
	return not_null;
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

	// Add Table Schema
	sget->mutable_named_table()->add_names(table_scan_bind_data.table->name);
	auto base_schema = new ::substrait::NamedStruct();
	auto type_info = new substrait::Type_Struct();
	type_info->set_nullability(substrait::Type_Nullability_NULLABILITY_REQUIRED);
	auto not_null_constraint = GetNotNullConstraintCol(*table_scan_bind_data.table);
	for (idx_t i = 0; i < dget.names.size(); i++) {
		auto cur_type = dget.returned_types[i];
		if (cur_type.id() == LogicalTypeId::STRUCT) {
			throw std::runtime_error("Structs are not yet accepted in table scans");
		}
		base_schema->add_names(dget.names[i]);
		auto column_statistics = dget.function.statistics(context, &table_scan_bind_data, i);
		bool not_null = not_null_constraint.find(i) != not_null_constraint.end();
		TransformTypeInfo(type_info, cur_type, *column_statistics, not_null);
	}
	base_schema->set_allocated_struct_(type_info);
	sget->set_allocated_base_schema(base_schema);
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
