#include "duckdb.hpp"

#include "duckdb/main/client_context.hpp"
#include "duckdb/planner/operator/list.hpp"
#include "duckdb/planner/expression/list.hpp"
#include "duckdb/parser/expression/list.hpp"
#include "duckdb/function/table/table_scan.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/planner/bound_tableref.hpp"
#include "duckdb/planner/tableref/bound_basetableref.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"

#include "Schema_generated.h"
#include "Plan_generated.h"
#include "Relation_generated.h"

#include "flatbuffers/minireflect.h"

#include "tpch-extension.hpp"

#include <string>

using namespace std;

namespace arrowir = org::apache::arrow::computeir::flatbuf;
namespace arrow = org::apache::arrow::flatbuf;

static arrow::Type transform_type(duckdb::LogicalType &type) {
	switch (type.id()) {
	case duckdb::LogicalTypeId::UTINYINT:
	case duckdb::LogicalTypeId::USMALLINT:
	case duckdb::LogicalTypeId::INTEGER:
	case duckdb::LogicalTypeId::BIGINT:
		return arrow::Type_Int; // TODO is this correct?
	case duckdb::LogicalTypeId::DECIMAL:
		return arrow::Type_Decimal;
	case duckdb::LogicalTypeId::DOUBLE:
		return arrow::Type_FloatingPoint;
	case duckdb::LogicalTypeId::VARCHAR:
		return arrow::Type_Binary;
	case duckdb::LogicalTypeId::BOOLEAN:
		return arrow::Type_Bool;
	case duckdb::LogicalTypeId::DATE:
		return arrow::Type_Date;
	default:
		throw std::runtime_error(type.ToString());
	}
}

static flatbuffers::Offset<arrowir::Expression> transform_constant(flatbuffers::FlatBufferBuilder &fbb,
                                                                   duckdb::Value &value) {
	flatbuffers::Offset<arrowir::Literal> literal;
	auto &type = value.type();
	switch (type.id()) {
	case duckdb::LogicalTypeId::BOOLEAN: {
		auto boolean = arrowir::CreateInt64Literal(fbb, value.value_.boolean);
		literal = arrowir::CreateLiteral(fbb, arrowir::LiteralImpl::LiteralImpl_BooleanLiteral, boolean.Union());
		break;
	}
	case duckdb::LogicalTypeId::BIGINT: {
		auto bigint = arrowir::CreateInt64Literal(fbb, value.value_.bigint);
		literal = arrowir::CreateLiteral(fbb, arrowir::LiteralImpl::LiteralImpl_Int64Literal, bigint.Union());
		break;
	}
	case duckdb::LogicalTypeId::INTEGER: {
		auto integer = arrowir::CreateInt32Literal(fbb, value.value_.integer);
		literal = arrowir::CreateLiteral(fbb, arrowir::LiteralImpl::LiteralImpl_Int32Literal, integer.Union());
		break;
	}
	case duckdb::LogicalTypeId::DOUBLE: {
		auto dbl = arrowir::CreateFloat64Literal(fbb, value.value_.double_);
		literal = arrowir::CreateLiteral(fbb, arrowir::LiteralImpl::LiteralImpl_Float64Literal, dbl.Union());
		break;
	}
	case duckdb::LogicalTypeId::VARCHAR: {
		auto str = arrowir::CreateStringLiteral(fbb, fbb.CreateString(value.str_value));
		literal = arrowir::CreateLiteral(fbb, arrowir::LiteralImpl::LiteralImpl_StringLiteral, str.Union());
		break;
	}
	case duckdb::LogicalTypeId::DECIMAL: {

		std::vector<int8_t> decimal_bytes_vec;

		switch (type.InternalType()) {
		case duckdb::PhysicalType::INT16:
			decimal_bytes_vec.resize(2);
			memcpy(decimal_bytes_vec.data(), &value.value_.smallint, sizeof(int16_t));
			break;
		case duckdb::PhysicalType::INT32:
			decimal_bytes_vec.resize(4);
			memcpy(decimal_bytes_vec.data(), &value.value_.integer, sizeof(int32_t));
			break;
		case duckdb::PhysicalType::INT64:
			decimal_bytes_vec.resize(8);
			memcpy(decimal_bytes_vec.data(), &value.value_.bigint, sizeof(int64_t));
			break;
		case duckdb::PhysicalType::INT128:
			decimal_bytes_vec.resize(16);
			memcpy(decimal_bytes_vec.data(), &value.value_.hugeint, sizeof(duckdb::hugeint_t));
			break;
		default:
			throw std::runtime_error(duckdb::TypeIdToString(type.InternalType()));
		}

		auto decimal =
		    arrowir::CreateDecimalLiteral(fbb, fbb.CreateVector(decimal_bytes_vec), duckdb::DecimalType::GetScale(type),
		                                  duckdb::DecimalType::GetWidth(type));
		literal = arrowir::CreateLiteral(fbb, arrowir::LiteralImpl::LiteralImpl_DecimalLiteral, decimal.Union());
		break;
	}
	default:
		throw std::runtime_error(type.ToString());
	}
	return arrowir::CreateExpression(fbb, arrowir::ExpressionImpl::ExpressionImpl_Literal, literal.Union());
}

static flatbuffers::Offset<arrowir::Expression> transform_expr(flatbuffers::FlatBufferBuilder &fbb,
                                                               duckdb::Expression &expr) {

	switch (expr.type) {
	case duckdb::ExpressionType::VALUE_CONSTANT: {
		auto &constant = (duckdb::BoundConstantExpression &)expr;
		return transform_constant(fbb, constant.value);
	}
	case duckdb::ExpressionType::BOUND_REF: {
		auto &bound_ref = (duckdb::BoundReferenceExpression &)expr;
		auto field_index = arrowir::CreateFieldIndex(fbb, bound_ref.index);
		auto field_ref = arrowir::CreateFieldRef(fbb, arrowir::Deref::Deref_FieldIndex, field_index.Union(), 0);
		return arrowir::CreateExpression(fbb, arrowir::ExpressionImpl::ExpressionImpl_FieldRef, field_ref.Union());
	}
	case duckdb::ExpressionType::CASE_EXPR: {
		// oof
		auto &case_expr = (duckdb::BoundCaseExpression &)expr;
		auto check_arrow = transform_expr(fbb, *case_expr.check);
		auto true_res_arrow = transform_expr(fbb, *case_expr.result_if_true);
		auto false_res_arrow = transform_expr(fbb, *case_expr.result_if_false);

		auto true_val = duckdb::Value::BOOLEAN(true);
		auto true_fragment_arrow = arrowir::CreateCaseFragment(fbb, transform_constant(fbb, true_val), true_res_arrow);
		auto field_ref = arrowir::CreateSimpleCase(
		    fbb, check_arrow, fbb.CreateVector<flatbuffers::Offset<arrowir::CaseFragment>>({true_fragment_arrow}),
		    false_res_arrow);
		return arrowir::CreateExpression(fbb, arrowir::ExpressionImpl::ExpressionImpl_SimpleCase, field_ref.Union());
	}
	case duckdb::ExpressionType::COMPARE_EQUAL:
	case duckdb::ExpressionType::COMPARE_NOTEQUAL:
	case duckdb::ExpressionType::COMPARE_LESSTHAN:
	case duckdb::ExpressionType::COMPARE_LESSTHANOREQUALTO:
	case duckdb::ExpressionType::COMPARE_GREATERTHAN:
	case duckdb::ExpressionType::COMPARE_GREATERTHANOREQUALTO: {
		auto &comp = (duckdb::BoundComparisonExpression &)expr;

		flatbuffers::Offset<flatbuffers::String> function_name;
		switch (expr.type) {
		case duckdb::ExpressionType::COMPARE_EQUAL:
			function_name = fbb.CreateString("equal");
			break;
		case duckdb::ExpressionType::COMPARE_NOTEQUAL:
			function_name = fbb.CreateString("notequal");
			break;
		case duckdb::ExpressionType::COMPARE_LESSTHAN:
			function_name = fbb.CreateString("lessthan");
			break;
		case duckdb::ExpressionType::COMPARE_LESSTHANOREQUALTO:
			function_name = fbb.CreateString("lessthanequal");
			break;
		case duckdb::ExpressionType::COMPARE_GREATERTHAN:
			function_name = fbb.CreateString("greatherthan");
			break;
		case duckdb::ExpressionType::COMPARE_GREATERTHANOREQUALTO:
			function_name = fbb.CreateString("greatherthanequal");
			break;
		default:
			throw std::runtime_error(duckdb::ExpressionTypeToString(expr.type));
		}

		auto arg_list = fbb.CreateVector<flatbuffers::Offset<arrowir::Expression>>(
		    {transform_expr(fbb, *comp.left), transform_expr(fbb, *comp.right)});
		auto call = arrowir::CreateCall(fbb, function_name, arg_list);
		return arrowir::CreateExpression(fbb, arrowir::ExpressionImpl::ExpressionImpl_Call, call.Union());
	}
	case duckdb::ExpressionType::CONJUNCTION_AND:
	case duckdb::ExpressionType::CONJUNCTION_OR: {
		auto &conj = (duckdb::BoundConjunctionExpression &)expr;

		flatbuffers::Offset<flatbuffers::String> function_name;
		switch (expr.type) {
		case duckdb::ExpressionType::CONJUNCTION_AND:
			function_name = fbb.CreateString("and");
			break;
		case duckdb::ExpressionType::CONJUNCTION_OR:
			function_name = fbb.CreateString("or");
			break;
		default:
			throw std::runtime_error(duckdb::ExpressionTypeToString(expr.type));
		}
		auto arg_list = fbb.CreateVector<flatbuffers::Offset<arrowir::Expression>>(
		    {transform_expr(fbb, *conj.children[0]), transform_expr(fbb, *conj.children[1])});
		auto call = arrowir::CreateCall(fbb, function_name, arg_list);
		return arrowir::CreateExpression(fbb, arrowir::ExpressionImpl::ExpressionImpl_Call, call.Union());
	}
	case duckdb::ExpressionType::BOUND_AGGREGATE: {
		auto &aggr = (duckdb::BoundAggregateExpression &)expr;
		auto function_name = fbb.CreateString(aggr.function.name);

		// TODO filter, ordering, distinct are missing here but irrelevant for TPC-H
		std::vector<flatbuffers::Offset<arrowir::Expression>> arguments_vec;
		for (auto &expr : aggr.children) {
			arguments_vec.push_back(transform_expr(fbb, *expr));
		}

		auto call = arrowir::CreateCall(fbb, function_name, fbb.CreateVector(arguments_vec));
		return arrowir::CreateExpression(fbb, arrowir::ExpressionImpl::ExpressionImpl_Call, call.Union());
	}
	case duckdb::ExpressionType::BOUND_FUNCTION: {
		auto &bound_function = (duckdb::BoundFunctionExpression &)expr;
		auto function_name = fbb.CreateString(bound_function.function.name);

		std::vector<flatbuffers::Offset<arrowir::Expression>> arguments_vec;
		for (auto &expr : bound_function.children) {
			arguments_vec.push_back(transform_expr(fbb, *expr));
		}
		auto call = arrowir::CreateCall(fbb, function_name, fbb.CreateVector(arguments_vec));
		return arrowir::CreateExpression(fbb, arrowir::ExpressionImpl::ExpressionImpl_Call, call.Union());
	}
	case duckdb::ExpressionType::OPERATOR_CAST: {
		auto &cast = (duckdb::BoundCastExpression &)expr;
		auto function_name = fbb.CreateString("cast");

		auto str =
		    arrowir::CreateStringLiteral(fbb, fbb.CreateString(arrow::EnumNameType(transform_type(cast.return_type))));
		auto literal = arrowir::CreateLiteral(fbb, arrowir::LiteralImpl::LiteralImpl_StringLiteral, str.Union());
		auto literal_expr =
		    arrowir::CreateExpression(fbb, arrowir::ExpressionImpl::ExpressionImpl_Literal, literal.Union());

		auto arg_list = fbb.CreateVector<flatbuffers::Offset<arrowir::Expression>>(
		    {transform_expr(fbb, *cast.child), literal_expr});
		auto call = arrowir::CreateCall(fbb, function_name, arg_list);
		return arrowir::CreateExpression(fbb, arrowir::ExpressionImpl::ExpressionImpl_Call, call.Union());
	}
	default:
		throw std::runtime_error(duckdb::ExpressionTypeToString(expr.type));
	}
}

static flatbuffers::Offset<arrowir::Expression> transform_join_condition(flatbuffers::FlatBufferBuilder &fbb,
                                                                         duckdb::JoinCondition &cond) {
	std::string join_comparision_name;
	switch (cond.comparison) {
	case duckdb::ExpressionType::COMPARE_EQUAL:
		join_comparision_name = "equals"; // ?
		break;
	case duckdb::ExpressionType::COMPARE_GREATERTHAN:
		join_comparision_name = "greaterthan"; // ?
		break;
	default:
		throw std::runtime_error(duckdb::ExpressionTypeToString(cond.comparison));
	}

	auto arg_list = fbb.CreateVector<flatbuffers::Offset<arrowir::Expression>>(
	    {transform_expr(fbb, *cond.left), transform_expr(fbb, *cond.right)});
	// TODO how do we tell field refs which child they come from?
	auto call = arrowir::CreateCall(fbb, fbb.CreateString(join_comparision_name), arg_list);
	return arrowir::CreateExpression(fbb, arrowir::ExpressionImpl::ExpressionImpl_Call, call.Union());
}

static flatbuffers::Offset<arrowir::Relation> transform_op(flatbuffers::FlatBufferBuilder &fbb,
                                                           duckdb::LogicalOperator &op) {

	// TODO what exactly is the point of the RelBase and the output mapping?
	auto rel_base = arrowir::CreateRelBase(fbb, arrowir::Emit_PassThrough, arrowir::CreatePassThrough(fbb).Union());

	switch (op.type) {
	case duckdb::LogicalOperatorType::LOGICAL_ORDER_BY: {
		auto &order = (duckdb::LogicalOrder &)op;

		std::vector<flatbuffers::Offset<arrowir::SortKey>> keys_vec;
		for (auto &order_exp : order.orders) {
			// oh my
			org::apache::arrow::computeir::flatbuf::Ordering arrow_ordering;
			switch (order_exp.type) {
			case duckdb::OrderType::ASCENDING:
				switch (order_exp.null_order) {
				case duckdb::OrderByNullType::NULLS_FIRST:
					arrow_ordering = arrowir::Ordering_NULLS_THEN_ASCENDING;
					break;
				case duckdb::OrderByNullType::NULLS_LAST:
					arrow_ordering = arrowir::Ordering_ASCENDING_THEN_NULLS;
					break;
				default:
					throw std::runtime_error("unknown null order type");
					break;
				}
			case duckdb::OrderType::DESCENDING:
				switch (order_exp.null_order) {
				case duckdb::OrderByNullType::NULLS_FIRST:
					arrow_ordering = arrowir::Ordering_NULLS_THEN_DESCENDING;
					break;
				case duckdb::OrderByNullType::NULLS_LAST:
					arrow_ordering = arrowir::Ordering_DESCENDING_THEN_NULLS;
					break;
				default:
					throw std::runtime_error("unknown null order type");
					break;
				}
			case duckdb::OrderType::ORDER_DEFAULT:
				arrow_ordering = arrowir::Ordering_NULLS_THEN_ASCENDING;
				break;
			default:
				throw std::runtime_error("unknown order type");
			}
			keys_vec.push_back(arrowir::CreateSortKey(fbb, transform_expr(fbb, *order_exp.expression), arrow_ordering));
		}
		auto arrow_order =
		    arrowir::CreateOrderBy(fbb, rel_base, transform_op(fbb, *op.children[0]), fbb.CreateVector(keys_vec));
		return arrowir::CreateRelation(fbb, arrowir::RelationImpl_OrderBy, arrow_order.Union());
	}
	case duckdb::LogicalOperatorType::LOGICAL_PROJECTION: {
		auto &proj = (duckdb::LogicalProjection &)op;
		std::vector<flatbuffers::Offset<arrowir::Expression>> expressions_vec;
		for (auto &expr : proj.expressions) {
			expressions_vec.push_back(transform_expr(fbb, *expr));
		}
		auto arrow_project = arrowir::CreateProject(fbb, rel_base, transform_op(fbb, *op.children[0]),
		                                            fbb.CreateVector(expressions_vec));
		return arrowir::CreateRelation(fbb, arrowir::RelationImpl_Project, arrow_project.Union());
	}
	case duckdb::LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY: {
		auto &aggr = (duckdb::LogicalAggregate &)op;

		std::vector<flatbuffers::Offset<arrowir::Expression>> groups_vec;
		for (auto &expr : aggr.groups) {
			groups_vec.push_back(transform_expr(fbb, *expr));
		}
		auto groups_vec_fb = fbb.CreateVector(groups_vec);
		std::vector<flatbuffers::Offset<arrowir::Grouping>> grouping_vec;
		grouping_vec.push_back(arrowir::CreateGrouping(fbb, groups_vec_fb));

		std::vector<flatbuffers::Offset<arrowir::Expression>> aggregates_vec;
		for (auto &expr : aggr.expressions) {
			aggregates_vec.push_back(transform_expr(fbb, *expr));
		}
		auto arrow_aggr = arrowir::CreateAggregate(fbb, rel_base, transform_op(fbb, *op.children[0]),
		                                           fbb.CreateVector(aggregates_vec), fbb.CreateVector(grouping_vec));

		return arrowir::CreateRelation(fbb, arrowir::RelationImpl_Aggregate, arrow_aggr.Union());
	}
	case duckdb::LogicalOperatorType::LOGICAL_GET: {
		auto &get = (duckdb::LogicalGet &)op;
		auto &table_scan_bind_data = (duckdb::TableScanBindData &)*get.bind_data;
		std::vector<flatbuffers::Offset<arrow::Field>> fields_vec;
		for (idx_t col_idx = 0; col_idx < get.types.size(); col_idx++) {
			fields_vec.push_back(arrow::CreateField(fbb, fbb.CreateString(get.names[col_idx]), true,
			                                        transform_type(get.types[col_idx])));
		}
		auto schema =
		    arrow::CreateSchema(fbb, org::apache::arrow::flatbuf::Endianness_Little, fbb.CreateVector(fields_vec));

		auto table_name = fbb.CreateString(table_scan_bind_data.table->name);
		auto arrow_table = arrowir::CreateSource(fbb, rel_base, table_name, schema);
		return arrowir::CreateRelation(fbb, arrowir::RelationImpl_Source, arrow_table.Union());
	}

	case duckdb::LogicalOperatorType::LOGICAL_TOP_N: {
		auto &topn = (duckdb::LogicalTopN &)op;
		auto arrow_topn =
		    arrowir::CreateLimit(fbb, rel_base, transform_op(fbb, *op.children[0]), topn.offset, topn.limit);
		return arrowir::CreateRelation(fbb, arrowir::RelationImpl_Limit, arrow_topn.Union());
	}

	case duckdb::LogicalOperatorType::LOGICAL_LIMIT: {
		auto &limit = (duckdb::LogicalLimit &)op;
		auto arrow_topn =
		    arrowir::CreateLimit(fbb, rel_base, transform_op(fbb, *op.children[0]), limit.offset_val, limit.limit_val);
		return arrowir::CreateRelation(fbb, arrowir::RelationImpl_Limit, arrow_topn.Union());
	}

	case duckdb::LogicalOperatorType::LOGICAL_FILTER: {
		auto &filter = (duckdb::LogicalFilter &)op;
		if (filter.expressions.size() == 0) {
			// this is a bug in the plan generation but we can work around this
			return transform_op(fbb, *op.children[0]);
		}
		// pretty ugly this
		auto filter_expr_arrow = transform_expr(fbb, *filter.expressions[0]);
		// loop from second element ^^
		auto function_name = fbb.CreateString("and");
		for (auto i = begin(filter.expressions) + 1, e = end(filter.expressions); i != e; ++i) {
			auto rhs_arrow = transform_expr(fbb, *i->get());
			auto arg_list = fbb.CreateVector<flatbuffers::Offset<arrowir::Expression>>({filter_expr_arrow, rhs_arrow});
			auto call = arrowir::CreateCall(fbb, function_name, arg_list);
			filter_expr_arrow =
			    arrowir::CreateExpression(fbb, arrowir::ExpressionImpl::ExpressionImpl_Call, call.Union());
		}

		auto arrow_filter = arrowir::CreateFilter(fbb, rel_base, transform_op(fbb, *op.children[0]), filter_expr_arrow);
		return arrowir::CreateRelation(fbb, arrowir::RelationImpl_Filter, arrow_filter.Union());
	}

	case duckdb::LogicalOperatorType::LOGICAL_COMPARISON_JOIN: {
		auto &join = (duckdb::LogicalComparisonJoin &)op;
		arrowir::JoinKind arrow_join_kind;
		switch (join.join_type) {
		case duckdb::JoinType::INNER:
			arrow_join_kind = arrowir::JoinKind_Inner;
			break;
		case duckdb::JoinType::SEMI:
			arrow_join_kind = arrowir::JoinKind_LeftSemi;
			break;
		case duckdb::JoinType::RIGHT:
			arrow_join_kind = arrowir::JoinKind_RightOuter;
			break;
		case duckdb::JoinType::ANTI:
			arrow_join_kind = arrowir::JoinKind_Anti;
			break;
		default:
			throw std::runtime_error("unsupported join type");
		}

		auto arrow_join_expression = transform_join_condition(fbb, join.conditions[0]);
		auto function_name = fbb.CreateString("and");
		for (auto i = begin(join.conditions) + 1, e = end(join.conditions); i != e; ++i) {
			auto rhs_arrow = transform_join_condition(fbb, *i);
			auto arg_list =
			    fbb.CreateVector<flatbuffers::Offset<arrowir::Expression>>({arrow_join_expression, rhs_arrow});
			auto call = arrowir::CreateCall(fbb, function_name, arg_list);
			arrow_join_expression =
			    arrowir::CreateExpression(fbb, arrowir::ExpressionImpl::ExpressionImpl_Call, call.Union());
		}

		auto arrow_join =
		    arrowir::CreateJoin(fbb, rel_base, transform_op(fbb, *op.children[0]), transform_op(fbb, *op.children[1]),
		                        arrow_join_expression, arrow_join_kind);
		return arrowir::CreateRelation(fbb, arrowir::RelationImpl_Join, arrow_join.Union());
	}

	default:
		throw std::runtime_error(duckdb::LogicalOperatorToString(op.type));
	}
}

static duckdb::LogicalType transform_type_back(const arrow::Field *field) {
	switch (field->type_type()) {
	case arrow::Type_Binary:
		return duckdb::LogicalType::VARCHAR;
	case arrow::Type_Int: // TODO which size is this type? unclear.
		return duckdb::LogicalType::BIGINT;
	case arrow::Type_Date:
		return duckdb::LogicalType::DATE;
	case arrow::Type_Decimal:
		// TODO need to transform those properly in transform_type
		return duckdb::LogicalType::DECIMAL(10, 2);
	default:
		throw std::runtime_error(arrow::EnumNameType(field->type_type()));
	}
}

// this needs some state so no collection of static methods ^^
struct ArrowPlanToDuckDB {
	ArrowPlanToDuckDB(duckdb::ClientContext& context_p) : context(context_p) {
		binder = duckdb::Binder::CreateBinder(context);
	}

	unique_ptr<duckdb::ParsedExpression> TransformParsed(const arrowir::Expression *expr) {
		switch (expr->impl_type()) {
		case arrowir::ExpressionImpl::ExpressionImpl_FieldRef: {
			auto arrow_expr = expr->impl_as_FieldRef();
			return duckdb::make_unique_base<duckdb::ParsedExpression, duckdb::PositionalReferenceExpression>(arrow_expr->relation_index() + 1); // positional references start at 1
		}
		case arrowir::ExpressionImpl::ExpressionImpl_Call: {
			auto arrow_expr = expr->impl_as_Call();
			vector<unique_ptr<duckdb::ParsedExpression>> children;
			// TODO fill children
			return duckdb::make_unique_base<duckdb::ParsedExpression, duckdb::FunctionExpression>(arrow_expr->name()->str(), move(children));
		}
		default:
			throw std::runtime_error(arrowir::EnumNameExpressionImpl(expr->impl_type()));
		}
	}


	unique_ptr<duckdb::Expression> TransformExpression(const arrowir::Expression *expr) {
		auto parsed_expr = TransformParsed(expr);
		duckdb::ExpressionBinder expression_binder(*binder, context);
		return expression_binder.Bind(parsed_expr);
	}

	unique_ptr<duckdb::LogicalOperator> TransformOperator(
																 const arrowir::Relation *arrow_rel) {
		switch (arrow_rel->impl_type()) {
		case arrowir::RelationImpl_Limit: {
			auto arrow_op = arrow_rel->impl_as_Limit();

			auto res = duckdb::make_unique<duckdb::LogicalLimit>(arrow_op->count(), arrow_op->offset(), nullptr, nullptr);
			res->children.push_back(TransformOperator(arrow_op->rel()));
			return res;
		}
//		case arrowir::RelationImpl_OrderBy: {
//			auto arrow_op = arrow_rel->impl_as_OrderBy();
//			vector<duckdb::BoundOrderByNode> order_nodes;
//			// TODO fill nodes
//			auto res = duckdb::make_unique<duckdb::LogicalOrder>(move(order_nodes));
//			res->children.push_back(TransformOperator(arrow_op->rel()));
//			return res;
//		}
		case arrowir::RelationImpl_Project: {
			auto arrow_op = arrow_rel->impl_as_Project();
			auto child = TransformOperator(arrow_op->rel());
			vector<unique_ptr<duckdb::Expression>> select_list;
			for (idx_t i = 0; i < arrow_op->expressions()->size(); i++) {
				select_list.push_back(TransformExpression(arrow_op->expressions()->Get(i)));
			}
			// TODO where do we get our table index?
			auto res = duckdb::make_unique<duckdb::LogicalProjection>(0, move(select_list));
			res->children.push_back(move(child));
			return res;
		}
//		case arrowir::RelationImpl_Aggregate: {
//			auto arrow_op = arrow_rel->impl_as_Aggregate();
//			vector<unique_ptr<duckdb::Expression>> aggregates;
//			// TODO this only works for one grouping set for now
//			if (arrow_op->groupings()->size() != 1) {
//				throw runtime_error("unsupported groups");
//			}
//			auto groups = arrow_op->groupings()->Get(0);
//			for (idx_t i = 0; i < groups->keys()->size(); i++) {
//				aggregates.push_back(TransformExpression(groups->keys()->Get(i)));
//			}
//			for (idx_t i = 0; i < arrow_op->measures()->size(); i++) {
//				aggregates.push_back(TransformExpression(arrow_op->measures()->Get(i)));
//			}
//			// TODO where do we get our table index?
//			auto res = duckdb::make_unique<duckdb::LogicalAggregate>(0, 0, move(aggregates));
//			res->children.push_back(move(TransformOperator(arrow_op->rel())));
//			return res;
//		}
		case arrowir::RelationImpl_Source: {
			auto arrow_op = arrow_rel->impl_as_Source();
			auto base_table_ref = duckdb::make_unique_base<duckdb::TableRef, duckdb::BaseTableRef>();
			auto& btr = (duckdb::BaseTableRef&) *base_table_ref;
			btr.schema_name = "main";
			btr.table_name = arrow_op->name()->str();
			auto bound_table_ref = binder->Bind(*base_table_ref);
			return move(((duckdb::BoundBaseTableRef*)bound_table_ref.get())->get);

			//		vector<duckdb::LogicalType> returned_types;
			//		vector<string> returned_names;
			//		for (idx_t i = 0; i < arrow_op->schema()->fields()->size(); i++) {
			//			auto field = arrow_op->schema()->fields()->Get(i);
			//			returned_types.push_back(transform_type_back(field));
			//			returned_names.push_back((field->name()->str()));
			//		}
			//
			//		auto table_or_view = duckdb::Catalog::GetCatalog(context).GetEntry(context, duckdb::CatalogType::TABLE_ENTRY,
			//		                                                                   "main", arrow_op->name()->str(), false);
			//		// blind cast woo
			//		auto table = (duckdb::TableCatalogEntry *)table_or_view;
			//		auto scan_function = duckdb::TableScanFunction::GetFunction();
			//		auto bind_data = duckdb::make_unique_base<duckdb::FunctionData, duckdb::TableScanBindData>(table);
			//
			//		// TODO where do we get our table index?
			//		return duckdb::make_unique<duckdb::LogicalGet>(0, scan_function, move(bind_data), move(returned_types),
			//		                                               move(returned_names));
		}
		default:
			throw runtime_error(arrowir::EnumNameRelationImpl(arrow_rel->impl_type()));
		}
	}


	shared_ptr<duckdb::Binder> binder;
	duckdb::ClientContext& context;
};




static void transform_plan(duckdb::Connection &con, std::string q) {
	auto plan = con.context->ExtractPlan(q);

	printf("\n%s\n", plan->ToString().c_str());

	flatbuffers::FlatBufferBuilder fbb;
	auto arrow_plan =
	    arrowir::CreatePlan(fbb, fbb.CreateVector<flatbuffers::Offset<arrowir::Relation>>({transform_op(fbb, *plan)}));
	fbb.Finish(arrow_plan);

	printf("\n%s\n", flatbuffers::FlatBufferToString(fbb.GetBufferPointer(), arrowir::PlanTypeTable(), true).c_str());

	// readback woo
	auto buffer = fbb.GetBufferPointer();
	auto arrow_plan_readback = arrowir::GetPlan(buffer);

	auto root_rel = arrow_plan_readback->sinks()->Get(0);
	ArrowPlanToDuckDB transformer(*con.context);
	auto new_plan = transformer.TransformOperator(root_rel);

	printf("\n%s\n", new_plan->ToString().c_str());

	// now lets try to create a physical plan and execute
	duckdb::PhysicalPlanGenerator planner(*con.context);

	auto physical_plan = planner.CreatePlan(move(new_plan));
	printf("\n%s\n", physical_plan->ToString().c_str());

	duckdb::Executor executor(*con.context);
	executor.Initialize(physical_plan.get());

	while (true) {
		auto chunk = executor.FetchChunk();
		if (chunk->size() == 0) {
			break;
		}
		chunk->Print();
	}
}

int main() {
	duckdb::DuckDB db;

	duckdb::TPCHExtension tpch;
	tpch.Load(db);

	duckdb::Connection con(db);
	con.BeginTransaction(); // somehow we need this
	// create TPC-H tables in duckdb catalog, but without any contents
	con.Query("call dbgen(sf=0.1)");

	transform_plan(con, "SELECT l_orderkey, l_returnflag FROM lineitem LIMIT 10");

	//	transform_plan(con, duckdb::TPCHExtension::GetQuery(1));
	//	// transform_plan(con, duckdb::TPCHExtension::GetQuery(2)); // delim join
	//	transform_plan(con, duckdb::TPCHExtension::GetQuery(3));
	//	transform_plan(con, duckdb::TPCHExtension::GetQuery(4));
	//	transform_plan(con, duckdb::TPCHExtension::GetQuery(5));
	//	transform_plan(con, duckdb::TPCHExtension::GetQuery(6));
	//	transform_plan(con, duckdb::TPCHExtension::GetQuery(7));
	//	transform_plan(con, duckdb::TPCHExtension::GetQuery(8));
	//	transform_plan(con, duckdb::TPCHExtension::GetQuery(9));
	//	transform_plan(con, duckdb::TPCHExtension::GetQuery(10));
	//	transform_plan(con, duckdb::TPCHExtension::GetQuery(11));
	//	transform_plan(con, duckdb::TPCHExtension::GetQuery(12));
	//	transform_plan(con, duckdb::TPCHExtension::GetQuery(13));
	//	transform_plan(con, duckdb::TPCHExtension::GetQuery(14));
	//	transform_plan(con, duckdb::TPCHExtension::GetQuery(15));
	//	// transform_plan(con, duckdb::TPCHExtension::GetQuery(16)); // mark join
	//	// transform_plan(con, duckdb::TPCHExtension::GetQuery(17)); // delim join
	//	// transform_plan(con, duckdb::TPCHExtension::GetQuery(18)); // hugeint
	//	transform_plan(con, duckdb::TPCHExtension::GetQuery(19));
	//	// transform_plan(con, duckdb::TPCHExtension::GetQuery(20)); // delim join
	//	// transform_plan(con, duckdb::TPCHExtension::GetQuery(21)); // delim join
	//	// transform_plan(con, duckdb::TPCHExtension::GetQuery(22)); // mark join

	// TODO translate back to duckdb plan, execute back translation, check results vs original plan
	// TODO translate missing queries
	// TODO optimize all delim joins away for tpch?
}
