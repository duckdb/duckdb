#include "duckdb.hpp"

#include "duckdb/main/client_context.hpp"
#include "duckdb/planner/operator/list.hpp"
#include "duckdb/planner/expression/list.hpp"
#include "duckdb/parser/expression/list.hpp"
#include "duckdb/function/table/table_scan.hpp"
#include "duckdb/planner/tableref/bound_basetableref.hpp"
#include "duckdb/planner/expression_binder/select_binder.hpp"
#include "duckdb/planner/query_node/bound_select_node.hpp"
#include "duckdb/main/relation/limit_relation.hpp"
#include "duckdb/main/relation/order_relation.hpp"
#include "duckdb/main/relation/projection_relation.hpp"
#include "duckdb/main/relation/aggregate_relation.hpp"
#include "duckdb/main/relation/filter_relation.hpp"
#include "duckdb/main/relation/join_relation.hpp"
#include "duckdb/planner/filter/constant_filter.hpp"
#include "duckdb/planner/filter/conjunction_filter.hpp"
#include "duckdb/planner/filter/null_filter.hpp"
#include "duckdb/common/types/date.hpp"

#include "Schema_generated.h"
#include "Plan_generated.h"
#include "Relation_generated.h"

#include "flatbuffers/minireflect.h"

#include "tpch-extension.hpp"

#include "compare_result.hpp"

#include <string>

using namespace std;

namespace arrowir = org::apache::arrow::computeir::flatbuf;
namespace arrow = org::apache::arrow::flatbuf;

struct DuckDBPlanToArrow {

	DuckDBPlanToArrow(flatbuffers::FlatBufferBuilder &fbb_p) : fbb(fbb_p) {
	}

	static arrow::Type TransformType(duckdb::LogicalType &type) {
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

	flatbuffers::Offset<arrowir::Expression> TransformConstant(duckdb::Value &value) {
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
		case duckdb::LogicalTypeId::DATE: {
			auto date = arrowir::CreateDateLiteral(fbb, duckdb::Date::Epoch(value.value_.date) * 1000);
			literal = arrowir::CreateLiteral(fbb, arrowir::LiteralImpl::LiteralImpl_DateLiteral, date.Union());
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
			    arrowir::CreateDecimalLiteral(fbb, fbb.CreateVector(decimal_bytes_vec),
			                                  duckdb::DecimalType::GetScale(type), duckdb::DecimalType::GetWidth(type));
			literal = arrowir::CreateLiteral(fbb, arrowir::LiteralImpl::LiteralImpl_DecimalLiteral, decimal.Union());
			break;
		}
		default:
			throw std::runtime_error(type.ToString());
		}
		return arrowir::CreateExpression(fbb, arrowir::ExpressionImpl::ExpressionImpl_Literal, literal.Union());
	}

	flatbuffers::Offset<arrowir::Expression> CreatePositionalReference(duckdb::idx_t pos) {
		auto field_index = arrowir::CreateFieldIndex(fbb, pos);
		auto field_ref = arrowir::CreateFieldRef(fbb, arrowir::Deref::Deref_FieldIndex, field_index.Union(), 0);
		return arrowir::CreateExpression(fbb, arrowir::ExpressionImpl::ExpressionImpl_FieldRef, field_ref.Union());
	}

	flatbuffers::Offset<arrowir::Expression> TransformExpr(duckdb::Expression &expr) {
		switch (expr.type) {
		case duckdb::ExpressionType::VALUE_CONSTANT: {
			auto &constant = (duckdb::BoundConstantExpression &)expr;
			return TransformConstant(constant.value);
		}
		case duckdb::ExpressionType::BOUND_REF: {
			auto &bound_ref = (duckdb::BoundReferenceExpression &)expr;
			return CreatePositionalReference(bound_ref.index);
		}
		case duckdb::ExpressionType::CASE_EXPR: {
			// oof
			auto &case_expr = (duckdb::BoundCaseExpression &)expr;
			auto check_arrow = TransformExpr(*case_expr.check);
			auto true_res_arrow = TransformExpr(*case_expr.result_if_true);
			auto false_res_arrow = TransformExpr(*case_expr.result_if_false);

			auto true_val = duckdb::Value::BOOLEAN(true);
			auto true_fragment_arrow = arrowir::CreateCaseFragment(fbb, TransformConstant(true_val), true_res_arrow);
			auto field_ref = arrowir::CreateSimpleCase(
			    fbb, check_arrow, fbb.CreateVector<flatbuffers::Offset<arrowir::CaseFragment>>({true_fragment_arrow}),
			    false_res_arrow);
			return arrowir::CreateExpression(fbb, arrowir::ExpressionImpl::ExpressionImpl_SimpleCase,
			                                 field_ref.Union());
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
				function_name = fbb.CreateString("greaterthan");
				break;
			case duckdb::ExpressionType::COMPARE_GREATERTHANOREQUALTO:
				function_name = fbb.CreateString("greaterthanequal");
				break;
			default:
				throw std::runtime_error(duckdb::ExpressionTypeToString(expr.type));
			}

			auto arg_list = fbb.CreateVector<flatbuffers::Offset<arrowir::Expression>>(
			    {TransformExpr(*comp.left), TransformExpr(*comp.right)});
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
			    {TransformExpr(*conj.children[0]), TransformExpr(*conj.children[1])});
			auto call = arrowir::CreateCall(fbb, function_name, arg_list);
			return arrowir::CreateExpression(fbb, arrowir::ExpressionImpl::ExpressionImpl_Call, call.Union());
		}
		case duckdb::ExpressionType::BOUND_AGGREGATE: {
			auto &aggr = (duckdb::BoundAggregateExpression &)expr;
			auto function_name = fbb.CreateString(aggr.function.name);

			// TODO filter, ordering, distinct are missing here but irrelevant for TPC-H
			std::vector<flatbuffers::Offset<arrowir::Expression>> arguments_vec;
			for (auto &expr : aggr.children) {
				arguments_vec.push_back(TransformExpr(*expr));
			}

			auto call = arrowir::CreateCall(fbb, function_name, fbb.CreateVector(arguments_vec));
			return arrowir::CreateExpression(fbb, arrowir::ExpressionImpl::ExpressionImpl_Call, call.Union());
		}
		case duckdb::ExpressionType::BOUND_FUNCTION: {
			auto &bound_function = (duckdb::BoundFunctionExpression &)expr;
			auto function_name = fbb.CreateString(bound_function.function.name);

			std::vector<flatbuffers::Offset<arrowir::Expression>> arguments_vec;
			for (auto &expr : bound_function.children) {
				arguments_vec.push_back(TransformExpr(*expr));
			}
			auto call = arrowir::CreateCall(fbb, function_name, fbb.CreateVector(arguments_vec));
			return arrowir::CreateExpression(fbb, arrowir::ExpressionImpl::ExpressionImpl_Call, call.Union());
		}
		case duckdb::ExpressionType::OPERATOR_CAST: {
			auto &cast = (duckdb::BoundCastExpression &)expr;
			auto function_name = fbb.CreateString("cast");

			auto str = arrowir::CreateStringLiteral(
			    fbb, fbb.CreateString(arrow::EnumNameType(TransformType(cast.return_type))));
			auto literal = arrowir::CreateLiteral(fbb, arrowir::LiteralImpl::LiteralImpl_StringLiteral, str.Union());
			auto literal_expr =
			    arrowir::CreateExpression(fbb, arrowir::ExpressionImpl::ExpressionImpl_Literal, literal.Union());

			auto arg_list =
			    fbb.CreateVector<flatbuffers::Offset<arrowir::Expression>>({TransformExpr(*cast.child), literal_expr});
			auto call = arrowir::CreateCall(fbb, function_name, arg_list);
			return arrowir::CreateExpression(fbb, arrowir::ExpressionImpl::ExpressionImpl_Call, call.Union());
		}
		default:
			throw std::runtime_error(duckdb::ExpressionTypeToString(expr.type));
		}
	}

	flatbuffers::Offset<arrowir::Expression> TransformFilter(idx_t col_idx, duckdb::TableFilter &filter) {

		switch (filter.filter_type) {
		case duckdb::TableFilterType::CONJUNCTION_AND: {
			auto &conjunction_and_filter = (duckdb::ConjunctionAndFilter &)filter;

			if (conjunction_and_filter.child_filters.size() != 2) {
				// TODO support arbitrary filter counts
				throw runtime_error("cant do more than two filters in a conjunction filter");
			}
			auto lhs = TransformFilter(col_idx, *conjunction_and_filter.child_filters[0]);
			auto rhs = TransformFilter(col_idx, *conjunction_and_filter.child_filters[1]);

			flatbuffers::Offset<flatbuffers::String> function_name = fbb.CreateString("and");
			auto arg_list = fbb.CreateVector<flatbuffers::Offset<arrowir::Expression>>({lhs, rhs});
			auto call = arrowir::CreateCall(fbb, function_name, arg_list);
			return arrowir::CreateExpression(fbb, arrowir::ExpressionImpl::ExpressionImpl_Call, call.Union());
		}
		case duckdb::TableFilterType::IS_NOT_NULL: {
			auto &is_not_null_filter = (duckdb::ConjunctionAndFilter &)filter;
			auto val = CreatePositionalReference(col_idx);

			flatbuffers::Offset<flatbuffers::String> function_name = fbb.CreateString("is_not_null");
			auto arg_list = fbb.CreateVector<flatbuffers::Offset<arrowir::Expression>>({val});
			auto call = arrowir::CreateCall(fbb, function_name, arg_list);
			return arrowir::CreateExpression(fbb, arrowir::ExpressionImpl::ExpressionImpl_Call, call.Union());
		}
		case duckdb::TableFilterType::CONSTANT_COMPARISON: {
			auto &constant_filter = (duckdb::ConstantFilter &)filter;
			auto lhs_expr = CreatePositionalReference(col_idx);
			auto rhs_expr = TransformConstant(constant_filter.constant);

			flatbuffers::Offset<flatbuffers::String> function_name;
			switch (constant_filter.comparison_type) {
			case duckdb::ExpressionType::COMPARE_EQUAL:
				function_name = fbb.CreateString("equal");
				break;
			case duckdb::ExpressionType::COMPARE_LESSTHANOREQUALTO:
				function_name = fbb.CreateString("lessthanequal");
				break;
			case duckdb::ExpressionType::COMPARE_LESSTHAN:
				function_name = fbb.CreateString("lessthan");
				break;
			case duckdb::ExpressionType::COMPARE_GREATERTHAN:
				function_name = fbb.CreateString("greaterthan");
				break;
			default:
				throw std::runtime_error(duckdb::ExpressionTypeToString(constant_filter.comparison_type));
			}

			auto arg_list = fbb.CreateVector<flatbuffers::Offset<arrowir::Expression>>({lhs_expr, rhs_expr});
			auto call = arrowir::CreateCall(fbb, function_name, arg_list);
			return arrowir::CreateExpression(fbb, arrowir::ExpressionImpl::ExpressionImpl_Call, call.Union());
		}
		default:
			throw runtime_error("Unsupported table filter type");
		}
	}

	flatbuffers::Offset<arrowir::Expression> TransformJoinCondition(duckdb::JoinCondition &cond) {
		std::string join_comparision_name;
		switch (cond.comparison) {
		case duckdb::ExpressionType::COMPARE_EQUAL:
			join_comparision_name = "equal"; // ?
			break;
		case duckdb::ExpressionType::COMPARE_GREATERTHAN:
			join_comparision_name = "greaterthan"; // ?
			break;
		default:
			throw std::runtime_error(duckdb::ExpressionTypeToString(cond.comparison));
		}

		auto arg_list = fbb.CreateVector<flatbuffers::Offset<arrowir::Expression>>(
		    {TransformExpr(*cond.left), TransformExpr(*cond.right)});
		// TODO how do we tell field refs which child they come from?
		auto call = arrowir::CreateCall(fbb, fbb.CreateString(join_comparision_name), arg_list);
		return arrowir::CreateExpression(fbb, arrowir::ExpressionImpl::ExpressionImpl_Call, call.Union());
	}

	flatbuffers::Offset<arrowir::Relation> TransformOp(duckdb::LogicalOperator &op) {
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
				keys_vec.push_back(arrowir::CreateSortKey(fbb, TransformExpr(*order_exp.expression), arrow_ordering));
			}
			auto arrow_order =
			    arrowir::CreateOrderBy(fbb, rel_base, TransformOp(*op.children[0]), fbb.CreateVector(keys_vec));
			return arrowir::CreateRelation(fbb, arrowir::RelationImpl_OrderBy, arrow_order.Union());
		}
		case duckdb::LogicalOperatorType::LOGICAL_PROJECTION: {
			auto &proj = (duckdb::LogicalProjection &)op;
			std::vector<flatbuffers::Offset<arrowir::Expression>> expressions_vec;
			for (auto &expr : proj.expressions) {
				expressions_vec.push_back(TransformExpr(*expr));
			}
			auto arrow_project =
			    arrowir::CreateProject(fbb, rel_base, TransformOp(*op.children[0]), fbb.CreateVector(expressions_vec));
			return arrowir::CreateRelation(fbb, arrowir::RelationImpl_Project, arrow_project.Union());
		}
		case duckdb::LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY: {
			auto &aggr = (duckdb::LogicalAggregate &)op;

			std::vector<flatbuffers::Offset<arrowir::Expression>> groups_vec;
			for (auto &expr : aggr.groups) {
				groups_vec.push_back(TransformExpr(*expr));
			}
			auto groups_vec_fb = fbb.CreateVector(groups_vec);
			std::vector<flatbuffers::Offset<arrowir::Grouping>> grouping_vec;
			grouping_vec.push_back(arrowir::CreateGrouping(fbb, groups_vec_fb));

			std::vector<flatbuffers::Offset<arrowir::Expression>> aggregates_vec;
			for (auto &expr : aggr.expressions) {
				aggregates_vec.push_back(TransformExpr(*expr));
			}
			auto arrow_aggr =
			    arrowir::CreateAggregate(fbb, rel_base, TransformOp(*op.children[0]), fbb.CreateVector(aggregates_vec),
			                             fbb.CreateVector(grouping_vec));

			return arrowir::CreateRelation(fbb, arrowir::RelationImpl_Aggregate, arrow_aggr.Union());
		}
		case duckdb::LogicalOperatorType::LOGICAL_GET: {
			auto &get = (duckdb::LogicalGet &)op;
			auto &table_scan_bind_data = (duckdb::TableScanBindData &)*get.bind_data;

			std::vector<flatbuffers::Offset<arrow::Field>> fields_vec;
			for (auto &col : table_scan_bind_data.table->columns) {
				fields_vec.push_back(
				    arrow::CreateField(fbb, fbb.CreateString(col.name), true, TransformType(col.type)));
			}
			auto schema =
			    arrow::CreateSchema(fbb, org::apache::arrow::flatbuf::Endianness_Little, fbb.CreateVector(fields_vec));

			auto table_name = fbb.CreateString(table_scan_bind_data.table->name);
			auto arrow_table = arrowir::CreateSource(fbb, rel_base, table_name, schema);
			auto rel = arrowir::CreateRelation(fbb, arrowir::RelationImpl_Source, arrow_table.Union());

			// we probably pushed some filters too
			if (get.table_filters.filters.size() != 1) {
				throw runtime_error("eek"); // TODO support none or more than one filter
			}
			flatbuffers::Offset<arrowir::Expression> filter_expression;
			for (auto &filter_entry : get.table_filters.filters) {
				auto col_idx = filter_entry.first;
				auto &filter = *filter_entry.second;

				filter_expression = TransformFilter(col_idx, filter);
			}
			auto arrow_filter = arrowir::CreateFilter(fbb, rel_base, rel, filter_expression);
			auto filter = arrowir::CreateRelation(fbb, arrowir::RelationImpl_Filter, arrow_filter.Union());

			// we have probably pushed some projection
			std::vector<flatbuffers::Offset<arrowir::Expression>> expressions_vec;
			for (auto column_index : get.column_ids) {
				expressions_vec.push_back(CreatePositionalReference(column_index));
			}
			auto arrow_project = arrowir::CreateProject(fbb, rel_base, filter, fbb.CreateVector(expressions_vec));
			return arrowir::CreateRelation(fbb, arrowir::RelationImpl_Project, arrow_project.Union());
		}

		case duckdb::LogicalOperatorType::LOGICAL_TOP_N: {
			auto &topn = (duckdb::LogicalTopN &)op;
			auto arrow_topn =
			    arrowir::CreateLimit(fbb, rel_base, TransformOp(*op.children[0]), topn.offset, topn.limit);
			return arrowir::CreateRelation(fbb, arrowir::RelationImpl_Limit, arrow_topn.Union());
		}

		case duckdb::LogicalOperatorType::LOGICAL_LIMIT: {
			auto &limit = (duckdb::LogicalLimit &)op;
			auto arrow_topn =
			    arrowir::CreateLimit(fbb, rel_base, TransformOp(*op.children[0]), limit.offset_val, limit.limit_val);
			return arrowir::CreateRelation(fbb, arrowir::RelationImpl_Limit, arrow_topn.Union());
		}

		case duckdb::LogicalOperatorType::LOGICAL_FILTER: {
			auto &filter = (duckdb::LogicalFilter &)op;
			if (filter.expressions.size() == 0) {
				// this is a bug in the plan generation but we can work around this
				return TransformOp(*op.children[0]);
			}
			// pretty ugly this
			auto filter_expr_arrow = TransformExpr(*filter.expressions[0]);
			// loop from second element ^^
			auto function_name = fbb.CreateString("and");
			for (auto i = begin(filter.expressions) + 1, e = end(filter.expressions); i != e; ++i) {
				auto rhs_arrow = TransformExpr(*i->get());
				auto arg_list =
				    fbb.CreateVector<flatbuffers::Offset<arrowir::Expression>>({filter_expr_arrow, rhs_arrow});
				auto call = arrowir::CreateCall(fbb, function_name, arg_list);
				filter_expr_arrow =
				    arrowir::CreateExpression(fbb, arrowir::ExpressionImpl::ExpressionImpl_Call, call.Union());
			}

			auto arrow_filter = arrowir::CreateFilter(fbb, rel_base, TransformOp(*op.children[0]), filter_expr_arrow);
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

			auto arrow_join_expression = TransformJoinCondition(join.conditions[0]);
			auto function_name = fbb.CreateString("and");
			for (auto i = begin(join.conditions) + 1, e = end(join.conditions); i != e; ++i) {
				auto rhs_arrow = TransformJoinCondition(*i);
				auto arg_list =
				    fbb.CreateVector<flatbuffers::Offset<arrowir::Expression>>({arrow_join_expression, rhs_arrow});
				auto call = arrowir::CreateCall(fbb, function_name, arg_list);
				arrow_join_expression =
				    arrowir::CreateExpression(fbb, arrowir::ExpressionImpl::ExpressionImpl_Call, call.Union());
			}

			auto arrow_join = arrowir::CreateJoin(fbb, rel_base, TransformOp(*op.children[0]),
			                                      TransformOp(*op.children[1]), arrow_join_expression, arrow_join_kind);
			return arrowir::CreateRelation(fbb, arrowir::RelationImpl_Join, arrow_join.Union());
		}

		default:
			throw std::runtime_error(duckdb::LogicalOperatorToString(op.type));
		}
	}
	flatbuffers::FlatBufferBuilder &fbb;
};

struct ArrowPlanToDuckDB {
	ArrowPlanToDuckDB(duckdb::Connection &conn_p) : conn(conn_p), context(*conn_p.context) {
	}

	unique_ptr<duckdb::ParsedExpression> TransformExpr(const arrowir::Expression *expr) {
		switch (expr->impl_type()) {
		case arrowir::ExpressionImpl::ExpressionImpl_FieldRef: {
			auto arrow_expr = expr->impl_as_FieldRef();
			return duckdb::make_unique<duckdb::PositionalReferenceExpression>(
			    arrow_expr->ref_as_FieldIndex()->position() + 1); // positional references start at 1
		}
		case arrowir::ExpressionImpl::ExpressionImpl_Call: {
			auto arrow_expr = expr->impl_as_Call();
			vector<unique_ptr<duckdb::ParsedExpression>> children;
			for (idx_t i = 0; i < arrow_expr->arguments()->size(); i++) {
				children.push_back(TransformExpr(arrow_expr->arguments()->Get(i)));
			}
			//  here we go recovering string name to function semantics
			if (arrow_expr->name()->str() == "and") {
				return duckdb::make_unique<duckdb::ConjunctionExpression>(duckdb::ExpressionType::CONJUNCTION_AND,
				                                                          move(children));
			}
			if (arrow_expr->name()->str() == "lessthan") {
				return duckdb::make_unique<duckdb::ComparisonExpression>(duckdb::ExpressionType::COMPARE_LESSTHAN,
				                                                         move(children[0]), move(children[1]));
			}
			if (arrow_expr->name()->str() == "lessthanequal") {
				return duckdb::make_unique<duckdb::ComparisonExpression>(
				    duckdb::ExpressionType::COMPARE_LESSTHANOREQUALTO, move(children[0]), move(children[1]));
			}
			if (arrow_expr->name()->str() == "equal") {
				return duckdb::make_unique<duckdb::ComparisonExpression>(duckdb::ExpressionType::COMPARE_EQUAL,
				                                                         move(children[0]), move(children[1]));
			}
			if (arrow_expr->name()->str() == "greaterthanequal") {
				return duckdb::make_unique<duckdb::ComparisonExpression>(
				    duckdb::ExpressionType::COMPARE_GREATERTHANOREQUALTO, move(children[0]), move(children[1]));
			}
			if (arrow_expr->name()->str() == "greaterthan") {
				return duckdb::make_unique<duckdb::ComparisonExpression>(duckdb::ExpressionType::COMPARE_GREATERTHAN,
				                                                         move(children[0]), move(children[1]));
			}
			if (arrow_expr->name()->str() == "is_not_null") {
				return duckdb::make_unique<duckdb::OperatorExpression>(duckdb::ExpressionType::OPERATOR_IS_NOT_NULL,
				                                                       move(children[0]));
			}

			return duckdb::make_unique<duckdb::FunctionExpression>(arrow_expr->name()->str(), move(children));
		}
		case arrowir::ExpressionImpl::ExpressionImpl_Literal: {
			auto arrow_expr = expr->impl_as_Literal();
			switch (arrow_expr->impl_type()) {
			case arrowir::LiteralImpl::LiteralImpl_DecimalLiteral: {
				auto arrow_decimal = arrow_expr->impl_as_DecimalLiteral();
				int64_t raw_val;
				memcpy(&raw_val, arrow_decimal->value()->Data(), arrow_decimal->value()->size());
				auto val = duckdb::Value::DECIMAL(raw_val, arrow_decimal->precision(), arrow_decimal->scale());
				return duckdb::make_unique<duckdb::ConstantExpression>(val);
			}
			case arrowir::LiteralImpl::LiteralImpl_DateLiteral: {
				auto arrow_date = arrow_expr->impl_as_DateLiteral();
				auto val = duckdb::Value::DATE(duckdb::Date::EpochToDate(arrow_date->value() / 1000));
				return duckdb::make_unique<duckdb::ConstantExpression>(val);
			}
			case arrowir::LiteralImpl::LiteralImpl_StringLiteral: {
				auto arrow_string = arrow_expr->impl_as_StringLiteral();
				auto val = duckdb::Value(arrow_string->value()->str());
				return duckdb::make_unique<duckdb::ConstantExpression>(val);
			}
			default:
				throw std::runtime_error(arrowir::EnumNameLiteralImpl(arrow_expr->impl_type()));
			}
		}
		default:
			throw std::runtime_error(arrowir::EnumNameExpressionImpl(expr->impl_type()));
		}
	}

	shared_ptr<duckdb::Relation> TransformRel(const arrowir::Relation *arrow_rel) {
		switch (arrow_rel->impl_type()) {
		case arrowir::RelationImpl_Filter: {
			auto arrow_op = arrow_rel->impl_as_Filter();
			return duckdb::make_shared<duckdb::FilterRelation>(TransformRel(arrow_op->rel()),
			                                                   TransformExpr(arrow_op->predicate()));
		}
		case arrowir::RelationImpl_Limit: {
			auto arrow_op = arrow_rel->impl_as_Limit();
			return duckdb::make_shared<duckdb::LimitRelation>(TransformRel(arrow_op->rel()), arrow_op->count(),
			                                                  arrow_op->offset());
		}
		case arrowir::RelationImpl_Join: {
			auto arrow_op = arrow_rel->impl_as_Join();
			duckdb::JoinType type;
			switch (arrow_op->join_kind()) {
			case arrowir::JoinKind::JoinKind_Inner:
				type = duckdb::JoinType::INNER;
				break;
			default:
				throw runtime_error(arrowir::EnumNameJoinKind(arrow_op->join_kind()));
			}

			return duckdb::make_shared<duckdb::JoinRelation>(TransformRel(arrow_op->left()),
			                                                 TransformRel(arrow_op->right()),
			                                                 TransformExpr(arrow_op->on_expression()), type);
		}
		case arrowir::RelationImpl_OrderBy: {
			auto arrow_op = arrow_rel->impl_as_OrderBy();
			vector<duckdb::OrderByNode> order_nodes;
			for (idx_t i = 0; i < arrow_op->keys()->size(); i++) {
				auto arrow_sort_key = arrow_op->keys()->Get(i);
				duckdb::OrderType type;
				duckdb::OrderByNullType null_order;
				switch (arrow_sort_key->ordering()) {
				case arrowir::Ordering_NULLS_THEN_ASCENDING:
					type = duckdb::OrderType::ASCENDING;
					null_order = duckdb::OrderByNullType::NULLS_FIRST;
					break;
				default:
					throw runtime_error(arrowir::EnumNameOrdering(arrow_sort_key->ordering()));
				}
				order_nodes.emplace_back(type, null_order, TransformExpr(arrow_sort_key->expression()));
			}
			return duckdb::make_shared<duckdb::OrderRelation>(TransformRel(arrow_op->rel()), move(order_nodes));
		}
		case arrowir::RelationImpl_Project: {
			auto arrow_op = arrow_rel->impl_as_Project();

			vector<unique_ptr<duckdb::ParsedExpression>> expressions;
			vector<string> aliases;

			vector<duckdb::LogicalType> types;
			for (idx_t i = 0; i < arrow_op->expressions()->size(); i++) {
				expressions.push_back(TransformExpr(arrow_op->expressions()->Get(i)));
				aliases.push_back("expr_" + to_string(i)); // we need a name but its ugly
			}
			return duckdb::make_shared<duckdb::ProjectionRelation>(TransformRel(arrow_op->rel()), move(expressions),
			                                                       move(aliases));
		}
		case arrowir::RelationImpl_Aggregate: {
			auto arrow_op = arrow_rel->impl_as_Aggregate();

			vector<unique_ptr<duckdb::ParsedExpression>> groups, expressions;
			// this only works for one grouping set, DuckDB does not support multiple sets (yet)
			if (arrow_op->groupings()->size() != 1) {
				throw runtime_error("unsupported multi-grouping");
			}
			auto arrow_groups = arrow_op->groupings()->Get(0);
			for (idx_t i = 0; i < arrow_groups->keys()->size(); i++) {
				groups.push_back(TransformExpr(arrow_groups->keys()->Get(i)));
				// we also need the expression because the groups are not automatically projected out (as they should
				// be)
				expressions.push_back(TransformExpr(arrow_groups->keys()->Get(i)));
			}
			for (idx_t i = 0; i < arrow_op->measures()->size(); i++) {
				expressions.push_back(TransformExpr(arrow_op->measures()->Get(i)));
			}
			return duckdb::make_shared<duckdb::AggregateRelation>(TransformRel(arrow_op->rel()), move(expressions),
			                                                      move(groups));
		}
		case arrowir::RelationImpl_Source: {
			// yay one liners
			return conn.Table(arrow_rel->impl_as_Source()->name()->str());
		}
		default:
			throw runtime_error(arrowir::EnumNameRelationImpl(arrow_rel->impl_type()));
		}
	}

	duckdb::Connection &conn;
	duckdb::ClientContext &context;
};

#include <fstream>

static void transform_plan(duckdb::Connection &con, std::string q) {
	auto plan = con.context->ExtractPlan(q);

	printf("\n%s\n", q.c_str());

	printf("\n%s\n", plan->ToString().c_str());

	flatbuffers::FlatBufferBuilder fbb;
	DuckDBPlanToArrow transformer_d2a(fbb);

	auto arrow_plan = arrowir::CreatePlan(
	    fbb, fbb.CreateVector<flatbuffers::Offset<arrowir::Relation>>({transformer_d2a.TransformOp(*plan)}));
	fbb.Finish(arrow_plan);

	printf("\n%s\n", flatbuffers::FlatBufferToString(fbb.GetBufferPointer(), arrowir::PlanTypeTable(), true).c_str());

	// readback woo
	auto buffer = fbb.GetBufferPointer();
	auto arrow_plan_readback = arrowir::GetPlan(buffer);

	auto root_rel = arrow_plan_readback->sinks()->Get(0);
	ArrowPlanToDuckDB transformer_a2d(con);
	auto duckdb_rel = transformer_a2d.TransformRel(root_rel);
	duckdb_rel->Print();
	duckdb_rel->Execute()->Print();
	auto res = duckdb_rel->Execute();

	// check the results
	std::string file_content;
	std::getline(std::ifstream("../../extension/tpch/dbgen/answers/sf0.1/q01.csv"), file_content, '\0');
	printf("%s\n", duckdb::compare_csv(*res, file_content, true).c_str());
}

int main() {
	duckdb::DuckDB db;

	duckdb::TPCHExtension tpch;
	tpch.Load(db);

	duckdb::Connection con(db);
	con.BeginTransaction(); // somehow we need this
	// create TPC-H tables in duckdb catalog, but without any contents
	con.Query("call dbgen(sf=0.1)");

	// transform_plan(con, "SELECT avg(l_discount) FROM lineitem");

	transform_plan(con, duckdb::TPCHExtension::GetQuery(1));
	//	// transform_plan(con, duckdb::TPCHExtension::GetQuery(2)); // delim join
	// transform_plan(con, duckdb::TPCHExtension::GetQuery(3));
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
