#include "duckdb.hpp"

#include "duckdb/main/client_context.hpp"
#include "duckdb/planner/operator/list.hpp"
#include "duckdb/planner/expression/list.hpp"
#include "duckdb/function/table/table_scan.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"

#include "Schema_generated.h"
#include "Plan_generated.h"
#include "Relation_generated.h"

#include "flatbuffers/minireflect.h"

#include "tpch-extension.hpp"

#include <string>

using namespace std;

namespace arrowir = org::apache::arrow::computeir::flatbuf;
namespace arrow = org::apache::arrow::flatbuf;
static flatbuffers::Offset<arrowir::Expression> transform_expr(flatbuffers::FlatBufferBuilder &fbb,
                                                               duckdb::Expression &expr) {
	flatbuffers::Offset<arrowir::Expression> res;

	switch (expr.type) {
	case duckdb::ExpressionType::VALUE_CONSTANT: {
		auto &constant = (duckdb::BoundConstantExpression &)expr;
		auto &type = constant.value.type();
		switch (type.id()) {
		case duckdb::LogicalTypeId::DECIMAL: {

			std::vector<int8_t> decimal_bytes_vec;

			switch (type.InternalType()) {
			case duckdb::PhysicalType::INT16:
				decimal_bytes_vec.resize(2);
				memcpy(decimal_bytes_vec.data(), &constant.value.value_.smallint, sizeof(int16_t));
				break;
			case duckdb::PhysicalType::INT32:
				decimal_bytes_vec.resize(4);
				memcpy(decimal_bytes_vec.data(), &constant.value.value_.integer, sizeof(int32_t));
				break;
			case duckdb::PhysicalType::INT64:
				decimal_bytes_vec.resize(8);
				memcpy(decimal_bytes_vec.data(), &constant.value.value_.bigint, sizeof(int64_t));
				break;
			case duckdb::PhysicalType::INT128:
				decimal_bytes_vec.resize(16);
				memcpy(decimal_bytes_vec.data(), &constant.value.value_.hugeint, sizeof(duckdb::hugeint_t));
				break;
			default:
				throw std::runtime_error(duckdb::TypeIdToString(type.InternalType()));
			}

			auto decimal =
			    arrowir::CreateDecimalLiteral(fbb, fbb.CreateVector(decimal_bytes_vec),
			                                  duckdb::DecimalType::GetScale(type), duckdb::DecimalType::GetWidth(type));
			auto literal =
			    arrowir::CreateLiteral(fbb, arrowir::LiteralImpl::LiteralImpl_DecimalLiteral, decimal.Union());
			res = arrowir::CreateExpression(fbb, arrowir::ExpressionImpl::ExpressionImpl_Literal, literal.Union());
			break;
		}
		default:
			throw std::runtime_error(type.ToString());
		}
		break;
	}
	case duckdb::ExpressionType::BOUND_REF: {
		auto &bound_ref = (duckdb::BoundReferenceExpression &)expr;
		auto field_index = arrowir::CreateFieldIndex(fbb, bound_ref.index);
		auto field_ref = arrowir::CreateFieldRef(fbb, arrowir::Deref::Deref_FieldIndex, field_index.Union(), 0);
		res = arrowir::CreateExpression(fbb, arrowir::ExpressionImpl::ExpressionImpl_FieldRef, field_ref.Union());
		break;
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
		res = arrowir::CreateExpression(fbb, arrowir::ExpressionImpl::ExpressionImpl_Call, call.Union());
		break;
	}
	case duckdb::ExpressionType::BOUND_FUNCTION: {
		auto &bound_function = (duckdb::BoundFunctionExpression &)expr;
		auto function_name = fbb.CreateString(bound_function.function.name);

		std::vector<flatbuffers::Offset<arrowir::Expression>> arguments_vec;
		for (auto &expr : bound_function.children) {
			arguments_vec.push_back(transform_expr(fbb, *expr));
		}
		auto call = arrowir::CreateCall(fbb, function_name, fbb.CreateVector(arguments_vec));
		res = arrowir::CreateExpression(fbb, arrowir::ExpressionImpl::ExpressionImpl_Call, call.Union());
		break;
	}
	default:
		throw std::runtime_error(duckdb::ExpressionTypeToString(expr.type));
	}
	return res;
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
			arrow::Type arrow_type;
			auto col_name = fbb.CreateString(get.names[col_idx]);
			auto &type = get.types[col_idx];
			switch (type.id()) {
			case duckdb::LogicalTypeId::INTEGER:
				arrow_type = arrow::Type_Int;
				break;
			case duckdb::LogicalTypeId::DECIMAL:
				arrow_type = arrow::Type_Decimal;
				break;
			case duckdb::LogicalTypeId::VARCHAR:
				arrow_type = arrow::Type_Binary;
				break;
			case duckdb::LogicalTypeId::DATE:
				arrow_type = arrow::Type_Date;
				break;
			default:
				throw std::runtime_error(type.ToString());
			}
			fields_vec.push_back(arrow::CreateField(fbb, col_name, true, arrow_type));
		}
		auto schema =
		    arrow::CreateSchema(fbb, org::apache::arrow::flatbuf::Endianness_Little, fbb.CreateVector(fields_vec));

		auto table_name = fbb.CreateString(table_scan_bind_data.table->name);
		auto arrow_table = arrowir::CreateTable(fbb, rel_base, table_name, schema);

		return arrowir::CreateRelation(fbb, arrowir::RelationImpl_Table, arrow_table.Union());
	}

	case duckdb::LogicalOperatorType::LOGICAL_TOP_N: {
		auto &topn = (duckdb::LogicalTopN &)op;
		auto arrow_topn =
		    arrowir::CreateLimit(fbb, rel_base, transform_op(fbb, *op.children[0]), topn.offset, topn.limit);
		return arrowir::CreateRelation(fbb, arrowir::RelationImpl_Limit, arrow_topn.Union());
	}

	case duckdb::LogicalOperatorType::LOGICAL_FILTER: {
		auto &filter = (duckdb::LogicalFilter &)op;
		if (filter.expressions.size() > 1) {
			// TODO we could push multiple filters but they really should handle multiple expressions
			// or give us a way to AND them together
			throw std::runtime_error("cant handle filters with more than one expr :/");
		}

		auto arrow_filter = arrowir::CreateFilter(fbb, rel_base, transform_op(fbb, *op.children[0]),
		                                          transform_expr(fbb, *filter.expressions[0]));
		return arrowir::CreateRelation(fbb, arrowir::RelationImpl_Filter, arrow_filter.Union());
	}

	case duckdb::LogicalOperatorType::LOGICAL_COMPARISON_JOIN: {
		auto &join = (duckdb::LogicalComparisonJoin &)op;
		arrowir::JoinKind arrow_join_kind;
		switch (join.join_type) {
		case duckdb::JoinType::INNER:
			arrow_join_kind = arrowir::JoinKind_Inner;
			break;
		default:
			throw std::runtime_error("unsupported join type");
		}
		// TODO join was forgotten in the relational union fbs :D
		if (join.expressions.size() > 1) {
			// TODO we could push multiple filters but they really should handle multiple expressions
			// or give us a way to AND them together
			throw std::runtime_error("cant handle filters with more than one expr :/");
		}
		auto arrow_join =
		    arrowir::CreateJoin(fbb, rel_base, transform_op(fbb, *op.children[0]), transform_op(fbb, *op.children[1]),
		                        transform_expr(fbb, *join.expressions[0]), arrow_join_kind);
		return arrowir::CreateRelation(fbb, arrowir::RelationImpl_Join, arrow_join.Union());
	}

	default:
		throw std::runtime_error(duckdb::LogicalOperatorToString(op.type));
	}
}

int main() {
	duckdb::DuckDB db;

	duckdb::TPCHExtension tpch;
	tpch.Load(db);

	duckdb::Connection con(db);
	// create TPC-H tables in duckdb catalog, but without any contents
	con.Query("call dbgen(sf=0.1)");

	auto plan = con.context->ExtractPlan(duckdb::TPCHExtension::GetQuery(1));

	printf("\n%s\n", plan->ToString().c_str());

	flatbuffers::FlatBufferBuilder fbb;
	std::vector<flatbuffers::Offset<arrowir::Relation>> relations_vec = {transform_op(fbb, *plan)};
	auto arrow_plan = arrowir::CreatePlan(fbb, fbb.CreateVector(relations_vec));
	fbb.Finish(arrow_plan);

	printf("\n%s\n", flatbuffers::FlatBufferToString(fbb.GetBufferPointer(), arrowir::PlanTypeTable(), true).c_str());

	// TODO translate back to duckdb plan
	// TODO execute back translation
	// TODO check results vs direct plan
	// TODO translate other queries
	// TODO optimize all delim joins away for tpch
}
