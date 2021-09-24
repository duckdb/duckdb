#include "catch.hpp"
#include "test_helpers.hpp"
#include "Schema_generated.h"

#include "Plan_generated.h"
#include "Relation_generated.h"

#include "duckdb/main/client_context.hpp"
#include "tpch-extension.hpp"
#include "extension_helper.hpp"
#include <string>

// Thank you Mark!
#include "duckdb/planner/operator/list.hpp"
#include "duckdb/planner/expression/list.hpp"

#include "duckdb/function/table/table_scan.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"

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
			auto decimal_bytes_vec_fb = fbb.CreateVector(decimal_bytes_vec);

			arrowir::DecimalLiteralBuilder decimal_builder(fbb);
			decimal_builder.add_precision(duckdb::DecimalType::GetWidth(type));
			decimal_builder.add_scale(duckdb::DecimalType::GetScale(type));

			decimal_builder.add_value(decimal_bytes_vec_fb);

			auto decimal = decimal_builder.Finish();
			auto literal =
			    arrowir::CreateLiteral(fbb, arrowir::LiteralImpl::LiteralImpl_DecimalLiteral, decimal.Union());
			res = arrowir::CreateExpression(fbb, arrowir::ExpressionImpl::ExpressionImpl_Literal, literal.Union());
			break;
		}
		default:
			throw std::runtime_error(type.ToString());
		}
		//			auto field_index = arrowir::CreateFieldIndex(fbb, bound_ref.index);
		//			auto field_ref = arrowir::CreateFieldRef(fbb, arrowir::Deref::Deref_FieldIndex, field_index.Union(),
		//0); 			res = arrowir::CreateLiteral(fbb, arrowir::LiteralImpl::LiteralImpl_BinaryLiteral, field_ref.Union());
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
		auto arguments_vec_fb = fbb.CreateVector(arguments_vec);

		arrowir::CallBuilder call_builder(fbb);
		call_builder.add_name(function_name);
		call_builder.add_arguments(arguments_vec_fb);
		auto call = call_builder.Finish();

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
		auto arguments_vec_fb = fbb.CreateVector(arguments_vec);

		arrowir::CallBuilder call_builder(fbb);
		call_builder.add_name(function_name);
		call_builder.add_arguments(arguments_vec_fb);
		auto call = call_builder.Finish();

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
	flatbuffers::Offset<void> res;

	// TODO what exactly is the point of the RelBase and the output mapping?
	arrowir::PassThroughBuilder passthrough_builder(fbb);
	auto passthrough = passthrough_builder.Finish().Union();

	arrowir::RelBaseBuilder rel_base_builder(fbb);
	rel_base_builder.add_output_mapping(passthrough);
	auto rel_base = rel_base_builder.Finish();

	switch (op.type) {
	case duckdb::LogicalOperatorType::LOGICAL_ORDER_BY: {
		auto child_rel = transform_op(fbb, *op.children[0]);

		std::vector<flatbuffers::Offset<arrowir::SortKey>> keys_vec;

		// TODO actually transform expressions
		auto keys_vec_fb = fbb.CreateVector(keys_vec);

		arrowir::OrderByBuilder orderby_builder(fbb);
		orderby_builder.add_base(rel_base);
		orderby_builder.add_keys(keys_vec_fb);
		orderby_builder.add_rel(child_rel);

		res = orderby_builder.Finish().Union();
		break;
	}
	case duckdb::LogicalOperatorType::LOGICAL_PROJECTION: {
		auto child_rel = transform_op(fbb, *op.children[0]);

		auto &proj = (duckdb::LogicalProjection &)op;
		std::vector<flatbuffers::Offset<arrowir::Expression>> expressions_vec;
		for (auto &expr : proj.expressions) {
			expressions_vec.push_back(transform_expr(fbb, *expr));
		}
		auto expressions_vec_fb = fbb.CreateVector(expressions_vec);

		arrowir::ProjectBuilder projection_builder(fbb);
		projection_builder.add_rel(child_rel);
		projection_builder.add_base(rel_base);
		projection_builder.add_expressions(expressions_vec_fb);
		res = projection_builder.Finish().Union();
		break;
	}
	case duckdb::LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY: {
		auto child_rel = transform_op(fbb, *op.children[0]);

		auto &aggr = (duckdb::LogicalAggregate &)op;

		std::vector<flatbuffers::Offset<arrowir::Expression>> groups_vec;
		for (auto &expr : aggr.groups) {
			groups_vec.push_back(transform_expr(fbb, *expr));
		}
		auto groups_vec_fb = fbb.CreateVector(groups_vec);
		std::vector<flatbuffers::Offset<arrowir::Grouping>> grouping_vec;
		grouping_vec.push_back(arrowir::CreateGrouping(fbb, groups_vec_fb));
		auto groupings_vec_fb = fbb.CreateVector(grouping_vec);

		std::vector<flatbuffers::Offset<arrowir::Expression>> aggregates_vec;
		for (auto &expr : aggr.expressions) {
			aggregates_vec.push_back(transform_expr(fbb, *expr));
		}
		auto aggregates_vec_fb = fbb.CreateVector(aggregates_vec);

		arrowir::AggregateBuilder aggregate_builder(fbb);
		aggregate_builder.add_base(rel_base);
		aggregate_builder.add_groupings(groupings_vec_fb);
		aggregate_builder.add_measures(aggregates_vec_fb);

		aggregate_builder.add_rel(child_rel);

		res = aggregate_builder.Finish().Union();
		break;
	}
	case duckdb::LogicalOperatorType::LOGICAL_GET: {
		auto &get = (duckdb::LogicalGet &)op;

		auto &table_scan_bind_data = (duckdb::TableScanBindData &)*get.bind_data;
		auto table_name = fbb.CreateString(table_scan_bind_data.table->name);
		arrow::SchemaBuilder schema_builder(fbb);

		// TODO actually create schema
		// schema_builder.add_fields();

		auto schema = schema_builder.Finish();
		arrowir::TableBuilder table_builder(fbb);
		table_builder.add_base(rel_base);
		table_builder.add_name(table_name);
		table_builder.add_schema(schema);
		res = table_builder.Finish().Union();
		break;
	}
	default:
		throw std::runtime_error(duckdb::LogicalOperatorToString(op.type));
	}
	arrowir::RelationBuilder rel_builder(fbb);
	rel_builder.add_impl(res);
	return rel_builder.Finish();
}

TEST_CASE("Test Arrow IR Usage", "[arrowir]") {
	duckdb::DuckDB db;
	duckdb::ExtensionHelper::LoadAllExtensions(db);
	duckdb::Connection con(db);
	// create TPC-H tables in duckdb catalog, but without any contents
	REQUIRE_NO_FAIL(con.Query("call dbgen(sf=0)"));

	auto plan = con.context->ExtractPlan(duckdb::TPCHExtension::GetQuery(1));
	//	printf("\n%s\n", plan->ToString().c_str());

	flatbuffers::FlatBufferBuilder fbb;

	std::vector<flatbuffers::Offset<arrowir::Relation>> relations_vec;
	relations_vec.push_back(transform_op(fbb, *plan));
	auto relations_vec_fb = fbb.CreateVector(relations_vec);
	arrowir::PlanBuilder plan_builder(fbb);
	plan_builder.add_sinks(relations_vec_fb);
	plan_builder.Finish();
}
