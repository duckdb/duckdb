#include "catch.hpp"
#include "test_helpers.hpp"
#include "Schema_generated.h"

#include "Plan_generated.h"
#include "Relation_generated.h"

#include "duckdb/main/client_context.hpp"
#include "tpch-extension.hpp"
#include "extension_helper.hpp"
#include <string>

#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/function/table/table_scan.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"

using namespace std;

namespace arrowir = org::apache::arrow::computeir::flatbuf;
namespace arrow = org::apache::arrow::flatbuf;

static flatbuffers::Offset<arrowir::Relation> transform_op(flatbuffers::FlatBufferBuilder& fbb, duckdb::LogicalOperator& op) {
	flatbuffers::Offset<void> res;

	// TODO this is still a bit odd?
	arrowir::PassThroughBuilder passthrough_builder(fbb);
	auto passthrough = passthrough_builder.Finish().Union();

	arrowir::RelBaseBuilder rel_base_builder(fbb);
	rel_base_builder.add_output_mapping(passthrough);
	auto rel_base = rel_base_builder.Finish();


	switch(op.type) {
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

		std::vector<flatbuffers::Offset<arrowir::Expression>> expressions_vec;

		// TODO actually transform expressions
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

		std::vector<flatbuffers::Offset<arrowir::Grouping>> groups_vec;

		// TODO actually transform expressions
		auto groups_vec_fb = fbb.CreateVector(groups_vec);

		std::vector<flatbuffers::Offset<arrowir::Expression>> aggregates_vec;

		// TODO actually transform expressions
		auto aggregates_vec_fb = fbb.CreateVector(aggregates_vec);

		arrowir::AggregateBuilder aggregate_builder(fbb);
		aggregate_builder.add_base(rel_base);
		aggregate_builder.add_groupings(groups_vec_fb);
		aggregate_builder.add_measures(aggregates_vec_fb);

		aggregate_builder.add_rel(child_rel);

		res = aggregate_builder.Finish().Union();
		break;
	}
	case duckdb::LogicalOperatorType::LOGICAL_GET: {
		auto& get = (duckdb::LogicalGet&) op;

		auto& table_scan_bind_data = (duckdb::TableScanBindData&) *get.bind_data;
		auto table_name = fbb.CreateString(table_scan_bind_data.table->name);
		arrow::SchemaBuilder schema_builder(fbb);

		// TODO actually create schema
		//schema_builder.add_fields();

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

