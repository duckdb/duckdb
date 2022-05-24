#include "duckdb/common/exception.hpp"
#include "duckdb/execution/operator/schema/physical_create_matview.hpp"
#include "duckdb/parser/parsed_data/create_matview_info.hpp"
#include "duckdb/planner/operator/logical_create_matview.hpp"

#include <fmt/format.h>

namespace duckdb {

unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(LogicalCreateMatView &op) {
	// extract dependencies from any default values
	for (auto &default_value : op.info->bound_defaults) {
		if (default_value) {
			ExtractDependencies(*default_value, op.info->dependencies);
		}
	}
	auto &create_info = (CreateMatViewInfo &)*op.info->base;
	auto &catalog = Catalog::GetCatalog(context);
	auto existing_entry =
	    catalog.GetEntry(context, CatalogType::MATVIEW_ENTRY, create_info.schema, create_info.table, true);
	if (existing_entry) {
		throw Exception(duckdb_fmt::format("{}.{} already exist!", create_info.schema, create_info.table));
	}

	bool replace = (op.info->Base().on_conflict == OnCreateConflict::REPLACE_ON_CONFLICT);
	if ((!existing_entry || replace) && !op.children.empty()) {
		D_ASSERT(op.children.size() == 1);
		auto create = make_unique<PhysicalCreateMatView>(op, op.schema, move(op.info), op.estimated_cardinality);
		auto plan = CreatePlan(*op.children[0]);
		create->children.push_back(move(plan));
		return move(create);
	} else {
		throw Exception("Create Materialized view failed!");
	}
}

} // namespace duckdb
