#include "duckdb/planner/logical_plan_generator.hpp"
#include "duckdb/planner/operator/logical_create_index.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/statement/bound_create_statement.hpp"
#include "duckdb/planner/operator/logical_create.hpp"
#include "duckdb/planner/operator/logical_create_table.hpp"
#include "duckdb/planner/operator/logical_create_index.hpp"
#include "duckdb/planner/parsed_data/bound_create_index_info.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"

using namespace duckdb;
using namespace std;

unique_ptr<LogicalOperator> LogicalPlanGenerator::CreatePlan(BoundCreateStatement &stmt) {
	switch (stmt.info->base->type) {
	case CatalogType::SCHEMA:
		return make_unique<LogicalCreate>(LogicalOperatorType::CREATE_SCHEMA, move(stmt.info));
	case CatalogType::VIEW:
		return make_unique<LogicalCreate>(LogicalOperatorType::CREATE_VIEW, move(stmt.info));
	case CatalogType::SEQUENCE:
		return make_unique<LogicalCreate>(LogicalOperatorType::CREATE_SEQUENCE, move(stmt.info));
	case CatalogType::INDEX: {
		auto &info = (BoundCreateIndexInfo &)*stmt.info;
		// first we visit the base table to create the root expression
		auto root = CreatePlan(*info.table);
		// this gives us a logical table scan
		// we take the required columns from here
		assert(root->type == LogicalOperatorType::GET);
		auto &get = (LogicalGet &)*root;
		// create the logical operator
		return make_unique<LogicalCreateIndex>(*get.table, get.column_ids, move(info.expressions),
		                                       unique_ptr_cast<CreateInfo, CreateIndexInfo>(move(info.base)));
	}
	case CatalogType::TABLE: {
		auto &info = (BoundCreateTableInfo &)*stmt.info;
		unique_ptr<LogicalOperator> root;
		if (info.query) {
			// create table from query
			root = CreatePlan(*info.query);
		}
		// create the logical operator
		auto create_table = make_unique<LogicalCreateTable>(
		    info.schema, unique_ptr_cast<BoundCreateInfo, BoundCreateTableInfo>(move(stmt.info)));
		if (root) {
			create_table->children.push_back(move(root));
		}
		return move(create_table);
	}
	default:
		throw Exception("Unrecognized type!");
	}
}
