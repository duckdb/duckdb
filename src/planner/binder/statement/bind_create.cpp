#include "duckdb/parser/statement/create_statement.hpp"
#include "duckdb/planner/operator/logical_create.hpp"
#include "duckdb/planner/operator/logical_create_table.hpp"
#include "duckdb/planner/operator/logical_create_index.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/parsed_data/bound_create_index_info.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/planner/binder.hpp"

using namespace duckdb;
using namespace std;

BoundStatement Binder::Bind(CreateStatement &stmt) {
	BoundStatement result;

	auto bound_info = BindCreateInfo(move(stmt.info));
	switch (stmt.info->type) {
	case CatalogType::SCHEMA:
		result.plan = make_unique<LogicalCreate>(LogicalOperatorType::CREATE_SCHEMA, move(bound_info));
		break;
	case CatalogType::VIEW:
		result.plan = make_unique<LogicalCreate>(LogicalOperatorType::CREATE_VIEW, move(bound_info));
		break;
	case CatalogType::SEQUENCE:
		result.plan = make_unique<LogicalCreate>(LogicalOperatorType::CREATE_SEQUENCE, move(bound_info));
		break;
	case CatalogType::INDEX: {
		auto &info = (BoundCreateIndexInfo &)*bound_info;
		// first we visit the base table to create the root expression
		auto root = Bind(*info.table);
		// this gives us a logical table scan
		// we take the required columns from here
		assert(root->type == LogicalOperatorType::GET);
		auto &get = (LogicalGet &)*root;
		// create the logical operator
		result.plan = make_unique<LogicalCreateIndex>(*get.table, get.column_ids, move(info.expressions),
		                                       unique_ptr_cast<CreateInfo, CreateIndexInfo>(move(info.base)));
		break;
	}
	case CatalogType::TABLE: {
		auto &info = (BoundCreateTableInfo &)*bound_info;
		auto root = move(info.query);

		// create the logical operator
		auto create_table = make_unique<LogicalCreateTable>(
		    info.schema, unique_ptr_cast<BoundCreateInfo, BoundCreateTableInfo>(move(bound_info)));
		if (root) {
			create_table->children.push_back(move(root));
		}
		result.plan = move(create_table);
		break;
	}
	default:
		throw Exception("Unrecognized type!");
	}
	return result;
}

unique_ptr<BoundCreateInfo> Binder::BindCreateInfo(unique_ptr<CreateInfo> info) {
	unique_ptr<BoundCreateInfo> result;
	if (info->schema == INVALID_SCHEMA) {
		info->schema = info->temporary ? TEMP_SCHEMA : DEFAULT_SCHEMA;
	}

	SchemaCatalogEntry *bound_schema = nullptr;
	if (!info->temporary) {
		// non-temporary create: not read only
		if (info->schema == TEMP_SCHEMA) {
			throw ParserException("Only TEMPORARY table names can use the \"temp\" schema");
		}
		this->read_only = false;
	} else {
		if (info->schema != TEMP_SCHEMA) {
			throw ParserException("TEMPORARY table names can *only* use the \"%s\" schema", TEMP_SCHEMA);
		}
	}
	if (info->type != CatalogType::SCHEMA) {
		// fetch the schema in which we want to create the object
		bound_schema = Catalog::GetCatalog(context).GetSchema(context, info->schema);
		info->schema = bound_schema->name;
	}
	switch (info->type) {
	case CatalogType::INDEX:
		result = BindCreateIndexInfo(move(info));
		break;
	case CatalogType::TABLE:
		result = BindCreateTableInfo(move(info));
		break;
	case CatalogType::VIEW:
		result = BindCreateViewInfo(move(info));
		break;
	default:
		result = make_unique<BoundCreateInfo>(move(info));
		break;
	}
	result->schema = bound_schema;
	return result;
}
