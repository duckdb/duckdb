#include "duckdb/parser/statement/create_statement.hpp"
#include "duckdb/planner/operator/logical_create.hpp"
#include "duckdb/planner/operator/logical_create_table.hpp"
#include "duckdb/planner/operator/logical_create_index.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression_binder/index_binder.hpp"
#include "duckdb/parser/parsed_data/create_view_info.hpp"
#include "duckdb/parser/parsed_data/create_index_info.hpp"
#include "duckdb/planner/bound_query_node.hpp"
#include "duckdb/planner/tableref/bound_basetableref.hpp"

using namespace duckdb;
using namespace std;

SchemaCatalogEntry *Binder::BindSchema(CreateInfo &info) {
	if (info.schema == INVALID_SCHEMA) {
		info.schema = info.temporary ? TEMP_SCHEMA : DEFAULT_SCHEMA;
	}

	if (!info.temporary) {
		// non-temporary create: not read only
		if (info.schema == TEMP_SCHEMA) {
			throw ParserException("Only TEMPORARY table names can use the \"temp\" schema");
		}
		this->read_only = false;
	} else {
		if (info.schema != TEMP_SCHEMA) {
			throw ParserException("TEMPORARY table names can *only* use the \"%s\" schema", TEMP_SCHEMA);
		}
	}
	// fetch the schema in which we want to create the object
	auto schema_obj = Catalog::GetCatalog(context).GetSchema(context, info.schema);
	assert(schema_obj->type == CatalogType::SCHEMA);
	info.schema = schema_obj->name;
	return schema_obj;
}

BoundStatement Binder::Bind(CreateStatement &stmt) {
	BoundStatement result;
	result.names = {"Count"};
	result.types = {SQLType::BIGINT};

	auto catalog_type = stmt.info->type;
	switch (catalog_type) {
	case CatalogType::SCHEMA:
		result.plan = make_unique<LogicalCreate>(LogicalOperatorType::CREATE_SCHEMA, move(stmt.info));
		break;
	case CatalogType::VIEW: {
		auto &base = (CreateViewInfo &)*stmt.info;
		// bind the schema
		auto schema = BindSchema(*stmt.info);

		// bind the view as if it were a query so we can catch errors
		// note that we bind a copy and don't actually use the bind result
		auto copy = base.query->Copy();
		auto query_node = Bind(*copy);
		if (base.aliases.size() > query_node.names.size()) {
			throw BinderException("More VIEW aliases than columns in query result");
		}
		// fill up the aliases with the remaining names of the bound query
		for (idx_t i = base.aliases.size(); i < query_node.names.size(); i++) {
			base.aliases.push_back(query_node.names[i]);
		}
		base.types = query_node.types;
		result.plan = make_unique<LogicalCreate>(LogicalOperatorType::CREATE_VIEW, move(stmt.info), schema);
		break;
	}
	case CatalogType::SEQUENCE: {
		auto schema = BindSchema(*stmt.info);
		result.plan = make_unique<LogicalCreate>(LogicalOperatorType::CREATE_SEQUENCE, move(stmt.info), schema);
		break;
	}
	case CatalogType::INDEX: {
		auto &base = (CreateIndexInfo &)*stmt.info;

		// visit the table reference
		auto bound_table = Bind(*base.table);
		if (bound_table->type != TableReferenceType::BASE_TABLE) {
			throw BinderException("Can only delete from base table!");
		}
		// bind the index expressions
		vector<unique_ptr<Expression>> expressions;
		IndexBinder binder(*this, context);
		for (auto &expr : base.expressions) {
			expressions.push_back(binder.Bind(expr));
		}

		auto plan = CreatePlan(*bound_table);
		if (plan->type != LogicalOperatorType::GET) {
			throw BinderException("Cannot create index on a view!");
		}
		auto &get = (LogicalGet &)*plan;
		for (auto &column_id : get.column_ids) {
			if (column_id == COLUMN_IDENTIFIER_ROW_ID) {
				throw BinderException("Cannot create an index on the rowid!");
			}
		}
		// this gives us a logical table scan
		// we take the required columns from here
		// create the logical operator
		result.plan = make_unique<LogicalCreateIndex>(*get.table, get.column_ids, move(expressions),
		                                              unique_ptr_cast<CreateInfo, CreateIndexInfo>(move(stmt.info)));
		break;
	}
	case CatalogType::TABLE: {
		auto bound_info = BindCreateTableInfo(move(stmt.info));
		auto root = move(bound_info->query);

		// create the logical operator
		auto create_table = make_unique<LogicalCreateTable>(bound_info->schema, move(bound_info));
		if (root) {
			create_table->children.push_back(move(root));
		}
		result.plan = move(create_table);
		return result;
	}
	default:
		throw Exception("Unrecognized type!");
	}
	return result;
}
