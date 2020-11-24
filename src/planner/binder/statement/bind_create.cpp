#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/expression/subquery_expression.hpp"
#include "duckdb/parser/parsed_data/create_index_info.hpp"
#include "duckdb/parser/parsed_data/create_macro_info.hpp"
#include "duckdb/parser/parsed_data/create_view_info.hpp"
#include "duckdb/parser/parsed_expression_iterator.hpp"
#include "duckdb/parser/statement/create_statement.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/bound_query_node.hpp"
#include "duckdb/planner/query_node/bound_select_node.hpp"
#include "duckdb/planner/expression_binder/aggregate_binder.hpp"
#include "duckdb/planner/expression_binder/index_binder.hpp"
#include "duckdb/planner/expression_binder/select_binder.hpp"
#include "duckdb/planner/operator/logical_create.hpp"
#include "duckdb/planner/operator/logical_create_index.hpp"
#include "duckdb/planner/operator/logical_create_table.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/parsed_data/bound_create_function_info.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"
#include "duckdb/planner/tableref/bound_basetableref.hpp"

namespace duckdb {
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
	D_ASSERT(schema_obj->type == CatalogType::SCHEMA_ENTRY);
	info.schema = schema_obj->name;
	return schema_obj;
}

void Binder::BindCreateViewInfo(CreateViewInfo &base) {
	// bind the view as if it were a query so we can catch errors
	// note that we bind the original, and replace the original with a copy
	// this is because the original has
	auto copy = base.query->Copy();
	auto query_node = Bind(*base.query);
	base.query = move(copy);
	if (base.aliases.size() > query_node.names.size()) {
		throw BinderException("More VIEW aliases than columns in query result");
	}
	// fill up the aliases with the remaining names of the bound query
	for (idx_t i = base.aliases.size(); i < query_node.names.size(); i++) {
		base.aliases.push_back(query_node.names[i]);
	}
	base.types = query_node.types;
}

SchemaCatalogEntry *Binder::BindCreateFunctionInfo(CreateInfo &info) {
	auto &base = (CreateMacroInfo &)info;

	if (base.function->expression->HasParameter()) {
		throw BinderException("Macro's do not support parameter expressions!");
	}

	// create macro binding in order to bind the function
	vector<LogicalType> dummy_types;
	vector<string> dummy_names;
	for (idx_t i = 0; i < base.function->parameters.size(); i++) {
		if (base.function->parameters[i]->expression_class != ExpressionClass::COLUMN_REF) {
			throw BinderException("Invalid parameter \"%s\"", base.function->parameters[i]->ToString());
		}

		auto param = (ColumnRefExpression &)*base.function->parameters[i];
		if (!param.table_name.empty()) {
			throw BinderException("Invalid parameter \"%s\"", param.ToString());
		}
		dummy_types.push_back(LogicalType::SQLNULL);
		dummy_names.push_back(param.column_name);
	}
	macro_binding = make_shared<MacroBinding>(dummy_types, dummy_names, base.name);

	// create a copy of the expression because we do not want to alter the original
	auto expression = base.function->expression->Copy();

	// bind it to verify the function was defined correctly
	string error;
	auto sel_node = make_unique<BoundSelectNode>();
	auto group_info = make_unique<BoundGroupInformation>();
	SelectBinder binder(*this, context, *sel_node, *group_info);
	error = binder.Bind(&expression, 0, false);

	if (!error.empty()) {
		throw BinderException(error);
	}
	macro_binding.reset();

	return BindSchema(info);
}

BoundStatement Binder::Bind(CreateStatement &stmt) {
	BoundStatement result;
	result.names = {"Count"};
	result.types = {LogicalType::BIGINT};

	auto catalog_type = stmt.info->type;
	switch (catalog_type) {
	case CatalogType::SCHEMA_ENTRY:
		result.plan = make_unique<LogicalCreate>(LogicalOperatorType::LOGICAL_CREATE_SCHEMA, move(stmt.info));
		break;
	case CatalogType::VIEW_ENTRY: {
		auto &base = (CreateViewInfo &)*stmt.info;
		// bind the schema
		auto schema = BindSchema(*stmt.info);

		BindCreateViewInfo(base);
		result.plan = make_unique<LogicalCreate>(LogicalOperatorType::LOGICAL_CREATE_VIEW, move(stmt.info), schema);
		break;
	}
	case CatalogType::SEQUENCE_ENTRY: {
		auto schema = BindSchema(*stmt.info);
		result.plan = make_unique<LogicalCreate>(LogicalOperatorType::LOGICAL_CREATE_SEQUENCE, move(stmt.info), schema);
		break;
	}
	case CatalogType::MACRO_ENTRY: {
		auto schema = BindCreateFunctionInfo(*stmt.info);
		result.plan = make_unique<LogicalCreate>(LogicalOperatorType::LOGICAL_CREATE_MACRO, move(stmt.info), schema);
		break;
	}
	case CatalogType::INDEX_ENTRY: {
		auto &base = (CreateIndexInfo &)*stmt.info;

		// visit the table reference
		auto bound_table = Bind(*base.table);
		if (bound_table->type != TableReferenceType::BASE_TABLE) {
			throw BinderException("Can only delete from base table!");
		}
		auto &table_binding = (BoundBaseTableRef &)*bound_table;
		auto table = table_binding.table;
		// bind the index expressions
		vector<unique_ptr<Expression>> expressions;
		IndexBinder binder(*this, context);
		for (auto &expr : base.expressions) {
			expressions.push_back(binder.Bind(expr));
		}

		auto plan = CreatePlan(*bound_table);
		if (plan->type != LogicalOperatorType::LOGICAL_GET) {
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
		result.plan = make_unique<LogicalCreateIndex>(*table, get.column_ids, move(expressions),
		                                              unique_ptr_cast<CreateInfo, CreateIndexInfo>(move(stmt.info)));
		break;
	}
	case CatalogType::TABLE_ENTRY: {
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

} // namespace duckdb
