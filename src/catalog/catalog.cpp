#include "catalog/catalog.hpp"

#include "catalog/catalog_entry/list.hpp"
#include "common/exception.hpp"
#include "main/client_context.hpp"
#include "parser/expression/function_expression.hpp"
#include "parser/parsed_data/alter_table_info.hpp"
#include "parser/parsed_data/create_index_info.hpp"
#include "parser/parsed_data/create_scalar_function_info.hpp"
#include "parser/parsed_data/create_schema_info.hpp"
#include "parser/parsed_data/create_sequence_info.hpp"
#include "parser/parsed_data/create_table_function_info.hpp"
#include "parser/parsed_data/create_view_info.hpp"
#include "parser/parsed_data/drop_info.hpp"
#include "planner/parsed_data/bound_create_table_info.hpp"
#include "storage/storage_manager.hpp"

using namespace duckdb;
using namespace std;

Catalog::Catalog(StorageManager &storage) : storage(storage), schemas(*this), dependency_manager(*this) {
}

void Catalog::CreateSchema(Transaction &transaction, CreateSchemaInfo *info) {
	unordered_set<CatalogEntry *> dependencies;
	auto entry = make_unique_base<CatalogEntry, SchemaCatalogEntry>(this, info->schema);
	if (!schemas.CreateEntry(transaction, info->schema, move(entry), dependencies)) {
		if (!info->if_not_exists) {
			throw CatalogException("Schema with name %s already exists!", info->schema.c_str());
		}
	}
}

void Catalog::DropSchema(Transaction &transaction, DropInfo *info) {
	if (info->name == DEFAULT_SCHEMA) {
		throw CatalogException("Cannot drop schema \"%s\" because it is required by the database system",
		                       info->name.c_str());
	}
	if (!schemas.DropEntry(transaction, info->name, info->cascade)) {
		if (!info->if_exists) {
			throw CatalogException("Schema with name \"%s\" does not exist!", info->name.c_str());
		}
	}
}

SchemaCatalogEntry *Catalog::GetSchema(Transaction &transaction, const string &schema_name) {
	auto entry = schemas.GetEntry(transaction, schema_name);
	if (!entry) {
		throw CatalogException("Schema with name %s does not exist!", schema_name.c_str());
	}
	return (SchemaCatalogEntry *)entry;
}

void Catalog::CreateTable(Transaction &transaction, BoundCreateTableInfo *info) {
	auto schema = GetSchema(transaction, info->base->schema);
	schema->CreateTable(transaction, info);
}

void Catalog::CreateView(Transaction &transaction, CreateViewInfo *info) {
	auto schema = GetSchema(transaction, info->schema);
	schema->CreateView(transaction, info);
}

void Catalog::DropView(Transaction &transaction, DropInfo *info) {
	auto schema = GetSchema(transaction, info->schema);
	schema->DropView(transaction, info);
}

void Catalog::DropTable(Transaction &transaction, DropInfo *info) {
	auto schema = GetSchema(transaction, info->schema);
	schema->DropTable(transaction, info);
}

void Catalog::CreateSequence(Transaction &transaction, CreateSequenceInfo *info) {
	auto schema = GetSchema(transaction, info->schema);
	schema->CreateSequence(transaction, info);
}

void Catalog::DropSequence(Transaction &transaction, DropInfo *info) {
	auto schema = GetSchema(transaction, info->schema);
	schema->DropSequence(transaction, info);
}

void Catalog::AlterTable(ClientContext &context, AlterTableInfo *info) {
	auto schema = GetSchema(context.ActiveTransaction(), info->schema);
	schema->AlterTable(context, info);
}

TableCatalogEntry *Catalog::GetTable(Transaction &transaction, const string &schema_name, const string &table_name) {
	auto table = GetTableOrView(transaction, schema_name, table_name);
	if (table->type != CatalogType::TABLE) {
		throw CatalogException("%s is not a table", table_name.c_str());
	}
	return (TableCatalogEntry *)table;
}

CatalogEntry *Catalog::GetTableOrView(Transaction &transaction, const string &schema_name, const string &table_name) {
	auto schema = GetSchema(transaction, schema_name);
	return schema->GetTableOrView(transaction, table_name);
}

SequenceCatalogEntry *Catalog::GetSequence(Transaction &transaction, const string &schema_name,
                                           const string &sequence) {
	auto schema = GetSchema(transaction, schema_name);
	return schema->GetSequence(transaction, sequence);
}

void Catalog::CreateTableFunction(Transaction &transaction, CreateTableFunctionInfo *info) {
	auto schema = GetSchema(transaction, info->schema);
	schema->CreateTableFunction(transaction, info);
}

TableFunctionCatalogEntry *Catalog::GetTableFunction(Transaction &transaction, FunctionExpression *expression) {
	auto schema = GetSchema(transaction, expression->schema);
	return schema->GetTableFunction(transaction, expression);
}

void Catalog::CreateScalarFunction(Transaction &transaction, CreateScalarFunctionInfo *info) {
	auto schema = GetSchema(transaction, info->schema);
	schema->CreateScalarFunction(transaction, info);
}

ScalarFunctionCatalogEntry *Catalog::GetScalarFunction(Transaction &transaction, const string &schema_name,
                                                       const string &name) {
	auto schema = GetSchema(transaction, schema_name);
	return schema->GetScalarFunction(transaction, name);
}

void Catalog::DropIndex(Transaction &transaction, DropInfo *info) {
	auto schema = GetSchema(transaction, info->schema);
	schema->DropIndex(transaction, info);
}
