#include "duckdb/catalog/catalog.hpp"

#include "duckdb/catalog/catalog_entry/list.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/parsed_data/alter_table_info.hpp"
#include "duckdb/parser/parsed_data/create_index_info.hpp"
#include "duckdb/parser/parsed_data/create_aggregate_function_info.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/parser/parsed_data/create_sequence_info.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/parser/parsed_data/create_view_info.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"
#include "duckdb/storage/storage_manager.hpp"

using namespace duckdb;
using namespace std;

Catalog::Catalog(StorageManager &storage) : storage(storage), schemas(*this), dependency_manager(*this) {
}

void Catalog::CreateSchema(Transaction &transaction, CreateSchemaInfo *info) {
	if (info->schema == INVALID_SCHEMA) {
		throw CatalogException("Schema not specified");
	}
	if (info->schema == TEMP_SCHEMA) {
		throw CatalogException("Cannot create built-in schema \"%s\"", info->schema.c_str());
	}

	unordered_set<CatalogEntry *> dependencies;
	auto entry = make_unique_base<CatalogEntry, SchemaCatalogEntry>(this, info->schema);
	if (!schemas.CreateEntry(transaction, info->schema, move(entry), dependencies)) {
		if (!info->if_not_exists) {
			throw CatalogException("Schema with name %s already exists!", info->schema.c_str());
		}
	}
}

void Catalog::DropSchema(Transaction &transaction, DropInfo *info) {
	if (info->name == INVALID_SCHEMA) {
		throw CatalogException("Schema not specified");
	}
	if (info->name == DEFAULT_SCHEMA || info->name == TEMP_SCHEMA) {
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
	if (schema_name == INVALID_SCHEMA) {
		throw CatalogException("Schema not specified");
	}
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

TableCatalogEntry *Catalog::GetTable(ClientContext &context, const string &schema_name, const string &table_name) {
	auto table = GetTableOrView(context, schema_name, table_name);
	if (table->type != CatalogType::TABLE) {
		throw CatalogException("%s is not a table", table_name.c_str());
	}
	return (TableCatalogEntry *)table;
}

CatalogEntry *Catalog::GetTableOrView(ClientContext &context, string schema_name, const string &table_name) {
	if (schema_name == INVALID_SCHEMA) {
		// invalid schema: search both the temporary objects and the normal catalog
		auto temp_result = context.temporary_objects->GetTableOrNull(context.ActiveTransaction(), table_name);
		if (temp_result) {
			return temp_result;
		}
		schema_name = DEFAULT_SCHEMA;
	} else if (schema_name == TEMP_SCHEMA) {
		// temp_schema: search only the temporary objects
		return context.temporary_objects->GetTable(context.ActiveTransaction(), table_name);
	}
	// default case: first search the schema and then find the table in the schema
	auto schema = GetSchema(context.ActiveTransaction(), schema_name);
	return schema->GetTableOrView(context.ActiveTransaction(), table_name);
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

void Catalog::CreateFunction(Transaction &transaction, CreateFunctionInfo *info) {
	auto schema = GetSchema(transaction, info->schema);
	schema->CreateFunction(transaction, info);
}

CatalogEntry *Catalog::GetFunction(Transaction &transaction, const string &schema_name, const string &name,
                                   bool if_exists) {
	auto schema = GetSchema(transaction, schema_name);
	return schema->GetFunction(transaction, name, if_exists);
}

void Catalog::DropIndex(Transaction &transaction, DropInfo *info) {
	auto schema = GetSchema(transaction, info->schema);
	schema->DropIndex(transaction, info);
}

void Catalog::ParseRangeVar(string input, string &schema, string &name) {
	idx_t idx = 0;
	vector<string> entries;
	string entry;
normal:
	// quote
	for(; idx < input.size(); idx++) {
		if (input[idx] == '"') {
			idx++;
			goto quoted;
		} else if (input[idx] == '.') {
			goto separator;
		}
		entry += input[idx];
	}
	goto end;
separator:
	entries.push_back(entry);
	entry = "";
	idx++;
	goto normal;
quoted:
	// look for another quote
	for(; idx < input.size(); idx++) {
		if (input[idx] == '"') {
			// unquote
			idx++;
			goto normal;
		}
		entry += input[idx];
	}
	throw ParserException("Unterminated quote in range var!");
end:
	if (entries.size() == 0) {
		schema = DEFAULT_SCHEMA;
		name = entry;
	} else if (entries.size() == 1) {
		schema = entries[0];
		name = entry;
	} else {
		throw ParserException("Expected schema.entry or entry: too many entries found");
	}
}
