#include "duckdb/catalog/catalog.hpp"

#include "duckdb/catalog/catalog_entry/list.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/parsed_data/alter_table_info.hpp"
#include "duckdb/parser/parsed_data/create_index_info.hpp"
#include "duckdb/parser/parsed_data/create_aggregate_function_info.hpp"
#include "duckdb/parser/parsed_data/create_collation_info.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/parser/parsed_data/create_sequence_info.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/parser/parsed_data/create_view_info.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"
#include "duckdb/storage/storage_manager.hpp"
#include "duckdb/main/database.hpp"

using namespace duckdb;
using namespace std;

Catalog::Catalog(StorageManager &storage) : storage(storage), schemas(*this), dependency_manager(*this) {
}

Catalog &Catalog::GetCatalog(ClientContext &context) {
	return context.catalog;
}

CatalogEntry *Catalog::CreateTable(ClientContext &context, BoundCreateTableInfo *info) {
	auto schema = GetSchema(context, info->base->schema);
	return schema->CreateTable(context, info);
}

CatalogEntry *Catalog::CreateView(ClientContext &context, CreateViewInfo *info) {
	auto schema = GetSchema(context, info->schema);
	return schema->CreateView(context, info);
}

CatalogEntry *Catalog::CreateSequence(ClientContext &context, CreateSequenceInfo *info) {
	auto schema = GetSchema(context, info->schema);
	return schema->CreateSequence(context, info);
}

CatalogEntry *Catalog::CreateTableFunction(ClientContext &context, CreateTableFunctionInfo *info) {
	auto schema = GetSchema(context, info->schema);
	return schema->CreateTableFunction(context, info);
}

CatalogEntry *Catalog::CreateFunction(ClientContext &context, CreateFunctionInfo *info) {
	auto schema = GetSchema(context, info->schema);
	return schema->CreateFunction(context, info);
}

CatalogEntry *Catalog::CreateCollation(ClientContext &context, CreateCollationInfo *info) {
	auto schema = GetSchema(context, info->schema);
	return schema->CreateCollation(context, info);
}

CatalogEntry *Catalog::CreateSchema(ClientContext &context, CreateSchemaInfo *info) {
	if (info->schema == INVALID_SCHEMA) {
		throw CatalogException("Schema not specified");
	}
	if (info->schema == TEMP_SCHEMA) {
		throw CatalogException("Cannot create built-in schema \"%s\"", info->schema.c_str());
	}

	unordered_set<CatalogEntry *> dependencies;
	auto entry = make_unique<SchemaCatalogEntry>(this, info->schema);
	auto result = entry.get();
	if (!schemas.CreateEntry(context.ActiveTransaction(), info->schema, move(entry), dependencies)) {
		if (info->on_conflict == OnCreateConflict::ERROR) {
			throw CatalogException("Schema with name %s already exists!", info->schema.c_str());
		} else {
			assert(info->on_conflict == OnCreateConflict::IGNORE);
		}
		return nullptr;
	}
	return result;
}

void Catalog::DropSchema(ClientContext &context, DropInfo *info) {
	if (info->name == INVALID_SCHEMA) {
		throw CatalogException("Schema not specified");
	}
	if (info->name == DEFAULT_SCHEMA || info->name == TEMP_SCHEMA) {
		throw CatalogException("Cannot drop schema \"%s\" because it is required by the database system",
		                       info->name.c_str());
	}

	if (!schemas.DropEntry(context.ActiveTransaction(), info->name, info->cascade)) {
		if (!info->if_exists) {
			throw CatalogException("Schema with name \"%s\" does not exist!", info->name.c_str());
		}
	}
}

void Catalog::DropEntry(ClientContext &context, DropInfo *info) {
	if (info->type == CatalogType::SCHEMA) {
		// DROP SCHEMA
		DropSchema(context, info);
	} else {
		if (info->schema == INVALID_SCHEMA) {
			// invalid schema: check if the entry is in the temp schema
			auto entry = GetEntry(context, info->type, TEMP_SCHEMA, info->name, true);
			info->schema = entry ? TEMP_SCHEMA : DEFAULT_SCHEMA;
		}
		auto schema = GetSchema(context, info->schema);
		schema->DropEntry(context, info);
	}
}

SchemaCatalogEntry *Catalog::GetSchema(ClientContext &context, const string &schema_name) {
	if (schema_name == INVALID_SCHEMA) {
		throw CatalogException("Schema not specified");
	}
	if (schema_name == TEMP_SCHEMA) {
		return context.temporary_objects.get();
	}
	auto entry = schemas.GetEntry(context.ActiveTransaction(), schema_name);
	if (!entry) {
		throw CatalogException("Schema with name %s does not exist!", schema_name.c_str());
	}
	return (SchemaCatalogEntry *)entry;
}

CatalogEntry *Catalog::GetEntry(ClientContext &context, CatalogType type, string schema_name, const string &name,
                                bool if_exists) {
	if (schema_name == INVALID_SCHEMA) {
		// invalid schema: first search the temporary schema
		auto entry = GetEntry(context, type, TEMP_SCHEMA, name, true);
		if (entry) {
			return entry;
		}
		// if the entry does not exist in the temp schema, search in the default schema
		schema_name = DEFAULT_SCHEMA;
	}
	auto schema = GetSchema(context, schema_name);
	return schema->GetEntry(context, type, name, if_exists);
}

template <>
TableCatalogEntry *Catalog::GetEntry(ClientContext &context, string schema_name, const string &name, bool if_exists) {
	auto entry = GetEntry(context, CatalogType::TABLE, move(schema_name), name, if_exists);
	if (!entry) {
		return nullptr;
	}
	if (entry->type != CatalogType::TABLE) {
		throw CatalogException("%s is not a table", name.c_str());
	}
	return (TableCatalogEntry *)entry;
}

template <>
SequenceCatalogEntry *Catalog::GetEntry(ClientContext &context, string schema_name, const string &name,
                                        bool if_exists) {
	return (SequenceCatalogEntry *)GetEntry(context, CatalogType::SEQUENCE, move(schema_name), name, if_exists);
}

template <>
TableFunctionCatalogEntry *Catalog::GetEntry(ClientContext &context, string schema_name, const string &name,
                                             bool if_exists) {
	return (TableFunctionCatalogEntry *)GetEntry(context, CatalogType::TABLE_FUNCTION, move(schema_name), name,
	                                             if_exists);
}

template <>
AggregateFunctionCatalogEntry *Catalog::GetEntry(ClientContext &context, string schema_name, const string &name,
                                                 bool if_exists) {
	auto entry = GetEntry(context, CatalogType::AGGREGATE_FUNCTION, move(schema_name), name, if_exists);
	if (entry->type != CatalogType::AGGREGATE_FUNCTION) {
		throw CatalogException("%s is not an aggregate function", name.c_str());
	}
	return (AggregateFunctionCatalogEntry *)entry;
}

template <>
CollateCatalogEntry *Catalog::GetEntry(ClientContext &context, string schema_name, const string &name, bool if_exists) {
	return (CollateCatalogEntry *)GetEntry(context, CatalogType::COLLATION, move(schema_name), name, if_exists);
}

void Catalog::AlterTable(ClientContext &context, AlterTableInfo *info) {
	if (info->schema == INVALID_SCHEMA) {
		// invalid schema, look for table in temp schema
		auto entry = GetEntry(context, CatalogType::TABLE, TEMP_SCHEMA, info->table, true);
		info->schema = entry ? TEMP_SCHEMA : DEFAULT_SCHEMA;
	}
	auto schema = GetSchema(context, info->schema);
	schema->AlterTable(context, info);
}

void Catalog::ParseRangeVar(string input, string &schema, string &name) {
	idx_t idx = 0;
	vector<string> entries;
	string entry;
normal:
	// quote
	for (; idx < input.size(); idx++) {
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
	for (; idx < input.size(); idx++) {
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
		schema = INVALID_SCHEMA;
		name = entry;
	} else if (entries.size() == 1) {
		schema = entries[0];
		name = entry;
	} else {
		throw ParserException("Expected schema.entry or entry: too many entries found");
	}
}
