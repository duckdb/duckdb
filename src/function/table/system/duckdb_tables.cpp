#include "duckdb/function/table/system_functions.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/client_data.hpp"
#include "duckdb/parser/constraint.hpp"
#include "duckdb/parser/constraints/unique_constraint.hpp"
#include "duckdb/storage/data_table.hpp"

namespace duckdb {

struct DuckDBTablesData : public GlobalTableFunctionState {
	DuckDBTablesData() : offset(0) {
	}

	vector<CatalogEntry *> entries;
	idx_t offset;
};

static unique_ptr<FunctionData> DuckDBTablesBind(ClientContext &context, TableFunctionBindInput &input,
                                                 vector<LogicalType> &return_types, vector<string> &names) {
	names.emplace_back("schema_name");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("schema_oid");
	return_types.emplace_back(LogicalType::BIGINT);

	names.emplace_back("table_name");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("table_oid");
	return_types.emplace_back(LogicalType::BIGINT);

	names.emplace_back("internal");
	return_types.emplace_back(LogicalType::BOOLEAN);

	names.emplace_back("temporary");
	return_types.emplace_back(LogicalType::BOOLEAN);

	names.emplace_back("has_primary_key");
	return_types.emplace_back(LogicalType::BOOLEAN);

	names.emplace_back("estimated_size");
	return_types.emplace_back(LogicalType::BIGINT);

	names.emplace_back("column_count");
	return_types.emplace_back(LogicalType::BIGINT);

	names.emplace_back("index_count");
	return_types.emplace_back(LogicalType::BIGINT);

	names.emplace_back("check_constraint_count");
	return_types.emplace_back(LogicalType::BIGINT);

	names.emplace_back("sql");
	return_types.emplace_back(LogicalType::VARCHAR);

	return nullptr;
}

unique_ptr<GlobalTableFunctionState> DuckDBTablesInit(ClientContext &context, TableFunctionInitInput &input) {
	auto result = make_unique<DuckDBTablesData>();

	// scan all the schemas for tables and collect themand collect them
	auto schemas = Catalog::GetCatalog(context).schemas->GetEntries<SchemaCatalogEntry>(context);
	for (auto &schema : schemas) {
		schema->Scan(context, CatalogType::TABLE_ENTRY, [&](CatalogEntry *entry) { result->entries.push_back(entry); });
	};

	// check the temp schema as well
	ClientData::Get(context).temporary_objects->Scan(context, CatalogType::TABLE_ENTRY,
	                                                 [&](CatalogEntry *entry) { result->entries.push_back(entry); });
	return move(result);
}

static bool TableHasPrimaryKey(TableCatalogEntry &table) {
	for (auto &constraint : table.constraints) {
		if (constraint->type == ConstraintType::UNIQUE) {
			auto &unique = (UniqueConstraint &)*constraint;
			if (unique.is_primary_key) {
				return true;
			}
		}
	}
	return false;
}

static idx_t CheckConstraintCount(TableCatalogEntry &table) {
	idx_t check_count = 0;
	for (auto &constraint : table.constraints) {
		if (constraint->type == ConstraintType::CHECK) {
			check_count++;
		}
	}
	return check_count;
}

void DuckDBTablesFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = (DuckDBTablesData &)*data_p.global_state;
	if (data.offset >= data.entries.size()) {
		// finished returning values
		return;
	}
	// start returning values
	// either fill up the chunk or return all the remaining columns
	idx_t count = 0;
	while (data.offset < data.entries.size() && count < STANDARD_VECTOR_SIZE) {
		auto &entry = data.entries[data.offset++];

		if (entry->type != CatalogType::TABLE_ENTRY) {
			continue;
		}
		auto &table = (TableCatalogEntry &)*entry;
		// return values:
		// schema_name, LogicalType::VARCHAR
		output.SetValue(0, count, Value(table.schema->name));
		// schema_oid, LogicalType::BIGINT
		output.SetValue(1, count, Value::BIGINT(table.schema->oid));
		// table_name, LogicalType::VARCHAR
		output.SetValue(2, count, Value(table.name));
		// table_oid, LogicalType::BIGINT
		output.SetValue(3, count, Value::BIGINT(table.oid));
		// internal, LogicalType::BOOLEAN
		output.SetValue(4, count, Value::BOOLEAN(table.internal));
		// temporary, LogicalType::BOOLEAN
		output.SetValue(5, count, Value::BOOLEAN(table.temporary));
		// has_primary_key, LogicalType::BOOLEAN
		output.SetValue(6, count, Value::BOOLEAN(TableHasPrimaryKey(table)));
		// estimated_size, LogicalType::BIGINT
		output.SetValue(7, count, Value::BIGINT(table.storage->info->cardinality.load()));
		// column_count, LogicalType::BIGINT
		output.SetValue(8, count, Value::BIGINT(table.columns.size()));
		// index_count, LogicalType::BIGINT
		output.SetValue(9, count, Value::BIGINT(table.storage->info->indexes.Count()));
		// check_constraint_count, LogicalType::BIGINT
		output.SetValue(10, count, Value::BIGINT(CheckConstraintCount(table)));
		// sql, LogicalType::VARCHAR
		output.SetValue(11, count, Value(table.ToSQL()));

		count++;
	}
	output.SetCardinality(count);
}

void DuckDBTablesFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(TableFunction("duckdb_tables", {}, DuckDBTablesFunction, DuckDBTablesBind, DuckDBTablesInit));
}

} // namespace duckdb
