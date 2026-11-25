#include "duckdb/function/table/system_functions.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/storage/storage_manager.hpp"

namespace duckdb {

struct DuckDBDatabasesData : public GlobalTableFunctionState {
	DuckDBDatabasesData() : offset(0) {
	}

	//! Rows are materialized at init so that no database reference outlives it
	vector<vector<Value>> rows;
	idx_t offset;
};

static unique_ptr<FunctionData> DuckDBDatabasesBind(ClientContext &context, TableFunctionBindInput &input,
                                                    vector<LogicalType> &return_types, vector<Identifier> &names) {
	names.emplace_back("database_name");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("database_oid");
	return_types.emplace_back(LogicalType::BIGINT);

	names.emplace_back("path");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("comment");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("tags");
	return_types.emplace_back(LogicalType::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR));

	names.emplace_back("internal");
	return_types.emplace_back(LogicalType::BOOLEAN);

	names.emplace_back("type");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("readonly");
	return_types.emplace_back(LogicalType::BOOLEAN);

	names.emplace_back("encrypted");
	return_types.emplace_back(LogicalType::BOOLEAN);

	names.emplace_back("cipher");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("options");
	return_types.emplace_back(LogicalType::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR));
	return nullptr;
}

static vector<Value> DuckDBDatabasesRow(AttachedDatabase &attached) {
	auto &catalog = attached.GetCatalog();
	bool is_internal = attached.IsSystem() || attached.IsTemporary();
	string cipher_str;
	Value db_path;
	if (!is_internal) {
		bool in_memory = catalog.InMemory();
		if (!in_memory) {
			db_path = Value(catalog.GetDBPath());
		}
		if (catalog.IsEncrypted()) {
			cipher_str = catalog.GetEncryptionCipher();
		}
	}
	InsertionOrderPreservingMap<string> options_map;
	for (const auto &option : attached.GetAttachOptions()) {
		options_map.insert(option.first, option.second.ToString());
	}
	vector<Value> row;
	// database_name, VARCHAR
	row.push_back(Value(attached.GetName()));
	// database_oid, BIGINT
	row.push_back(Value::BIGINT(NumericCast<int64_t>(attached.oid)));
	// path, VARCHAR
	row.push_back(std::move(db_path));
	// comment, VARCHAR
	row.push_back(Value(attached.comment));
	// tags, MAP
	row.push_back(Value::MAP(attached.tags));
	// internal, BOOLEAN
	row.push_back(Value::BOOLEAN(is_internal));
	// type, VARCHAR
	row.push_back(Value(catalog.GetCatalogType()));
	// readonly, BOOLEAN
	row.push_back(Value::BOOLEAN(attached.IsReadOnly()));
	// encrypted, BOOLEAN
	row.push_back(Value::BOOLEAN(catalog.IsEncrypted()));
	// cipher, VARCHAR
	row.push_back(cipher_str.empty() ? Value() : Value(cipher_str));
	// options, MAP
	row.push_back(Value::MAP(options_map));
	return row;
}

unique_ptr<GlobalTableFunctionState> DuckDBDatabasesInit(ClientContext &context, TableFunctionInitInput &input) {
	auto result = make_uniq<DuckDBDatabasesData>();

	auto &db_manager = DatabaseManager::Get(context);
	for (auto &entry : db_manager.GetDatabases(context)) {
		if (entry->GetVisibility() != AttachVisibility::HIDDEN) {
			result->rows.push_back(DuckDBDatabasesRow(*entry));
		}
		// a concurrent DETACH can have left this as the last reference
		AttachedDatabase::InvokeCloseIfLastReference(entry, context);
	}
	return std::move(result);
}

void DuckDBDatabasesFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.global_state->Cast<DuckDBDatabasesData>();
	idx_t count = 0;
	while (data.offset < data.rows.size() && count < STANDARD_VECTOR_SIZE) {
		auto &row = data.rows[data.offset++];
		for (idx_t col = 0; col < row.size(); col++) {
			output.data[col].Append(row[col]);
		}
		count++;
	}
}

void DuckDBDatabasesFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(
	    TableFunction("duckdb_databases", {}, DuckDBDatabasesFunction, DuckDBDatabasesBind, DuckDBDatabasesInit));
}

} // namespace duckdb
