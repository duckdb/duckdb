#include "duckdb/common/types.hpp"
#include "duckdb/function/table/range.hpp"
#include "duckdb/function/function_set.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/storage/wal_reader.hpp"
#include "duckdb/main/database_manager.hpp"
//#include "json_serializer.hpp"

namespace duckdb {

// The bind data holding references needed during execution
struct ReadWalBindData : public TableFunctionData {
	//! The database instance
	AttachedDatabase &db;
	//! The path to the WAL file
	string wal_path;
	explicit ReadWalBindData(AttachedDatabase &db) : db(db) {
	}

	static constexpr const idx_t RECORD_TYPE_COLUMN = 0;
	static constexpr const idx_t RECORD_SIZE_COLUMN = 1;
	static constexpr const idx_t RECORD_INFO_COLUMN = 2;
	static constexpr const idx_t RECORD_DATA_COLUMN = 3;
};

static unique_ptr<FunctionData> ReadWalBind(ClientContext &context, TableFunctionBindInput &input,
                                            vector<LogicalType> &return_types, vector<string> &names) {
	auto &db_manager = DatabaseManager::Get(context);
	auto databases = db_manager.GetDatabases(context);
	auto db_paths = db_manager.GetAttachedDatabasePaths();
	auto &db = databases[0].get();
	auto result = make_uniq<ReadWalBindData>(db);

	// Check if a WAL file path was provided as a named parameter
	if (input.named_parameters.count("wal_file") > 0 && !input.named_parameters["wal_file"].IsNull()) {
		result->wal_path = input.named_parameters["wal_file"].GetValue<string>();
	} else {
		// Default to the first database's WAL file
		result->wal_path = db_paths[0] + ".wal";
	}

	// Return schema
	names.push_back("record_type");
	return_types.push_back(LogicalType::VARCHAR);
	names.push_back("record_size");
	return_types.push_back(LogicalType::UBIGINT);
	names.push_back("record_info");
	return_types.push_back(LogicalType::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR));
	names.push_back("record_data");
	return_types.push_back(LogicalType::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR));

	return std::move(result);
}

static void ReadWalOperation(ClientContext &context, TableFunctionInput &input, DataChunk &output) {
	auto &bind_data = input.bind_data->Cast<ReadWalBindData>();

	// Get a file handle to the WAL file
	auto file_system = FileSystem::CreateLocal();
	auto handle = file_system->OpenFile(bind_data.wal_path, FileFlags::FILE_FLAGS_READ);

	// Create a WAL reader
	auto file_reader = make_uniq<BufferedFileReader>(FileSystem::Get(bind_data.db), std::move(handle));
	WALReader reader(bind_data.db, std::move(file_reader));

	// Vectors to store the results
	auto &entry_type_vec = output.data[ReadWalBindData::RECORD_TYPE_COLUMN];
	auto &entry_size_vec = output.data[ReadWalBindData::RECORD_SIZE_COLUMN];
	auto &entry_info_vec = output.data[ReadWalBindData::RECORD_INFO_COLUMN];
	auto &entry_data_vec = output.data[ReadWalBindData::RECORD_DATA_COLUMN];

	// Read entries until we fill up a chunk or reach EOF
	idx_t count = 0;
	while (count < STANDARD_VECTOR_SIZE) {
		// Try to read the next entry
		auto entry = reader.Next();
		if (!entry) {
			break;
		}

		// Create the JSON document for this entry
		// auto doc = yyjson_mut_doc_new(nullptr);

		// Serialize the entry to JSON using JsonSerializer
		// JsonSerializer serializer(doc, false, false, false);
		// entry->Serialize(serializer);

		// Convert the JSON document to a string
		InsertionOrderPreservingMap<string> rec_info; // WriteJsonToString(doc)
		InsertionOrderPreservingMap<string> rec_data; // Placeholder for actual data

		// Get the entry type
		string type_str = EnumUtil::ToChars(entry->info_type);

		// Add the entry type and JSON string to the output vectors
		entry_type_vec.SetValue(count, Value(type_str));
		entry_size_vec.SetValue(count, Value::UBIGINT(entry->size));
		entry_info_vec.SetValue(count, Value::MAP(rec_info));
		entry_data_vec.SetValue(count, Value::MAP(rec_data));
		count++;
	}
	output.SetCardinality(count);
}

void ReadWalFunction::RegisterFunction(BuiltinFunctions &set) {
	TableFunction read_wal("read_wal", {}, ReadWalOperation, ReadWalBind, nullptr);
	read_wal.named_parameters["wal_file"] = LogicalType::VARCHAR;
	set.AddFunction(read_wal);
}

} // namespace duckdb
