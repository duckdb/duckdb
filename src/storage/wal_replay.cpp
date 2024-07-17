#include "duckdb/catalog/catalog_entry/duck_index_entry.hpp"
#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/catalog/catalog_entry/scalar_macro_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/type_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/view_catalog_entry.hpp"
#include "duckdb/common/checksum.hpp"
#include "duckdb/common/printer.hpp"
#include "duckdb/common/serializer/binary_deserializer.hpp"
#include "duckdb/common/serializer/buffered_file_reader.hpp"
#include "duckdb/common/serializer/memory_stream.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/execution/index/art/art.hpp"
#include "duckdb/execution/index/index_type_set.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/parser/parsed_data/alter_table_info.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/parser/parsed_data/create_view_info.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression_binder/index_binder.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"
#include "duckdb/storage/storage_manager.hpp"
#include "duckdb/storage/table/delete_state.hpp"
#include "duckdb/storage/write_ahead_log.hpp"
#include "duckdb/transaction/meta_transaction.hpp"

namespace duckdb {

class ReplayState {
public:
	ReplayState(AttachedDatabase &db, ClientContext &context) : db(db), context(context), catalog(db.GetCatalog()) {
	}

	AttachedDatabase &db;
	ClientContext &context;
	Catalog &catalog;
	optional_ptr<TableCatalogEntry> current_table;
	MetaBlockPointer checkpoint_id;
	idx_t wal_version = 1;
};

class WriteAheadLogDeserializer {
public:
	WriteAheadLogDeserializer(ReplayState &state_p, BufferedFileReader &stream_p, bool deserialize_only = false)
	    : state(state_p), db(state.db), context(state.context), catalog(state.catalog), data(nullptr),
	      stream(nullptr, 0), deserializer(stream_p), deserialize_only(deserialize_only) {
	}
	WriteAheadLogDeserializer(ReplayState &state_p, unique_ptr<data_t[]> data_p, idx_t size,
	                          bool deserialize_only = false)
	    : state(state_p), db(state.db), context(state.context), catalog(state.catalog), data(std::move(data_p)),
	      stream(data.get(), size), deserializer(stream), deserialize_only(deserialize_only) {
	}

	static WriteAheadLogDeserializer Open(ReplayState &state_p, BufferedFileReader &stream,
	                                      bool deserialize_only = false) {
		if (state_p.wal_version == 1) {
			// old WAL versions do not have checksums
			return WriteAheadLogDeserializer(state_p, stream, deserialize_only);
		}
		if (state_p.wal_version != 2) {
			throw IOException("Failed to read WAL of version %llu - can only read version 1 and 2",
			                  state_p.wal_version);
		}
		// read the checksum and size
		auto size = stream.Read<uint64_t>();
		auto stored_checksum = stream.Read<uint64_t>();
		auto offset = stream.CurrentOffset();
		auto file_size = stream.FileSize();

		if (offset + size > file_size) {
			throw SerializationException(
			    "Corrupt WAL file: entry size exceeded remaining data in file at byte position %llu "
			    "(found entry with size %llu bytes, file size %llu bytes)",
			    offset, size, file_size);
		}

		// allocate a buffer and read data into the buffer
		auto buffer = unique_ptr<data_t[]>(new data_t[size]);
		stream.ReadData(buffer.get(), size);

		// compute and verify the checksum
		auto computed_checksum = Checksum(buffer.get(), size);
		if (stored_checksum != computed_checksum) {
			throw SerializationException(
			    "Corrupt WAL file: entry at byte position %llu computed checksum %llu does not match "
			    "stored checksum %llu",
			    offset, computed_checksum, stored_checksum);
		}
		return WriteAheadLogDeserializer(state_p, std::move(buffer), size, deserialize_only);
	}

	bool ReplayEntry() {
		deserializer.Begin();
		auto wal_type = deserializer.ReadProperty<WALType>(100, "wal_type");
		if (wal_type == WALType::WAL_FLUSH) {
			deserializer.End();
			return true;
		}
		ReplayEntry(wal_type);
		deserializer.End();
		return false;
	}

	bool DeserializeOnly() {
		return deserialize_only;
	}

protected:
	void ReplayEntry(WALType wal_type);

	void ReplayVersion();

	void ReplayCreateTable();
	void ReplayDropTable();
	void ReplayAlter();

	void ReplayCreateView();
	void ReplayDropView();

	void ReplayCreateSchema();
	void ReplayDropSchema();

	void ReplayCreateType();
	void ReplayDropType();

	void ReplayCreateSequence();
	void ReplayDropSequence();
	void ReplaySequenceValue();

	void ReplayCreateMacro();
	void ReplayDropMacro();

	void ReplayCreateTableMacro();
	void ReplayDropTableMacro();

	void ReplayCreateIndex();
	void ReplayDropIndex();

	void ReplayUseTable();
	void ReplayInsert();
	void ReplayDelete();
	void ReplayUpdate();
	void ReplayCheckpoint();

private:
	ReplayState &state;
	AttachedDatabase &db;
	ClientContext &context;
	Catalog &catalog;
	unique_ptr<data_t[]> data;
	MemoryStream stream;
	BinaryDeserializer deserializer;
	bool deserialize_only;
};

//===--------------------------------------------------------------------===//
// Replay
//===--------------------------------------------------------------------===//
bool WriteAheadLog::Replay(AttachedDatabase &database, unique_ptr<FileHandle> handle) {
	Connection con(database.GetDatabase());
	auto wal_path = handle->GetPath();
	BufferedFileReader reader(FileSystem::Get(database), std::move(handle));
	if (reader.Finished()) {
		// WAL is empty
		return false;
	}

	con.BeginTransaction();
	MetaTransaction::Get(*con.context).ModifyDatabase(database);

	auto &config = DBConfig::GetConfig(database.GetDatabase());
	// first deserialize the WAL to look for a checkpoint flag
	// if there is a checkpoint flag, we might have already flushed the contents of the WAL to disk
	ReplayState checkpoint_state(database, *con.context);
	try {
		while (true) {
			// read the current entry (deserialize only)
			auto deserializer = WriteAheadLogDeserializer::Open(checkpoint_state, reader, true);
			if (deserializer.ReplayEntry()) {
				// check if the file is exhausted
				if (reader.Finished()) {
					// we finished reading the file: break
					break;
				}
			}
		}
	} catch (std::exception &ex) { // LCOV_EXCL_START
		ErrorData error(ex);
		// ignore serialization exceptions - they signal a torn WAL
		if (error.Type() != ExceptionType::SERIALIZATION) {
			error.Throw("Failure while replaying WAL file \"" + wal_path + "\": ");
		}
	} // LCOV_EXCL_STOP
	if (checkpoint_state.checkpoint_id.IsValid()) {
		// there is a checkpoint flag: check if we need to deserialize the WAL
		auto &manager = database.GetStorageManager();
		if (manager.IsCheckpointClean(checkpoint_state.checkpoint_id)) {
			// the contents of the WAL have already been checkpointed
			// we can safely truncate the WAL and ignore its contents
			return true;
		}
	}

	// we need to recover from the WAL: actually set up the replay state
	ReplayState state(database, *con.context);

	// reset the reader - we are going to read the WAL from the beginning again
	reader.Reset();

	// replay the WAL
	// note that everything is wrapped inside a try/catch block here
	// there can be errors in WAL replay because of a corrupt WAL file
	try {
		while (true) {
			// read the current entry
			auto deserializer = WriteAheadLogDeserializer::Open(state, reader);
			if (deserializer.ReplayEntry()) {
				con.Commit();
				// check if the file is exhausted
				if (reader.Finished()) {
					// we finished reading the file: break
					break;
				}
				con.BeginTransaction();
				MetaTransaction::Get(*con.context).ModifyDatabase(database);
			}
		}
	} catch (std::exception &ex) { // LCOV_EXCL_START
		// exception thrown in WAL replay: rollback
		con.Query("ROLLBACK");
		ErrorData error(ex);
		// serialization failure means a truncated WAL
		// these failures are ignored unless abort_on_wal_failure is true
		// other failures always result in an error
		if (config.options.abort_on_wal_failure || error.Type() != ExceptionType::SERIALIZATION) {
			error.Throw("Failure while replaying WAL file \"" + wal_path + "\": ");
		}
	} catch (...) {
		// exception thrown in WAL replay: rollback
		con.Query("ROLLBACK");
		throw;
	} // LCOV_EXCL_STOP
	return false;
}

//===--------------------------------------------------------------------===//
// Replay Entries
//===--------------------------------------------------------------------===//
void WriteAheadLogDeserializer::ReplayEntry(WALType entry_type) {
	switch (entry_type) {
	case WALType::WAL_VERSION:
		ReplayVersion();
		break;
	case WALType::CREATE_TABLE:
		ReplayCreateTable();
		break;
	case WALType::DROP_TABLE:
		ReplayDropTable();
		break;
	case WALType::ALTER_INFO:
		ReplayAlter();
		break;
	case WALType::CREATE_VIEW:
		ReplayCreateView();
		break;
	case WALType::DROP_VIEW:
		ReplayDropView();
		break;
	case WALType::CREATE_SCHEMA:
		ReplayCreateSchema();
		break;
	case WALType::DROP_SCHEMA:
		ReplayDropSchema();
		break;
	case WALType::CREATE_SEQUENCE:
		ReplayCreateSequence();
		break;
	case WALType::DROP_SEQUENCE:
		ReplayDropSequence();
		break;
	case WALType::SEQUENCE_VALUE:
		ReplaySequenceValue();
		break;
	case WALType::CREATE_MACRO:
		ReplayCreateMacro();
		break;
	case WALType::DROP_MACRO:
		ReplayDropMacro();
		break;
	case WALType::CREATE_TABLE_MACRO:
		ReplayCreateTableMacro();
		break;
	case WALType::DROP_TABLE_MACRO:
		ReplayDropTableMacro();
		break;
	case WALType::CREATE_INDEX:
		ReplayCreateIndex();
		break;
	case WALType::DROP_INDEX:
		ReplayDropIndex();
		break;
	case WALType::USE_TABLE:
		ReplayUseTable();
		break;
	case WALType::INSERT_TUPLE:
		ReplayInsert();
		break;
	case WALType::DELETE_TUPLE:
		ReplayDelete();
		break;
	case WALType::UPDATE_TUPLE:
		ReplayUpdate();
		break;
	case WALType::CHECKPOINT:
		ReplayCheckpoint();
		break;
	case WALType::CREATE_TYPE:
		ReplayCreateType();
		break;
	case WALType::DROP_TYPE:
		ReplayDropType();
		break;
	default:
		throw InternalException("Invalid WAL entry type!");
	}
}

//===--------------------------------------------------------------------===//
// Replay Version
//===--------------------------------------------------------------------===//
void WriteAheadLogDeserializer::ReplayVersion() {
	state.wal_version = deserializer.ReadProperty<idx_t>(101, "version");
}

//===--------------------------------------------------------------------===//
// Replay Table
//===--------------------------------------------------------------------===//
void WriteAheadLogDeserializer::ReplayCreateTable() {
	auto info = deserializer.ReadProperty<unique_ptr<CreateInfo>>(101, "table");
	if (DeserializeOnly()) {
		return;
	}
	// bind the constraints to the table again
	auto binder = Binder::CreateBinder(context);
	auto &schema = catalog.GetSchema(context, info->schema);
	auto bound_info = Binder::BindCreateTableCheckpoint(std::move(info), schema);

	catalog.CreateTable(context, *bound_info);
}

void WriteAheadLogDeserializer::ReplayDropTable() {
	DropInfo info;

	info.type = CatalogType::TABLE_ENTRY;
	info.schema = deserializer.ReadProperty<string>(101, "schema");
	info.name = deserializer.ReadProperty<string>(102, "name");
	if (DeserializeOnly()) {
		return;
	}

	catalog.DropEntry(context, info);
}

void WriteAheadLogDeserializer::ReplayAlter() {
	auto info = deserializer.ReadProperty<unique_ptr<ParseInfo>>(101, "info");
	auto &alter_info = info->Cast<AlterInfo>();
	if (DeserializeOnly()) {
		return;
	}
	catalog.Alter(context, alter_info);
}

//===--------------------------------------------------------------------===//
// Replay View
//===--------------------------------------------------------------------===//
void WriteAheadLogDeserializer::ReplayCreateView() {
	auto entry = deserializer.ReadProperty<unique_ptr<CreateInfo>>(101, "view");
	if (DeserializeOnly()) {
		return;
	}
	catalog.CreateView(context, entry->Cast<CreateViewInfo>());
}

void WriteAheadLogDeserializer::ReplayDropView() {
	DropInfo info;
	info.type = CatalogType::VIEW_ENTRY;
	info.schema = deserializer.ReadProperty<string>(101, "schema");
	info.name = deserializer.ReadProperty<string>(102, "name");
	if (DeserializeOnly()) {
		return;
	}
	catalog.DropEntry(context, info);
}

//===--------------------------------------------------------------------===//
// Replay Schema
//===--------------------------------------------------------------------===//
void WriteAheadLogDeserializer::ReplayCreateSchema() {
	CreateSchemaInfo info;
	info.schema = deserializer.ReadProperty<string>(101, "schema");
	if (DeserializeOnly()) {
		return;
	}

	catalog.CreateSchema(context, info);
}

void WriteAheadLogDeserializer::ReplayDropSchema() {
	DropInfo info;

	info.type = CatalogType::SCHEMA_ENTRY;
	info.name = deserializer.ReadProperty<string>(101, "schema");
	if (DeserializeOnly()) {
		return;
	}

	catalog.DropEntry(context, info);
}

//===--------------------------------------------------------------------===//
// Replay Custom Type
//===--------------------------------------------------------------------===//
void WriteAheadLogDeserializer::ReplayCreateType() {
	auto info = deserializer.ReadProperty<unique_ptr<CreateInfo>>(101, "type");
	info->on_conflict = OnCreateConflict::IGNORE_ON_CONFLICT;
	catalog.CreateType(context, info->Cast<CreateTypeInfo>());
}

void WriteAheadLogDeserializer::ReplayDropType() {
	DropInfo info;

	info.type = CatalogType::TYPE_ENTRY;
	info.schema = deserializer.ReadProperty<string>(101, "schema");
	info.name = deserializer.ReadProperty<string>(102, "name");
	if (DeserializeOnly()) {
		return;
	}

	catalog.DropEntry(context, info);
}

//===--------------------------------------------------------------------===//
// Replay Sequence
//===--------------------------------------------------------------------===//
void WriteAheadLogDeserializer::ReplayCreateSequence() {
	auto entry = deserializer.ReadProperty<unique_ptr<CreateInfo>>(101, "sequence");
	if (DeserializeOnly()) {
		return;
	}

	catalog.CreateSequence(context, entry->Cast<CreateSequenceInfo>());
}

void WriteAheadLogDeserializer::ReplayDropSequence() {
	DropInfo info;
	info.type = CatalogType::SEQUENCE_ENTRY;
	info.schema = deserializer.ReadProperty<string>(101, "schema");
	info.name = deserializer.ReadProperty<string>(102, "name");
	if (DeserializeOnly()) {
		return;
	}

	catalog.DropEntry(context, info);
}

void WriteAheadLogDeserializer::ReplaySequenceValue() {
	auto schema = deserializer.ReadProperty<string>(101, "schema");
	auto name = deserializer.ReadProperty<string>(102, "name");
	auto usage_count = deserializer.ReadProperty<uint64_t>(103, "usage_count");
	auto counter = deserializer.ReadProperty<int64_t>(104, "counter");
	if (DeserializeOnly()) {
		return;
	}

	// fetch the sequence from the catalog
	auto &seq = catalog.GetEntry<SequenceCatalogEntry>(context, schema, name);
	seq.ReplayValue(usage_count, counter);
}

//===--------------------------------------------------------------------===//
// Replay Macro
//===--------------------------------------------------------------------===//
void WriteAheadLogDeserializer::ReplayCreateMacro() {
	auto entry = deserializer.ReadProperty<unique_ptr<CreateInfo>>(101, "macro");
	if (DeserializeOnly()) {
		return;
	}

	catalog.CreateFunction(context, entry->Cast<CreateMacroInfo>());
}

void WriteAheadLogDeserializer::ReplayDropMacro() {
	DropInfo info;
	info.type = CatalogType::MACRO_ENTRY;
	info.schema = deserializer.ReadProperty<string>(101, "schema");
	info.name = deserializer.ReadProperty<string>(102, "name");
	if (DeserializeOnly()) {
		return;
	}

	catalog.DropEntry(context, info);
}

//===--------------------------------------------------------------------===//
// Replay Table Macro
//===--------------------------------------------------------------------===//
void WriteAheadLogDeserializer::ReplayCreateTableMacro() {
	auto entry = deserializer.ReadProperty<unique_ptr<CreateInfo>>(101, "table_macro");
	if (DeserializeOnly()) {
		return;
	}
	catalog.CreateFunction(context, entry->Cast<CreateMacroInfo>());
}

void WriteAheadLogDeserializer::ReplayDropTableMacro() {
	DropInfo info;
	info.type = CatalogType::TABLE_MACRO_ENTRY;
	info.schema = deserializer.ReadProperty<string>(101, "schema");
	info.name = deserializer.ReadProperty<string>(102, "name");
	if (DeserializeOnly()) {
		return;
	}

	catalog.DropEntry(context, info);
}

//===--------------------------------------------------------------------===//
// Replay Index
//===--------------------------------------------------------------------===//
void WriteAheadLogDeserializer::ReplayCreateIndex() {
	auto create_info = deserializer.ReadProperty<unique_ptr<CreateInfo>>(101, "index_catalog_entry");
	auto index_info = deserializer.ReadProperty<IndexStorageInfo>(102, "index_storage_info");
	D_ASSERT(index_info.IsValid() && !index_info.name.empty());

	auto &storage_manager = db.GetStorageManager();
	auto &single_file_sm = storage_manager.Cast<SingleFileStorageManager>();
	auto &block_manager = single_file_sm.block_manager;
	auto &buffer_manager = block_manager->buffer_manager;

	deserializer.ReadList(103, "index_storage", [&](Deserializer::List &list, idx_t i) {
		auto &data_info = index_info.allocator_infos[i];

		// read the data into buffer handles and convert them to blocks on disk
		// then, update the block pointer
		for (idx_t j = 0; j < data_info.allocation_sizes.size(); j++) {

			// read the data into a buffer handle
			shared_ptr<BlockHandle> block_handle;
			buffer_manager.Allocate(MemoryTag::ART_INDEX, block_manager->GetBlockSize(), false, &block_handle);
			auto buffer_handle = buffer_manager.Pin(block_handle);
			auto data_ptr = buffer_handle.Ptr();

			list.ReadElement<bool>(data_ptr, data_info.allocation_sizes[j]);

			// now convert the buffer handle to a persistent block and remember the block id
			auto block_id = block_manager->GetFreeBlockId();
			block_manager->ConvertToPersistent(block_id, std::move(block_handle));
			data_info.block_pointers[j].block_id = block_id;
		}
	});

	if (DeserializeOnly()) {
		return;
	}
	auto &info = create_info->Cast<CreateIndexInfo>();

	// Ensure the index type exists
	if (info.index_type.empty()) {
		info.index_type = ART::TYPE_NAME;
	}

	auto index_type = context.db->config.GetIndexTypes().FindByName(info.index_type);
	if (!index_type) {
		throw InternalException("Index type \"%s\" not recognized", info.index_type);
	}

	// create the index in the catalog
	auto &table = catalog.GetEntry<TableCatalogEntry>(context, create_info->schema, info.table).Cast<DuckTableEntry>();
	auto &index = table.schema.CreateIndex(context, info, table)->Cast<DuckIndexEntry>();

	// add the table to the bind context to bind the parsed expressions
	auto binder = Binder::CreateBinder(context);
	vector<LogicalType> column_types;
	vector<string> column_names;
	for (auto &col : table.GetColumns().Logical()) {
		column_types.push_back(col.Type());
		column_names.push_back(col.Name());
	}

	// create a binder to bind the parsed expressions
	vector<column_t> column_ids;
	binder->bind_context.AddBaseTable(0, info.table, column_names, column_types, column_ids, &table);
	IndexBinder idx_binder(*binder, context);

	// bind the parsed expressions to create unbound expressions
	vector<unique_ptr<Expression>> unbound_expressions;
	unbound_expressions.reserve(index.parsed_expressions.size());
	for (auto &expr : index.parsed_expressions) {
		auto copy = expr->Copy();
		unbound_expressions.push_back(idx_binder.Bind(copy));
	}

	auto &data_table = table.GetStorage();

	CreateIndexInput input(TableIOManager::Get(data_table), data_table.db, info.constraint_type, info.index_name,
	                       info.column_ids, unbound_expressions, index_info, info.options);

	auto index_instance = index_type->create_instance(input);
	data_table.AddIndex(std::move(index_instance));
}

void WriteAheadLogDeserializer::ReplayDropIndex() {
	DropInfo info;
	info.type = CatalogType::INDEX_ENTRY;
	info.schema = deserializer.ReadProperty<string>(101, "schema");
	info.name = deserializer.ReadProperty<string>(102, "name");
	if (DeserializeOnly()) {
		return;
	}

	catalog.DropEntry(context, info);
}

//===--------------------------------------------------------------------===//
// Replay Data
//===--------------------------------------------------------------------===//
void WriteAheadLogDeserializer::ReplayUseTable() {
	auto schema_name = deserializer.ReadProperty<string>(101, "schema");
	auto table_name = deserializer.ReadProperty<string>(102, "table");
	if (DeserializeOnly()) {
		return;
	}
	state.current_table = &catalog.GetEntry<TableCatalogEntry>(context, schema_name, table_name);
}

void WriteAheadLogDeserializer::ReplayInsert() {
	DataChunk chunk;
	deserializer.ReadObject(101, "chunk", [&](Deserializer &object) { chunk.Deserialize(object); });
	if (DeserializeOnly()) {
		return;
	}
	if (!state.current_table) {
		throw InternalException("Corrupt WAL: insert without table");
	}

	// append to the current table
	// we don't do any constraint verification here
	vector<unique_ptr<BoundConstraint>> bound_constraints;
	state.current_table->GetStorage().LocalAppend(*state.current_table, context, chunk, bound_constraints);
}

void WriteAheadLogDeserializer::ReplayDelete() {
	DataChunk chunk;
	deserializer.ReadObject(101, "chunk", [&](Deserializer &object) { chunk.Deserialize(object); });
	if (DeserializeOnly()) {
		return;
	}
	if (!state.current_table) {
		throw InternalException("Corrupt WAL: delete without table");
	}

	D_ASSERT(chunk.ColumnCount() == 1 && chunk.data[0].GetType() == LogicalType::ROW_TYPE);
	row_t row_ids[1];
	Vector row_identifiers(LogicalType::ROW_TYPE, data_ptr_cast(row_ids));

	auto source_ids = FlatVector::GetData<row_t>(chunk.data[0]);
	// delete the tuples from the current table
	TableDeleteState delete_state;
	for (idx_t i = 0; i < chunk.size(); i++) {
		row_ids[0] = source_ids[i];
		state.current_table->GetStorage().Delete(delete_state, context, row_identifiers, 1);
	}
}

void WriteAheadLogDeserializer::ReplayUpdate() {
	auto column_path = deserializer.ReadProperty<vector<column_t>>(101, "column_indexes");

	DataChunk chunk;
	deserializer.ReadObject(102, "chunk", [&](Deserializer &object) { chunk.Deserialize(object); });

	if (DeserializeOnly()) {
		return;
	}
	if (!state.current_table) {
		throw InternalException("Corrupt WAL: update without table");
	}

	if (column_path[0] >= state.current_table->GetColumns().PhysicalColumnCount()) {
		throw InternalException("Corrupt WAL: column index for update out of bounds");
	}

	// remove the row id vector from the chunk
	auto row_ids = std::move(chunk.data.back());
	chunk.data.pop_back();

	// now perform the update
	state.current_table->GetStorage().UpdateColumn(*state.current_table, context, row_ids, column_path, chunk);
}

void WriteAheadLogDeserializer::ReplayCheckpoint() {
	state.checkpoint_id = deserializer.ReadProperty<MetaBlockPointer>(101, "meta_block");
}

} // namespace duckdb
