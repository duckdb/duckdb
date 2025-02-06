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
#include "duckdb/storage/table/column_data.hpp"

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
		deserializer.Set<Catalog &>(catalog);
	}
	WriteAheadLogDeserializer(ReplayState &state_p, unique_ptr<data_t[]> data_p, idx_t size,
	                          bool deserialize_only = false)
	    : state(state_p), db(state.db), context(state.context), catalog(state.catalog), data(std::move(data_p)),
	      stream(data.get(), size), deserializer(stream), deserialize_only(deserialize_only) {
		deserializer.Set<Catalog &>(catalog);
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
			throw IOException("Corrupt WAL file: entry at byte position %llu computed checksum %llu does not match "
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
	void ReplayRowGroupData();
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
unique_ptr<WriteAheadLog> WriteAheadLog::Replay(FileSystem &fs, AttachedDatabase &db, const string &wal_path) {
	auto handle = fs.OpenFile(wal_path, FileFlags::FILE_FLAGS_READ | FileFlags::FILE_FLAGS_NULL_IF_NOT_EXISTS);
	if (!handle) {
		// WAL does not exist - instantiate an empty WAL
		return make_uniq<WriteAheadLog>(db, wal_path);
	}
	auto wal_handle = ReplayInternal(db, std::move(handle));
	if (wal_handle) {
		return wal_handle;
	}
	// replay returning NULL indicates we can nuke the WAL entirely - but only if this is not a read-only connection
	if (!db.IsReadOnly()) {
		fs.RemoveFile(wal_path);
	}
	return make_uniq<WriteAheadLog>(db, wal_path);
}
unique_ptr<WriteAheadLog> WriteAheadLog::ReplayInternal(AttachedDatabase &database, unique_ptr<FileHandle> handle) {
	Connection con(database.GetDatabase());
	auto wal_path = handle->GetPath();
	BufferedFileReader reader(FileSystem::Get(database), std::move(handle));
	if (reader.Finished()) {
		// WAL file exists but it is empty - we can delete the file
		return nullptr;
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
			return nullptr;
		}
	}

	// we need to recover from the WAL: actually set up the replay state
	ReplayState state(database, *con.context);

	// reset the reader - we are going to read the WAL from the beginning again
	reader.Reset();

	// replay the WAL
	// note that everything is wrapped inside a try/catch block here
	// there can be errors in WAL replay because of a corrupt WAL file
	idx_t successful_offset = 0;
	bool all_succeeded = false;
	try {
		while (true) {
			// read the current entry
			auto deserializer = WriteAheadLogDeserializer::Open(state, reader);
			if (deserializer.ReplayEntry()) {
				con.Commit();
				successful_offset = reader.offset;
				// check if the file is exhausted
				if (reader.Finished()) {
					// we finished reading the file: break
					all_succeeded = true;
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
	auto init_state = all_succeeded ? WALInitState::UNINITIALIZED : WALInitState::UNINITIALIZED_REQUIRES_TRUNCATE;
	return make_uniq<WriteAheadLog>(database, wal_path, successful_offset, init_state);
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
	case WALType::ROW_GROUP_DATA:
		ReplayRowGroupData();
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

void ReplayWithoutIndex(ClientContext &context, Catalog &catalog, AlterInfo &info, const bool only_deserialize) {
	if (only_deserialize) {
		return;
	}
	catalog.Alter(context, info);
}

void ReplayIndexData(AttachedDatabase &db, BinaryDeserializer &deserializer, IndexStorageInfo &info,
                     const bool deserialize_only) {
	D_ASSERT(info.IsValid() && !info.name.empty());

	auto &storage_manager = db.GetStorageManager();
	auto &single_file_sm = storage_manager.Cast<SingleFileStorageManager>();
	auto &block_manager = single_file_sm.block_manager;
	auto &buffer_manager = block_manager->buffer_manager;

	deserializer.ReadList(103, "index_storage", [&](Deserializer::List &list, idx_t i) {
		auto &data_info = info.allocator_infos[i];

		// Read the data into buffer handles and convert them to blocks on disk.
		for (idx_t j = 0; j < data_info.allocation_sizes.size(); j++) {

			// Read the data into a buffer handle.
			auto buffer_handle = buffer_manager.Allocate(MemoryTag::ART_INDEX, block_manager->GetBlockSize(), false);
			auto block_handle = buffer_handle.GetBlockHandle();
			auto data_ptr = buffer_handle.Ptr();

			list.ReadElement<bool>(data_ptr, data_info.allocation_sizes[j]);

			// Convert the buffer handle to a persistent block and store the block id.
			if (!deserialize_only) {
				auto block_id = block_manager->GetFreeBlockId();
				block_manager->ConvertToPersistent(block_id, std::move(block_handle), std::move(buffer_handle));
				data_info.block_pointers[j].block_id = block_id;
			}
		}
	});
}

void WriteAheadLogDeserializer::ReplayAlter() {
	auto info = deserializer.ReadProperty<unique_ptr<ParseInfo>>(101, "info");
	auto &alter_info = info->Cast<AlterInfo>();
	if (!alter_info.IsAddPrimaryKey()) {
		return ReplayWithoutIndex(context, catalog, alter_info, DeserializeOnly());
	}

	auto index_storage_info = deserializer.ReadProperty<IndexStorageInfo>(102, "index_storage_info");
	ReplayIndexData(db, deserializer, index_storage_info, DeserializeOnly());
	if (DeserializeOnly()) {
		return;
	}

	auto &table_info = alter_info.Cast<AlterTableInfo>();
	auto &constraint_info = table_info.Cast<AddConstraintInfo>();
	auto &unique_info = constraint_info.constraint->Cast<UniqueConstraint>();

	auto &table =
	    catalog.GetEntry<TableCatalogEntry>(context, table_info.schema, table_info.name).Cast<DuckTableEntry>();
	auto &column_list = table.GetColumns();

	// Add the table to the bind context to bind the parsed expressions.
	auto binder = Binder::CreateBinder(context);
	vector<LogicalType> column_types;
	vector<string> column_names;
	for (auto &col : column_list.Logical()) {
		column_types.push_back(col.Type());
		column_names.push_back(col.Name());
	}

	// Create a binder to bind the parsed expressions.
	vector<ColumnIndex> column_indexes;
	binder->bind_context.AddBaseTable(0, string(), column_names, column_types, column_indexes, table);
	IndexBinder idx_binder(*binder, context);

	// Bind the parsed expressions to create unbound expressions.
	vector<unique_ptr<Expression>> unbound_expressions;
	auto logical_indexes = unique_info.GetLogicalIndexes(column_list);
	for (const auto &logical_index : logical_indexes) {
		auto &col = column_list.GetColumn(logical_index);
		unique_ptr<ParsedExpression> parsed = make_uniq<ColumnRefExpression>(col.GetName(), table_info.name);
		unbound_expressions.push_back(idx_binder.Bind(parsed));
	}

	vector<column_t> column_ids;
	for (auto &column_index : column_indexes) {
		column_ids.push_back(column_index.GetPrimaryIndex());
	}

	auto &storage = table.GetStorage();
	CreateIndexInput input(TableIOManager::Get(storage), storage.db, IndexConstraintType::PRIMARY,
	                       index_storage_info.name, column_ids, unbound_expressions, index_storage_info,
	                       index_storage_info.options);

	auto index_type = context.db->config.GetIndexTypes().FindByName(ART::TYPE_NAME);
	auto index_instance = index_type->create_instance(input);
	storage.AddIndex(std::move(index_instance));

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

	ReplayIndexData(db, deserializer, index_info, DeserializeOnly());
	if (DeserializeOnly()) {
		return;
	}

	auto &info = create_info->Cast<CreateIndexInfo>();

	// Ensure that the index type exists.
	if (info.index_type.empty()) {
		info.index_type = ART::TYPE_NAME;
	}

	auto index_type = context.db->config.GetIndexTypes().FindByName(info.index_type);
	if (!index_type) {
		throw InternalException("Index type \"%s\" not recognized", info.index_type);
	}

	// Create the index in the catalog.
	auto &table = catalog.GetEntry<TableCatalogEntry>(context, create_info->schema, info.table).Cast<DuckTableEntry>();
	auto &index = table.schema.CreateIndex(context, info, table)->Cast<DuckIndexEntry>();

	// Add the table to the bind context to bind the parsed expressions.
	auto binder = Binder::CreateBinder(context);
	vector<LogicalType> column_types;
	vector<string> column_names;
	for (auto &col : table.GetColumns().Logical()) {
		column_types.push_back(col.Type());
		column_names.push_back(col.Name());
	}

	// create a binder to bind the parsed expressions
	vector<ColumnIndex> column_ids;
	binder->bind_context.AddBaseTable(0, string(), column_names, column_types, column_ids, table);
	IndexBinder idx_binder(*binder, context);

	// Bind the parsed expressions to create unbound expressions.
	vector<unique_ptr<Expression>> unbound_expressions;
	for (auto &expr : index.parsed_expressions) {
		auto copy = expr->Copy();
		unbound_expressions.push_back(idx_binder.Bind(copy));
	}

	auto &storage = table.GetStorage();
	CreateIndexInput input(TableIOManager::Get(storage), storage.db, info.constraint_type, info.index_name,
	                       info.column_ids, unbound_expressions, index_info, info.options);
	auto index_instance = index_type->create_instance(input);
	storage.AddIndex(std::move(index_instance));
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

	// Append to the current table without constraint verification.
	vector<unique_ptr<BoundConstraint>> bound_constraints;
	auto &storage = state.current_table->GetStorage();
	storage.LocalWALAppend(*state.current_table, context, chunk, bound_constraints);
}

static void MarkBlocksAsUsed(BlockManager &manager, const PersistentColumnData &col_data) {
	for (auto &pointer : col_data.pointers) {
		auto block_id = pointer.block_pointer.block_id;
		if (block_id != INVALID_BLOCK) {
			manager.MarkBlockAsUsed(block_id);
		}
		if (pointer.segment_state) {
			for (auto &block : pointer.segment_state->blocks) {
				manager.MarkBlockAsUsed(block);
			}
		}
	}
	for (auto &child_column : col_data.child_columns) {
		MarkBlocksAsUsed(manager, child_column);
	}
}

void WriteAheadLogDeserializer::ReplayRowGroupData() {
	auto &block_manager = db.GetStorageManager().GetBlockManager();
	PersistentCollectionData data;
	deserializer.Set<DatabaseInstance &>(db.GetDatabase());
	CompressionInfo compression_info(block_manager.GetBlockSize());
	deserializer.Set<const CompressionInfo &>(compression_info);
	deserializer.ReadProperty(101, "row_group_data", data);
	deserializer.Unset<const CompressionInfo>();
	deserializer.Unset<DatabaseInstance>();
	if (DeserializeOnly()) {
		// label blocks in data as used - they will be used after the WAL replay is finished
		// we need to do this during the deserialization phase to ensure the blocks will not be overwritten
		// by previous deserialization steps
		for (auto &group : data.row_group_data) {
			for (auto &col_data : group.column_data) {
				MarkBlocksAsUsed(block_manager, col_data);
			}
		}
		return;
	}
	if (!state.current_table) {
		throw InternalException("Corrupt WAL: insert without table");
	}
	auto &storage = state.current_table->GetStorage();
	auto &table_info = storage.GetDataTableInfo();
	RowGroupCollection new_row_groups(table_info, table_info->GetIOManager(), storage.GetTypes(), 0);
	new_row_groups.Initialize(data);
	TableIndexList index_list;
	storage.MergeStorage(new_row_groups, index_list, nullptr);
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
