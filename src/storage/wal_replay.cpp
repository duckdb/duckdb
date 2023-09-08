#include "duckdb/catalog/catalog_entry/scalar_macro_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/type_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/view_catalog_entry.hpp"
#include "duckdb/common/printer.hpp"
#include "duckdb/common/serializer/buffered_file_reader.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/parser/parsed_data/alter_table_info.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/parser/parsed_data/create_view_info.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/storage/write_ahead_log.hpp"
#include "duckdb/storage/storage_manager.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/execution/index/art/art.hpp"
#include "duckdb/catalog/catalog_entry/duck_index_entry.hpp"
#include "duckdb/common/serializer/binary_deserializer.hpp"

namespace duckdb {

bool WriteAheadLog::Replay(AttachedDatabase &database, string &path) {
	Connection con(database.GetDatabase());
	auto initial_source = make_uniq<BufferedFileReader>(FileSystem::Get(database), path.c_str());
	if (initial_source->Finished()) {
		// WAL is empty
		return false;
	}

	con.BeginTransaction();

	// first deserialize the WAL to look for a checkpoint flag
	// if there is a checkpoint flag, we might have already flushed the contents of the WAL to disk
	ReplayState checkpoint_state(database, *con.context);
	checkpoint_state.deserialize_only = true;
	try {
		while (true) {
			// read the current entry
			BinaryDeserializer deserializer(*initial_source);
			deserializer.Begin();
			auto entry_type = deserializer.ReadProperty<WALType>(100, "wal_type");
			if (entry_type == WALType::WAL_FLUSH) {
				deserializer.End();
				// check if the file is exhausted
				if (initial_source->Finished()) {
					// we finished reading the file: break
					break;
				}
			} else {
				// replay the entry
				checkpoint_state.ReplayEntry(entry_type, deserializer);
				deserializer.End();
			}
		}
	} catch (std::exception &ex) { // LCOV_EXCL_START
		Printer::PrintF("Exception in WAL playback during initial read: %s\n", ex.what());
		return false;
	} catch (...) {
		Printer::Print("Unknown Exception in WAL playback during initial read");
		return false;
	} // LCOV_EXCL_STOP
	initial_source.reset();
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
	BufferedFileReader reader(FileSystem::Get(database), path.c_str());
	ReplayState state(database, *con.context);

	// replay the WAL
	// note that everything is wrapped inside a try/catch block here
	// there can be errors in WAL replay because of a corrupt WAL file
	// in this case we should throw a warning but startup anyway
	try {
		while (true) {
			// read the current entry
			BinaryDeserializer deserializer(reader);
			deserializer.Begin();
			auto entry_type = deserializer.ReadProperty<WALType>(100, "wal_type");
			if (entry_type == WALType::WAL_FLUSH) {
				deserializer.End();
				con.Commit();
				// check if the file is exhausted
				if (reader.Finished()) {
					// we finished reading the file: break
					break;
				}
				con.BeginTransaction();
			} else {
				// replay the entry
				state.ReplayEntry(entry_type, deserializer);
				deserializer.End();
			}
		}
	} catch (std::exception &ex) { // LCOV_EXCL_START
		// FIXME: this should report a proper warning in the connection
		Printer::PrintF("Exception in WAL playback: %s\n", ex.what());
		// exception thrown in WAL replay: rollback
		con.Rollback();
	} catch (...) {
		Printer::Print("Unknown Exception in WAL playback: %s\n");
		// exception thrown in WAL replay: rollback
		con.Rollback();
	} // LCOV_EXCL_STOP
	return false;
}

//===--------------------------------------------------------------------===//
// Replay Entries
//===--------------------------------------------------------------------===//
void ReplayState::ReplayEntry(WALType entry_type, BinaryDeserializer &deserializer) {
	switch (entry_type) {
	case WALType::CREATE_TABLE:
		ReplayCreateTable(deserializer);
		break;
	case WALType::DROP_TABLE:
		ReplayDropTable(deserializer);
		break;
	case WALType::ALTER_INFO:
		ReplayAlter(deserializer);
		break;
	case WALType::CREATE_VIEW:
		ReplayCreateView(deserializer);
		break;
	case WALType::DROP_VIEW:
		ReplayDropView(deserializer);
		break;
	case WALType::CREATE_SCHEMA:
		ReplayCreateSchema(deserializer);
		break;
	case WALType::DROP_SCHEMA:
		ReplayDropSchema(deserializer);
		break;
	case WALType::CREATE_SEQUENCE:
		ReplayCreateSequence(deserializer);
		break;
	case WALType::DROP_SEQUENCE:
		ReplayDropSequence(deserializer);
		break;
	case WALType::SEQUENCE_VALUE:
		ReplaySequenceValue(deserializer);
		break;
	case WALType::CREATE_MACRO:
		ReplayCreateMacro(deserializer);
		break;
	case WALType::DROP_MACRO:
		ReplayDropMacro(deserializer);
		break;
	case WALType::CREATE_TABLE_MACRO:
		ReplayCreateTableMacro(deserializer);
		break;
	case WALType::DROP_TABLE_MACRO:
		ReplayDropTableMacro(deserializer);
		break;
	case WALType::CREATE_INDEX:
		ReplayCreateIndex(deserializer);
		break;
	case WALType::DROP_INDEX:
		ReplayDropIndex(deserializer);
		break;
	case WALType::USE_TABLE:
		ReplayUseTable(deserializer);
		break;
	case WALType::INSERT_TUPLE:
		ReplayInsert(deserializer);
		break;
	case WALType::DELETE_TUPLE:
		ReplayDelete(deserializer);
		break;
	case WALType::UPDATE_TUPLE:
		ReplayUpdate(deserializer);
		break;
	case WALType::CHECKPOINT:
		ReplayCheckpoint(deserializer);
		break;
	case WALType::CREATE_TYPE:
		ReplayCreateType(deserializer);
		break;
	case WALType::DROP_TYPE:
		ReplayDropType(deserializer);
		break;
	default:
		throw InternalException("Invalid WAL entry type!");
	}
}

//===--------------------------------------------------------------------===//
// Replay Table
//===--------------------------------------------------------------------===//
void ReplayState::ReplayCreateTable(BinaryDeserializer &deserializer) {
	auto info = deserializer.ReadProperty<unique_ptr<CreateInfo>>(101, "table");
	if (deserialize_only) {
		return;
	}
	// bind the constraints to the table again
	auto binder = Binder::CreateBinder(context);
	auto &schema = catalog.GetSchema(context, info->schema);
	auto bound_info = binder->BindCreateTableInfo(std::move(info), schema);

	catalog.CreateTable(context, *bound_info);
}

void ReplayState::ReplayDropTable(BinaryDeserializer &deserializer) {

	DropInfo info;

	info.type = CatalogType::TABLE_ENTRY;
	info.schema = deserializer.ReadProperty<string>(101, "schema");
	info.name = deserializer.ReadProperty<string>(102, "name");
	if (deserialize_only) {
		return;
	}

	catalog.DropEntry(context, info);
}

void ReplayState::ReplayAlter(BinaryDeserializer &deserializer) {

	auto info = deserializer.ReadProperty<unique_ptr<ParseInfo>>(101, "info");
	auto &alter_info = info->Cast<AlterInfo>();
	if (deserialize_only) {
		return;
	}
	catalog.Alter(context, alter_info);
}

//===--------------------------------------------------------------------===//
// Replay View
//===--------------------------------------------------------------------===//
void ReplayState::ReplayCreateView(BinaryDeserializer &deserializer) {
	auto entry = deserializer.ReadProperty<unique_ptr<CreateInfo>>(101, "view");
	if (deserialize_only) {
		return;
	}
	catalog.CreateView(context, entry->Cast<CreateViewInfo>());
}

void ReplayState::ReplayDropView(BinaryDeserializer &deserializer) {
	DropInfo info;
	info.type = CatalogType::VIEW_ENTRY;
	info.schema = deserializer.ReadProperty<string>(101, "schema");
	info.name = deserializer.ReadProperty<string>(102, "name");
	if (deserialize_only) {
		return;
	}
	catalog.DropEntry(context, info);
}

//===--------------------------------------------------------------------===//
// Replay Schema
//===--------------------------------------------------------------------===//
void ReplayState::ReplayCreateSchema(BinaryDeserializer &deserializer) {
	CreateSchemaInfo info;
	info.schema = deserializer.ReadProperty<string>(101, "schema");
	if (deserialize_only) {
		return;
	}

	catalog.CreateSchema(context, info);
}

void ReplayState::ReplayDropSchema(BinaryDeserializer &deserializer) {
	DropInfo info;

	info.type = CatalogType::SCHEMA_ENTRY;
	info.name = deserializer.ReadProperty<string>(101, "schema");
	if (deserialize_only) {
		return;
	}

	catalog.DropEntry(context, info);
}

//===--------------------------------------------------------------------===//
// Replay Custom Type
//===--------------------------------------------------------------------===//
void ReplayState::ReplayCreateType(BinaryDeserializer &deserializer) {
	auto info = deserializer.ReadProperty<unique_ptr<CreateInfo>>(101, "type");
	info->on_conflict = OnCreateConflict::IGNORE_ON_CONFLICT;
	catalog.CreateType(context, info->Cast<CreateTypeInfo>());
}

void ReplayState::ReplayDropType(BinaryDeserializer &deserializer) {
	DropInfo info;

	info.type = CatalogType::TYPE_ENTRY;
	info.schema = deserializer.ReadProperty<string>(101, "schema");
	info.name = deserializer.ReadProperty<string>(102, "name");
	if (deserialize_only) {
		return;
	}

	catalog.DropEntry(context, info);
}

//===--------------------------------------------------------------------===//
// Replay Sequence
//===--------------------------------------------------------------------===//
void ReplayState::ReplayCreateSequence(BinaryDeserializer &deserializer) {
	auto entry = deserializer.ReadProperty<unique_ptr<CreateInfo>>(101, "sequence");
	if (deserialize_only) {
		return;
	}

	catalog.CreateSequence(context, entry->Cast<CreateSequenceInfo>());
}

void ReplayState::ReplayDropSequence(BinaryDeserializer &deserializer) {
	DropInfo info;
	info.type = CatalogType::SEQUENCE_ENTRY;
	info.schema = deserializer.ReadProperty<string>(101, "schema");
	info.name = deserializer.ReadProperty<string>(102, "name");
	if (deserialize_only) {
		return;
	}

	catalog.DropEntry(context, info);
}

void ReplayState::ReplaySequenceValue(BinaryDeserializer &deserializer) {
	auto schema = deserializer.ReadProperty<string>(101, "schema");
	auto name = deserializer.ReadProperty<string>(102, "name");
	auto usage_count = deserializer.ReadProperty<uint64_t>(103, "usage_count");
	auto counter = deserializer.ReadProperty<int64_t>(104, "counter");
	if (deserialize_only) {
		return;
	}

	// fetch the sequence from the catalog
	auto &seq = catalog.GetEntry<SequenceCatalogEntry>(context, schema, name);
	if (usage_count > seq.usage_count) {
		seq.usage_count = usage_count;
		seq.counter = counter;
	}
}

//===--------------------------------------------------------------------===//
// Replay Macro
//===--------------------------------------------------------------------===//
void ReplayState::ReplayCreateMacro(BinaryDeserializer &deserializer) {
	auto entry = deserializer.ReadProperty<unique_ptr<CreateInfo>>(101, "macro");
	if (deserialize_only) {
		return;
	}

	catalog.CreateFunction(context, entry->Cast<CreateMacroInfo>());
}

void ReplayState::ReplayDropMacro(BinaryDeserializer &deserializer) {
	DropInfo info;
	info.type = CatalogType::MACRO_ENTRY;
	info.schema = deserializer.ReadProperty<string>(101, "schema");
	info.name = deserializer.ReadProperty<string>(102, "name");
	if (deserialize_only) {
		return;
	}

	catalog.DropEntry(context, info);
}

//===--------------------------------------------------------------------===//
// Replay Table Macro
//===--------------------------------------------------------------------===//
void ReplayState::ReplayCreateTableMacro(BinaryDeserializer &deserializer) {
	auto entry = deserializer.ReadProperty<unique_ptr<CreateInfo>>(101, "table_macro");
	if (deserialize_only) {
		return;
	}
	catalog.CreateFunction(context, entry->Cast<CreateMacroInfo>());
}

void ReplayState::ReplayDropTableMacro(BinaryDeserializer &deserializer) {
	DropInfo info;
	info.type = CatalogType::TABLE_MACRO_ENTRY;
	info.schema = deserializer.ReadProperty<string>(101, "schema");
	info.name = deserializer.ReadProperty<string>(102, "name");
	if (deserialize_only) {
		return;
	}

	catalog.DropEntry(context, info);
}

//===--------------------------------------------------------------------===//
// Replay Index
//===--------------------------------------------------------------------===//
void ReplayState::ReplayCreateIndex(BinaryDeserializer &deserializer) {
	auto info = deserializer.ReadProperty<unique_ptr<CreateInfo>>(101, "index");
	if (deserialize_only) {
		return;
	}
	auto &index_info = info->Cast<CreateIndexInfo>();

	// get the physical table to which we'll add the index
	auto &table = catalog.GetEntry<TableCatalogEntry>(context, info->schema, index_info.table);
	auto &data_table = table.GetStorage();

	// bind the parsed expressions
	if (index_info.expressions.empty()) {
		for (auto &parsed_expr : index_info.parsed_expressions) {
			index_info.expressions.push_back(parsed_expr->Copy());
		}
	}
	auto binder = Binder::CreateBinder(context);
	auto expressions = binder->BindCreateIndexExpressions(table, index_info);

	// create the empty index
	unique_ptr<Index> index;
	switch (index_info.index_type) {
	case IndexType::ART: {
		index = make_uniq<ART>(index_info.column_ids, TableIOManager::Get(data_table), expressions,
		                       index_info.constraint_type, data_table.db);
		break;
	}
	default:
		throw InternalException("Unimplemented index type");
	}

	// add the index to the catalog
	auto &index_entry = catalog.CreateIndex(context, index_info)->Cast<DuckIndexEntry>();
	index_entry.index = index.get();
	index_entry.info = data_table.info;
	for (auto &parsed_expr : index_info.parsed_expressions) {
		index_entry.parsed_expressions.push_back(parsed_expr->Copy());
	}

	// physically add the index to the data table storage
	data_table.WALAddIndex(context, std::move(index), expressions);
}

void ReplayState::ReplayDropIndex(BinaryDeserializer &deserializer) {
	DropInfo info;
	info.type = CatalogType::INDEX_ENTRY;
	info.schema = deserializer.ReadProperty<string>(101, "schema");
	info.name = deserializer.ReadProperty<string>(102, "name");
	if (deserialize_only) {
		return;
	}

	catalog.DropEntry(context, info);
}

//===--------------------------------------------------------------------===//
// Replay Data
//===--------------------------------------------------------------------===//
void ReplayState::ReplayUseTable(BinaryDeserializer &deserializer) {
	auto schema_name = deserializer.ReadProperty<string>(101, "schema");
	auto table_name = deserializer.ReadProperty<string>(102, "table");
	if (deserialize_only) {
		return;
	}
	current_table = &catalog.GetEntry<TableCatalogEntry>(context, schema_name, table_name);
}

void ReplayState::ReplayInsert(BinaryDeserializer &deserializer) {
	DataChunk chunk;
	deserializer.ReadObject(101, "chunk", [&](Deserializer &object) { chunk.Deserialize(object); });
	if (deserialize_only) {
		return;
	}
	if (!current_table) {
		throw Exception("Corrupt WAL: insert without table");
	}

	// append to the current table
	current_table->GetStorage().LocalAppend(*current_table, context, chunk);
}

void ReplayState::ReplayDelete(BinaryDeserializer &deserializer) {
	DataChunk chunk;
	deserializer.ReadObject(101, "chunk", [&](Deserializer &object) { chunk.Deserialize(object); });
	if (deserialize_only) {
		return;
	}
	if (!current_table) {
		throw InternalException("Corrupt WAL: delete without table");
	}

	D_ASSERT(chunk.ColumnCount() == 1 && chunk.data[0].GetType() == LogicalType::ROW_TYPE);
	row_t row_ids[1];
	Vector row_identifiers(LogicalType::ROW_TYPE, data_ptr_cast(row_ids));

	auto source_ids = FlatVector::GetData<row_t>(chunk.data[0]);
	// delete the tuples from the current table
	for (idx_t i = 0; i < chunk.size(); i++) {
		row_ids[0] = source_ids[i];
		current_table->GetStorage().Delete(*current_table, context, row_identifiers, 1);
	}
}

void ReplayState::ReplayUpdate(BinaryDeserializer &deserializer) {
	auto column_path = deserializer.ReadProperty<vector<column_t>>(101, "column_indexes");

	DataChunk chunk;
	deserializer.ReadObject(102, "chunk", [&](Deserializer &object) { chunk.Deserialize(object); });

	if (deserialize_only) {
		return;
	}
	if (!current_table) {
		throw InternalException("Corrupt WAL: update without table");
	}

	if (column_path[0] >= current_table->GetColumns().PhysicalColumnCount()) {
		throw InternalException("Corrupt WAL: column index for update out of bounds");
	}

	// remove the row id vector from the chunk
	auto row_ids = std::move(chunk.data.back());
	chunk.data.pop_back();

	// now perform the update
	current_table->GetStorage().UpdateColumn(*current_table, context, row_ids, column_path, chunk);
}

void ReplayState::ReplayCheckpoint(BinaryDeserializer &deserializer) {
	checkpoint_id = deserializer.ReadProperty<MetaBlockPointer>(101, "meta_block");
}

} // namespace duckdb
