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

namespace duckdb {

bool WriteAheadLog::Replay(AttachedDatabase &database, string &path) {
	Connection con(database.GetDatabase());
	auto initial_reader = make_uniq<BufferedFileReader>(FileSystem::Get(database), path.c_str(), con.context.get());
	if (initial_reader->Finished()) {
		// WAL is empty
		return false;
	}

	con.BeginTransaction();

	// first deserialize the WAL to look for a checkpoint flag
	// if there is a checkpoint flag, we might have already flushed the contents of the WAL to disk
	ReplayState checkpoint_state(database, *con.context, *initial_reader);
	initial_reader->catalog = &checkpoint_state.catalog;
	checkpoint_state.deserialize_only = true;
	try {
		while (true) {
			// read the current entry
			WALType entry_type = initial_reader->Read<WALType>();
			if (entry_type == WALType::WAL_FLUSH) {
				// check if the file is exhausted
				if (initial_reader->Finished()) {
					// we finished reading the file: break
					break;
				}
			} else {
				// replay the entry
				checkpoint_state.ReplayEntry(entry_type);
			}
		}
	} catch (std::exception &ex) { // LCOV_EXCL_START
		Printer::Print(StringUtil::Format("Exception in WAL playback during initial read: %s\n", ex.what()));
		return false;
	} catch (...) {
		Printer::Print("Unknown Exception in WAL playback during initial read");
		return false;
	} // LCOV_EXCL_STOP
	initial_reader.reset();
	if (checkpoint_state.checkpoint_id != INVALID_BLOCK) {
		// there is a checkpoint flag: check if we need to deserialize the WAL
		auto &manager = database.GetStorageManager();
		if (manager.IsCheckpointClean(checkpoint_state.checkpoint_id)) {
			// the contents of the WAL have already been checkpointed
			// we can safely truncate the WAL and ignore its contents
			return true;
		}
	}

	// we need to recover from the WAL: actually set up the replay state
	BufferedFileReader reader(FileSystem::Get(database), path.c_str(), con.context.get());
	reader.catalog = &checkpoint_state.catalog;
	ReplayState state(database, *con.context, reader);

	// replay the WAL
	// note that everything is wrapped inside a try/catch block here
	// there can be errors in WAL replay because of a corrupt WAL file
	// in this case we should throw a warning but startup anyway
	try {
		while (true) {
			// read the current entry
			WALType entry_type = reader.Read<WALType>();
			if (entry_type == WALType::WAL_FLUSH) {
				// flush: commit the current transaction
				con.Commit();
				// check if the file is exhausted
				if (reader.Finished()) {
					// we finished reading the file: break
					break;
				}
				// otherwise we keep on reading
				con.BeginTransaction();
			} else {
				// replay the entry
				state.ReplayEntry(entry_type);
			}
		}
	} catch (std::exception &ex) { // LCOV_EXCL_START
		// FIXME: this should report a proper warning in the connection
		Printer::Print(StringUtil::Format("Exception in WAL playback: %s\n", ex.what()));
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
void ReplayState::ReplayEntry(WALType entry_type) {
	switch (entry_type) {
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
// Replay Table
//===--------------------------------------------------------------------===//
void ReplayState::ReplayCreateTable() {

	auto info = TableCatalogEntry::Deserialize(source, context);
	if (deserialize_only) {
		return;
	}

	// bind the constraints to the table again
	auto binder = Binder::CreateBinder(context);
	auto bound_info = binder->BindCreateTableInfo(std::move(info));

	catalog.CreateTable(context, bound_info.get());
}

void ReplayState::ReplayDropTable() {
	DropInfo info;

	info.type = CatalogType::TABLE_ENTRY;
	info.schema = source.Read<string>();
	info.name = source.Read<string>();
	if (deserialize_only) {
		return;
	}

	catalog.DropEntry(context, &info);
}

void ReplayState::ReplayAlter() {
	auto info = AlterInfo::Deserialize(source);
	if (deserialize_only) {
		return;
	}
	catalog.Alter(context, info.get());
}

//===--------------------------------------------------------------------===//
// Replay View
//===--------------------------------------------------------------------===//
void ReplayState::ReplayCreateView() {
	auto entry = ViewCatalogEntry::Deserialize(source, context);
	if (deserialize_only) {
		return;
	}

	catalog.CreateView(context, entry.get());
}

void ReplayState::ReplayDropView() {
	DropInfo info;
	info.type = CatalogType::VIEW_ENTRY;
	info.schema = source.Read<string>();
	info.name = source.Read<string>();
	if (deserialize_only) {
		return;
	}
	catalog.DropEntry(context, &info);
}

//===--------------------------------------------------------------------===//
// Replay Schema
//===--------------------------------------------------------------------===//
void ReplayState::ReplayCreateSchema() {
	CreateSchemaInfo info;
	info.schema = source.Read<string>();
	if (deserialize_only) {
		return;
	}

	catalog.CreateSchema(context, &info);
}

void ReplayState::ReplayDropSchema() {
	DropInfo info;

	info.type = CatalogType::SCHEMA_ENTRY;
	info.name = source.Read<string>();
	if (deserialize_only) {
		return;
	}

	catalog.DropEntry(context, &info);
}

//===--------------------------------------------------------------------===//
// Replay Custom Type
//===--------------------------------------------------------------------===//
void ReplayState::ReplayCreateType() {
	auto info = TypeCatalogEntry::Deserialize(source);
	if (Catalog::TypeExists(context, info->catalog, info->schema, info->name)) {
		return;
	}
	catalog.CreateType(context, info.get());
}

void ReplayState::ReplayDropType() {
	DropInfo info;

	info.type = CatalogType::TYPE_ENTRY;
	info.schema = source.Read<string>();
	info.name = source.Read<string>();
	if (deserialize_only) {
		return;
	}

	catalog.DropEntry(context, &info);
}

//===--------------------------------------------------------------------===//
// Replay Sequence
//===--------------------------------------------------------------------===//
void ReplayState::ReplayCreateSequence() {
	auto entry = SequenceCatalogEntry::Deserialize(source);
	if (deserialize_only) {
		return;
	}

	catalog.CreateSequence(context, entry.get());
}

void ReplayState::ReplayDropSequence() {
	DropInfo info;
	info.type = CatalogType::SEQUENCE_ENTRY;
	info.schema = source.Read<string>();
	info.name = source.Read<string>();
	if (deserialize_only) {
		return;
	}

	catalog.DropEntry(context, &info);
}

void ReplayState::ReplaySequenceValue() {
	auto schema = source.Read<string>();
	auto name = source.Read<string>();
	auto usage_count = source.Read<uint64_t>();
	auto counter = source.Read<int64_t>();
	if (deserialize_only) {
		return;
	}

	// fetch the sequence from the catalog
	auto seq = catalog.GetEntry<SequenceCatalogEntry>(context, schema, name);
	if (usage_count > seq->usage_count) {
		seq->usage_count = usage_count;
		seq->counter = counter;
	}
}

//===--------------------------------------------------------------------===//
// Replay Macro
//===--------------------------------------------------------------------===//
void ReplayState::ReplayCreateMacro() {
	auto entry = ScalarMacroCatalogEntry::Deserialize(source, context);
	if (deserialize_only) {
		return;
	}

	catalog.CreateFunction(context, entry.get());
}

void ReplayState::ReplayDropMacro() {
	DropInfo info;
	info.type = CatalogType::MACRO_ENTRY;
	info.schema = source.Read<string>();
	info.name = source.Read<string>();
	if (deserialize_only) {
		return;
	}

	catalog.DropEntry(context, &info);
}

//===--------------------------------------------------------------------===//
// Replay Table Macro
//===--------------------------------------------------------------------===//
void ReplayState::ReplayCreateTableMacro() {
	auto entry = TableMacroCatalogEntry::Deserialize(source, context);
	if (deserialize_only) {
		return;
	}

	catalog.CreateFunction(context, entry.get());
}

void ReplayState::ReplayDropTableMacro() {
	DropInfo info;
	info.type = CatalogType::TABLE_MACRO_ENTRY;
	info.schema = source.Read<string>();
	info.name = source.Read<string>();
	if (deserialize_only) {
		return;
	}

	catalog.DropEntry(context, &info);
}

//===--------------------------------------------------------------------===//
// Replay Index
//===--------------------------------------------------------------------===//
void ReplayState::ReplayCreateIndex() {

	auto info = IndexCatalogEntry::Deserialize(source, context);
	if (deserialize_only) {
		return;
	}

	// get the physical table to which we'll add the index
	auto table = catalog.GetEntry<TableCatalogEntry>(context, info->schema, info->table->table_name);
	auto &data_table = table->GetStorage();

	// bind the parsed expressions
	if (info->expressions.empty()) {
		for (auto &parsed_expr : info->parsed_expressions) {
			info->expressions.push_back(parsed_expr->Copy());
		}
	}
	auto binder = Binder::CreateBinder(context);
	auto expressions = binder->BindCreateIndexExpressions(table, info.get());

	// create the empty index
	unique_ptr<Index> index;
	switch (info->index_type) {
	case IndexType::ART: {
		index = make_uniq<ART>(info->column_ids, TableIOManager::Get(data_table), expressions, info->constraint_type,
		                         data_table.db);
		break;
	}
	default:
		throw InternalException("Unimplemented index type");
	}

	// add the index to the catalog
	auto index_entry = (DuckIndexEntry *)catalog.CreateIndex(context, info.get());
	index_entry->index = index.get();
	index_entry->info = data_table.info;
	for (auto &parsed_expr : info->parsed_expressions) {
		index_entry->parsed_expressions.push_back(parsed_expr->Copy());
	}

	// physically add the index to the data table storage
	data_table.WALAddIndex(context, std::move(index), expressions);
}

void ReplayState::ReplayDropIndex() {

	DropInfo info;
	info.type = CatalogType::INDEX_ENTRY;
	info.schema = source.Read<string>();
	info.name = source.Read<string>();
	if (deserialize_only) {
		return;
	}

	catalog.DropEntry(context, &info);
}

//===--------------------------------------------------------------------===//
// Replay Data
//===--------------------------------------------------------------------===//
void ReplayState::ReplayUseTable() {
	auto schema_name = source.Read<string>();
	auto table_name = source.Read<string>();
	if (deserialize_only) {
		return;
	}
	current_table = catalog.GetEntry<TableCatalogEntry>(context, schema_name, table_name);
}

void ReplayState::ReplayInsert() {
	DataChunk chunk;
	chunk.Deserialize(source);
	if (deserialize_only) {
		return;
	}
	if (!current_table) {
		throw Exception("Corrupt WAL: insert without table");
	}

	// append to the current table
	current_table->GetStorage().LocalAppend(*current_table, context, chunk);
}

void ReplayState::ReplayDelete() {
	DataChunk chunk;
	chunk.Deserialize(source);
	if (deserialize_only) {
		return;
	}
	if (!current_table) {
		throw InternalException("Corrupt WAL: delete without table");
	}

	D_ASSERT(chunk.ColumnCount() == 1 && chunk.data[0].GetType() == LogicalType::ROW_TYPE);
	row_t row_ids[1];
	Vector row_identifiers(LogicalType::ROW_TYPE, (data_ptr_t)row_ids);

	auto source_ids = FlatVector::GetData<row_t>(chunk.data[0]);
	// delete the tuples from the current table
	for (idx_t i = 0; i < chunk.size(); i++) {
		row_ids[0] = source_ids[i];
		current_table->GetStorage().Delete(*current_table, context, row_identifiers, 1);
	}
}

void ReplayState::ReplayUpdate() {
	vector<column_t> column_path;
	auto column_index_count = source.Read<idx_t>();
	column_path.reserve(column_index_count);
	for (idx_t i = 0; i < column_index_count; i++) {
		column_path.push_back(source.Read<column_t>());
	}
	DataChunk chunk;
	chunk.Deserialize(source);
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

void ReplayState::ReplayCheckpoint() {
	checkpoint_id = source.Read<block_id_t>();
}

} // namespace duckdb
