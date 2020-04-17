#include "duckdb/storage/write_ahead_log.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/common/serializer/buffered_file_reader.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/view_catalog_entry.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/parser/parsed_data/alter_table_info.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/parser/parsed_data/create_view_info.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"

using namespace duckdb;
using namespace std;

class ReplayState {
public:
	ReplayState(DuckDB &db, ClientContext &context, Deserializer &source)
	    : db(db), context(context), source(source), current_table(nullptr) {
	}

	DuckDB &db;
	ClientContext &context;
	Deserializer &source;
	TableCatalogEntry *current_table;

public:
	void ReplayEntry(WALType entry_type);

private:
	void ReplayCreateTable();
	void ReplayDropTable();
	void ReplayAlter();

	void ReplayCreateView();
	void ReplayDropView();

	void ReplayCreateSchema();
	void ReplayDropSchema();

	void ReplayCreateSequence();
	void ReplayDropSequence();
	void ReplaySequenceValue();

	void ReplayUseTable();
	void ReplayInsert();
	void ReplayDelete();
	void ReplayUpdate();
};

void WriteAheadLog::Replay(DuckDB &database, string &path) {
	BufferedFileReader reader(*database.file_system, path.c_str());

	if (reader.Finished()) {
		// WAL is empty
		return;
	}

	ClientContext context(database);
	context.transaction.SetAutoCommit(false);
	context.transaction.BeginTransaction();

	ReplayState state(database, context, reader);

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
				context.transaction.Commit();
				context.transaction.SetAutoCommit(false);
				// check if the file is exhausted
				if (reader.Finished()) {
					// we finished reading the file: break
					break;
				}
				// otherwise we keep on reading
				context.transaction.BeginTransaction();
			} else {
				// replay the entry
				state.ReplayEntry(entry_type);
			}
		}
	} catch (std::exception &ex) {
		// FIXME: this report a proper warning in the connection
		fprintf(stderr, "Exception in WAL playback: %s\n", ex.what());
		// exception thrown in WAL replay: rollback
		context.transaction.Rollback();
	}
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
	default:
		throw Exception("Invalid WAL entry type!");
	}
}

//===--------------------------------------------------------------------===//
// Replay Table
//===--------------------------------------------------------------------===//
void ReplayState::ReplayCreateTable() {
	auto info = TableCatalogEntry::Deserialize(source);

	// bind the constraints to the table again
	Binder binder(context);
	auto bound_info = binder.BindCreateTableInfo(move(info));

	db.catalog->CreateTable(context, bound_info.get());
}

void ReplayState::ReplayDropTable() {
	DropInfo info;

	info.type = CatalogType::TABLE;
	info.schema = source.Read<string>();
	info.name = source.Read<string>();

	db.catalog->DropEntry(context, &info);
}

void ReplayState::ReplayAlter() {
	auto info = AlterInfo::Deserialize(source);
	if (info->type != AlterType::ALTER_TABLE) {
		throw Exception("Expected ALTER TABLE!");
	}

	db.catalog->AlterTable(context, (AlterTableInfo *)info.get());
}

//===--------------------------------------------------------------------===//
// Replay View
//===--------------------------------------------------------------------===//
void ReplayState::ReplayCreateView() {
	auto entry = ViewCatalogEntry::Deserialize(source);

	db.catalog->CreateView(context, entry.get());
}

void ReplayState::ReplayDropView() {
	DropInfo info;
	info.type = CatalogType::VIEW;
	info.schema = source.Read<string>();
	info.name = source.Read<string>();
	db.catalog->DropEntry(context, &info);
}

//===--------------------------------------------------------------------===//
// Replay Schema
//===--------------------------------------------------------------------===//
void ReplayState::ReplayCreateSchema() {
	CreateSchemaInfo info;
	info.schema = source.Read<string>();

	db.catalog->CreateSchema(context, &info);
}

void ReplayState::ReplayDropSchema() {
	DropInfo info;

	info.type = CatalogType::SCHEMA;
	info.name = source.Read<string>();

	db.catalog->DropEntry(context, &info);
}

//===--------------------------------------------------------------------===//
// Replay Sequence
//===--------------------------------------------------------------------===//
void ReplayState::ReplayCreateSequence() {
	auto entry = SequenceCatalogEntry::Deserialize(source);

	db.catalog->CreateSequence(context, entry.get());
}

void ReplayState::ReplayDropSequence() {
	DropInfo info;
	info.type = CatalogType::SEQUENCE;
	info.schema = source.Read<string>();
	info.name = source.Read<string>();

	db.catalog->DropEntry(context, &info);
}

void ReplayState::ReplaySequenceValue() {
	auto schema = source.Read<string>();
	auto name = source.Read<string>();
	auto usage_count = source.Read<uint64_t>();
	auto counter = source.Read<int64_t>();

	// fetch the sequence from the catalog
	auto seq = db.catalog->GetEntry<SequenceCatalogEntry>(context, schema, name);
	if (usage_count > seq->usage_count) {
		seq->usage_count = usage_count;
		seq->counter = counter;
	}
}

//===--------------------------------------------------------------------===//
// Replay Data
//===--------------------------------------------------------------------===//
void ReplayState::ReplayUseTable() {
	auto schema_name = source.Read<string>();
	auto table_name = source.Read<string>();
	current_table = db.catalog->GetEntry<TableCatalogEntry>(context, schema_name, table_name);
}

void ReplayState::ReplayInsert() {
	if (!current_table) {
		throw Exception("Corrupt WAL: insert without table");
	}
	DataChunk chunk;
	chunk.Deserialize(source);

	// append to the current table
	current_table->storage->Append(*current_table, context, chunk);
}

void ReplayState::ReplayDelete() {
	if (!current_table) {
		throw Exception("Corrupt WAL: delete without table");
	}
	DataChunk chunk;
	chunk.Deserialize(source);

	assert(chunk.column_count() == 1 && chunk.data[0].type == ROW_TYPE);
	row_t row_ids[1];
	Vector row_identifiers(ROW_TYPE, (data_ptr_t)row_ids);

	auto source_ids = FlatVector::GetData<row_t>(chunk.data[0]);
	// delete the tuples from the current table
	for (idx_t i = 0; i < chunk.size(); i++) {
		row_ids[0] = source_ids[i];
		current_table->storage->Delete(*current_table, context, row_identifiers, 1);
	}
}

void ReplayState::ReplayUpdate() {
	if (!current_table) {
		throw Exception("Corrupt WAL: update without table");
	}

	idx_t column_index = source.Read<column_t>();

	DataChunk chunk;
	chunk.Deserialize(source);

	vector<column_t> column_ids{column_index};
	if (column_index >= current_table->columns.size()) {
		throw Exception("Corrupt WAL: column index for update out of bounds");
	}

	// remove the row id vector from the chunk
	auto row_ids = move(chunk.data.back());
	chunk.data.pop_back();

	// now perform the update
	current_table->storage->Update(*current_table, context, row_ids, column_ids, chunk);
}
