#include "duckdb/storage/checkpoint_manager.hpp"
#include "duckdb/storage/block_manager.hpp"
#include "duckdb/storage/meta_block_reader.hpp"

#include "duckdb/common/serializer.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/common/types/null_value.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/macro_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/sequence_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/view_catalog_entry.hpp"

#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/parser/parsed_data/create_view_info.hpp"

#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"

#include "duckdb/main/client_context.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/database.hpp"

#include "duckdb/transaction/transaction_manager.hpp"

#include "duckdb/storage/checkpoint/table_data_writer.hpp"
#include "duckdb/storage/checkpoint/table_data_reader.hpp"
#include "duckdb/main/config.hpp"

namespace duckdb {

CheckpointManager::CheckpointManager(DatabaseInstance &db) : db(db) {
}

void CheckpointManager::CreateCheckpoint() {
	auto &config = DBConfig::GetConfig(db);
	auto &storage_manager = StorageManager::GetStorageManager(db);
	if (storage_manager.InMemory()) {
		return;
	}
	// assert that the checkpoint manager hasn't been used before
	D_ASSERT(!metadata_writer);

	auto &block_manager = BlockManager::GetBlockManager(db);
	block_manager.StartCheckpoint();

	//! Set up the writers for the checkpoints
	metadata_writer = make_unique<MetaBlockWriter>(db);
	tabledata_writer = make_unique<MetaBlockWriter>(db);

	// get the id of the first meta block
	block_id_t meta_block = metadata_writer->block->id;

	vector<SchemaCatalogEntry *> schemas;
	// we scan the set of committed schemas
	auto &catalog = Catalog::GetCatalog(db);
	catalog.schemas->Scan([&](CatalogEntry *entry) { schemas.push_back((SchemaCatalogEntry *)entry); });
	// write the actual data into the database
	// write the amount of schemas
	metadata_writer->Write<uint32_t>(schemas.size());
	for (auto &schema : schemas) {
		WriteSchema(*schema);
	}
	// flush the meta data to disk
	metadata_writer->Flush();
	tabledata_writer->Flush();

	// write a checkpoint flag to the WAL
	// this protects against the rare event that the database crashes AFTER writing the file, but BEFORE truncating the
	// WAL we write an entry CHECKPOINT "meta_block_id" into the WAL upon loading, if we see there is an entry
	// CHECKPOINT "meta_block_id", and the id MATCHES the head idin the file we know that the database was successfully
	// checkpointed, so we know that we should avoid replaying the WAL to avoid duplicating data
	auto wal = storage_manager.GetWriteAheadLog();
	wal->WriteCheckpoint(meta_block);
	wal->Flush();

	if (config.checkpoint_abort == CheckpointAbort::DEBUG_ABORT_BEFORE_HEADER) {
		throw IOException("Checkpoint aborted before header write because of PRAGMA checkpoint_abort flag");
	}

	// finally write the updated header
	DatabaseHeader header;
	header.meta_block = meta_block;
	block_manager.WriteHeader(header);

	if (config.checkpoint_abort == CheckpointAbort::DEBUG_ABORT_BEFORE_TRUNCATE) {
		throw IOException("Checkpoint aborted before truncate because of PRAGMA checkpoint_abort flag");
	}

	// truncate the WAL
	wal->Truncate(0);

	// mark all blocks written as part of the metadata as modified
	for (auto &block_id : metadata_writer->written_blocks) {
		block_manager.MarkBlockAsModified(block_id);
	}
	for (auto &block_id : tabledata_writer->written_blocks) {
		block_manager.MarkBlockAsModified(block_id);
	}
}

void CheckpointManager::LoadFromStorage() {
	auto &block_manager = BlockManager::GetBlockManager(db);
	block_id_t meta_block = block_manager.GetMetaBlock();
	if (meta_block < 0) {
		// storage is empty
		return;
	}

	Connection con(db);
	con.BeginTransaction();
	// create the MetaBlockReader to read from the storage
	MetaBlockReader reader(db, meta_block);
	uint32_t schema_count = reader.Read<uint32_t>();
	for (uint32_t i = 0; i < schema_count; i++) {
		ReadSchema(*con.context, reader);
	}
	con.Commit();
}

//===--------------------------------------------------------------------===//
// Schema
//===--------------------------------------------------------------------===//
void CheckpointManager::WriteSchema(SchemaCatalogEntry &schema) {
	// write the schema data
	schema.Serialize(*metadata_writer);
	// then, we fetch the tables/views/sequences information
	vector<TableCatalogEntry *> tables;
	vector<ViewCatalogEntry *> views;
	schema.Scan(CatalogType::TABLE_ENTRY, [&](CatalogEntry *entry) {
		if (entry->type == CatalogType::TABLE_ENTRY) {
			tables.push_back((TableCatalogEntry *)entry);
		} else if (entry->type == CatalogType::VIEW_ENTRY) {
			views.push_back((ViewCatalogEntry *)entry);
		} else {
			throw NotImplementedException("Catalog type for entries");
		}
	});
	vector<SequenceCatalogEntry *> sequences;
	schema.Scan(CatalogType::SEQUENCE_ENTRY,
	            [&](CatalogEntry *entry) { sequences.push_back((SequenceCatalogEntry *)entry); });

	vector<MacroCatalogEntry *> macros;
	schema.Scan(CatalogType::SCALAR_FUNCTION_ENTRY, [&](CatalogEntry *entry) {
		if (entry->type == CatalogType::MACRO_ENTRY) {
			macros.push_back((MacroCatalogEntry *)entry);
		}
	});

	// write the sequences
	metadata_writer->Write<uint32_t>(sequences.size());
	for (auto &seq : sequences) {
		WriteSequence(*seq);
	}
	// now write the tables
	metadata_writer->Write<uint32_t>(tables.size());
	for (auto &table : tables) {
		WriteTable(*table);
	}
	// now write the views
	metadata_writer->Write<uint32_t>(views.size());
	for (auto &view : views) {
		WriteView(*view);
	}
	// finally write the macro's
	metadata_writer->Write<uint32_t>(macros.size());
	for (auto &macro : macros) {
		WriteMacro(*macro);
	}
}

void CheckpointManager::ReadSchema(ClientContext &context, MetaBlockReader &reader) {
	auto &catalog = Catalog::GetCatalog(db);

	// read the schema and create it in the catalog
	auto info = SchemaCatalogEntry::Deserialize(reader);
	// we set create conflict to ignore to ignore the failure of recreating the main schema
	info->on_conflict = OnCreateConflict::IGNORE_ON_CONFLICT;
	catalog.CreateSchema(context, info.get());

	// read the sequences
	uint32_t seq_count = reader.Read<uint32_t>();
	for (uint32_t i = 0; i < seq_count; i++) {
		ReadSequence(context, reader);
	}
	// read the table count and recreate the tables
	uint32_t table_count = reader.Read<uint32_t>();
	for (uint32_t i = 0; i < table_count; i++) {
		ReadTable(context, reader);
	}
	// now read the views
	uint32_t view_count = reader.Read<uint32_t>();
	for (uint32_t i = 0; i < view_count; i++) {
		ReadView(context, reader);
	}
	// finally read the macro's
	uint32_t macro_count = reader.Read<uint32_t>();
	for (uint32_t i = 0; i < macro_count; i++) {
		ReadMacro(context, reader);
	}
}

//===--------------------------------------------------------------------===//
// Views
//===--------------------------------------------------------------------===//
void CheckpointManager::WriteView(ViewCatalogEntry &view) {
	view.Serialize(*metadata_writer);
}

void CheckpointManager::ReadView(ClientContext &context, MetaBlockReader &reader) {
	auto info = ViewCatalogEntry::Deserialize(reader);

	auto &catalog = Catalog::GetCatalog(db);
	catalog.CreateView(context, info.get());
}

//===--------------------------------------------------------------------===//
// Sequences
//===--------------------------------------------------------------------===//
void CheckpointManager::WriteSequence(SequenceCatalogEntry &seq) {
	seq.Serialize(*metadata_writer);
}

void CheckpointManager::ReadSequence(ClientContext &context, MetaBlockReader &reader) {
	auto info = SequenceCatalogEntry::Deserialize(reader);

	auto &catalog = Catalog::GetCatalog(db);
	catalog.CreateSequence(context, info.get());
}

//===--------------------------------------------------------------------===//
// Macro's
//===--------------------------------------------------------------------===//
void CheckpointManager::WriteMacro(MacroCatalogEntry &macro) {
	macro.Serialize(*metadata_writer);
}

void CheckpointManager::ReadMacro(ClientContext &context, MetaBlockReader &reader) {
	auto info = MacroCatalogEntry::Deserialize(reader);

	auto &catalog = Catalog::GetCatalog(db);
	catalog.CreateFunction(context, info.get());
}

//===--------------------------------------------------------------------===//
// Table Metadata
//===--------------------------------------------------------------------===//
void CheckpointManager::WriteTable(TableCatalogEntry &table) {
	// write the table meta data
	table.Serialize(*metadata_writer);
	// now we need to write the table data
	TableDataWriter writer(db, table, *tabledata_writer);
	auto pointer = writer.WriteTableData();

	//! write the block pointer for the table info
	metadata_writer->Write<block_id_t>(pointer.block_id);
	metadata_writer->Write<uint64_t>(pointer.offset);
}

void CheckpointManager::ReadTable(ClientContext &context, MetaBlockReader &reader) {
	// deserialize the table meta data
	auto info = TableCatalogEntry::Deserialize(reader);
	// bind the info
	auto binder = Binder::CreateBinder(context);
	auto bound_info = binder->BindCreateTableInfo(move(info));

	// now read the actual table data and place it into the create table info
	auto block_id = reader.Read<block_id_t>();
	auto offset = reader.Read<uint64_t>();
	MetaBlockReader table_data_reader(db, block_id);
	table_data_reader.offset = offset;
	TableDataReader data_reader(table_data_reader, *bound_info);
	data_reader.ReadTableData();

	// finally create the table in the catalog
	auto &catalog = Catalog::GetCatalog(db);
	catalog.CreateTable(context, bound_info.get());
}

} // namespace duckdb
