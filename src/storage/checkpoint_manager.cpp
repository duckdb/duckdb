#include "duckdb/storage/checkpoint_manager.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/index_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/scalar_macro_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/sequence_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/type_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/view_catalog_entry.hpp"
#include "duckdb/common/field_writer.hpp"
#include "duckdb/common/serializer.hpp"
#include "duckdb/execution/index/art/art.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/parser/column_definition.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/parser/parsed_data/create_view_info.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression_binder/index_binder.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"
#include "duckdb/storage/block_manager.hpp"
#include "duckdb/storage/checkpoint/table_data_reader.hpp"
#include "duckdb/storage/checkpoint/table_data_writer.hpp"
#include "duckdb/storage/meta_block_reader.hpp"
#include "duckdb/transaction/transaction_manager.hpp"
#include "duckdb/parser/tableref/basetableref.hpp"
#include "duckdb/planner/bound_tableref.hpp"

namespace duckdb {

void ReorderTableEntries(vector<TableCatalogEntry *> &tables);

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
	FlushPartialSegments();
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

	if (config.options.checkpoint_abort == CheckpointAbort::DEBUG_ABORT_BEFORE_HEADER) {
		throw IOException("Checkpoint aborted before header write because of PRAGMA checkpoint_abort flag");
	}

	// finally write the updated header
	DatabaseHeader header;
	header.meta_block = meta_block;
	block_manager.WriteHeader(header);

	if (config.options.checkpoint_abort == CheckpointAbort::DEBUG_ABORT_BEFORE_TRUNCATE) {
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
		if (entry->internal) {
			return;
		}
		if (entry->type == CatalogType::TABLE_ENTRY) {
			tables.push_back((TableCatalogEntry *)entry);
		} else if (entry->type == CatalogType::VIEW_ENTRY) {
			views.push_back((ViewCatalogEntry *)entry);
		} else {
			throw NotImplementedException("Catalog type for entries");
		}
	});
	vector<SequenceCatalogEntry *> sequences;
	schema.Scan(CatalogType::SEQUENCE_ENTRY, [&](CatalogEntry *entry) {
		if (entry->internal) {
			return;
		}
		sequences.push_back((SequenceCatalogEntry *)entry);
	});

	vector<TypeCatalogEntry *> custom_types;
	schema.Scan(CatalogType::TYPE_ENTRY, [&](CatalogEntry *entry) {
		if (entry->internal) {
			return;
		}
		custom_types.push_back((TypeCatalogEntry *)entry);
	});

	vector<ScalarMacroCatalogEntry *> macros;
	schema.Scan(CatalogType::SCALAR_FUNCTION_ENTRY, [&](CatalogEntry *entry) {
		if (entry->internal) {
			return;
		}
		if (entry->type == CatalogType::MACRO_ENTRY) {
			macros.push_back((ScalarMacroCatalogEntry *)entry);
		}
	});

	vector<TableMacroCatalogEntry *> table_macros;
	schema.Scan(CatalogType::TABLE_FUNCTION_ENTRY, [&](CatalogEntry *entry) {
		if (entry->internal) {
			return;
		}
		if (entry->type == CatalogType::TABLE_MACRO_ENTRY) {
			table_macros.push_back((TableMacroCatalogEntry *)entry);
		}
	});

	vector<IndexCatalogEntry *> indexes;
	schema.Scan(CatalogType::INDEX_ENTRY, [&](CatalogEntry *entry) {
		D_ASSERT(!entry->internal);
		indexes.push_back((IndexCatalogEntry *)entry);
	});

	FieldWriter writer(*metadata_writer);
	writer.WriteField<uint32_t>(custom_types.size());
	writer.WriteField<uint32_t>(sequences.size());
	writer.WriteField<uint32_t>(tables.size());
	writer.WriteField<uint32_t>(views.size());
	writer.WriteField<uint32_t>(macros.size());
	writer.WriteField<uint32_t>(table_macros.size());
	writer.WriteField<uint32_t>(indexes.size());
	writer.Finalize();

	// write the custom_types
	for (auto &custom_type : custom_types) {
		WriteType(*custom_type);
	}

	// write the sequences
	for (auto &seq : sequences) {
		WriteSequence(*seq);
	}
	// reorder tables because of foreign key constraint
	ReorderTableEntries(tables);
	// Write the tables
	for (auto &table : tables) {
		WriteTable(*table);
	}
	// Write the views
	for (auto &view : views) {
		WriteView(*view);
	}

	// Write the macros
	for (auto &macro : macros) {
		WriteMacro(*macro);
	}

	// Write the table's macros
	for (auto &macro : table_macros) {
		WriteTableMacro(*macro);
	}
	// Write the indexes
	for (auto &index : indexes) {
		WriteIndex(*index);
	}
}

void CheckpointManager::ReadSchema(ClientContext &context, MetaBlockReader &reader) {
	auto &catalog = Catalog::GetCatalog(db);

	// read the schema and create it in the catalog
	auto info = SchemaCatalogEntry::Deserialize(reader);
	// we set create conflict to ignore to ignore the failure of recreating the main schema
	info->on_conflict = OnCreateConflict::IGNORE_ON_CONFLICT;
	catalog.CreateSchema(context, info.get());

	// first read all the counts
	FieldReader field_reader(reader);
	uint32_t enum_count = field_reader.ReadRequired<uint32_t>();
	uint32_t seq_count = field_reader.ReadRequired<uint32_t>();
	uint32_t table_count = field_reader.ReadRequired<uint32_t>();
	uint32_t view_count = field_reader.ReadRequired<uint32_t>();
	uint32_t macro_count = field_reader.ReadRequired<uint32_t>();
	uint32_t table_macro_count = field_reader.ReadRequired<uint32_t>();
	uint32_t table_index_count = field_reader.ReadRequired<uint32_t>();
	field_reader.Finalize();

	// now read the enums
	for (uint32_t i = 0; i < enum_count; i++) {
		ReadType(context, reader);
	}

	// read the sequences
	for (uint32_t i = 0; i < seq_count; i++) {
		ReadSequence(context, reader);
	}
	// read the table count and recreate the tables
	for (uint32_t i = 0; i < table_count; i++) {
		ReadTable(context, reader);
	}
	// now read the views
	for (uint32_t i = 0; i < view_count; i++) {
		ReadView(context, reader);
	}

	// finally read the macro's
	for (uint32_t i = 0; i < macro_count; i++) {
		ReadMacro(context, reader);
	}

	for (uint32_t i = 0; i < table_macro_count; i++) {
		ReadTableMacro(context, reader);
	}
	for (uint32_t i = 0; i < table_index_count; i++) {
		ReadIndex(context, reader);
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
// Indexes
//===--------------------------------------------------------------------===//
void CheckpointManager::WriteIndex(IndexCatalogEntry &index_catalog) {
	// Write the index data and metadata
	// Serialize the necessary meta data for index catalog construction.
	auto root_offset = index_catalog.index->Serialize(*tabledata_writer);
	index_catalog.Serialize(*metadata_writer);
	// Serialize the Block id and offset of root node
	metadata_writer->Write(root_offset.block_id);
	metadata_writer->Write(root_offset.offset);
}

void CheckpointManager::ReadIndex(ClientContext &context, MetaBlockReader &reader) {

	// Deserialize the index meta data
	auto info = IndexCatalogEntry::Deserialize(reader);

	// Create index in the catalog
	auto &catalog = Catalog::GetCatalog(db);
	auto schema_catalog = catalog.GetSchema(context, info->schema);
	auto table_catalog =
	    (TableCatalogEntry *)catalog.GetEntry(context, CatalogType::TABLE_ENTRY, info->schema, info->table->table_name);
	auto index_catalog = (IndexCatalogEntry *)schema_catalog->CreateIndex(context, info.get(), table_catalog);
	index_catalog->info = table_catalog->storage->info;
	// Here we just gotta read the root node
	auto root_block_id = reader.Read<block_id_t>();
	auto root_offset = reader.Read<uint32_t>();

	// create an adaptive radix tree around the expressions
	vector<unique_ptr<Expression>> unbound_expressions;
	vector<unique_ptr<ParsedExpression>> parsed_expressions;

	for (auto &p_exp : info->parsed_expressions) {
		parsed_expressions.push_back(p_exp->Copy());
	}

	auto binder = Binder::CreateBinder(context);
	auto table_ref = (TableRef *)info->table.get();
	auto bound_table = binder->Bind(*table_ref);
	D_ASSERT(bound_table->type == TableReferenceType::BASE_TABLE);
	IndexBinder idx_binder(*binder, context);
	unbound_expressions.reserve(parsed_expressions.size());
	for (auto &expr : parsed_expressions) {
		unbound_expressions.push_back(idx_binder.Bind(expr));
	}

	if (parsed_expressions.empty()) {
		// If no parsed_expressions are present, this means this is a PK/FK index, so we create the necessary bound
		// column refs
		unbound_expressions.reserve(info->column_ids.size());
		for (idx_t key_nr = 0; key_nr < info->column_ids.size(); key_nr++) {
			unbound_expressions.push_back(make_unique<BoundColumnRefExpression>(
			    table_catalog->columns[info->column_ids[key_nr]].GetName(),
			    table_catalog->columns[info->column_ids[key_nr]].GetType(), ColumnBinding(0, key_nr)));
		}
	}

	switch (info->index_type) {
	case IndexType::ART: {
		auto art = make_unique<ART>(info->column_ids, move(unbound_expressions), info->constraint_type, db,
		                            root_block_id, root_offset);
		index_catalog->index = art.get();
		table_catalog->storage->info->indexes.AddIndex(move(art));
		break;
	}
	default:
		throw InternalException("Can't read this index type");
	}
}

//===--------------------------------------------------------------------===//
// Custom Types
//===--------------------------------------------------------------------===//
void CheckpointManager::WriteType(TypeCatalogEntry &table) {
	table.Serialize(*metadata_writer);
}

void CheckpointManager::ReadType(ClientContext &context, MetaBlockReader &reader) {
	auto info = TypeCatalogEntry::Deserialize(reader);

	auto &catalog = Catalog::GetCatalog(db);
	catalog.CreateType(context, info.get());
}

//===--------------------------------------------------------------------===//
// Macro's
//===--------------------------------------------------------------------===//
void CheckpointManager::WriteMacro(ScalarMacroCatalogEntry &macro) {
	macro.Serialize(*metadata_writer);
}

void CheckpointManager::ReadMacro(ClientContext &context, MetaBlockReader &reader) {
	auto info = ScalarMacroCatalogEntry::Deserialize(reader);
	auto &catalog = Catalog::GetCatalog(db);
	catalog.CreateFunction(context, info.get());
}

void CheckpointManager::WriteTableMacro(TableMacroCatalogEntry &macro) {
	macro.Serialize(*metadata_writer);
}

void CheckpointManager::ReadTableMacro(ClientContext &context, MetaBlockReader &reader) {
	auto info = TableMacroCatalogEntry::Deserialize(reader);
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
	TableDataWriter writer(db, *this, table, *tabledata_writer, *metadata_writer);
	writer.WriteTableData();
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

	// Get any indexes block info
	idx_t num_indexes = reader.Read<idx_t>();
	for (idx_t i = 0; i < num_indexes; i++) {
		auto idx_block_id = reader.Read<idx_t>();
		auto idx_offset = reader.Read<idx_t>();
		bound_info->indexes.emplace_back(idx_block_id, idx_offset);
	}

	// finally create the table in the catalog
	auto &catalog = Catalog::GetCatalog(db);
	catalog.CreateTable(context, bound_info.get());
}

//===--------------------------------------------------------------------===//
// Partial Blocks
//===--------------------------------------------------------------------===//
bool CheckpointManager::GetPartialBlock(ColumnSegment *segment, idx_t segment_size, block_id_t &block_id,
                                        uint32_t &offset_in_block, PartialBlock *&partial_block_ptr,
                                        unique_ptr<PartialBlock> &owned_partial_block) {
	auto entry = partially_filled_blocks.lower_bound(segment_size);
	if (entry == partially_filled_blocks.end()) {
		return false;
	}
	// found a partially filled block! fill in the info
	auto partial_block = move(entry->second);
	partial_block_ptr = partial_block.get();
	block_id = partial_block->block_id;
	offset_in_block = Storage::BLOCK_SIZE - entry->first;
	partially_filled_blocks.erase(entry);
	PartialColumnSegment partial_segment;
	partial_segment.segment = segment;
	partial_segment.offset_in_block = offset_in_block;
	partial_block->segments.push_back(partial_segment);

	D_ASSERT(offset_in_block > 0);
	D_ASSERT(ValueIsAligned(offset_in_block));

	// check if the block is STILL partially filled after adding the segment_size
	auto new_size = AlignValue(offset_in_block + segment_size);
	if (new_size <= CheckpointManager::PARTIAL_BLOCK_THRESHOLD) {
		// the block is still partially filled: add it to the partially_filled_blocks list
		auto new_space_left = Storage::BLOCK_SIZE - new_size;
		partially_filled_blocks.insert(make_pair(new_space_left, move(partial_block)));
		// should not write the block yet: perhaps more columns will be added
	} else {
		// we are done with this block after the current write: write it to disk
		owned_partial_block = move(partial_block);
	}
	return true;
}

void CheckpointManager::RegisterPartialBlock(ColumnSegment *segment, idx_t segment_size, block_id_t block_id) {
	D_ASSERT(segment_size <= CheckpointManager::PARTIAL_BLOCK_THRESHOLD);
	auto partial_block = make_unique<PartialBlock>();
	partial_block->block_id = block_id;
	partial_block->block = segment->block;

	PartialColumnSegment partial_segment;
	partial_segment.segment = segment;
	partial_segment.offset_in_block = 0;
	partial_block->segments.push_back(partial_segment);
	auto space_left = Storage::BLOCK_SIZE - AlignValue(segment_size);
	partially_filled_blocks.insert(make_pair(space_left, move(partial_block)));
}

void CheckpointManager::FlushPartialSegments() {
	for (auto &entry : partially_filled_blocks) {
		entry.second->FlushToDisk(db);
	}
}

void PartialBlock::FlushToDisk(DatabaseInstance &db) {
	auto &buffer_manager = BufferManager::GetBufferManager(db);
	auto &block_manager = BlockManager::GetBlockManager(db);

	// the data for the block might already exists in-memory of our block
	// instead of copying the data we alter some metadata so the buffer points to an on-disk block
	block = buffer_manager.ConvertToPersistent(block_manager, block_id, move(block));

	// now set this block as the block for all segments
	for (auto &seg : segments) {
		seg.segment->ConvertToPersistent(block, block_id, seg.offset_in_block);
	}
}

} // namespace duckdb
