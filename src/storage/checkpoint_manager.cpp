#include "duckdb/storage/checkpoint_manager.hpp"

#include "duckdb/catalog/duck_catalog.hpp"
#include "duckdb/catalog/catalog_entry/duck_index_entry.hpp"
#include "duckdb/catalog/catalog_entry/scalar_macro_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/sequence_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
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
#include "duckdb/parser/tableref/basetableref.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/bound_tableref.hpp"
#include "duckdb/planner/expression_binder/index_binder.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"
#include "duckdb/storage/block_manager.hpp"
#include "duckdb/storage/checkpoint/table_data_reader.hpp"
#include "duckdb/storage/checkpoint/table_data_writer.hpp"
#include "duckdb/storage/meta_block_reader.hpp"
#include "duckdb/storage/table/column_checkpoint_state.hpp"
#include "duckdb/transaction/transaction_manager.hpp"
#include "duckdb/main/attached_database.hpp"

namespace duckdb {

void ReorderTableEntries(vector<TableCatalogEntry *> &tables);

SingleFileCheckpointWriter::SingleFileCheckpointWriter(AttachedDatabase &db, BlockManager &block_manager)
    : CheckpointWriter(db), partial_block_manager(block_manager) {
}

BlockManager &SingleFileCheckpointWriter::GetBlockManager() {
	auto &storage_manager = (SingleFileStorageManager &)db.GetStorageManager();
	return *storage_manager.block_manager;
}

MetaBlockWriter &SingleFileCheckpointWriter::GetMetaBlockWriter() {
	return *metadata_writer;
}

unique_ptr<TableDataWriter> SingleFileCheckpointWriter::GetTableDataWriter(TableCatalogEntry &table) {
	return make_uniq<SingleFileTableDataWriter>(*this, table, *table_metadata_writer, GetMetaBlockWriter());
}

void SingleFileCheckpointWriter::CreateCheckpoint() {
	auto &config = DBConfig::Get(db);
	auto &storage_manager = (SingleFileStorageManager &)db.GetStorageManager();
	if (storage_manager.InMemory()) {
		return;
	}
	// assert that the checkpoint manager hasn't been used before
	D_ASSERT(!metadata_writer);

	auto &block_manager = GetBlockManager();

	//! Set up the writers for the checkpoints
	metadata_writer = make_uniq<MetaBlockWriter>(block_manager);
	table_metadata_writer = make_uniq<MetaBlockWriter>(block_manager);

	// get the id of the first meta block
	block_id_t meta_block = metadata_writer->GetBlockPointer().block_id;

	vector<SchemaCatalogEntry *> schemas;
	// we scan the set of committed schemas
	auto &catalog = (DuckCatalog &)Catalog::GetCatalog(db);
	catalog.ScanSchemas([&](CatalogEntry *entry) { schemas.push_back((SchemaCatalogEntry *)entry); });
	// write the actual data into the database
	// write the amount of schemas
	metadata_writer->Write<uint32_t>(schemas.size());
	for (auto &schema : schemas) {
		WriteSchema(*schema);
	}
	partial_block_manager.FlushPartialBlocks();
	// flush the meta data to disk
	metadata_writer->Flush();
	table_metadata_writer->Flush();

	// write a checkpoint flag to the WAL
	// this protects against the rare event that the database crashes AFTER writing the file, but BEFORE truncating the
	// WAL we write an entry CHECKPOINT "meta_block_id" into the WAL upon loading, if we see there is an entry
	// CHECKPOINT "meta_block_id", and the id MATCHES the head idin the file we know that the database was successfully
	// checkpointed, so we know that we should avoid replaying the WAL to avoid duplicating data
	auto wal = storage_manager.GetWriteAheadLog();
	wal->WriteCheckpoint(meta_block);
	wal->Flush();

	if (config.options.checkpoint_abort == CheckpointAbort::DEBUG_ABORT_BEFORE_HEADER) {
		throw FatalException("Checkpoint aborted before header write because of PRAGMA checkpoint_abort flag");
	}

	// finally write the updated header
	DatabaseHeader header;
	header.meta_block = meta_block;
	block_manager.WriteHeader(header);

	if (config.options.checkpoint_abort == CheckpointAbort::DEBUG_ABORT_BEFORE_TRUNCATE) {
		throw FatalException("Checkpoint aborted before truncate because of PRAGMA checkpoint_abort flag");
	}

	// truncate the WAL
	wal->Truncate(0);

	// mark all blocks written as part of the metadata as modified
	metadata_writer->MarkWrittenBlocks();
	table_metadata_writer->MarkWrittenBlocks();
}

void SingleFileCheckpointReader::LoadFromStorage() {
	auto &block_manager = *storage.block_manager;
	block_id_t meta_block = block_manager.GetMetaBlock();
	if (meta_block < 0) {
		// storage is empty
		return;
	}

	Connection con(storage.GetDatabase());
	con.BeginTransaction();
	// create the MetaBlockReader to read from the storage
	MetaBlockReader reader(block_manager, meta_block);
	reader.SetCatalog(&catalog.GetAttached().GetCatalog());
	reader.SetContext(con.context.get());
	LoadCheckpoint(*con.context, reader);
	con.Commit();
}

void CheckpointReader::LoadCheckpoint(ClientContext &context, MetaBlockReader &reader) {
	uint32_t schema_count = reader.Read<uint32_t>();
	for (uint32_t i = 0; i < schema_count; i++) {
		ReadSchema(context, reader);
	}
}

//===--------------------------------------------------------------------===//
// Schema
//===--------------------------------------------------------------------===//
void CheckpointWriter::WriteSchema(SchemaCatalogEntry &schema) {
	// write the schema data
	schema.Serialize(GetMetaBlockWriter());
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

	FieldWriter writer(GetMetaBlockWriter());
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

void CheckpointReader::ReadSchema(ClientContext &context, MetaBlockReader &reader) {
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
void CheckpointWriter::WriteView(ViewCatalogEntry &view) {
	view.Serialize(GetMetaBlockWriter());
}

void CheckpointReader::ReadView(ClientContext &context, MetaBlockReader &reader) {
	auto info = ViewCatalogEntry::Deserialize(reader, context);
	catalog.CreateView(context, info.get());
}

//===--------------------------------------------------------------------===//
// Sequences
//===--------------------------------------------------------------------===//
void CheckpointWriter::WriteSequence(SequenceCatalogEntry &seq) {
	seq.Serialize(GetMetaBlockWriter());
}

void CheckpointReader::ReadSequence(ClientContext &context, MetaBlockReader &reader) {
	auto info = SequenceCatalogEntry::Deserialize(reader);
	catalog.CreateSequence(context, info.get());
}

//===--------------------------------------------------------------------===//
// Indexes
//===--------------------------------------------------------------------===//
void CheckpointWriter::WriteIndex(IndexCatalogEntry &index_catalog) {
	// The index data should already have been written as part of WriteTableData.
	// Here, we need only serialize the pointer to that data.
	auto root_offset = index_catalog.index->GetSerializedDataPointer();
	auto &metadata_writer = GetMetaBlockWriter();
	index_catalog.Serialize(metadata_writer);
	// Serialize the Block id and offset of root node
	metadata_writer.Write(root_offset.block_id);
	metadata_writer.Write(root_offset.offset);
}

void CheckpointReader::ReadIndex(ClientContext &context, MetaBlockReader &reader) {

	// deserialize the index metadata
	auto info = IndexCatalogEntry::Deserialize(reader, context);

	// create the index in the catalog
	auto schema_catalog = catalog.GetSchema(context, info->schema);
	auto table_catalog =
	    (DuckTableEntry *)catalog.GetEntry(context, CatalogType::TABLE_ENTRY, info->schema, info->table->table_name);
	auto index_catalog = (DuckIndexEntry *)schema_catalog->CreateIndex(context, info.get(), table_catalog);
	index_catalog->info = table_catalog->GetStorage().info;

	// we deserialize the index lazily, i.e., we do not need to load any node information
	// except the root block id and offset
	auto root_block_id = reader.Read<block_id_t>();
	auto root_offset = reader.Read<uint32_t>();

	// obtain the expressions of the ART from the index metadata
	vector<unique_ptr<Expression>> unbound_expressions;
	vector<unique_ptr<ParsedExpression>> parsed_expressions;
	for (auto &p_exp : info->parsed_expressions) {
		parsed_expressions.push_back(p_exp->Copy());
	}

	// bind the parsed expressions
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
		// this is a PK/FK index: we create the necessary bound column ref expressions
		unbound_expressions.reserve(info->column_ids.size());
		for (idx_t key_nr = 0; key_nr < info->column_ids.size(); key_nr++) {
			auto &col = table_catalog->GetColumn(LogicalIndex(info->column_ids[key_nr]));
			unbound_expressions.push_back(
			    make_uniq<BoundColumnRefExpression>(col.GetName(), col.GetType(), ColumnBinding(0, key_nr)));
		}
	}

	// create the index and add it to the storage
	switch (info->index_type) {
	case IndexType::ART: {
		auto &storage = table_catalog->GetStorage();
		auto art = make_uniq<ART>(info->column_ids, TableIOManager::Get(storage), std::move(unbound_expressions),
		                            info->constraint_type, storage.db, root_block_id, root_offset);
		index_catalog->index = art.get();
		storage.info->indexes.AddIndex(std::move(art));
		break;
	}
	default:
		throw InternalException("Unknown index type for ReadIndex");
	}
}

//===--------------------------------------------------------------------===//
// Custom Types
//===--------------------------------------------------------------------===//
void CheckpointWriter::WriteType(TypeCatalogEntry &type) {
	type.Serialize(GetMetaBlockWriter());
}

void CheckpointReader::ReadType(ClientContext &context, MetaBlockReader &reader) {
	auto info = TypeCatalogEntry::Deserialize(reader);
	auto catalog_entry = (TypeCatalogEntry *)catalog.CreateType(context, info.get());
	if (info->type.id() == LogicalTypeId::ENUM) {
		EnumType::SetCatalog(info->type, catalog_entry);
	}
}

//===--------------------------------------------------------------------===//
// Macro's
//===--------------------------------------------------------------------===//
void CheckpointWriter::WriteMacro(ScalarMacroCatalogEntry &macro) {
	macro.Serialize(GetMetaBlockWriter());
}

void CheckpointReader::ReadMacro(ClientContext &context, MetaBlockReader &reader) {
	auto info = ScalarMacroCatalogEntry::Deserialize(reader, context);
	catalog.CreateFunction(context, info.get());
}

void CheckpointWriter::WriteTableMacro(TableMacroCatalogEntry &macro) {
	macro.Serialize(GetMetaBlockWriter());
}

void CheckpointReader::ReadTableMacro(ClientContext &context, MetaBlockReader &reader) {
	auto info = TableMacroCatalogEntry::Deserialize(reader, context);
	catalog.CreateFunction(context, info.get());
}

//===--------------------------------------------------------------------===//
// Table Metadata
//===--------------------------------------------------------------------===//
void CheckpointWriter::WriteTable(TableCatalogEntry &table) {
	// write the table meta data
	table.Serialize(GetMetaBlockWriter());
	// now we need to write the table data.
	if (auto writer = GetTableDataWriter(table)) {
		writer->WriteTableData();
	}
}

void CheckpointReader::ReadTable(ClientContext &context, MetaBlockReader &reader) {
	// deserialize the table meta data
	auto info = TableCatalogEntry::Deserialize(reader, context);
	// bind the info
	auto binder = Binder::CreateBinder(context);
	auto schema = catalog.GetSchema(context, info->schema);
	auto bound_info = binder->BindCreateTableInfo(std::move(info), schema);

	// now read the actual table data and place it into the create table info
	ReadTableData(context, reader, *bound_info);

	// finally create the table in the catalog
	catalog.CreateTable(context, bound_info.get());
}

void CheckpointReader::ReadTableData(ClientContext &context, MetaBlockReader &reader,
                                     BoundCreateTableInfo &bound_info) {
	auto block_id = reader.Read<block_id_t>();
	auto offset = reader.Read<uint64_t>();

	MetaBlockReader table_data_reader(reader.block_manager, block_id);
	table_data_reader.offset = offset;
	TableDataReader data_reader(table_data_reader, bound_info);

	data_reader.ReadTableData();
	bound_info.data->total_rows = reader.Read<idx_t>();

	// Get any indexes block info
	idx_t num_indexes = reader.Read<idx_t>();
	for (idx_t i = 0; i < num_indexes; i++) {
		auto idx_block_id = reader.Read<idx_t>();
		auto idx_offset = reader.Read<idx_t>();
		bound_info.indexes.emplace_back(idx_block_id, idx_offset);
	}
}

} // namespace duckdb
