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
#include "duckdb/storage/metadata/metadata_reader.hpp"
#include "duckdb/storage/table/column_checkpoint_state.hpp"
#include "duckdb/transaction/transaction_manager.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/catalog/dependency_manager.hpp"

namespace duckdb {

void ReorderTableEntries(catalog_entry_vector_t &tables);

SingleFileCheckpointWriter::SingleFileCheckpointWriter(AttachedDatabase &db, BlockManager &block_manager)
    : CheckpointWriter(db), partial_block_manager(block_manager, CheckpointType::FULL_CHECKPOINT) {
}

BlockManager &SingleFileCheckpointWriter::GetBlockManager() {
	auto &storage_manager = db.GetStorageManager().Cast<SingleFileStorageManager>();
	return *storage_manager.block_manager;
}

MetadataWriter &SingleFileCheckpointWriter::GetMetadataWriter() {
	return *metadata_writer;
}

MetadataManager &SingleFileCheckpointWriter::GetMetadataManager() {
	return GetBlockManager().GetMetadataManager();
}

unique_ptr<TableDataWriter> SingleFileCheckpointWriter::GetTableDataWriter(TableCatalogEntry &table) {
	return make_uniq<SingleFileTableDataWriter>(*this, table, *table_metadata_writer, GetMetadataWriter());
}

void SingleFileCheckpointWriter::CreateCheckpoint() {
	auto &config = DBConfig::Get(db);
	auto &storage_manager = db.GetStorageManager().Cast<SingleFileStorageManager>();
	if (storage_manager.InMemory()) {
		return;
	}
	// assert that the checkpoint manager hasn't been used before
	D_ASSERT(!metadata_writer);

	auto &block_manager = GetBlockManager();
	auto &metadata_manager = GetMetadataManager();

	//! Set up the writers for the checkpoints
	metadata_writer = make_uniq<MetadataWriter>(metadata_manager);
	table_metadata_writer = make_uniq<MetadataWriter>(metadata_manager);

	// get the id of the first meta block
	auto meta_block = metadata_writer->GetMetaBlockPointer();

	vector<reference<SchemaCatalogEntry>> schemas;
	// we scan the set of committed schemas
	auto &catalog = Catalog::GetCatalog(db).Cast<DuckCatalog>();

	catalog_entry_vector_t catalog_entries;
	D_ASSERT(catalog.IsDuckCatalog());

	auto &duck_catalog = catalog.Cast<DuckCatalog>();
	auto &dependency_manager = duck_catalog.GetDependencyManager();
	catalog_entries = dependency_manager.GetExportOrder();

	// write the actual data into the database
	// write the amount of entries (including schemas)
	metadata_writer->Write<uint32_t>(catalog_entries.size());

	for (auto &entry : catalog_entries) {
		WriteEntry(entry);
	}
	partial_block_manager.FlushPartialBlocks();
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
	header.meta_block = meta_block.block_pointer;
	block_manager.WriteHeader(header);

	if (config.options.checkpoint_abort == CheckpointAbort::DEBUG_ABORT_BEFORE_TRUNCATE) {
		throw FatalException("Checkpoint aborted before truncate because of PRAGMA checkpoint_abort flag");
	}

	// truncate the WAL
	wal->Truncate(0);

	// truncate the file
	block_manager.Truncate();

	metadata_manager.MarkBlocksAsModified();
}

MetadataManager &SingleFileCheckpointReader::GetMetadataManager() {
	return storage.block_manager->GetMetadataManager();
}

void SingleFileCheckpointReader::LoadFromStorage() {
	auto &block_manager = *storage.block_manager;
	auto &metadata_manager = GetMetadataManager();
	MetaBlockPointer meta_block(block_manager.GetMetaBlock(), 0);
	if (!meta_block.IsValid()) {
		// storage is empty
		return;
	}

	Connection con(storage.GetDatabase());
	con.BeginTransaction();
	// create the MetadataReader to read from the storage
	MetadataReader reader(metadata_manager, meta_block);
	//	reader.SetContext(*con.context);
	LoadCheckpoint(*con.context, reader);
	con.Commit();
}

void CheckpointReader::LoadCheckpoint(ClientContext &context, MetadataReader &reader) {
	uint32_t entry_count = reader.Read<uint32_t>();
	for (uint32_t i = 0; i < entry_count; i++) {
		ReadEntry(context, reader);
	}
}

void CheckpointWriter::WriteEntry(CatalogEntry &entry) {
	FieldWriter writer(GetMetadataWriter());
	writer.WriteField<CatalogType>(entry.type);
	writer.Finalize();

	switch (entry.type) {
	case CatalogType::SCHEMA_ENTRY: {
		auto &schema = entry.Cast<SchemaCatalogEntry>();
		WriteSchema(schema);
		break;
	}
	case CatalogType::TYPE_ENTRY: {
		auto &custom_type = entry.Cast<TypeCatalogEntry>();
		WriteType(custom_type);
		break;
	}
	case CatalogType::SEQUENCE_ENTRY: {
		auto &seq = entry.Cast<SequenceCatalogEntry>();
		WriteSequence(seq);
		break;
	}
	case CatalogType::TABLE_ENTRY: {
		auto &table = entry.Cast<TableCatalogEntry>();
		WriteTable(table);
		break;
	}
	case CatalogType::VIEW_ENTRY: {
		auto &view = entry.Cast<ViewCatalogEntry>();
		WriteView(view);
		break;
	}
	case CatalogType::MACRO_ENTRY: {
		auto &macro = entry.Cast<ScalarMacroCatalogEntry>();
		WriteMacro(macro);
		break;
	}
	case CatalogType::TABLE_MACRO_ENTRY: {
		auto &macro = entry.Cast<TableMacroCatalogEntry>();
		WriteTableMacro(macro);
		break;
	}
	case CatalogType::INDEX_ENTRY: {
		auto &index = entry.Cast<IndexCatalogEntry>();
		WriteIndex(index);
		break;
	}
	default:
		throw InternalException("Unrecognized catalog type in CheckpointWriter::WriteEntry");
	}
}

//===--------------------------------------------------------------------===//
// Schema
//===--------------------------------------------------------------------===//
void CheckpointWriter::WriteSchema(SchemaCatalogEntry &schema) {
	// write the schema data
	schema.Serialize(GetMetadataWriter());
}

void CheckpointReader::ReadEntry(ClientContext &context, MetadataReader &reader) {
	FieldReader field_reader(reader);
	auto type = field_reader.ReadRequired<CatalogType>();
	field_reader.Finalize();
	switch (type) {
	case CatalogType::SCHEMA_ENTRY: {
		ReadSchema(context, reader);
		break;
	}
	case CatalogType::TYPE_ENTRY: {
		ReadType(context, reader);
		break;
	}
	case CatalogType::SEQUENCE_ENTRY: {
		ReadSequence(context, reader);
		break;
	}
	case CatalogType::TABLE_ENTRY: {
		ReadTable(context, reader);
		break;
	}
	case CatalogType::VIEW_ENTRY: {
		ReadView(context, reader);
		break;
	}
	case CatalogType::MACRO_ENTRY: {
		ReadMacro(context, reader);
		break;
	}
	case CatalogType::TABLE_MACRO_ENTRY: {
		ReadTableMacro(context, reader);
		break;
	}
	case CatalogType::INDEX_ENTRY: {
		ReadIndex(context, reader);
		break;
	}
	default:
		throw InternalException("Unrecognized catalog type in CheckpointWriter::WriteEntry");
	}
}

void CheckpointReader::ReadSchema(ClientContext &context, MetadataReader &reader) {
	// read the schema and create it in the catalog
	reader.SetContext(context);
	auto info = CatalogEntry::Deserialize(reader);
	// we set create conflict to ignore to ignore the failure of recreating the main schema
	info->on_conflict = OnCreateConflict::IGNORE_ON_CONFLICT;
	catalog.CreateSchema(context, info->Cast<CreateSchemaInfo>());
}

//===--------------------------------------------------------------------===//
// Views
//===--------------------------------------------------------------------===//
void CheckpointWriter::WriteView(ViewCatalogEntry &view) {
	view.Serialize(GetMetadataWriter());
}

void CheckpointReader::ReadView(ClientContext &context, MetadataReader &reader) {
	auto info = CatalogEntry::Deserialize(reader);
	catalog.CreateView(context, info->Cast<CreateViewInfo>());
}

//===--------------------------------------------------------------------===//
// Sequences
//===--------------------------------------------------------------------===//
void CheckpointWriter::WriteSequence(SequenceCatalogEntry &seq) {
	seq.Serialize(GetMetadataWriter());
}

void CheckpointReader::ReadSequence(ClientContext &context, MetadataReader &reader) {
	auto info = SequenceCatalogEntry::Deserialize(reader);
	catalog.CreateSequence(context, info->Cast<CreateSequenceInfo>());
}

//===--------------------------------------------------------------------===//
// Indexes
//===--------------------------------------------------------------------===//
void CheckpointWriter::WriteIndex(IndexCatalogEntry &index_catalog) {
	// The index data should already have been written as part of WriteTableData.
	// Here, we need only serialize the pointer to that data.
	auto root_offset = index_catalog.index->GetSerializedDataPointer();
	auto &metadata_writer = GetMetadataWriter();
	index_catalog.Serialize(metadata_writer);
	// Serialize the Block id and offset of root node
	metadata_writer.Write(root_offset.block_id);
	metadata_writer.Write(root_offset.offset);
}

void CheckpointReader::ReadIndex(ClientContext &context, MetadataReader &reader) {
	// deserialize the index metadata
	auto info = IndexCatalogEntry::Deserialize(reader);
	auto &index_info = info->Cast<CreateIndexInfo>();

	// create the index in the catalog
	auto &schema_catalog = catalog.GetSchema(context, info->schema);
	auto &table_catalog =
	    catalog.GetEntry(context, CatalogType::TABLE_ENTRY, info->schema, index_info.table).Cast<DuckTableEntry>();
	auto &index_catalog = schema_catalog.CreateIndex(context, index_info, table_catalog)->Cast<DuckIndexEntry>();
	index_catalog.info = table_catalog.GetStorage().info;

	// we deserialize the index lazily, i.e., we do not need to load any node information
	// except the root block id and offset
	auto root_block_id = reader.Read<block_id_t>();
	auto root_offset = reader.Read<uint32_t>();

	// obtain the expressions of the ART from the index metadata
	vector<unique_ptr<Expression>> unbound_expressions;
	vector<unique_ptr<ParsedExpression>> parsed_expressions;
	for (auto &p_exp : index_info.parsed_expressions) {
		parsed_expressions.push_back(p_exp->Copy());
	}

	// bind the parsed expressions
	// add the table to the bind context
	auto binder = Binder::CreateBinder(context);
	vector<LogicalType> column_types;
	vector<string> column_names;
	for (auto &col : table_catalog.GetColumns().Logical()) {
		column_types.push_back(col.Type());
		column_names.push_back(col.Name());
	}
	vector<column_t> column_ids;
	binder->bind_context.AddBaseTable(0, index_info.table, column_names, column_types, column_ids, &table_catalog);
	IndexBinder idx_binder(*binder, context);
	unbound_expressions.reserve(parsed_expressions.size());
	for (auto &expr : parsed_expressions) {
		unbound_expressions.push_back(idx_binder.Bind(expr));
	}

	if (parsed_expressions.empty()) {
		// this is a PK/FK index: we create the necessary bound column ref expressions
		unbound_expressions.reserve(index_info.column_ids.size());
		for (idx_t key_nr = 0; key_nr < index_info.column_ids.size(); key_nr++) {
			auto &col = table_catalog.GetColumn(LogicalIndex(index_info.column_ids[key_nr]));
			unbound_expressions.push_back(
			    make_uniq<BoundColumnRefExpression>(col.GetName(), col.GetType(), ColumnBinding(0, key_nr)));
		}
	}

	// create the index and add it to the storage
	switch (index_info.index_type) {
	case IndexType::ART: {
		auto &storage = table_catalog.GetStorage();
		auto art =
		    make_uniq<ART>(index_info.column_ids, TableIOManager::Get(storage), std::move(unbound_expressions),
		                   index_info.constraint_type, storage.db, nullptr, BlockPointer(root_block_id, root_offset));
		index_catalog.index = art.get();
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
	type.Serialize(GetMetadataWriter());
}

void CheckpointReader::ReadType(ClientContext &context, MetadataReader &reader) {
	auto info = TypeCatalogEntry::Deserialize(reader);
	catalog.CreateType(context, info->Cast<CreateTypeInfo>());
}

//===--------------------------------------------------------------------===//
// Macro's
//===--------------------------------------------------------------------===//
void CheckpointWriter::WriteMacro(ScalarMacroCatalogEntry &macro) {
	macro.Serialize(GetMetadataWriter());
}

void CheckpointReader::ReadMacro(ClientContext &context, MetadataReader &reader) {
	auto info = MacroCatalogEntry::Deserialize(reader);
	catalog.CreateFunction(context, info->Cast<CreateMacroInfo>());
}

void CheckpointWriter::WriteTableMacro(TableMacroCatalogEntry &macro) {
	macro.Serialize(GetMetadataWriter());
}

void CheckpointReader::ReadTableMacro(ClientContext &context, MetadataReader &reader) {
	auto info = MacroCatalogEntry::Deserialize(reader);
	catalog.CreateFunction(context, info->Cast<CreateMacroInfo>());
}

//===--------------------------------------------------------------------===//
// Table Metadata
//===--------------------------------------------------------------------===//
void CheckpointWriter::WriteTable(TableCatalogEntry &table) {
	// write the table meta data
	table.Serialize(GetMetadataWriter());
	// now we need to write the table data.
	if (auto writer = GetTableDataWriter(table)) {
		writer->WriteTableData();
	}
}

void CheckpointReader::ReadTable(ClientContext &context, MetadataReader &reader) {
	// deserialize the table meta data
	auto info = TableCatalogEntry::Deserialize(reader);
	// bind the info
	auto binder = Binder::CreateBinder(context);
	auto &schema = catalog.GetSchema(context, info->schema);
	auto bound_info = binder->BindCreateTableInfo(std::move(info), schema);

	// now read the actual table data and place it into the create table info
	ReadTableData(context, reader, *bound_info);

	// finally create the table in the catalog
	catalog.CreateTable(context, *bound_info);
}

void CheckpointReader::ReadTableData(ClientContext &context, MetadataReader &reader, BoundCreateTableInfo &bound_info) {
	auto block_pointer = reader.Read<idx_t>();
	auto offset = reader.Read<uint64_t>();

	MetadataReader table_data_reader(reader.GetMetadataManager(), MetaBlockPointer(block_pointer, offset));
	TableDataReader data_reader(table_data_reader, bound_info);

	data_reader.ReadTableData();
	bound_info.data->total_rows = reader.Read<idx_t>();

	// Get any indexes block info
	idx_t num_indexes = reader.Read<idx_t>();
	for (idx_t i = 0; i < num_indexes; i++) {
		auto idx_block_id = reader.Read<block_id_t>();
		auto idx_offset = reader.Read<uint32_t>();
		bound_info.indexes.emplace_back(idx_block_id, idx_offset);
	}
}

} // namespace duckdb
