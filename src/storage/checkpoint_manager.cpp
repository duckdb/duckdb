#include "duckdb/storage/checkpoint_manager.hpp"

#include "duckdb/catalog/catalog_entry/duck_index_entry.hpp"
#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/catalog/catalog_entry/scalar_macro_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/sequence_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/type_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/view_catalog_entry.hpp"
#include "duckdb/catalog/duck_catalog.hpp"
#include "duckdb/common/serializer/binary_deserializer.hpp"
#include "duckdb/common/serializer/binary_serializer.hpp"
#include "duckdb/execution/index/art/art.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/parser/parsed_data/create_view_info.hpp"
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
	return make_uniq<SingleFileTableDataWriter>(*this, table, *table_metadata_writer);
}

static catalog_entry_vector_t GetCatalogEntries(vector<reference<SchemaCatalogEntry>> &schemas) {
	catalog_entry_vector_t entries;
	for (auto &schema_p : schemas) {
		auto &schema = schema_p.get();
		entries.push_back(schema);
		schema.Scan(CatalogType::TYPE_ENTRY, [&](CatalogEntry &entry) {
			if (entry.internal) {
				return;
			}
			entries.push_back(entry);
		});

		schema.Scan(CatalogType::SEQUENCE_ENTRY, [&](CatalogEntry &entry) {
			if (entry.internal) {
				return;
			}
			entries.push_back(entry);
		});

		catalog_entry_vector_t tables;
		vector<reference<ViewCatalogEntry>> views;
		schema.Scan(CatalogType::TABLE_ENTRY, [&](CatalogEntry &entry) {
			if (entry.internal) {
				return;
			}
			if (entry.type == CatalogType::TABLE_ENTRY) {
				tables.push_back(entry.Cast<TableCatalogEntry>());
			} else if (entry.type == CatalogType::VIEW_ENTRY) {
				views.push_back(entry.Cast<ViewCatalogEntry>());
			} else {
				throw NotImplementedException("Catalog type for entries");
			}
		});
		// Reorder tables because of foreign key constraint
		ReorderTableEntries(tables);
		for (auto &table : tables) {
			entries.push_back(table.get());
		}
		for (auto &view : views) {
			entries.push_back(view.get());
		}

		schema.Scan(CatalogType::SCALAR_FUNCTION_ENTRY, [&](CatalogEntry &entry) {
			if (entry.internal) {
				return;
			}
			if (entry.type == CatalogType::MACRO_ENTRY) {
				entries.push_back(entry);
			}
		});

		schema.Scan(CatalogType::TABLE_FUNCTION_ENTRY, [&](CatalogEntry &entry) {
			if (entry.internal) {
				return;
			}
			if (entry.type == CatalogType::TABLE_MACRO_ENTRY) {
				entries.push_back(entry);
			}
		});

		schema.Scan(CatalogType::INDEX_ENTRY, [&](CatalogEntry &entry) {
			D_ASSERT(!entry.internal);
			entries.push_back(entry);
		});
	}
	return entries;
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
	catalog.ScanSchemas([&](SchemaCatalogEntry &entry) { schemas.push_back(entry); });
	// write the actual data into the database

	// Create a serializer to write the checkpoint data
	// The serialized format is roughly:
	/*
	    {
	        schemas: [
	            {
	                schema: <schema_info>,
	                custom_types: [ { type: <type_info> }, ... ],
	                sequences: [ { sequence: <sequence_info> }, ... ],
	                tables: [ { table: <table_info> }, ... ],
	                views: [ { view: <view_info> }, ... ],
	                macros: [ { macro: <macro_info> }, ... ],
	                table_macros: [ { table_macro: <table_macro_info> }, ... ],
	                indexes: [ { index: <index_info>, root_offset <block_ptr> }, ... ]
	            }
	        ]
	    }
	 */
	auto catalog_entries = GetCatalogEntries(schemas);
	BinarySerializer serializer(*metadata_writer);
	serializer.Begin();
	serializer.WriteList(100, "catalog_entries", catalog_entries.size(), [&](Serializer::List &list, idx_t i) {
		auto &entry = catalog_entries[i];
		list.WriteObject([&](Serializer &obj) { WriteEntry(entry.get(), obj); });
	});
	serializer.End();

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

	// truncate the file
	block_manager.Truncate();

	// truncate the WAL
	wal->Truncate(0);
}

void CheckpointReader::LoadCheckpoint(ClientContext &context, MetadataReader &reader) {
	BinaryDeserializer deserializer(reader);
	deserializer.Begin();
	deserializer.ReadList(100, "catalog_entries", [&](Deserializer::List &list, idx_t i) {
		return list.ReadObject([&](Deserializer &obj) { ReadEntry(context, obj); });
	});
	deserializer.End();
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

void CheckpointWriter::WriteEntry(CatalogEntry &entry, Serializer &serializer) {
	serializer.WriteProperty(99, "catalog_type", entry.type);

	switch (entry.type) {
	case CatalogType::SCHEMA_ENTRY: {
		auto &schema = entry.Cast<SchemaCatalogEntry>();
		WriteSchema(schema, serializer);
		break;
	}
	case CatalogType::TYPE_ENTRY: {
		auto &custom_type = entry.Cast<TypeCatalogEntry>();
		WriteType(custom_type, serializer);
		break;
	}
	case CatalogType::SEQUENCE_ENTRY: {
		auto &seq = entry.Cast<SequenceCatalogEntry>();
		WriteSequence(seq, serializer);
		break;
	}
	case CatalogType::TABLE_ENTRY: {
		auto &table = entry.Cast<TableCatalogEntry>();
		WriteTable(table, serializer);
		break;
	}
	case CatalogType::VIEW_ENTRY: {
		auto &view = entry.Cast<ViewCatalogEntry>();
		WriteView(view, serializer);
		break;
	}
	case CatalogType::MACRO_ENTRY: {
		auto &macro = entry.Cast<ScalarMacroCatalogEntry>();
		WriteMacro(macro, serializer);
		break;
	}
	case CatalogType::TABLE_MACRO_ENTRY: {
		auto &macro = entry.Cast<TableMacroCatalogEntry>();
		WriteTableMacro(macro, serializer);
		break;
	}
	case CatalogType::INDEX_ENTRY: {
		auto &index = entry.Cast<IndexCatalogEntry>();
		WriteIndex(index, serializer);
		break;
	}
	default:
		throw InternalException("Unrecognized catalog type in CheckpointWriter::WriteEntry");
	}
}

//===--------------------------------------------------------------------===//
// Schema
//===--------------------------------------------------------------------===//
void CheckpointWriter::WriteSchema(SchemaCatalogEntry &schema, Serializer &serializer) {
	// write the schema data
	serializer.WriteProperty(100, "schema", &schema);
}

void CheckpointReader::ReadEntry(ClientContext &context, Deserializer &deserializer) {
	auto type = deserializer.ReadProperty<CatalogType>(99, "type");

	switch (type) {
	case CatalogType::SCHEMA_ENTRY: {
		ReadSchema(context, deserializer);
		break;
	}
	case CatalogType::TYPE_ENTRY: {
		ReadType(context, deserializer);
		break;
	}
	case CatalogType::SEQUENCE_ENTRY: {
		ReadSequence(context, deserializer);
		break;
	}
	case CatalogType::TABLE_ENTRY: {
		ReadTable(context, deserializer);
		break;
	}
	case CatalogType::VIEW_ENTRY: {
		ReadView(context, deserializer);
		break;
	}
	case CatalogType::MACRO_ENTRY: {
		ReadMacro(context, deserializer);
		break;
	}
	case CatalogType::TABLE_MACRO_ENTRY: {
		ReadTableMacro(context, deserializer);
		break;
	}
	case CatalogType::INDEX_ENTRY: {
		ReadIndex(context, deserializer);
		break;
	}
	default:
		throw InternalException("Unrecognized catalog type in CheckpointWriter::WriteEntry");
	}
}

void CheckpointReader::ReadSchema(ClientContext &context, Deserializer &deserializer) {
	// Read the schema and create it in the catalog
	auto info = deserializer.ReadProperty<unique_ptr<CreateInfo>>(100, "schema");
	auto &schema_info = info->Cast<CreateSchemaInfo>();

	// we set create conflict to IGNORE_ON_CONFLICT, so that we can ignore a failure when recreating the main schema
	schema_info.on_conflict = OnCreateConflict::IGNORE_ON_CONFLICT;
	catalog.CreateSchema(context, schema_info);
}

//===--------------------------------------------------------------------===//
// Views
//===--------------------------------------------------------------------===//
void CheckpointWriter::WriteView(ViewCatalogEntry &view, Serializer &serializer) {
	serializer.WriteProperty(100, "view", &view);
}

void CheckpointReader::ReadView(ClientContext &context, Deserializer &deserializer) {
	auto info = deserializer.ReadProperty<unique_ptr<CreateInfo>>(100, "view");
	auto &view_info = info->Cast<CreateViewInfo>();
	catalog.CreateView(context, view_info);
}

//===--------------------------------------------------------------------===//
// Sequences
//===--------------------------------------------------------------------===//
void CheckpointWriter::WriteSequence(SequenceCatalogEntry &seq, Serializer &serializer) {
	serializer.WriteProperty(100, "sequence", &seq);
}

void CheckpointReader::ReadSequence(ClientContext &context, Deserializer &deserializer) {
	auto info = deserializer.ReadProperty<unique_ptr<CreateInfo>>(100, "sequence");
	auto &sequence_info = info->Cast<CreateSequenceInfo>();
	catalog.CreateSequence(context, sequence_info);
}

//===--------------------------------------------------------------------===//
// Indexes
//===--------------------------------------------------------------------===//
void CheckpointWriter::WriteIndex(IndexCatalogEntry &index_catalog_entry, Serializer &serializer) {
	// The index data is written as part of WriteTableData
	// Here, we serialize the index catalog entry

	// we need to keep the tag "index", even though it is slightly misleading
	serializer.WriteProperty(100, "index", &index_catalog_entry);
}

void CheckpointReader::ReadIndex(ClientContext &context, Deserializer &deserializer) {

	// we need to keep the tag "index", even though it is slightly misleading.
	auto create_info = deserializer.ReadProperty<unique_ptr<CreateInfo>>(100, "index");
	auto &info = create_info->Cast<CreateIndexInfo>();

	// also, we have to read the root_block_pointer, which will not be valid for newer storage versions.
	// This leads to different code paths in this function.
	auto root_block_pointer =
	    deserializer.ReadPropertyWithDefault<BlockPointer>(101, "root_block_pointer", BlockPointer());

	// create the index in the catalog
	auto &table =
	    catalog.GetEntry(context, CatalogType::TABLE_ENTRY, create_info->schema, info.table).Cast<DuckTableEntry>();
	auto &index = catalog.CreateIndex(context, info)->Cast<DuckIndexEntry>();
	index.info = table.GetStorage().info;

	// insert the parsed expressions into the index so that we can (de)serialize them during consecutive checkpoints
	for (auto &parsed_expr : info.parsed_expressions) {
		index.parsed_expressions.push_back(parsed_expr->Copy());
	}

	// obtain the parsed expressions of the ART from the index metadata
	vector<unique_ptr<ParsedExpression>> parsed_expressions;
	for (auto &parsed_expr : info.parsed_expressions) {
		parsed_expressions.push_back(parsed_expr->Copy());
	}
	D_ASSERT(!parsed_expressions.empty());

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
	unbound_expressions.reserve(parsed_expressions.size());
	for (auto &expr : parsed_expressions) {
		unbound_expressions.push_back(idx_binder.Bind(expr));
	}

	auto &data_table = table.GetStorage();
	IndexStorageInfo index_storage_info;
	if (root_block_pointer.IsValid()) {
		// this code path is necessary to read older duckdb files
		index_storage_info.name = info.index_name;
		index_storage_info.root_block_ptr = root_block_pointer;

	} else {
		// get the matching index storage info
		for (auto const &elem : data_table.info->index_storage_infos) {
			if (elem.name == info.index_name) {
				index_storage_info = elem;
				break;
			}
		}
	}

	D_ASSERT(index_storage_info.IsValid() && !index_storage_info.name.empty());
	auto art = make_uniq<ART>(info.index_name, info.constraint_type, info.column_ids, TableIOManager::Get(data_table),
	                          std::move(unbound_expressions), data_table.db, nullptr, index_storage_info);
	data_table.info->indexes.AddIndex(std::move(art));
}

//===--------------------------------------------------------------------===//
// Custom Types
//===--------------------------------------------------------------------===//
void CheckpointWriter::WriteType(TypeCatalogEntry &type, Serializer &serializer) {
	serializer.WriteProperty(100, "type", &type);
}

void CheckpointReader::ReadType(ClientContext &context, Deserializer &deserializer) {
	auto info = deserializer.ReadProperty<unique_ptr<CreateInfo>>(100, "type");
	auto &type_info = info->Cast<CreateTypeInfo>();
	catalog.CreateType(context, type_info);
}

//===--------------------------------------------------------------------===//
// Macro's
//===--------------------------------------------------------------------===//
void CheckpointWriter::WriteMacro(ScalarMacroCatalogEntry &macro, Serializer &serializer) {
	serializer.WriteProperty(100, "macro", &macro);
}

void CheckpointReader::ReadMacro(ClientContext &context, Deserializer &deserializer) {
	auto info = deserializer.ReadProperty<unique_ptr<CreateInfo>>(100, "macro");
	auto &macro_info = info->Cast<CreateMacroInfo>();
	catalog.CreateFunction(context, macro_info);
}

void CheckpointWriter::WriteTableMacro(TableMacroCatalogEntry &macro, Serializer &serializer) {
	serializer.WriteProperty(100, "table_macro", &macro);
}

void CheckpointReader::ReadTableMacro(ClientContext &context, Deserializer &deserializer) {
	auto info = deserializer.ReadProperty<unique_ptr<CreateInfo>>(100, "table_macro");
	auto &macro_info = info->Cast<CreateMacroInfo>();
	catalog.CreateFunction(context, macro_info);
}

//===--------------------------------------------------------------------===//
// Table Metadata
//===--------------------------------------------------------------------===//
void CheckpointWriter::WriteTable(TableCatalogEntry &table, Serializer &serializer) {
	// Write the table metadata
	serializer.WriteProperty(100, "table", &table);

	// Write the table data
	if (auto writer = GetTableDataWriter(table)) {
		writer->WriteTableData(serializer);
	}
}

void CheckpointReader::ReadTable(ClientContext &context, Deserializer &deserializer) {
	// deserialize the table meta data
	auto info = deserializer.ReadProperty<unique_ptr<CreateInfo>>(100, "table");
	auto binder = Binder::CreateBinder(context);
	auto &schema = catalog.GetSchema(context, info->schema);
	auto bound_info = binder->BindCreateTableInfo(std::move(info), schema);

	// now read the actual table data and place it into the CreateTableInfo
	ReadTableData(context, deserializer, *bound_info);

	// finally create the table in the catalog
	catalog.CreateTable(context, *bound_info);
}

void CheckpointReader::ReadTableData(ClientContext &context, Deserializer &deserializer,
                                     BoundCreateTableInfo &bound_info) {

	// written in "SingleFileTableDataWriter::FinalizeTable"
	auto table_pointer = deserializer.ReadProperty<MetaBlockPointer>(101, "table_pointer");
	auto total_rows = deserializer.ReadProperty<idx_t>(102, "total_rows");

	// old file read
	auto index_pointers = deserializer.ReadPropertyWithDefault<vector<BlockPointer>>(103, "index_pointers", {});
	// new file read
	auto index_storage_infos =
	    deserializer.ReadPropertyWithDefault<vector<IndexStorageInfo>>(104, "index_storage_infos", {});

	if (!index_storage_infos.empty()) {
		bound_info.indexes = index_storage_infos;

	} else {
		// old duckdb file containing index pointers
		for (idx_t i = 0; i < index_pointers.size(); i++) {
			IndexStorageInfo index_storage_info;
			index_storage_info.root_block_ptr = index_pointers[i];
			bound_info.indexes.push_back(index_storage_info);
		}
	}

	// FIXME: icky downcast to get the underlying MetadataReader
	auto &binary_deserializer = dynamic_cast<BinaryDeserializer &>(deserializer);
	auto &reader = dynamic_cast<MetadataReader &>(binary_deserializer.GetStream());

	MetadataReader table_data_reader(reader.GetMetadataManager(), table_pointer);
	TableDataReader data_reader(table_data_reader, bound_info);
	data_reader.ReadTableData();

	bound_info.data->total_rows = total_rows;
}

} // namespace duckdb
