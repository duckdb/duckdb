#include "duckdb/storage/checkpoint_manager.hpp"

#include "duckdb/catalog/duck_catalog.hpp"
#include "duckdb/catalog/catalog_entry/duck_index_entry.hpp"
#include "duckdb/catalog/catalog_entry/scalar_macro_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/sequence_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/catalog/catalog_entry/type_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/view_catalog_entry.hpp"
#include "duckdb/execution/index/art/art.hpp"
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
#include "duckdb/main/attached_database.hpp"
#include "duckdb/common/serializer/binary_serializer.hpp"
#include "duckdb/common/serializer/binary_deserializer.hpp"

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
	BinarySerializer serializer(*metadata_writer);
	serializer.Begin();
	serializer.WriteList(100, "schemas", schemas.size(), [&](Serializer::List &list, idx_t i) {
		auto &schema = schemas[i];
		list.WriteObject([&](Serializer &obj) { WriteSchema(schema.get(), obj); });
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

	// truncate the WAL
	wal->Truncate(0);

	// truncate the file
	block_manager.Truncate();

	metadata_manager.MarkBlocksAsModified();
}

void CheckpointReader::LoadCheckpoint(ClientContext &context, MetadataReader &reader) {
	BinaryDeserializer deserializer(reader);
	deserializer.Begin();
	deserializer.ReadList(100, "schemas", [&](Deserializer::List &list, idx_t i) {
		return list.ReadObject([&](Deserializer &obj) { ReadSchema(context, obj); });
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

//===--------------------------------------------------------------------===//
// Schema
//===--------------------------------------------------------------------===//
void CheckpointWriter::WriteSchema(SchemaCatalogEntry &schema, Serializer &serializer) {
	// write the schema data
	serializer.WriteProperty(100, "schema", &schema);

	// Write the custom types
	vector<reference<TypeCatalogEntry>> custom_types;
	schema.Scan(CatalogType::TYPE_ENTRY, [&](CatalogEntry &entry) {
		if (entry.internal) {
			return;
		}
		custom_types.push_back(entry.Cast<TypeCatalogEntry>());
	});

	serializer.WriteList(101, "custom_types", custom_types.size(), [&](Serializer::List &list, idx_t i) {
		auto &entry = custom_types[i];
		list.WriteObject([&](Serializer &obj) { WriteType(entry, obj); });
	});

	// Write the sequences
	vector<reference<SequenceCatalogEntry>> sequences;
	schema.Scan(CatalogType::SEQUENCE_ENTRY, [&](CatalogEntry &entry) {
		if (entry.internal) {
			return;
		}
		sequences.push_back(entry.Cast<SequenceCatalogEntry>());
	});

	serializer.WriteList(102, "sequences", sequences.size(), [&](Serializer::List &list, idx_t i) {
		auto &entry = sequences[i];
		list.WriteObject([&](Serializer &obj) { WriteSequence(entry, obj); });
	});

	// Read the tables and views
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
	// Tables
	serializer.WriteList(103, "tables", tables.size(), [&](Serializer::List &list, idx_t i) {
		auto &entry = tables[i];
		auto &table = entry.get().Cast<TableCatalogEntry>();
		list.WriteObject([&](Serializer &obj) { WriteTable(table, obj); });
	});

	// Views
	serializer.WriteList(104, "views", views.size(), [&](Serializer::List &list, idx_t i) {
		auto &entry = views[i];
		list.WriteObject([&](Serializer &obj) { WriteView(entry.get(), obj); });
	});

	// Scalar macros
	vector<reference<ScalarMacroCatalogEntry>> macros;
	schema.Scan(CatalogType::SCALAR_FUNCTION_ENTRY, [&](CatalogEntry &entry) {
		if (entry.internal) {
			return;
		}
		if (entry.type == CatalogType::MACRO_ENTRY) {
			macros.push_back(entry.Cast<ScalarMacroCatalogEntry>());
		}
	});
	serializer.WriteList(105, "macros", macros.size(), [&](Serializer::List &list, idx_t i) {
		auto &entry = macros[i];
		list.WriteObject([&](Serializer &obj) { WriteMacro(entry.get(), obj); });
	});

	// Table macros
	vector<reference<TableMacroCatalogEntry>> table_macros;
	schema.Scan(CatalogType::TABLE_FUNCTION_ENTRY, [&](CatalogEntry &entry) {
		if (entry.internal) {
			return;
		}
		if (entry.type == CatalogType::TABLE_MACRO_ENTRY) {
			table_macros.push_back(entry.Cast<TableMacroCatalogEntry>());
		}
	});
	serializer.WriteList(106, "table_macros", table_macros.size(), [&](Serializer::List &list, idx_t i) {
		auto &entry = table_macros[i];
		list.WriteObject([&](Serializer &obj) { WriteTableMacro(entry.get(), obj); });
	});

	// Indexes
	vector<reference<IndexCatalogEntry>> indexes;
	schema.Scan(CatalogType::INDEX_ENTRY, [&](CatalogEntry &entry) {
		D_ASSERT(!entry.internal);
		indexes.push_back(entry.Cast<IndexCatalogEntry>());
	});

	serializer.WriteList(107, "indexes", indexes.size(), [&](Serializer::List &list, idx_t i) {
		auto &entry = indexes[i];
		list.WriteObject([&](Serializer &obj) { WriteIndex(entry.get(), obj); });
	});
}

void CheckpointReader::ReadSchema(ClientContext &context, Deserializer &deserializer) {
	// Read the schema and create it in the catalog
	auto info = deserializer.ReadProperty<unique_ptr<CreateInfo>>(100, "schema");
	auto &schema_info = info->Cast<CreateSchemaInfo>();

	// we set create conflict to IGNORE_ON_CONFLICT, so that we can ignore a failure when recreating the main schema
	schema_info.on_conflict = OnCreateConflict::IGNORE_ON_CONFLICT;
	catalog.CreateSchema(context, schema_info);

	// Read the custom types
	deserializer.ReadList(101, "custom_types", [&](Deserializer::List &list, idx_t i) {
		return list.ReadObject([&](Deserializer &obj) { ReadType(context, obj); });
	});

	// Read the sequences
	deserializer.ReadList(102, "sequences", [&](Deserializer::List &list, idx_t i) {
		return list.ReadObject([&](Deserializer &obj) { ReadSequence(context, obj); });
	});

	// Read the tables
	deserializer.ReadList(103, "tables", [&](Deserializer::List &list, idx_t i) {
		return list.ReadObject([&](Deserializer &obj) { ReadTable(context, obj); });
	});

	// Read the views
	deserializer.ReadList(104, "views", [&](Deserializer::List &list, idx_t i) {
		return list.ReadObject([&](Deserializer &obj) { ReadView(context, obj); });
	});

	// Read the macros
	deserializer.ReadList(105, "macros", [&](Deserializer::List &list, idx_t i) {
		return list.ReadObject([&](Deserializer &obj) { ReadMacro(context, obj); });
	});

	// Read the table macros
	deserializer.ReadList(106, "table_macros", [&](Deserializer::List &list, idx_t i) {
		return list.ReadObject([&](Deserializer &obj) { ReadTableMacro(context, obj); });
	});

	// Read the indexes
	deserializer.ReadList(107, "indexes", [&](Deserializer::List &list, idx_t i) {
		return list.ReadObject([&](Deserializer &obj) { ReadIndex(context, obj); });
	});
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
void CheckpointWriter::WriteIndex(IndexCatalogEntry &index_catalog, Serializer &serializer) {
	// The index data is written as part of WriteTableData.
	// Here, we need only serialize the pointer to that data.
	auto root_block_pointer = index_catalog.index->GetRootBlockPointer();
	serializer.WriteProperty(100, "index", &index_catalog);
	serializer.WriteProperty(101, "root_block_pointer", root_block_pointer);
}

void CheckpointReader::ReadIndex(ClientContext &context, Deserializer &deserializer) {

	// deserialize the index create info
	auto create_info = deserializer.ReadProperty<unique_ptr<CreateInfo>>(100, "index");
	auto &info = create_info->Cast<CreateIndexInfo>();

	// create the index in the catalog
	auto &schema = catalog.GetSchema(context, create_info->schema);
	auto &table =
	    catalog.GetEntry(context, CatalogType::TABLE_ENTRY, create_info->schema, info.table).Cast<DuckTableEntry>();

	auto &index = schema.CreateIndex(context, info, table)->Cast<DuckIndexEntry>();

	index.info = table.GetStorage().info;
	// insert the parsed expressions into the stored index so that we correctly (de)serialize it during consecutive
	// checkpoints
	for (auto &parsed_expr : info.parsed_expressions) {
		index.parsed_expressions.push_back(parsed_expr->Copy());
	}

	// we deserialize the index lazily, i.e., we do not need to load any node information
	// except the root block pointer
	auto root_block_pointer = deserializer.ReadProperty<BlockPointer>(101, "root_block_pointer");

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

	// create the index and add it to the storage
	switch (info.index_type) {
	case IndexType::ART: {
		auto &storage = table.GetStorage();
		auto art = make_uniq<ART>(info.column_ids, TableIOManager::Get(storage), std::move(unbound_expressions),
		                          info.constraint_type, storage.db, nullptr, root_block_pointer);

		index.index = art.get();
		storage.info->indexes.AddIndex(std::move(art));
	} break;
	default:
		throw InternalException("Unknown index type for ReadIndex");
	}
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
	// Write the table meta data
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

	// now read the actual table data and place it into the create table info
	ReadTableData(context, deserializer, *bound_info);

	// finally create the table in the catalog
	catalog.CreateTable(context, *bound_info);
}

void CheckpointReader::ReadTableData(ClientContext &context, Deserializer &deserializer,
                                     BoundCreateTableInfo &bound_info) {

	// This is written in "SingleFileTableDataWriter::FinalizeTable"
	auto table_pointer = deserializer.ReadProperty<MetaBlockPointer>(101, "table_pointer");
	auto total_rows = deserializer.ReadProperty<idx_t>(102, "total_rows");
	auto index_pointers = deserializer.ReadProperty<vector<BlockPointer>>(103, "index_pointers");

	// FIXME: icky downcast to get the underlying MetadataReader
	auto &binary_deserializer = dynamic_cast<BinaryDeserializer &>(deserializer);
	auto &reader = dynamic_cast<MetadataReader &>(binary_deserializer.GetStream());

	MetadataReader table_data_reader(reader.GetMetadataManager(), table_pointer);
	TableDataReader data_reader(table_data_reader, bound_info);
	data_reader.ReadTableData();

	bound_info.data->total_rows = total_rows;
	bound_info.indexes = index_pointers;
}

} // namespace duckdb
