//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/write_ahead_log.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/helper.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/enums/wal_type.hpp"
#include "duckdb/common/serializer/buffered_file_writer.hpp"
#include "duckdb/catalog/catalog_entry/scalar_macro_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/sequence_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_macro_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/index_catalog_entry.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/storage/storage_info.hpp"

namespace duckdb {

struct AlterInfo;

class AttachedDatabase;
class Catalog;
class DatabaseInstance;
class SchemaCatalogEntry;
class SequenceCatalogEntry;
class ScalarMacroCatalogEntry;
class ViewCatalogEntry;
class TypeCatalogEntry;
class TableCatalogEntry;
class Transaction;
class TransactionManager;

class ReplayState {
public:
	ReplayState(AttachedDatabase &db, ClientContext &context)
	    : db(db), context(context), catalog(db.GetCatalog()), deserialize_only(false) {
	}

	AttachedDatabase &db;
	ClientContext &context;
	Catalog &catalog;
	optional_ptr<TableCatalogEntry> current_table;
	bool deserialize_only;
	MetaBlockPointer checkpoint_id;

public:
	void ReplayEntry(WALType entry_type, BinaryDeserializer &deserializer);

protected:
	virtual void ReplayCreateTable(BinaryDeserializer &deserializer);
	void ReplayDropTable(BinaryDeserializer &deserializer);
	void ReplayAlter(BinaryDeserializer &deserializer);

	void ReplayCreateView(BinaryDeserializer &deserializer);
	void ReplayDropView(BinaryDeserializer &deserializer);

	void ReplayCreateSchema(BinaryDeserializer &deserializer);
	void ReplayDropSchema(BinaryDeserializer &deserializer);

	void ReplayCreateType(BinaryDeserializer &deserializer);
	void ReplayDropType(BinaryDeserializer &deserializer);

	void ReplayCreateSequence(BinaryDeserializer &deserializer);
	void ReplayDropSequence(BinaryDeserializer &deserializer);
	void ReplaySequenceValue(BinaryDeserializer &deserializer);

	void ReplayCreateMacro(BinaryDeserializer &deserializer);
	void ReplayDropMacro(BinaryDeserializer &deserializer);

	void ReplayCreateTableMacro(BinaryDeserializer &deserializer);
	void ReplayDropTableMacro(BinaryDeserializer &deserializer);

	void ReplayCreateIndex(BinaryDeserializer &deserializer);
	void ReplayDropIndex(BinaryDeserializer &deserializer);

	void ReplayUseTable(BinaryDeserializer &deserializer);
	void ReplayInsert(BinaryDeserializer &deserializer);
	void ReplayDelete(BinaryDeserializer &deserializer);
	void ReplayUpdate(BinaryDeserializer &deserializer);
	void ReplayCheckpoint(BinaryDeserializer &deserializer);
};

//! The WriteAheadLog (WAL) is a log that is used to provide durability. Prior
//! to committing a transaction it writes the changes the transaction made to
//! the database to the log, which can then be replayed upon startup in case the
//! server crashes or is shut down.
class WriteAheadLog {
public:
	//! Initialize the WAL in the specified directory
	explicit WriteAheadLog(AttachedDatabase &database, const string &path);
	virtual ~WriteAheadLog();

	//! Skip writing to the WAL
	bool skip_writing;

public:
	//! Replay the WAL
	static bool Replay(AttachedDatabase &database, string &path);

	//! Returns the current size of the WAL in bytes
	int64_t GetWALSize();
	//! Gets the total bytes written to the WAL since startup
	idx_t GetTotalWritten();

	virtual void WriteCreateTable(const TableCatalogEntry &entry);
	void WriteDropTable(const TableCatalogEntry &entry);

	void WriteCreateSchema(const SchemaCatalogEntry &entry);
	void WriteDropSchema(const SchemaCatalogEntry &entry);

	void WriteCreateView(const ViewCatalogEntry &entry);
	void WriteDropView(const ViewCatalogEntry &entry);

	void WriteCreateSequence(const SequenceCatalogEntry &entry);
	void WriteDropSequence(const SequenceCatalogEntry &entry);
	void WriteSequenceValue(const SequenceCatalogEntry &entry, SequenceValue val);

	void WriteCreateMacro(const ScalarMacroCatalogEntry &entry);
	void WriteDropMacro(const ScalarMacroCatalogEntry &entry);

	void WriteCreateTableMacro(const TableMacroCatalogEntry &entry);
	void WriteDropTableMacro(const TableMacroCatalogEntry &entry);

	void WriteCreateIndex(const IndexCatalogEntry &entry);
	void WriteDropIndex(const IndexCatalogEntry &entry);

	void WriteCreateType(const TypeCatalogEntry &entry);
	void WriteDropType(const TypeCatalogEntry &entry);
	//! Sets the table used for subsequent insert/delete/update commands
	void WriteSetTable(string &schema, string &table);

	void WriteAlter(const AlterInfo &info);

	void WriteInsert(DataChunk &chunk);
	void WriteDelete(DataChunk &chunk);
	//! Write a single (sub-) column update to the WAL. Chunk must be a pair of (COL, ROW_ID).
	//! The column_path vector is a *path* towards a column within the table
	//! i.e. if we have a table with a single column S STRUCT(A INT, B INT)
	//! and we update the validity mask of "S.B"
	//! the column path is:
	//! 0 (first column of table)
	//! -> 1 (second subcolumn of struct)
	//! -> 0 (first subcolumn of INT)
	void WriteUpdate(DataChunk &chunk, const vector<column_t> &column_path);

	//! Truncate the WAL to a previous size, and clear anything currently set in the writer
	void Truncate(int64_t size);
	//! Delete the WAL file on disk. The WAL should not be used after this point.
	void Delete();
	void Flush();

	void WriteCheckpoint(MetaBlockPointer meta_block);

protected:
	AttachedDatabase &database;
	unique_ptr<BufferedFileWriter> writer;
	string wal_path;
};

} // namespace duckdb
