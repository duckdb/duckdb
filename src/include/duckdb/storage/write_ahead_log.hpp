//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/write_ahead_log.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/catalog/catalog_entry/index_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/scalar_macro_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/sequence_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_macro_catalog_entry.hpp"
#include "duckdb/common/enums/wal_type.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/serializer/buffered_file_writer.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/storage/block.hpp"
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
class WriteAheadLogDeserializer;

//! The WriteAheadLog (WAL) is a log that is used to provide durability. Prior
//! to committing a transaction it writes the changes the transaction made to
//! the database to the log, which can then be replayed upon startup in case the
//! server crashes or is shut down.
class WriteAheadLog {
public:
	//! Initialize the WAL in the specified directory
	explicit WriteAheadLog(AttachedDatabase &database, const string &wal_path);
	virtual ~WriteAheadLog();

public:
	//! Replay the WAL
	static bool Replay(AttachedDatabase &database, unique_ptr<FileHandle> handle);

	//! Gets the total bytes written to the WAL since startup
	idx_t GetWALSize();
	//! Gets the total bytes written to the WAL since startup
	idx_t GetTotalWritten();

	//! A WAL is initialized, if a writer to a file exists.
	bool Initialized() {
		return initialized;
	}
	//! Initializes the file of the WAL by creating the file writer.
	BufferedFileWriter &Initialize();

	void WriteVersion();

	virtual void WriteCreateTable(const TableCatalogEntry &entry);
	void WriteDropTable(const TableCatalogEntry &entry);

	void WriteCreateSchema(const SchemaCatalogEntry &entry);
	void WriteDropSchema(const SchemaCatalogEntry &entry);

	void WriteCreateView(const ViewCatalogEntry &entry);
	void WriteDropView(const ViewCatalogEntry &entry);

	void WriteCreateSequence(const SequenceCatalogEntry &entry);
	void WriteDropSequence(const SequenceCatalogEntry &entry);
	void WriteSequenceValue(SequenceValue val);

	void WriteCreateMacro(const ScalarMacroCatalogEntry &entry);
	void WriteDropMacro(const ScalarMacroCatalogEntry &entry);

	void WriteCreateTableMacro(const TableMacroCatalogEntry &entry);
	void WriteDropTableMacro(const TableMacroCatalogEntry &entry);

	void WriteCreateIndex(const IndexCatalogEntry &entry);
	void WriteDropIndex(const IndexCatalogEntry &entry);

	void WriteCreateType(const TypeCatalogEntry &entry);
	void WriteDropType(const TypeCatalogEntry &entry);
	//! Sets the table used for subsequent insert/delete/update commands
	void WriteSetTable(const string &schema, const string &table);

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
	void Truncate(idx_t size);
	//! Delete the WAL file on disk. The WAL should not be used after this point.
	void Delete();
	void Flush();

	void WriteCheckpoint(MetaBlockPointer meta_block);

protected:
	AttachedDatabase &database;
	unique_ptr<BufferedFileWriter> writer;
	string wal_path;
	atomic<idx_t> wal_size;
	atomic<bool> initialized;
};

} // namespace duckdb
