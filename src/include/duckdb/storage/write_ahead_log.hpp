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
#include "duckdb/catalog/catalog_entry/sequence_catalog_entry.hpp"
#include "duckdb/storage/storage_info.hpp"

#include "duckdb/catalog/catalog_entry/scalar_macro_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_macro_catalog_entry.hpp"

namespace duckdb {

struct AlterInfo;

class BufferedSerializer;
class Catalog;
class DatabaseInstance;
class SchemaCatalogEntry;
class SequenceCatalogEntry;
class ScalarMacroCatalogEntry;
class ViewCatalogEntry;
class MatViewCatalogEntry;
class TypeCatalogEntry;
class TableCatalogEntry;
class Transaction;
class TransactionManager;

//! The WriteAheadLog (WAL) is a log that is used to provide durability. Prior
//! to committing a transaction it writes the changes the transaction made to
//! the database to the log, which can then be replayed upon startup in case the
//! server crashes or is shut down.
class WriteAheadLog {
public:
	explicit WriteAheadLog(DatabaseInstance &database);

	//! Whether or not the WAL has been initialized
	bool initialized;
	//! Skip writing to the WAL
	bool skip_writing;

public:
	//! Replay the WAL
	static bool Replay(DatabaseInstance &database, string &path);

	//! Initialize the WAL in the specified directory
	void Initialize(string &path);
	//! Returns the current size of the WAL in bytes
	int64_t GetWALSize();
	//! Gets the total bytes written to the WAL since startup
	idx_t GetTotalWritten();

	void WriteCreateTable(TableCatalogEntry *entry);
	void WriteDropTable(TableCatalogEntry *entry);

	void WriteCreateSchema(SchemaCatalogEntry *entry);
	void WriteDropSchema(SchemaCatalogEntry *entry);

	void WriteCreateView(ViewCatalogEntry *entry);
	void WriteDropView(ViewCatalogEntry *entry);

    void WriteCreateMatView(MatViewCatalogEntry *entry);
    void WriteDropMatView(MatViewCatalogEntry *entry);

	void WriteCreateSequence(SequenceCatalogEntry *entry);
	void WriteDropSequence(SequenceCatalogEntry *entry);
	void WriteSequenceValue(SequenceCatalogEntry *entry, SequenceValue val);

	void WriteCreateMacro(ScalarMacroCatalogEntry *entry);
	void WriteDropMacro(ScalarMacroCatalogEntry *entry);

	void WriteCreateTableMacro(TableMacroCatalogEntry *entry);
	void WriteDropTableMacro(TableMacroCatalogEntry *entry);

	void WriteCreateType(TypeCatalogEntry *entry);
	void WriteDropType(TypeCatalogEntry *entry);
	//! Sets the table used for subsequent insert/delete/update commands
	void WriteSetTable(string &schema, string &table);

	void WriteAlter(AlterInfo &info);

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

	void WriteCheckpoint(block_id_t meta_block);

private:
	DatabaseInstance &database;
	unique_ptr<BufferedFileWriter> writer;
	string wal_path;
};

} // namespace duckdb
