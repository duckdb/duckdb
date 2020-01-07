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

namespace duckdb {

struct AlterInfo;

class BufferedSerializer;
class Catalog;
class DuckDB;
class SchemaCatalogEntry;
class SequenceCatalogEntry;
class ViewCatalogEntry;
class TableCatalogEntry;
class Transaction;
class TransactionManager;

//! The WriteAheadLog (WAL) is a log that is used to provide durability. Prior
//! to committing a transaction it writes the changes the transaction made to
//! the database to the log, which can then be replayed upon startup in case the
//! server crashes or is shut down.
class WriteAheadLog {
public:
	WriteAheadLog(DuckDB &database);

	//! Whether or not the WAL has been initialized
	bool initialized;

public:
	//! Replay the WAL
	static void Replay(DuckDB &database, string &path);

	//! Initialize the WAL in the specified directory
	void Initialize(string &path);
	//! Returns the current size of the WAL in bytes
	int64_t GetWALSize();

	void WriteCreateTable(TableCatalogEntry *entry);
	void WriteDropTable(TableCatalogEntry *entry);

	void WriteCreateSchema(SchemaCatalogEntry *entry);
	void WriteDropSchema(SchemaCatalogEntry *entry);

	void WriteCreateView(ViewCatalogEntry *entry);
	void WriteDropView(ViewCatalogEntry *entry);

	void WriteCreateSequence(SequenceCatalogEntry *entry);
	void WriteDropSequence(SequenceCatalogEntry *entry);
	void WriteSequenceValue(SequenceCatalogEntry *entry, SequenceValue val);

	//! Sets the table used for subsequent insert/delete/update commands
	void WriteSetTable(string &schema, string &table);

	void WriteAlter(AlterInfo &info);

	void WriteInsert(DataChunk &chunk);
	void WriteDelete(DataChunk &chunk);
	void WriteUpdate(DataChunk &chunk, column_t col_idx);

	//! Truncate the WAL to a previous size, and clear anything currently set in the writer
	void Truncate(int64_t size);
	void Flush();

private:
	DuckDB &database;
	unique_ptr<BufferedFileWriter> writer;
};

} // namespace duckdb
