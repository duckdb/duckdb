//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/write_ahead_log.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/helper.hpp"
#include "common/types/data_chunk.hpp"
#include "common/enums/wal_type.hpp"
#include "common/serializer/buffered_file_writer.hpp"
#include "catalog/catalog_entry/sequence_catalog_entry.hpp"

namespace duckdb {

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

	void WriteCreateTable(TableCatalogEntry *entry);
	void WriteDropTable(TableCatalogEntry *entry);

	void WriteCreateSchema(SchemaCatalogEntry *entry);
	void WriteDropSchema(SchemaCatalogEntry *entry);

	void WriteCreateView(ViewCatalogEntry *entry);
	void WriteDropView(ViewCatalogEntry *entry);

	void WriteCreateSequence(SequenceCatalogEntry *entry);
	void WriteDropSequence(SequenceCatalogEntry *entry);
	void WriteSequenceValue(SequenceCatalogEntry *entry, SequenceValue val);

	void WriteInsert(string &schema, string &table, DataChunk &chunk);
	void WriteQuery(string &query);

	void Flush();

private:
	DuckDB &database;
	unique_ptr<BufferedFileWriter> writer;
};

} // namespace duckdb
