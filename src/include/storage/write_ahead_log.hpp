//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/write_ahead_log.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/exception.hpp"
#include "common/helper.hpp"
#include "common/types/data_chunk.hpp"

#include <vector>

namespace duckdb {

class Catalog;
class DuckDB;
class SchemaCatalogEntry;
class ViewCatalogEntry;
class TableCatalogEntry;
class Transaction;
class TransactionManager;

//! The type of WAL entry
typedef uint8_t wal_type_t;

struct WALEntry {
	static constexpr wal_type_t INVALID = 0;
	static constexpr wal_type_t DROP_TABLE = 1;
	static constexpr wal_type_t CREATE_TABLE = 2;
	static constexpr wal_type_t DROP_SCHEMA = 3;
	static constexpr wal_type_t CREATE_SCHEMA = 4;
	static constexpr wal_type_t DROP_VIEW = 5;
	static constexpr wal_type_t CREATE_VIEW = 6;
	static constexpr wal_type_t INSERT_TUPLE = 7;
	static constexpr wal_type_t QUERY = 8;
	static constexpr wal_type_t WAL_FLUSH = 100;

	static bool TypeIsValid(wal_type_t type) {
		return type == WALEntry::WAL_FLUSH || (type >= WALEntry::DROP_TABLE && type <= WALEntry::QUERY);
	}

	wal_type_t type;
	uint32_t size;
};

struct WALEntryData {
	WALEntry entry;
	unique_ptr<uint8_t[]> data;
};

//! The WriteAheadLog (WAL) is a log that is used to provide durability. Prior
//! to committing a transaction it writes the changes the transaction made to
//! the database to the log, which can then be replayed upon startup in case the
//! server crashes or is shut down.
class WriteAheadLog {
public:
	WriteAheadLog(DuckDB &database) : initialized(false), database(database), wal_file(nullptr) {
	}
	~WriteAheadLog();

	bool IsInitialized() {
		return initialized;
	}

	//! Replay the WAL
	void Replay(string &path);
	//! Initialize the WAL in the specified directory
	void Initialize(string &path);

	void WriteCreateTable(TableCatalogEntry *entry);
	void WriteDropTable(TableCatalogEntry *entry);

	void WriteCreateSchema(SchemaCatalogEntry *entry);
	void WriteDropSchema(SchemaCatalogEntry *entry);

	void WriteCreateView(ViewCatalogEntry *entry);
	void WriteDropView(ViewCatalogEntry *entry);

	void WriteInsert(string &schema, string &table, DataChunk &chunk);
	void WriteQuery(string &query);

	void Flush();

private:
	template <class T> void Write(T val);
	void WriteData(uint8_t *dataptr, uint64_t data_size);

	void WriteEntry(wal_type_t type, Serializer &serializer);

	bool initialized;

	DuckDB &database;
	FILE *wal_file;
};

} // namespace duckdb
