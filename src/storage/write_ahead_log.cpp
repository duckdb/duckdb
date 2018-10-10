
#include "storage/write_ahead_log.hpp"

#include "catalog/catalog.hpp"
#include "catalog/schema_catalog.hpp"

#include "main/client_context.hpp"
#include "main/connection.hpp"
#include "main/database.hpp"

#include "transaction/transaction.hpp"
#include "transaction/transaction_manager.hpp"

#include "common/file_system.hpp"
#include "common/serializer.hpp"

using namespace duckdb;
using namespace std;

WriteAheadLog::~WriteAheadLog() {
	if (wal_file) {
		fclose(wal_file);
	}
}

//===--------------------------------------------------------------------===//
// Read Helper Function
//===--------------------------------------------------------------------===//
static bool ReadEntry(WALEntry &entry, FILE *wal_file) {
	return fread(&entry.type, sizeof(wal_type_t), 1, wal_file) == 1 &&
	       fread(&entry.size, sizeof(uint32_t), 1, wal_file) == 1;
}

//===--------------------------------------------------------------------===//
// Replay & Initialize Log
//===--------------------------------------------------------------------===//
static bool ReplayEntry(ClientContext &context, DuckDB &database,
                        WALEntry entry, Deserializer &source);

void WriteAheadLog::Replay(string &path) {
	auto wal_file = fopen(path.c_str(), "r");
	if (!wal_file) {
		throw IOException("WAL could not be opened for reading");
	}
	ClientContext context(database);

	vector<WALEntryData> stored_entries;
	WALEntry entry;
	while (ReadEntry(entry, wal_file)) {
		// read the entry
		if (entry.type == WALEntry::WAL_FLUSH) {
			// flush
			bool failed = false;
			context.transaction.BeginTransaction();
			for (auto &stored_entry : stored_entries) {
				Deserializer deserializer(stored_entry.data.get(),
				                          stored_entry.entry.size);

				if (!ReplayEntry(context, database, stored_entry.entry,
				                 deserializer)) {
					// failed to replay entry in log
					context.transaction.Rollback();
					failed = true;
					break;
				}
			}
			if (failed) {
				// failed to replay entry: stop replaying the WAL
				break;
			}
			stored_entries.clear();
			context.transaction.Commit();
		} else {
			// check the integrity of the type
			if (!WALEntry::TypeIsValid(entry.type)) {
				// read invalid WAL entry type! stop replaying the WAL
				break;
			}
			// store the WAL entry for replay after we encounter a flush
			WALEntryData data;
			data.entry = entry;
			data.data = unique_ptr<uint8_t[]>(new uint8_t[entry.size]);
			// read the data
			if (fread(data.data.get(), entry.size, 1, wal_file) != 1) {
				// could not read the data for this entry, stop replaying the
				// WAL
				break;
			}
			stored_entries.push_back(move(data));
		}
	}
	fclose(wal_file);
}

void WriteAheadLog::Initialize(std::string &path) {
	wal_file = fopen(path.c_str(), "a+");
	initialized = true;
}

//===--------------------------------------------------------------------===//
// Replay Entries
//===--------------------------------------------------------------------===//
bool ReplayDropTable(Transaction &transaction, Catalog &catalog,
                     Deserializer &source);
bool ReplayCreateTable(Transaction &transaction, Catalog &catalog,
                       Deserializer &source);
bool ReplayDropSchema(Transaction &transaction, Catalog &catalog,
                      Deserializer &source);
bool ReplayCreateSchema(Transaction &transaction, Catalog &catalog,
                        Deserializer &source);
bool ReplayInsert(ClientContext &context, Catalog &catalog,
                  Deserializer &source);
bool ReplayQuery(ClientContext &context, Deserializer &source);

bool ReplayEntry(ClientContext &context, DuckDB &database, WALEntry entry,
                 Deserializer &source) {
	switch (entry.type) {
	case WALEntry::DROP_TABLE:
		return ReplayDropTable(context.ActiveTransaction(), database.catalog,
		                       source);
	case WALEntry::CREATE_TABLE:
		return ReplayCreateTable(context.ActiveTransaction(), database.catalog,
		                         source);
	case WALEntry::DROP_SCHEMA:
		return ReplayDropSchema(context.ActiveTransaction(), database.catalog,
		                        source);
	case WALEntry::CREATE_SCHEMA:
		return ReplayCreateSchema(context.ActiveTransaction(), database.catalog,
		                          source);
	case WALEntry::INSERT_TUPLE:
		return ReplayInsert(context, database.catalog, source);
	case WALEntry::QUERY:
		return ReplayQuery(context, source);
	default:
		// unrecognized WAL entry type
		return false;
	}
	return false;
}

//===--------------------------------------------------------------------===//
// Write Helper Functions
//===--------------------------------------------------------------------===//
template <class T> void WriteAheadLog::Write(T val) {
	if (fwrite(&val, sizeof(T), 1, wal_file) != 1) {
		throw IOException("WAL - Failed to write (%s)!", strerror(errno));
	}
}

void WriteAheadLog::WriteData(uint8_t *dataptr, size_t data_size) {
	if (data_size == 0) {
		return;
	}
	if (fwrite(dataptr, data_size, 1, wal_file) != 1) {
		throw IOException("WAL - Failed to write (%s)!", strerror(errno));
	}
}

void WriteAheadLog::WriteEntry(wal_type_t type, Serializer &serializer) {
	auto blob = serializer.GetData();

	Write<wal_type_t>(type);
	Write<uint32_t>(blob.size);
	WriteData(blob.data.get(), blob.size);
}

//===--------------------------------------------------------------------===//
// Write Entries
//===--------------------------------------------------------------------===//
//===--------------------------------------------------------------------===//
// CREATE TABLE
//===--------------------------------------------------------------------===//
void WriteAheadLog::WriteCreateTable(TableCatalogEntry *entry) {
	Serializer serializer;
	serializer.WriteString(entry->schema->name);
	serializer.WriteString(entry->name);
	serializer.Write<uint32_t>(entry->columns.size());
	for (auto &column : entry->columns) {
		serializer.WriteString(column.name);
		serializer.Write<int>((int)column.type);
	}
	serializer.Write<uint32_t>(entry->constraints.size());
	for (auto &constraint : entry->constraints) {
		constraint->Serialize(serializer);
	}

	WriteEntry(WALEntry::CREATE_TABLE, serializer);
}

bool ReplayCreateTable(Transaction &transaction, Catalog &catalog,
                       Deserializer &source) {
	bool failed = false;
	CreateTableInformation info;

	info.schema = source.Read<string>(failed);
	info.table = source.Read<string>(failed);
	auto column_count = source.Read<uint32_t>(failed);
	if (failed) {
		return false;
	}

	for (size_t i = 0; i < column_count; i++) {
		auto column_name = source.Read<string>(failed);
		auto column_type = (TypeId)source.Read<int>(failed);
		if (failed) {
			return false;
		}
		info.columns.push_back(
		    ColumnDefinition(column_name, column_type, false));
	}
	auto constraint_count = source.Read<uint32_t>(failed);
	if (failed) {
		return false;
	}

	for (size_t i = 0; i < constraint_count; i++) {
		auto constraint = Constraint::Deserialize(source);
		if (!constraint) {
			return false;
		}
		info.constraints.push_back(move(constraint));
	}

	try {
		catalog.CreateTable(transaction, &info);
	} catch (...) {
		return false;
	}
	return true;
}

//===--------------------------------------------------------------------===//
// DROP TABLE
//===--------------------------------------------------------------------===//
void WriteAheadLog::WriteDropTable(TableCatalogEntry *entry) {
	Serializer serializer;
	serializer.WriteString(entry->schema->name);
	serializer.WriteString(entry->name);

	WriteEntry(WALEntry::DROP_TABLE, serializer);
}

bool ReplayDropTable(Transaction &transaction, Catalog &catalog,
                     Deserializer &source) {
	bool failed = false;
	auto schema_name = source.Read<string>(failed);
	auto table_name = source.Read<string>(failed);
	if (failed) {
		return false;
	}

	try {
		catalog.DropTable(transaction, schema_name, table_name);
	} catch (...) {
		return false;
	}
	return true;
}

//===--------------------------------------------------------------------===//
// CREATE SCHEMA
//===--------------------------------------------------------------------===//
void WriteAheadLog::WriteCreateSchema(SchemaCatalogEntry *entry) {
	Serializer serializer;
	serializer.WriteString(entry->name);

	WriteEntry(WALEntry::CREATE_SCHEMA, serializer);
}

bool ReplayCreateSchema(Transaction &transaction, Catalog &catalog,
                        Deserializer &source) {
	bool failed = false;
	auto schema_name = source.Read<string>(failed);
	if (failed) {
		return false;
	}

	try {
		catalog.CreateSchema(transaction, schema_name);
	} catch (...) {
		return false;
	}
	return true;
}

//===--------------------------------------------------------------------===//
// DROP SCHEMA
//===--------------------------------------------------------------------===//
void WriteAheadLog::WriteDropSchema(SchemaCatalogEntry *entry) {
	Serializer serializer;
	serializer.WriteString(entry->name);

	WriteEntry(WALEntry::DROP_SCHEMA, serializer);
}

bool ReplayDropSchema(Transaction &transaction, Catalog &catalog,
                      Deserializer &source) {
	throw NotImplementedException("Did not implement DROP SCHEMA yet!");
}

//===--------------------------------------------------------------------===//
// INSERT
//===--------------------------------------------------------------------===//
void WriteAheadLog::WriteInsert(std::string &schema, std::string &table,
                                DataChunk &chunk) {
	if (chunk.count == 0) {
		return;
	}
	chunk.Verify();
	if (chunk.sel_vector) {
		throw NotImplementedException(
		    "Cannot insert into WAL from chunk with SEL vector");
	}

	Serializer serializer;
	serializer.WriteString(schema);
	serializer.WriteString(table);
	chunk.Serialize(serializer);

	WriteEntry(WALEntry::INSERT_TUPLE, serializer);
}

bool ReplayInsert(ClientContext &context, Catalog &catalog,
                  Deserializer &source) {
	bool failed = false;
	auto schema_name = source.Read<string>(failed);
	auto table_name = source.Read<string>(failed);
	DataChunk chunk;

	failed |= !chunk.Deserialize(source);
	if (failed) {
		return false;
	}

	Transaction &transaction = context.ActiveTransaction();

	try {
		// first find the table
		auto table = catalog.GetTable(transaction, schema_name, table_name);
		// now append to the chunk
		table->storage->Append(context, chunk);
	} catch (...) {
		return false;
	}
	return true;
}

//===--------------------------------------------------------------------===//
// QUERY
//===--------------------------------------------------------------------===//
void WriteAheadLog::WriteQuery(std::string &query) {
	Serializer serializer;
	serializer.WriteString(query);

	WriteEntry(WALEntry::QUERY, serializer);
}

bool ReplayQuery(ClientContext &context, Deserializer &source) {
	// read the query
	bool failed = false;
	auto query = source.Read<string>(failed);
	if (failed) {
		return false;
	}

	auto result = DuckDBConnection::GetQueryResult(context, query);
	return result->GetSuccess();
}

//===--------------------------------------------------------------------===//
// FLUSH
//===--------------------------------------------------------------------===//
void WriteAheadLog::Flush() {
	// write an empty entry
	Write<wal_type_t>(WALEntry::WAL_FLUSH);
	Write<uint32_t>(0);
	// flushes all changes made to the WAL to disk
	fflush(wal_file);
	FileSync(wal_file);
}
