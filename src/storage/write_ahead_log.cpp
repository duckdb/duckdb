
#include "storage/write_ahead_log.hpp"

#include "catalog/catalog.hpp"
#include "catalog/schema_catalog.hpp"

#include "main/client_context.hpp"
#include "main/connection.hpp"
#include "main/database.hpp"

#include "transaction/transaction.hpp"
#include "transaction/transaction_manager.hpp"

#include "common/file_system.hpp"

using namespace duckdb;
using namespace std;

WriteAheadLog::~WriteAheadLog() {
	if (wal_file) {
		fclose(wal_file);
	}
}

//===--------------------------------------------------------------------===//
// Read Helper Functions
//===--------------------------------------------------------------------===//
template <class T>
static T Read(uint8_t *&dataptr, uint8_t *endptr, bool &fail);
template <> string Read(uint8_t *&dataptr, uint8_t *endptr, bool &fail);

template <class T>
static T Read(uint8_t *&dataptr, uint8_t *endptr, bool &fail) {
	if (dataptr + sizeof(T) >= endptr) {
		fail = true;
		return (T)0;
	}
	T value = *((T *)dataptr);
	dataptr += sizeof(T);
	return value;
}

template <> string Read(uint8_t *&dataptr, uint8_t *endptr, bool &fail) {
	auto size = Read<uint32_t>(dataptr, endptr, fail);
	if (dataptr + size >= endptr) {
		fail = true;
		return string();
	}
	auto value = string((char *)dataptr, size);
	dataptr += size;
	return value;
}

static bool ReadEntry(WALEntry &entry, FILE *wal_file) {
	return fread(&entry.type, sizeof(wal_type_t), 1, wal_file) == 1 &&
	       fread(&entry.size, sizeof(uint32_t), 1, wal_file) == 1;
}

//===--------------------------------------------------------------------===//
// Replay & Initialize Log
//===--------------------------------------------------------------------===//
static bool ReplayEntry(ClientContext &context, DuckDB &database,
                        WALEntry entry, uint8_t *dataptr);

void WriteAheadLog::Replay(string &path) {
	auto wal_file = fopen(path.c_str(), "r");
	if (!wal_file) {
		throw Exception("WAL could not be opened for reading");
	}
	ClientContext context(database);

	vector<WALEntryData> stored_entries;
	WALEntry entry;
	while (ReadEntry(entry, wal_file)) {
		// read the entry
		if (entry.type == WALEntry::WAL_FLUSH) {
			// flush
			context.transaction.BeginTransaction();
			for (auto &stored_entry : stored_entries) {
				if (!ReplayEntry(context, database, stored_entry.entry,
				                 stored_entry.data.get())) {
					// failed to replay entry in log
					context.transaction.Rollback();
					break;
				}
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
// Write Helper Functions
//===--------------------------------------------------------------------===//
template <class T> size_t WriteAheadLog::WriteSize() { return sizeof(T); }

size_t WriteAheadLog::WriteSize(string &val) {
	return val.size() + WriteSize<uint32_t>();
}

template <class T> void WriteAheadLog::Write(T val, size_t &sz) {
	sz -= WriteSize<T>();
	if (fwrite(&val, WriteSize<T>(), 1, wal_file) != 1) {
		throw IOException("WAL - Failed to write (%s)!", strerror(errno));
	}
}

void WriteAheadLog::WriteString(string &val, size_t &sz) {
	Write<uint32_t>(val.size(), sz);
	if (val.size() > 0) {
		sz -= val.size();
		if (fwrite(val.c_str(), val.size(), 1, wal_file) != 1) {
			throw IOException("WAL - Failed to write (%s)!", strerror(errno));
		}
	}
}

void WriteAheadLog::WriteData(uint8_t *dataptr, size_t data_size, size_t &sz) {
	sz -= data_size;
	if (fwrite(dataptr, data_size, 1, wal_file) != 1) {
		throw IOException("WAL - Failed to write (%s)!", strerror(errno));
	}
}

//===--------------------------------------------------------------------===//
// Write Entries
//===--------------------------------------------------------------------===//
void WriteAheadLog::WriteCreateTable(TableCatalogEntry *entry) {
	size_t size = 0;
	size += WriteSize(entry->schema->name);
	size += WriteSize(entry->name);
	size += WriteSize<uint32_t>(); // column count
	for (auto &column : entry->columns) {
		size += WriteSize(column.name);
		size += WriteSize<TypeId>();
	}
	WriteEntry(WALEntry::CREATE_TABLE, size);
	WriteString(entry->schema->name, size);
	WriteString(entry->name, size);
	Write<uint32_t>(entry->columns.size(), size);
	for (auto &column : entry->columns) {
		WriteString(column.name, size);
		Write<TypeId>(column.type, size);
	}
	assert(size == 0);
}

void WriteAheadLog::WriteDropTable(TableCatalogEntry *entry) {
	size_t size = 0;
	size += WriteSize(entry->schema->name);
	size += WriteSize(entry->name);
	WriteEntry(WALEntry::DROP_TABLE, size);
	WriteString(entry->schema->name, size);
	WriteString(entry->name, size);

	assert(size == 0);
}

void WriteAheadLog::WriteCreateSchema(SchemaCatalogEntry *entry) {
	size_t size = 0;
	size += WriteSize(entry->name);
	WriteEntry(WALEntry::CREATE_SCHEMA, size);
	WriteString(entry->name, size);

	assert(size == 0);
}

void WriteAheadLog::WriteDropSchema(SchemaCatalogEntry *entry) {
	size_t size = 0;
	size += WriteSize(entry->name);
	WriteEntry(WALEntry::DROP_SCHEMA, size);
	WriteString(entry->name, size);

	assert(size == 0);
}

void WriteAheadLog::WriteInsert(std::string &schema, std::string &table,
                                DataChunk &chunk) {
	chunk.Verify();
	if (chunk.sel_vector) {
		throw Exception("Cannot insert into WAL from chunk with SEL vector");
	}
	size_t size = 0;
	size += WriteSize(schema);
	size += WriteSize(table);
	// serialize the chunk
	size_t data_size = 0;
	auto data = chunk.Serialize(data_size);
	size += data_size;
	// now write the entry
	WriteEntry(WALEntry::INSERT_TUPLE, size);
	WriteString(schema, size);
	WriteString(table, size);
	WriteData(data.get(), data_size, size);

	assert(size == 0);
}

void WriteAheadLog::WriteQuery(std::string &query) {
	size_t size = 0;
	size += WriteSize(query);
	WriteEntry(WALEntry::QUERY, size);
	WriteString(query, size);

	assert(size == 0);
}

void WriteAheadLog::Flush() {
	WriteEntry(WALEntry::WAL_FLUSH, 0);
	// flushes all changes made to the WAL to disk
	fflush(wal_file);
	FileSync(wal_file);
}

//===--------------------------------------------------------------------===//
// Replay Entries
//===--------------------------------------------------------------------===//
bool ReplayDropTable(Transaction &transaction, Catalog &catalog,
                     uint8_t *dataptr, uint8_t *endptr);
bool ReplayCreateTable(Transaction &transaction, Catalog &catalog,
                       uint8_t *dataptr, uint8_t *endptr);
bool ReplayDropSchema(Transaction &transaction, Catalog &catalog,
                      uint8_t *dataptr, uint8_t *endptr);
bool ReplayCreateSchema(Transaction &transaction, Catalog &catalog,
                        uint8_t *dataptr, uint8_t *endptr);
bool ReplayInsert(Transaction &transaction, Catalog &catalog, uint8_t *dataptr,
                  uint8_t *endptr);
bool ReplayQuery(ClientContext &context, uint8_t *dataptr, uint8_t *endptr);

bool ReplayEntry(ClientContext &context, DuckDB &database, WALEntry entry,
                 uint8_t *dataptr) {
	uint8_t *endptr = dataptr + entry.size + 1;
	switch (entry.type) {
	case WALEntry::DROP_TABLE:
		return ReplayDropTable(context.ActiveTransaction(), database.catalog,
		                       dataptr, endptr);
	case WALEntry::CREATE_TABLE:
		return ReplayCreateTable(context.ActiveTransaction(), database.catalog,
		                         dataptr, endptr);
	case WALEntry::DROP_SCHEMA:
		return ReplayDropSchema(context.ActiveTransaction(), database.catalog,
		                        dataptr, endptr);
	case WALEntry::CREATE_SCHEMA:
		return ReplayCreateSchema(context.ActiveTransaction(), database.catalog,
		                          dataptr, endptr);
	case WALEntry::INSERT_TUPLE:
		return ReplayInsert(context.ActiveTransaction(), database.catalog,
		                    dataptr, endptr);
	case WALEntry::QUERY:
		return ReplayQuery(context, dataptr, endptr);
	default:
		// unrecognized WAL entry type
		return false;
	}
	return false;
}

bool ReplayCreateTable(Transaction &transaction, Catalog &catalog,
                       uint8_t *dataptr, uint8_t *endptr) {
	bool failed = false;
	auto schema_name = Read<std::string>(dataptr, endptr, failed);
	auto table_name = Read<std::string>(dataptr, endptr, failed);
	auto column_count = Read<uint32_t>(dataptr, endptr, failed);
	if (failed) {
		return false;
	}

	std::vector<ColumnDefinition> columns;
	for (size_t i = 0; i < column_count; i++) {
		auto column_name = Read<std::string>(dataptr, endptr, failed);
		auto column_type = Read<TypeId>(dataptr, endptr, failed);
		if (failed) {
			return false;
		}
		columns.push_back(ColumnDefinition(column_name, column_type, false));
	}
	try {
		catalog.CreateTable(transaction, schema_name, table_name, columns);
	} catch (...) {
		return false;
	}
	return true;
}

bool ReplayDropTable(Transaction &transaction, Catalog &catalog,
                     uint8_t *dataptr, uint8_t *endptr) {
	bool failed = false;
	auto schema_name = Read<std::string>(dataptr, endptr, failed);
	auto table_name = Read<std::string>(dataptr, endptr, failed);
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

bool ReplayCreateSchema(Transaction &transaction, Catalog &catalog,
                        uint8_t *dataptr, uint8_t *endptr) {
	bool failed = false;
	auto schema_name = Read<std::string>(dataptr, endptr, failed);
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

bool ReplayDropSchema(Transaction &transaction, Catalog &catalog,
                      uint8_t *dataptr, uint8_t *endptr) {
	throw NotImplementedException("Did not implement DROP SCHEMA yet!");
}

bool ReplayInsert(Transaction &transaction, Catalog &catalog, uint8_t *dataptr,
                  uint8_t *endptr) {
	bool failed = false;
	auto schema_name = Read<std::string>(dataptr, endptr, failed);
	auto table_name = Read<std::string>(dataptr, endptr, failed);
	DataChunk chunk;
	failed |= !chunk.Deserialize(dataptr, endptr - dataptr);
	if (failed) {
		return false;
	}

	// try {
	// first find the table
	auto table = catalog.GetTable(transaction, schema_name, table_name);
	// now append to the chunk
	table->storage->Append(transaction, chunk);
	// } catch(...) {
	// 	return false;
	// }
	return true;
}

bool ReplayQuery(ClientContext &context, uint8_t *dataptr, uint8_t *endptr) {
	// read the query
	bool failed = false;
	auto query = Read<std::string>(dataptr, endptr, failed);
	if (failed) {
		return false;
	}

	auto result = DuckDBConnection::GetQueryResult(context, query);
	return result->GetSuccess();
}
