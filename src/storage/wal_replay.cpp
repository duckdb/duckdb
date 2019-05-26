#include "storage/write_ahead_log.hpp"
#include "common/serializer/buffered_file_reader.hpp"

#include "parser/parsed_data/drop_info.hpp"

using namespace duckdb;
using namespace std;

static void ReplayEntry(ClientContext &context, DuckDB &database, WALType entry_type, Deserializer &source);

void WriteAheadLog::Replay(DuckDB &database, string &path) {
	BufferedFileReader reader(*database.file_system, path.c_str());

	if (reader.Finished()) {
		// WAL is empty
		return;
	}

	ClientContext context(database);
	context.transaction.SetAutoCommit(false);
	context.transaction.BeginTransaction();

	// replay the WAL
	// note that everything is wrapped inside a try/catch block here
	// there can be errors in WAL replay because of a corrupt WAL file
	// in this case we should throw a warning but startup anyway
	try {
		while (true) {
			// read the current entry
			WALType entry_type = reader.Read<WALType>();
			if (entry_type == WALType::WAL_FLUSH) {
				// flush: commit the current transaction
				context.transaction.Commit();
				// check if the file is exhausted
				if (reader.Finished()) {
					// we finished reading the file: break
					break;
				}
				// otherwise we keep on reading
				context.transaction.BeginTransaction();
			} else {
				// replay the entry
				ReplayEntry(context, database, entry_type, reader);
			}
		}
	} catch (Exception &ex) {
		// FIXME: this report a proper warning in the connection
		fprintf(stderr, "Exception in WAL playback: %s\n", ex.what());
		// exception thrown in WAL replay: rollback
		context.transaction.Rollback();
	} catch (...) {
		// exception thrown in WAL replay: rollback
		context.transaction.Rollback();
	}
}

//===--------------------------------------------------------------------===//
// Replay Entries
//===--------------------------------------------------------------------===//
void ReplayCreateTable(ClientContext &context, Catalog &catalog, Deserializer &source);
void ReplayDropTable(ClientContext &context, Catalog &catalog, Deserializer &source);

void ReplayCreateView(ClientContext &context, Catalog &catalog, Deserializer &source);
void ReplayDropView(ClientContext &context, Catalog &catalog, Deserializer &source);

void ReplayCreateSchema(ClientContext &context, Catalog &catalog, Deserializer &source);
void ReplayDropSchema(ClientContext &context, Catalog &catalog, Deserializer &source);

void ReplayCreateSequence(ClientContext &context, Catalog &catalog, Deserializer &source);
void ReplayDropSequence(ClientContext &context, Catalog &catalog, Deserializer &source);
void ReplaySequenceValue(ClientContext &context, Catalog &catalog, Deserializer &source);

void ReplayInsert(ClientContext &context, Catalog &catalog, Deserializer &source);
void ReplayQuery(ClientContext &context, Deserializer &source);

void ReplayEntry(ClientContext &context, DuckDB &database, WALType entry_type, Deserializer &source) {
	switch (entry_type) {
	case WALType::CREATE_TABLE:
		ReplayCreateTable(context, *database.catalog, source);
		break;
	case WALType::DROP_TABLE:
		ReplayDropTable(context, *database.catalog, source);
		break;
	case WALType::CREATE_VIEW:
		ReplayCreateView(context, *database.catalog, source);
		break;
	case WALType::DROP_VIEW:
		ReplayDropView(context, *database.catalog, source);
		break;
	case WALType::CREATE_SCHEMA:
		ReplayCreateSchema(context, *database.catalog, source);
		break;
	case WALType::DROP_SCHEMA:
		ReplayDropSchema(context, *database.catalog, source);
		break;
	case WALType::CREATE_SEQUENCE:
		ReplayCreateSequence(context, *database.catalog, source);
		break;
	case WALType::DROP_SEQUENCE:
		ReplayDropSequence(context, *database.catalog, source);
		break;
	case WALType::SEQUENCE_VALUE:
		ReplaySequenceValue(context, *database.catalog, source);
		break;
	case WALType::INSERT_TUPLE:
		ReplayInsert(context, *database.catalog, source);
		break;
	case WALType::QUERY:
		ReplayQuery(context, source);
		break;
	default:
		throw Exception("Invalid WAL entry type!");
	}
}

//===--------------------------------------------------------------------===//
// Replay Table
//===--------------------------------------------------------------------===//
void ReplayCreateTable(ClientContext &context, Catalog &catalog, Deserializer &source) {
	auto info = TableCatalogEntry::Deserialize(source);

	// bind the constraints to the table again
	Binder binder(context);
	auto bound_info = binder.BindCreateTableInfo(move(info));

	catalog.CreateTable(context.ActiveTransaction(), bound_info.get());
}

void ReplayDropTable(ClientContext &context, Catalog &catalog, Deserializer &source) {
	DropInfo info;

	info.type = CatalogType::TABLE;
	info.schema = source.Read<string>();
	info.name = source.Read<string>();

	catalog.DropTable(context.ActiveTransaction(), &info);
}

//===--------------------------------------------------------------------===//
// Replay View
//===--------------------------------------------------------------------===//
void ReplayCreateView(ClientContext &context, Catalog &catalog, Deserializer &source) {
	auto entry = ViewCatalogEntry::Deserialize(source);

	catalog.CreateView(context.ActiveTransaction(), entry.get());
}

void ReplayDropView(ClientContext &context, Catalog &catalog, Deserializer &source) {
	DropInfo info;
	info.type = CatalogType::VIEW;
	info.schema = source.Read<string>();
	info.name = source.Read<string>();
	catalog.DropView(context.ActiveTransaction(), &info);
}

//===--------------------------------------------------------------------===//
// Replay Schema
//===--------------------------------------------------------------------===//
void ReplayCreateSchema(ClientContext &context, Catalog &catalog, Deserializer &source) {
	CreateSchemaInfo info;
	info.schema = source.Read<string>();

	catalog.CreateSchema(context.ActiveTransaction(), &info);
}

void ReplayDropSchema(ClientContext &context, Catalog &catalog, Deserializer &source) {
	DropInfo info;

	info.type = CatalogType::SCHEMA;
	info.name = source.Read<string>();

	catalog.DropSchema(context.ActiveTransaction(), &info);
}

//===--------------------------------------------------------------------===//
// Replay Sequence
//===--------------------------------------------------------------------===//
void ReplayCreateSequence(ClientContext &context, Catalog &catalog, Deserializer &source) {
	auto entry = SequenceCatalogEntry::Deserialize(source);

	catalog.CreateSequence(context.ActiveTransaction(), entry.get());
}

void ReplayDropSequence(ClientContext &context, Catalog &catalog, Deserializer &source) {
	DropInfo info;
	info.type = CatalogType::SEQUENCE;
	info.schema = source.Read<string>();
	info.name = source.Read<string>();

	catalog.DropSequence(context.ActiveTransaction(), &info);
}

void ReplaySequenceValue(ClientContext &context, Catalog &catalog, Deserializer &source) {
	auto schema = source.Read<string>();
	auto name = source.Read<string>();
	auto usage_count = source.Read<uint64_t>();
	auto counter = source.Read<int64_t>();

	// fetch the sequence from the catalog
	auto seq = catalog.GetSequence(context.ActiveTransaction(), schema, name);
	if (usage_count > seq->usage_count) {
		seq->usage_count = usage_count;
		seq->counter = counter;
	}
}

//===--------------------------------------------------------------------===//
// Replay Data
//===--------------------------------------------------------------------===//
void ReplayInsert(ClientContext &context, Catalog &catalog, Deserializer &source) {
	auto schema_name = source.Read<string>();
	auto table_name = source.Read<string>();
	DataChunk chunk;

	chunk.Deserialize(source);

	// first find the table
	auto table = catalog.GetTable(context.ActiveTransaction(), schema_name, table_name);
	// now append to the chunk
	table->storage->Append(*table, context, chunk);
}

void ReplayQuery(ClientContext &context, Deserializer &source) {
	// read the query
	auto query = source.Read<string>();

	context.Query(query, false);
}
