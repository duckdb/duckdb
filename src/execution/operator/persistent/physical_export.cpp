#include "duckdb/execution/operator/persistent/physical_export.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/transaction/transaction.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"

#include <algorithm>
#include <sstream>

namespace duckdb {
using namespace std;

static void WriteCatalogEntries(stringstream& ss, vector<CatalogEntry*> &entries) {
	for(auto &entry : entries) {
		ss << entry->ToSQL() << std::endl;
	}
	ss << std::endl;
}

void PhysicalExport::GetChunkInternal(ExecutionContext &context, DataChunk &chunk, PhysicalOperatorState *state) {
	auto &ccontext = context.client;
	auto &fs = FileSystem::GetFileSystem(ccontext);

	// gather all catalog types to export
	vector<CatalogEntry*> schemas;
	vector<CatalogEntry*> sequences;
	vector<CatalogEntry*> tables;
	vector<CatalogEntry*> views;
	vector<CatalogEntry*> indexes;

	auto &transaction = Transaction::GetTransaction(ccontext);
	Catalog::GetCatalog(ccontext).schemas->Scan(transaction, [&](CatalogEntry *entry) {
		auto schema = (SchemaCatalogEntry *)entry;
		if (schema->name != DEFAULT_SCHEMA) {
			// export schema
			schemas.push_back(schema);
		}
		schema->tables.Scan(transaction, [&](CatalogEntry *entry) {
			if (entry->type == CatalogType::TABLE) {
				tables.push_back(entry);
			} else {
				views.push_back(entry);
			}
		});
		schema->sequences.Scan(transaction, [&](CatalogEntry *entry) {
			sequences.push_back(entry);
		});
		schema->indexes.Scan(transaction, [&](CatalogEntry *entry) {
			indexes.push_back(entry);
		});
	});

	// now write the schema.sql file
	// export order is SCHEMA -> SEQUENCE -> TABLE -> VIEW -> INDEX

	stringstream ss;
	WriteCatalogEntries(ss, schemas);
	WriteCatalogEntries(ss, sequences);
	WriteCatalogEntries(ss, tables);
	WriteCatalogEntries(ss, views);
	WriteCatalogEntries(ss, indexes);

	auto schema_str = ss.str();
	// write the final schema to the file
	auto schema_file = fs.JoinPath(info->file_path, "schema.sql");
	auto handle = fs.OpenFile(schema_file, FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_FILE_CREATE_NEW, FileLockType::WRITE_LOCK);
	fs.Write(*handle, (void *)schema_str.c_str(), schema_str.size());
	handle.reset();



	state->finished = true;
}

} // namespace duckdb
