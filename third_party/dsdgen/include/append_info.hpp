#include "catalog/catalog_entry/table_catalog_entry.hpp"
#include "common/types/data_chunk.hpp"
#include "main/client_context.hpp"

#include "config.h"
#include "porting.h"

#include "tdefs.h"

namespace tpcds {

struct tpcds_append_information {
	duckdb::TableCatalogEntry *table;
	tdef *table_def;
	duckdb::DataChunk chunk;
	duckdb::ClientContext *context;
	size_t col;
	size_t row;
};

} // namespace tpcds
