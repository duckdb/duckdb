#include "duckdb/storage/checkpoint/table_data_writer.hpp"

#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/common/types/null_value.hpp"

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/serializer/buffered_serializer.hpp"

#include "duckdb/storage/numeric_segment.hpp"
#include "duckdb/storage/string_segment.hpp"
#include "duckdb/storage/table/validity_segment.hpp"
#include "duckdb/storage/table/column_segment.hpp"
#include "duckdb/storage/table/persistent_segment.hpp"
#include "duckdb/storage/table/transient_segment.hpp"
#include "duckdb/storage/table/update_segment.hpp"
#include "duckdb/storage/column_data.hpp"
#include "duckdb/storage/table/morsel_info.hpp"

namespace duckdb {

TableDataWriter::TableDataWriter(DatabaseInstance &db, TableCatalogEntry &table, MetaBlockWriter &meta_writer)
    : db(db), table(table), meta_writer(meta_writer) {
}

TableDataWriter::~TableDataWriter() {
}

void TableDataWriter::WriteTableData() {
	// start scanning the table and append the data to the uncompressed segments
	table.storage->Checkpoint(*this);

	// then we checkpoint the deleted tuples
	table.storage->CheckpointDeletes(*this);
}

void TableDataWriter::CheckpointDeletes(MorselInfo *morsel_info) {
	// deletes! write them after the data pointers
	while (morsel_info) {
		if (morsel_info->root) {
			// first count how many ChunkInfo's we need to deserialize
			idx_t chunk_info_count = 0;
			for (idx_t vector_idx = 0; vector_idx < MorselInfo::MORSEL_VECTOR_COUNT; vector_idx++) {
				auto chunk_info = morsel_info->root->info[vector_idx].get();
				if (!chunk_info) {
					continue;
				}
				chunk_info_count++;
			}
			meta_writer.Write<idx_t>(chunk_info_count);
			for (idx_t vector_idx = 0; vector_idx < MorselInfo::MORSEL_VECTOR_COUNT; vector_idx++) {
				auto chunk_info = morsel_info->root->info[vector_idx].get();
				if (!chunk_info) {
					continue;
				}
				meta_writer.Write<idx_t>(vector_idx);
				chunk_info->Serialize(meta_writer);
			}
		} else {
			meta_writer.Write<idx_t>(0);
		}
		morsel_info = (MorselInfo *)morsel_info->next.get();
	}
}

} // namespace duckdb
