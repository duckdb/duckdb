#include "duckdb/storage/checkpoint/checkpointed_table_size.hpp"

#include "duckdb/common/serializer/binary_deserializer.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/function/compression_info.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/storage/metadata/metadata_reader.hpp"
#include "duckdb/storage/table/column_data.hpp"
#include "duckdb/storage/table/data_table_info.hpp"
#include "duckdb/storage/table/row_group.hpp"
#include "duckdb/storage/table_io_manager.hpp"

namespace duckdb {

CheckpointedTableSize::CheckpointedTableSize(MetaBlockPointer directory_p, idx_t row_group_count_p,
                                             vector<LogicalType> column_types_p)
    : directory(directory_p), row_group_count(row_group_count_p) {
	if (row_group_count != 0) {
		column_types = std::move(column_types_p);
	}
}

static bool AccumulateStorageSize(const PersistentColumnData &column, double &bytes,
                                  unordered_set<block_id_t> &overflow_blocks) {
	for (auto &pointer : column.pointers) {
		if (pointer.block_pointer.IsValid()) {
			if (!pointer.byte_size) {
				return false;
			}
			bytes += *pointer.byte_size;
		}
		if (pointer.segment_state) {
			const auto &blocks = pointer.segment_state->blocks;
			overflow_blocks.insert(blocks.begin(), blocks.end());
		}
	}
	for (auto &child : column.child_columns) {
		if (!AccumulateStorageSize(child, bytes, overflow_blocks)) {
			return false;
		}
	}
	return true;
}

optional_idx CheckpointedTableSize::Estimate(ClientContext &context, DataTableInfo &table, idx_t sample_row_groups) {
	D_ASSERT(sample_row_groups > 0);
	if (row_group_count == 0) {
		return 0;
	}
	const auto sample_count = MinValue(sample_row_groups, row_group_count);
	lock_guard<mutex> guard(cache_lock);
	if (cached_sample_count != sample_count) {
		cached_bytes = Sample(context, table, sample_count);
		cached_sample_count = sample_count;
	}
	return cached_bytes;
}

optional_idx CheckpointedTableSize::Sample(ClientContext &context, DataTableInfo &table, idx_t sample_count) const {
	auto &block_manager = table.GetIOManager().GetBlockManagerForRowData();
	auto &metadata_manager = block_manager.GetMetadataManager();
	auto &db = table.GetDB();
	CompressionInfo compression_info(block_manager);
	MetadataReader reader(metadata_manager, directory);
	BinaryDeserializer groups(reader);
	RandomEngine random;
	const auto groups_per_stratum = row_group_count / sample_count;
	const auto remainder = row_group_count % sample_count;
	double estimated_bytes = 0;
	for (idx_t stratum = 0; stratum < sample_count; stratum++) {
		const auto group_count = groups_per_stratum + (stratum < remainder);
		RowGroupPointer sample;
		double stratum_rows = 0;
		for (idx_t i = 0; i < group_count; i++) {
			context.InterruptCheck();
			groups.Begin();
			auto group = RowGroup::Deserialize(groups);
			groups.End();
			if (SelectRowGroup(random, double(group.tuple_count), stratum_rows)) {
				sample = std::move(group);
			}
		}
		if (stratum_rows == 0) {
			continue;
		}
		if (sample.data_pointers.size() != column_types.size()) {
			throw DataCorruptionException("Row group column count is unaligned with table column count");
		}
		double sample_bytes = 0;
		unordered_set<block_id_t> overflow_blocks;
		for (idx_t i = 0; i < column_types.size(); i++) {
			context.InterruptCheck();
			MetadataReader column_reader(metadata_manager, sample.data_pointers[i]);
			BinaryDeserializer deserializer(column_reader);
			deserializer.Set<DatabaseInstance &>(db.GetDatabase());
			deserializer.Set<Catalog &>(db.GetCatalog());
			deserializer.Set<const CompressionInfo &>(compression_info);
			deserializer.Set<const LogicalType &>(column_types[i]);
			deserializer.Begin();
			auto column = PersistentColumnData::Deserialize(deserializer);
			deserializer.End();
			if (!AccumulateStorageSize(column, sample_bytes, overflow_blocks)) {
				// Older storage formats can lack segment byte sizes.
				return optional_idx();
			}
		}
		sample_bytes += double(overflow_blocks.size()) * double(block_manager.GetBlockSize());
		// The sampled group's inclusion probability is its share of the stratum's rows.
		estimated_bytes += sample_bytes * (stratum_rows / double(sample.tuple_count));
	}
	if (estimated_bytes >= double(NumericLimits<int64_t>::Maximum())) {
		return optional_idx();
	}
	return idx_t(estimated_bytes);
}

} // namespace duckdb
