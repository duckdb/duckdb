#include "duckdb/storage/index_storage_info.hpp"

#include "duckdb/common/allocator.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/common/types/data_chunk.hpp"

namespace duckdb {

BufferedIndexDataInfo BufferedIndexDataInfo::FromCollection(BufferedIndexReplay replay_type,
                                                            const ColumnDataCollection &collection) {
	BufferedIndexDataInfo info;
	info.replay_type = replay_type;
	const auto column_count = collection.ColumnCount();
	info.types = collection.Types();
	info.values.resize(column_count);
	for (auto &chunk : collection.Chunks()) {
		for (idx_t c = 0; c < chunk.ColumnCount(); c++) {
			for (idx_t r = 0; r < chunk.size(); r++) {
				info.values[c].push_back(chunk.GetValue(c, r));
			}
		}
	}
	return info;
}

unique_ptr<ColumnDataCollection> BufferedIndexDataInfo::ToColumnDataCollection(Allocator &allocator) const {
	auto collection = make_uniq<ColumnDataCollection>(allocator, types);
	if (values.empty() || types.empty()) {
		return collection;
	}
	DataChunk chunk;
	chunk.Initialize(allocator, types);

	const idx_t row_count = values[0].size();
	for (idx_t row = 0; row < row_count; row++) {
		for (idx_t col = 0; col < types.size(); col++) {
			chunk.SetValue(col, chunk.size(), values[col][row]);
		}
		chunk.SetCardinality(chunk.size() + 1);
		if (chunk.size() == STANDARD_VECTOR_SIZE) {
			collection->Append(chunk);
			chunk.Reset();
		}
	}
	if (chunk.size() > 0) {
		collection->Append(chunk);
	}
	return collection;
}

} // namespace duckdb
