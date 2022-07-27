#include "duckdb/storage/table/column_data_checkpointer.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/storage/table/update_segment.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/parser/column_definition.hpp"
namespace duckdb {

ColumnDataCheckpointer::ColumnDataCheckpointer(ColumnData &col_data_p, RowGroup &row_group_p,
                                               ColumnCheckpointState &state_p, ColumnCheckpointInfo &checkpoint_info_p)
    : col_data(col_data_p), row_group(row_group_p), state(state_p),
      is_validity(GetType().id() == LogicalTypeId::VALIDITY),
      intermediate(is_validity ? LogicalType::BOOLEAN : GetType(), true, is_validity),
      checkpoint_info(checkpoint_info_p) {
	auto &config = DBConfig::GetConfig(GetDatabase());
	compression_functions = config.GetCompressionFunctions(GetType().InternalType());
}

DatabaseInstance &ColumnDataCheckpointer::GetDatabase() {
	return col_data.GetDatabase();
}

const LogicalType &ColumnDataCheckpointer::GetType() const {
	return col_data.type;
}

ColumnData &ColumnDataCheckpointer::GetColumnData() {
	return col_data;
}

RowGroup &ColumnDataCheckpointer::GetRowGroup() {
	return row_group;
}

ColumnCheckpointState &ColumnDataCheckpointer::GetCheckpointState() {
	return state;
}

void ColumnDataCheckpointer::ScanSegments(const std::function<void(Vector &, idx_t)> &callback) {
	Vector scan_vector(intermediate.GetType(), nullptr);
	for (auto segment = (ColumnSegment *)owned_segment.get(); segment; segment = (ColumnSegment *)segment->next.get()) {
		ColumnScanState scan_state;
		scan_state.current = segment;
		segment->InitializeScan(scan_state);

		for (idx_t base_row_index = 0; base_row_index < segment->count; base_row_index += STANDARD_VECTOR_SIZE) {
			scan_vector.Reference(intermediate);

			idx_t count = MinValue<idx_t>(segment->count - base_row_index, STANDARD_VECTOR_SIZE);
			scan_state.row_index = segment->start + base_row_index;

			col_data.CheckpointScan(segment, scan_state, row_group.start, count, scan_vector);

			callback(scan_vector, count);
		}
	}
}

void ColumnDataCheckpointer::WriteToDisk() {
	// there were changes or transient segments
	// we need to rewrite the column segments to disk

	// first we check the current segments
	// if there are any persistent segments, we will mark their old block ids as modified
	// since the segments will be rewritten their old on disk data is no longer required
	auto &block_manager = BlockManager::GetBlockManager(GetDatabase());
	for (auto segment = (ColumnSegment *)owned_segment.get(); segment; segment = (ColumnSegment *)segment->next.get()) {
		if (segment->segment_type == ColumnSegmentType::PERSISTENT) {
			// persistent segment has updates: mark it as modified and rewrite the block with the merged updates
			auto block_id = segment->GetBlockId();
			if (block_id != INVALID_BLOCK) {
				block_manager.MarkBlockAsModified(block_id);
			}
		}
	}

	// now we need to write our segment
	// we will first run an analyze step that determines which compression function to use
	if (is_validity) {
		row_group.DetectBestCompressionMethod(&col_data, checkpoint_info.compression_type, is_validity);
	}

	auto best_function = compression_functions[col_data.compression_idx];
	auto analyze_state = move(col_data.state);

	if (!analyze_state) {
		throw InternalException("No suitable compression/storage method found to store column");
	}

	// now that we have analyzed the compression functions we can start writing to disk
	auto compress_state = best_function->init_compression(*this, move(analyze_state));
	ScanSegments(
	    [&](Vector &scan_vector, idx_t count) { best_function->compress(*compress_state, scan_vector, count); });
	best_function->compress_finalize(*compress_state);

	// now we actually write the data to disk
	owned_segment.reset();
}

bool ColumnDataCheckpointer::HasChanges() {
	for (auto segment = (ColumnSegment *)owned_segment.get(); segment; segment = (ColumnSegment *)segment->next.get()) {
		if (segment->segment_type == ColumnSegmentType::TRANSIENT) {
			// transient segment: always need to write to disk
			return true;
		} else {
			// persistent segment; check if there were any updates or deletions in this segment
			idx_t start_row_idx = segment->start - row_group.start;
			idx_t end_row_idx = start_row_idx + segment->count;
			if (col_data.updates && col_data.updates->HasUpdates(start_row_idx, end_row_idx)) {
				return true;
			}
		}
	}
	return false;
}

void ColumnDataCheckpointer::WritePersistentSegments() {
	// all segments are persistent and there are no updates
	// we only need to write the metadata
	auto segment = (ColumnSegment *)owned_segment.get();
	while (segment) {
		auto next_segment = move(segment->next);

		D_ASSERT(segment->segment_type == ColumnSegmentType::PERSISTENT);

		// set up the data pointer directly using the data from the persistent segment
		DataPointer pointer;
		pointer.block_pointer.block_id = segment->GetBlockId();
		pointer.block_pointer.offset = segment->GetBlockOffset();
		pointer.row_start = segment->start;
		pointer.tuple_count = segment->count;
		pointer.compression_type = segment->function->type;
		pointer.statistics = segment->stats.statistics->Copy();

		// merge the persistent stats into the global column stats
		state.global_stats->Merge(*segment->stats.statistics);

		// directly append the current segment to the new tree
		state.new_tree.AppendSegment(move(owned_segment));

		state.data_pointers.push_back(move(pointer));

		// move to the next segment in the list
		owned_segment = move(next_segment);
		segment = (ColumnSegment *)owned_segment.get();
	}
}

void ColumnDataCheckpointer::Checkpoint(unique_ptr<SegmentBase> segment) {
	D_ASSERT(!owned_segment);
	this->owned_segment = move(segment);
	// first check if any of the segments have changes
	if (!HasChanges()) {
		// no changes: only need to write the metadata for this column
		WritePersistentSegments();
	} else {
		// there are changes: rewrite the set of columns
		WriteToDisk();
	}
}

} // namespace duckdb
