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

void ForceCompression(vector<CompressionFunction *> &compression_functions, CompressionType compression_type) {
	// On of the force_compression flags has been set
	// check if this compression method is available
	bool found = false;
	for (idx_t i = 0; i < compression_functions.size(); i++) {
		if (compression_functions[i]->type == compression_type) {
			found = true;
			break;
		}
	}
	if (found) {
		// the force_compression method is available
		// clear all other compression methods
		for (idx_t i = 0; i < compression_functions.size(); i++) {
			if (compression_functions[i]->type != compression_type) {
				compression_functions[i] = nullptr;
			}
		}
	}
}

unique_ptr<AnalyzeState> ColumnDataCheckpointer::DetectBestCompressionMethod(idx_t &compression_idx) {
	D_ASSERT(!compression_functions.empty());
	auto &config = DBConfig::GetConfig(GetDatabase());

	auto compression_type = checkpoint_info.compression_type;
	if (compression_type != CompressionType::COMPRESSION_AUTO) {
		ForceCompression(compression_functions, compression_type);
	}
	if (compression_type == CompressionType::COMPRESSION_AUTO &&
	    config.options.force_compression != CompressionType::COMPRESSION_AUTO) {
		ForceCompression(compression_functions, config.options.force_compression);
	}
	// set up the analyze states for each compression method
	vector<unique_ptr<AnalyzeState>> analyze_states;
	analyze_states.reserve(compression_functions.size());
	for (idx_t i = 0; i < compression_functions.size(); i++) {
		if (!compression_functions[i]) {
			analyze_states.push_back(nullptr);
			continue;
		}
		analyze_states.push_back(compression_functions[i]->init_analyze(col_data, col_data.type.InternalType()));
	}

	// scan over all the segments and run the analyze step
	ScanSegments([&](Vector &scan_vector, idx_t count) {
		for (idx_t i = 0; i < compression_functions.size(); i++) {
			if (!compression_functions[i]) {
				continue;
			}
			auto success = compression_functions[i]->analyze(*analyze_states[i], scan_vector, count);
			if (!success) {
				// could not use this compression function on this data set
				// erase it
				compression_functions[i] = nullptr;
				analyze_states[i].reset();
			}
		}
	});

	// now that we have passed over all the data, we need to figure out the best method
	// we do this using the final_analyze method
	unique_ptr<AnalyzeState> state;
	compression_idx = DConstants::INVALID_INDEX;
	idx_t best_score = NumericLimits<idx_t>::Maximum();
	for (idx_t i = 0; i < compression_functions.size(); i++) {
		if (!compression_functions[i]) {
			continue;
		}
		auto score = compression_functions[i]->final_analyze(*analyze_states[i]);
		if (score < best_score) {
			compression_idx = i;
			best_score = score;
			state = move(analyze_states[i]);
		}
	}
	return state;
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
	idx_t compression_idx;
	auto analyze_state = DetectBestCompressionMethod(compression_idx);

	if (!analyze_state) {
		throw InternalException("No suitable compression/storage method found to store column");
	}

	// now that we have analyzed the compression functions we can start writing to disk
	auto best_function = compression_functions[compression_idx];
	auto compress_state = best_function->init_compression(*this, move(analyze_state));
	ScanSegments(
	    [&](Vector &scan_vector, idx_t count) { best_function->compress(*compress_state, scan_vector, count); });
	best_function->compress_finalize(*compress_state);

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
