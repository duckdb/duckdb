#include "duckdb/storage/table/column_data_checkpointer.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/storage/table/update_segment.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/parser/column_definition.hpp"
#include "duckdb/storage/table/scan_state.hpp"

namespace duckdb {

ColumnDataCheckpointer::ColumnDataCheckpointer(ColumnData &col_data_p, RowGroup &row_group_p,
                                               ColumnCheckpointState &state_p, ColumnCheckpointInfo &checkpoint_info_p)
    : col_data(col_data_p), row_group(row_group_p), state(state_p),
      is_validity(GetType().id() == LogicalTypeId::VALIDITY),
      intermediate(is_validity ? LogicalType::BOOLEAN : GetType(), true, is_validity),
      checkpoint_info(checkpoint_info_p) {

	auto &config = DBConfig::GetConfig(GetDatabase());
	auto functions = config.GetCompressionFunctions(GetType().InternalType());
	for (auto &func : functions) {
		compression_functions.push_back(&func.get());
	}
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
	for (idx_t segment_idx = 0; segment_idx < nodes.size(); segment_idx++) {
		auto &segment = *nodes[segment_idx].node;
		ColumnScanState scan_state;
		scan_state.current = &segment;
		segment.InitializeScan(scan_state);

		for (idx_t base_row_index = 0; base_row_index < segment.count; base_row_index += STANDARD_VECTOR_SIZE) {
			scan_vector.Reference(intermediate);

			idx_t count = MinValue<idx_t>(segment.count - base_row_index, STANDARD_VECTOR_SIZE);
			scan_state.row_index = segment.start + base_row_index;

			col_data.CheckpointScan(segment, scan_state, row_group.start, count, scan_vector);

			callback(scan_vector, count);
		}
	}
}

CompressionType ForceCompression(vector<optional_ptr<CompressionFunction>> &compression_functions,
                                 CompressionType compression_type) {
// On of the force_compression flags has been set
// check if this compression method is available
#ifdef DEBUG
	if (CompressionTypeIsDeprecated(compression_type)) {
		throw InternalException("Deprecated compression type: %s", CompressionTypeToString(compression_type));
	}
#endif
	bool found = false;
	for (idx_t i = 0; i < compression_functions.size(); i++) {
		auto &compression_function = *compression_functions[i];
		if (compression_function.type == compression_type) {
			found = true;
			break;
		}
	}
	if (found) {
		// the force_compression method is available
		// clear all other compression methods
		// except the uncompressed method, so we can fall back on that
		for (idx_t i = 0; i < compression_functions.size(); i++) {
			auto &compression_function = *compression_functions[i];
			if (compression_function.type == CompressionType::COMPRESSION_UNCOMPRESSED) {
				continue;
			}
			if (compression_function.type != compression_type) {
				compression_functions[i] = nullptr;
			}
		}
	}
	return found ? compression_type : CompressionType::COMPRESSION_AUTO;
}

unique_ptr<AnalyzeState> ColumnDataCheckpointer::DetectBestCompressionMethod(idx_t &compression_idx) {
	D_ASSERT(!compression_functions.empty());
	auto &config = DBConfig::GetConfig(GetDatabase());
	CompressionType forced_method = CompressionType::COMPRESSION_AUTO;

	auto compression_type = checkpoint_info.GetCompressionType();
	if (compression_type != CompressionType::COMPRESSION_AUTO) {
		forced_method = ForceCompression(compression_functions, compression_type);
	}
	if (compression_type == CompressionType::COMPRESSION_AUTO &&
	    config.options.force_compression != CompressionType::COMPRESSION_AUTO) {
		forced_method = ForceCompression(compression_functions, config.options.force_compression);
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
			bool success = false;
			if (analyze_states[i]) {
				success = compression_functions[i]->analyze(*analyze_states[i], scan_vector, count);
			}
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
		if (!analyze_states[i]) {
			continue;
		}
		//! Check if the method type is the forced method (if forced is used)
		bool forced_method_found = compression_functions[i]->type == forced_method;
		auto score = compression_functions[i]->final_analyze(*analyze_states[i]);

		//! The finalize method can return this value from final_analyze to indicate it should not be used.
		if (score == DConstants::INVALID_INDEX) {
			continue;
		}

		if (score < best_score || forced_method_found) {
			compression_idx = i;
			best_score = score;
			state = std::move(analyze_states[i]);
		}
		//! If we have found the forced method, we're done
		if (forced_method_found) {
			break;
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
	for (idx_t segment_idx = 0; segment_idx < nodes.size(); segment_idx++) {
		auto segment = nodes[segment_idx].node.get();
		segment->CommitDropSegment();
	}

	// now we need to write our segment
	// we will first run an analyze step that determines which compression function to use
	idx_t compression_idx;
	auto analyze_state = DetectBestCompressionMethod(compression_idx);

	if (!analyze_state) {
		throw FatalException("No suitable compression/storage method found to store column");
	}

	// now that we have analyzed the compression functions we can start writing to disk
	auto best_function = compression_functions[compression_idx];
	auto compress_state = best_function->init_compression(*this, std::move(analyze_state));

	ScanSegments(
	    [&](Vector &scan_vector, idx_t count) { best_function->compress(*compress_state, scan_vector, count); });
	best_function->compress_finalize(*compress_state);

	nodes.clear();
}

bool ColumnDataCheckpointer::HasChanges() {
	for (idx_t segment_idx = 0; segment_idx < nodes.size(); segment_idx++) {
		auto segment = nodes[segment_idx].node.get();
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
	for (idx_t segment_idx = 0; segment_idx < nodes.size(); segment_idx++) {
		auto segment = nodes[segment_idx].node.get();
		auto pointer = segment->GetDataPointer();

		// merge the persistent stats into the global column stats
		state.global_stats->Merge(segment->stats.statistics);

		// directly append the current segment to the new tree
		state.new_tree.AppendSegment(std::move(nodes[segment_idx].node));

		state.data_pointers.push_back(std::move(pointer));
	}
}

void ColumnDataCheckpointer::Checkpoint(vector<SegmentNode<ColumnSegment>> nodes_p) {
	D_ASSERT(!nodes_p.empty());
	this->nodes = std::move(nodes_p);
	// first check if any of the segments have changes
	if (!HasChanges()) {
		// no changes: only need to write the metadata for this column
		WritePersistentSegments();
	} else {
		// there are changes: rewrite the set of columns);
		WriteToDisk();
	}
}

CompressionFunction &ColumnDataCheckpointer::GetCompressionFunction(CompressionType compression_type) {
	auto &db = GetDatabase();
	auto &column_type = GetType();
	auto &config = DBConfig::GetConfig(db);
	return *config.GetCompressionFunction(compression_type, column_type.InternalType());
}

} // namespace duckdb
