#include "duckdb/storage/table/column_data_checkpointer.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/storage/table/update_segment.hpp"
#include "duckdb/storage/compression/empty_validity.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/parser/column_definition.hpp"
#include "duckdb/storage/table/scan_state.hpp"
#include "duckdb/main/database.hpp"

namespace duckdb {

//! ColumnDataCheckpointData

CompressionFunction &ColumnDataCheckpointData::GetCompressionFunction(CompressionType compression_type) {
	auto &db = col_data->GetDatabase();
	auto &column_type = col_data->type;
	auto &config = DBConfig::GetConfig(db);
	return *config.GetCompressionFunction(compression_type, column_type.InternalType());
}

DatabaseInstance &ColumnDataCheckpointData::GetDatabase() {
	return col_data->GetDatabase();
}

const LogicalType &ColumnDataCheckpointData::GetType() const {
	return col_data->type;
}

ColumnData &ColumnDataCheckpointData::GetColumnData() {
	return *col_data;
}

RowGroup &ColumnDataCheckpointData::GetRowGroup() {
	return *row_group;
}

ColumnCheckpointState &ColumnDataCheckpointData::GetCheckpointState() {
	return *checkpoint_state;
}

//! ColumnDataCheckpointer

static Vector CreateIntermediateVector(vector<reference<ColumnCheckpointState>> &states) {
	D_ASSERT(!states.empty());

	auto &first_state = states[0];
	auto &col_data = first_state.get().column_data;
	auto &type = col_data.type;
	if (type.id() == LogicalTypeId::VALIDITY) {
		return Vector(LogicalType::BOOLEAN, true, /* initialize_to_zero = */ true);
	}
	return Vector(type, true, false);
}

ColumnDataCheckpointer::ColumnDataCheckpointer(vector<reference<ColumnCheckpointState>> &checkpoint_states,
                                               DatabaseInstance &db, RowGroup &row_group,
                                               ColumnCheckpointInfo &checkpoint_info)
    : checkpoint_states(checkpoint_states), db(db), row_group(row_group),
      intermediate(CreateIntermediateVector(checkpoint_states)), checkpoint_info(checkpoint_info) {

	auto &config = DBConfig::GetConfig(db);
	compression_functions.resize(checkpoint_states.size());
	for (idx_t i = 0; i < checkpoint_states.size(); i++) {
		auto &col_data = checkpoint_states[i].get().column_data;
		auto to_add = config.GetCompressionFunctions(col_data.type.InternalType());
		auto &functions = compression_functions[i];
		for (auto &func : to_add) {
			functions.push_back(&func.get());
		}
	}
}

void ColumnDataCheckpointer::ScanSegments(const std::function<void(Vector &, idx_t)> &callback) {
	Vector scan_vector(intermediate.GetType(), nullptr);
	auto &first_state = checkpoint_states[0];
	auto &col_data = first_state.get().column_data;
	auto &nodes = col_data.data.ReferenceSegments();

	// TODO: scan all the nodes from all segments, no need for CheckpointScan to virtualize this I think..
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
	if (!found) {
		return CompressionType::COMPRESSION_AUTO;
	}
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
	return compression_type;
}

void ColumnDataCheckpointer::InitAnalyze() {
	analyze_states.resize(checkpoint_states.size());
	for (idx_t i = 0; i < checkpoint_states.size(); i++) {
		if (!has_changes[i]) {
			continue;
		}

		auto &functions = compression_functions[i];
		auto &states = analyze_states[i];
		auto &checkpoint_state = checkpoint_states[i];
		auto &coldata = checkpoint_state.get().column_data;
		states.resize(functions.size());
		for (idx_t j = 0; j < functions.size(); j++) {
			auto &func = functions[j];
			if (!func) {
				continue;
			}
			states[j] = func->init_analyze(coldata, coldata.type.InternalType());
		}
	}
}

vector<CheckpointAnalyzeResult> ColumnDataCheckpointer::DetectBestCompressionMethod() {
	D_ASSERT(!compression_functions.empty());
	auto &config = DBConfig::GetConfig(db);
	vector<CompressionType> forced_methods(checkpoint_states.size(), CompressionType::COMPRESSION_AUTO);

	auto compression_type = checkpoint_info.GetCompressionType();
	for (idx_t i = 0; i < checkpoint_states.size(); i++) {
		auto &functions = compression_functions[i];
		if (compression_type != CompressionType::COMPRESSION_AUTO) {
			forced_methods[i] = ForceCompression(functions, compression_type);
		}
		if (compression_type == CompressionType::COMPRESSION_AUTO &&
		    config.options.force_compression != CompressionType::COMPRESSION_AUTO) {
			forced_methods[i] = ForceCompression(functions, config.options.force_compression);
		}
	}

	InitAnalyze();

	// scan over all the segments and run the analyze step
	ScanSegments([&](Vector &scan_vector, idx_t count) {
		for (idx_t i = 0; i < checkpoint_states.size(); i++) {
			if (!has_changes[i]) {
				continue;
			}

			auto &functions = compression_functions[i];
			auto &states = analyze_states[i];
			for (idx_t j = 0; j < functions.size(); j++) {
				auto &state = states[j];
				auto &func = functions[j];

				if (!state) {
					continue;
				}
				if (!func->analyze(*state, scan_vector, count)) {
					state = nullptr;
					func = nullptr;
				}
			}
		}
	});

	vector<CheckpointAnalyzeResult> result;
	result.resize(checkpoint_states.size());

	for (idx_t i = 0; i < checkpoint_states.size(); i++) {
		if (!has_changes[i]) {
			continue;
		}
		auto &functions = compression_functions[i];
		auto &states = analyze_states[i];
		auto &forced_method = forced_methods[i];

		unique_ptr<AnalyzeState> chosen_state;
		idx_t best_score = NumericLimits<idx_t>::Maximum();
		idx_t compression_idx = DConstants::INVALID_INDEX;

		D_ASSERT(functions.size() == states.size());
		for (idx_t j = 0; j < functions.size(); j++) {
			auto &function = functions[j];
			auto &state = states[j];

			if (!state) {
				continue;
			}

			//! Check if the method type is the forced method (if forced is used)
			bool forced_method_found = function->type == forced_method;
			// now that we have passed over all the data, we need to figure out the best method
			// we do this using the final_analyze method
			auto score = function->final_analyze(*state);

			//! The finalize method can return this value from final_analyze to indicate it should not be used.
			if (score == DConstants::INVALID_INDEX) {
				continue;
			}

			if (score < best_score || forced_method_found) {
				compression_idx = j;
				best_score = score;
				chosen_state = std::move(state);
			}
			//! If we have found the forced method, we're done
			if (forced_method_found) {
				break;
			}
		}

		auto &checkpoint_state = checkpoint_states[i];
		auto &col_data = checkpoint_state.get().column_data;
		if (!chosen_state) {
			throw FatalException("No suitable compression/storage method found to store column of type %s",
			                     col_data.type.ToString());
		}
		D_ASSERT(compression_idx != DConstants::INVALID_INDEX);

		auto &best_function = *functions[compression_idx];
		DUCKDB_LOG_INFO(db, "duckdb.ColumnDataCheckPointer", "FinalAnalyze(%s) result for %s.%s.%d(%s): %d",
		                EnumUtil::ToString(best_function.type), col_data.info.GetSchemaName(),
		                col_data.info.GetTableName(), col_data.column_index, col_data.type.ToString(), best_score);
		result[i] = CheckpointAnalyzeResult(std::move(chosen_state), best_function);
	}
	return result;
}

void ColumnDataCheckpointer::DropSegments() {
	// first we check the current segments
	// if there are any persistent segments, we will mark their old block ids as modified
	// since the segments will be rewritten their old on disk data is no longer required

	for (idx_t i = 0; i < checkpoint_states.size(); i++) {
		if (!has_changes[i]) {
			continue;
		}

		auto &state = checkpoint_states[i];
		auto &col_data = state.get().column_data;
		auto &nodes = col_data.data.ReferenceSegments();

		// Drop the segments, as we'll be replacing them with new ones, because there are changes
		for (idx_t segment_idx = 0; segment_idx < nodes.size(); segment_idx++) {
			auto segment = nodes[segment_idx].node.get();
			segment->CommitDropSegment();
		}
	}
}

bool ColumnDataCheckpointer::ValidityCoveredByBasedata(vector<CheckpointAnalyzeResult> &result) {
	if (result.size() != 2) {
		return false;
	}
	if (!has_changes[0]) {
		// The base data had no changes so it will not be rewritten
		return false;
	}
	auto &base = result[0];
	D_ASSERT(base.function);
	return base.function->validity == CompressionValidity::NO_VALIDITY_REQUIRED;
}

void ColumnDataCheckpointer::WriteToDisk() {
	DropSegments();

	// Analyze the candidate functions to select one of them to use for compression
	auto analyze_result = DetectBestCompressionMethod();
	if (ValidityCoveredByBasedata(analyze_result)) {
		D_ASSERT(analyze_result.size() == 2);
		auto &validity = analyze_result[1];
		auto &config = DBConfig::GetConfig(db);
		// Override the function to the COMPRESSION_EMPTY
		// turning the compression+final compress steps into a no-op, saving a single empty segment
		validity.function = config.GetCompressionFunction(CompressionType::COMPRESSION_EMPTY, PhysicalType::BIT);
	}

	// Initialize the compression for the selected function
	D_ASSERT(analyze_result.size() == checkpoint_states.size());
	vector<ColumnDataCheckpointData> checkpoint_data(checkpoint_states.size());
	vector<unique_ptr<CompressionState>> compression_states(checkpoint_states.size());
	for (idx_t i = 0; i < analyze_result.size(); i++) {
		if (!has_changes[i]) {
			continue;
		}
		auto &analyze_state = analyze_result[i].analyze_state;
		auto &function = analyze_result[i].function;

		auto &checkpoint_state = checkpoint_states[i];
		auto &col_data = checkpoint_state.get().column_data;

		checkpoint_data[i] =
		    ColumnDataCheckpointData(checkpoint_state, col_data, col_data.GetDatabase(), row_group, checkpoint_info);
		compression_states[i] = function->init_compression(checkpoint_data[i], std::move(analyze_state));
	}

	// Scan over the existing segment + changes and compress the data
	ScanSegments([&](Vector &scan_vector, idx_t count) {
		for (idx_t i = 0; i < checkpoint_states.size(); i++) {
			if (!has_changes[i]) {
				continue;
			}
			auto &function = analyze_result[i].function;
			auto &compression_state = compression_states[i];
			function->compress(*compression_state, scan_vector, count);
		}
	});

	// Finalize the compression
	for (idx_t i = 0; i < checkpoint_states.size(); i++) {
		if (!has_changes[i]) {
			continue;
		}
		auto &function = analyze_result[i].function;
		auto &compression_state = compression_states[i];
		function->compress_finalize(*compression_state);
	}
}

bool ColumnDataCheckpointer::HasChanges(ColumnData &col_data) {
	auto &nodes = col_data.data.ReferenceSegments();
	for (idx_t segment_idx = 0; segment_idx < nodes.size(); segment_idx++) {
		auto segment = nodes[segment_idx].node.get();
		if (segment->segment_type == ColumnSegmentType::TRANSIENT) {
			// transient segment: always need to write to disk
			return true;
		}
		// persistent segment; check if there were any updates or deletions in this segment
		idx_t start_row_idx = segment->start - row_group.start;
		idx_t end_row_idx = start_row_idx + segment->count;
		if (col_data.HasChanges(start_row_idx, end_row_idx)) {
			return true;
		}
	}
	return false;
}

void ColumnDataCheckpointer::WritePersistentSegments(ColumnCheckpointState &state) {
	// all segments are persistent and there are no updates
	// we only need to write the metadata

	auto &col_data = state.column_data;
	auto nodes = col_data.data.MoveSegments();

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

void ColumnDataCheckpointer::Checkpoint() {
	for (idx_t i = 0; i < checkpoint_states.size(); i++) {
		auto &state = checkpoint_states[i];
		auto &col_data = state.get().column_data;
		has_changes.push_back(HasChanges(col_data));
	}

	bool any_has_changes = false;
	for (idx_t i = 0; i < has_changes.size(); i++) {
		if (has_changes[i]) {
			any_has_changes = true;
			break;
		}
	}
	if (!any_has_changes) {
		// Nothing has undergone any changes, no need to checkpoint
		// just move on to finalizing
		return;
	}

	WriteToDisk();
}

void ColumnDataCheckpointer::FinalizeCheckpoint() {
	for (idx_t i = 0; i < checkpoint_states.size(); i++) {
		auto &state = checkpoint_states[i].get();
		auto &col_data = state.column_data;
		if (has_changes[i]) {
			// Move the existing segments out of the column data
			// they will be destructed at the end of the scope
			auto to_delete = col_data.data.MoveSegments();
		} else {
			WritePersistentSegments(state);
		}

		// reset the compression function
		col_data.compression.reset();
		// replace the old tree with the new one
		auto new_segments = state.new_tree.MoveSegments();
		auto l = col_data.data.Lock();
		for (auto &new_segment : new_segments) {
			col_data.AppendSegment(l, std::move(new_segment.node));
		}
		col_data.ClearUpdates();
	}
}

} // namespace duckdb
