#include "duckdb/storage/table/column_data_checkpointer.hpp"
#include "duckdb/storage/compression/standard_compression_state.hpp"

#include "duckdb/main/config.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/settings.hpp"
#include "duckdb/logging/log_manager.hpp"
#include "duckdb/parser/column_definition.hpp"
#include "duckdb/storage/table/data_table_info.hpp"
#include "duckdb/storage/table/scan_state.hpp"
#include "duckdb/storage/table/update_segment.hpp"
#include "duckdb/storage/data_table.hpp"

namespace duckdb {

//! ColumnDataCheckpointData

const CompressionFunction &ColumnDataCheckpointData::GetCompressionFunction(CompressionType compression_type) {
	auto &db = col_data->GetDatabase();
	auto &column_type = col_data->type;
	auto &config = DBConfig::GetConfig(db);
	return config.GetCompressionFunction(compression_type, column_type.InternalType());
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

const RowGroup &ColumnDataCheckpointData::GetRowGroup() {
	return *row_group;
}

ColumnCheckpointState &ColumnDataCheckpointData::GetCheckpointState() {
	return *checkpoint_state;
}

StorageManager &ColumnDataCheckpointData::GetStorageManager() {
	return *storage_manager;
}

//! ColumnDataCheckpointer

static bool CanReuseBaseSegment(ColumnSegment &segment) {
	auto &function = segment.GetCompressionFunction();
	return segment.GetSegmentType() == ColumnSegmentType::PERSISTENT &&
	       function.validity == CompressionValidity::REQUIRES_VALIDITY;
}

struct ColumnDataCheckpointer::BaseSegmentCheckpointPlan {
	//! Minimum unchanged tuple ratio; higher values favor full rewrites
	static constexpr double MINIMUM_REUSED_TUPLE_RATIO = 0.125;

	//! Maximum number of independent analyze/compression pipelines allowed in a local rewrite
	static constexpr idx_t MAX_LOCAL_REWRITE_RANGES = 4;

	//! More dirty ranges incur higher analyze/compression costs.
	//! Higher values require more reused tuples and favor full rewrites for fragmented updates.
	static constexpr idx_t MINIMUM_REUSED_VECTORS_PER_EXTRA_DIRTY_RANGE = 5;

	//! Maximum estimated Segment-count overhead; higher values tolerate more fragmented block layouts
	static constexpr double MAXIMUM_PROJECTED_SEGMENT_OVERHEAD_RATIO = 0.25;

	struct SegmentPlan {
		SegmentPlan(idx_t row_start_p, idx_t tuple_count_p, bool reuse_p)
		    : row_start(row_start_p), tuple_count(tuple_count_p), reuse(reuse_p) {
		}

		idx_t row_start;
		idx_t tuple_count;
		bool reuse;
		CheckpointAnalyzeResult rewrite_range_result;
	};

	explicit BaseSegmentCheckpointPlan(vector<SegmentPlan> segment_plans_p)
	    : segment_plans(std::move(segment_plans_p)) {
	}

	template <class FUNC>
	void ForEachSegment(const ColumnData &column_data, FUNC &&callback) {
		D_ASSERT(column_data.data.GetSegmentCount() == segment_plans.size());
		idx_t segment_plan_index = 0;
		for (auto &segment_node : column_data.data.SegmentNodes()) {
			auto &segment_plan = segment_plans[segment_plan_index++];
			D_ASSERT(segment_plan.row_start == segment_node.GetRowStart());
			D_ASSERT(segment_plan.tuple_count == segment_node.GetNode().count.load());
			callback(segment_plan, segment_node);
		}
	}

	bool IsProfitable() const {
		idx_t total_tuple_count = 0;
		idx_t largest_segment_tuple_count = 0;
		idx_t reused_tuple_count = 0;
		for (auto &segment_plan : segment_plans) {
			total_tuple_count += segment_plan.tuple_count;
			largest_segment_tuple_count = MaxValue(largest_segment_tuple_count, segment_plan.tuple_count);
			if (segment_plan.reuse) {
				reused_tuple_count += segment_plan.tuple_count;
			}
		}

		auto minimum_reused_tuple_count =
		    LossyNumericCast<idx_t>(static_cast<double>(total_tuple_count) * MINIMUM_REUSED_TUPLE_RATIO);
		if (reused_tuple_count == 0 || reused_tuple_count < minimum_reused_tuple_count) {
			return false;
		}

		idx_t dirty_range_count = 0;
		idx_t projected_segment_count = 0;
		idx_t segment_plan_index = 0;
		while (segment_plan_index < segment_plans.size()) {
			if (segment_plans[segment_plan_index].reuse) {
				projected_segment_count++;
				segment_plan_index++;
				continue;
			}

			dirty_range_count++;
			idx_t dirty_range_tuple_count = 0;
			while (segment_plan_index < segment_plans.size() && !segment_plans[segment_plan_index].reuse) {
				dirty_range_tuple_count += segment_plans[segment_plan_index++].tuple_count;
			}
			auto estimated_dirty_segment_count = (dirty_range_tuple_count - 1) / largest_segment_tuple_count + 1;
			// Codec output boundaries are unknown before compression, so reserve one extra Segment per dirty range.
			projected_segment_count += estimated_dirty_segment_count + 1;
		}
		if (dirty_range_count > MAX_LOCAL_REWRITE_RANGES) {
			return false;
		}

		auto extra_dirty_range_count = dirty_range_count > 0 ? dirty_range_count - 1 : 0;
		auto range_reuse_requirement =
		    extra_dirty_range_count * MINIMUM_REUSED_VECTORS_PER_EXTRA_DIRTY_RANGE * STANDARD_VECTOR_SIZE;
		auto required_reused_tuple_count = MaxValue(minimum_reused_tuple_count, range_reuse_requirement);
		if (reused_tuple_count < required_reused_tuple_count) {
			return false;
		}

		auto estimated_compact_segment_count = (total_tuple_count - 1) / largest_segment_tuple_count + 1;
		auto projected_segment_overhead = LossyNumericCast<idx_t>(static_cast<double>(estimated_compact_segment_count) *
		                                                          MAXIMUM_PROJECTED_SEGMENT_OVERHEAD_RATIO);
		auto maximum_projected_segment_count =
		    estimated_compact_segment_count + MaxValue<idx_t>(1, projected_segment_overhead);

		return projected_segment_count <= maximum_projected_segment_count;
	}

	vector<SegmentPlan> segment_plans;
};

static void CreateIntermediateVector(vector<reference<ColumnCheckpointState>> &states, DataChunk &chunk) {
	D_ASSERT(!states.empty());

	auto &first_state = states[0];
	auto &col_data = first_state.get().original_column;
	auto &type = col_data.type;

	vector<LogicalType> types;
	if (type.id() == LogicalTypeId::VALIDITY) {
		types.emplace_back(LogicalType::BOOLEAN);
	} else if (type.InternalType() == PhysicalType::LIST) {
		types.emplace_back(LogicalType::UBIGINT);
	} else {
		types.emplace_back(type);
	}
	chunk.Initialize(Allocator::DefaultAllocator(), types);
	if (type.id() == LogicalTypeId::VALIDITY) {
		auto data = FlatVector::GetData<bool>(chunk.data[0]);
		memset((void *)data, 0, sizeof(bool) * STANDARD_VECTOR_SIZE);
	}
}

ColumnDataCheckpointer::ColumnDataCheckpointer(vector<reference<ColumnCheckpointState>> &checkpoint_states,
                                               StorageManager &storage_manager, const RowGroup &row_group,
                                               ColumnCheckpointInfo &checkpoint_info)
    : checkpoint_states(checkpoint_states), storage_manager(storage_manager), row_group(row_group),
      checkpoint_info(checkpoint_info) {
	auto &db = storage_manager.GetDatabase();
	auto &config = DBConfig::GetConfig(db);
	compression_functions.resize(checkpoint_states.size());
	for (idx_t i = 0; i < checkpoint_states.size(); i++) {
		auto &col_data = checkpoint_states[i].get().original_column;
		auto to_add = config.GetCompressionFunctions(col_data.type.InternalType());
		auto &functions = compression_functions[i];
		for (auto &func : to_add) {
			functions.push_back(&func.get());
		}
	}
	CreateIntermediateVector(checkpoint_states, intermediate);
}

ColumnDataCheckpointer::~ColumnDataCheckpointer() = default;

void ColumnDataCheckpointer::ScanSegment(const ColumnData &col_data, SegmentNode<ColumnSegment> &segment_node,
                                         const std::function<void(Vector &)> &callback) {
	auto &segment = segment_node.GetNode();
	ColumnScanState scan_state(nullptr);
	scan_state.current = segment_node;
	segment.InitializeScan(scan_state);

	auto &scan_vector = intermediate.data[0];
	for (idx_t base_row_index = 0; base_row_index < segment.count; base_row_index += STANDARD_VECTOR_SIZE) {
		intermediate.Reset();

		idx_t count = MinValue<idx_t>(segment.count - base_row_index, STANDARD_VECTOR_SIZE);
		scan_state.offset_in_column = segment_node.GetRowStart() + base_row_index;

		col_data.CheckpointScan(segment, scan_state, count, scan_vector);
		scan_vector.BufferMutable().SetVectorSize(count);
		callback(scan_vector);
	}
}

void ColumnDataCheckpointer::ScanSegments(const ColumnData &col_data, const std::function<void(Vector &)> &callback) {
	// TODO: scan all the nodes from all segments, no need for CheckpointScan to virtualize this I think..
	for (auto &segment_node : col_data.data.SegmentNodes()) {
		ScanSegment(col_data, segment_node, callback);
	}
}

CompressionType ForceCompression(StorageManager &storage_manager,
                                 vector<optional_ptr<const CompressionFunction>> &compression_functions,
                                 CompressionType compression_type) {
	// One of the force_compression flags has been set
	// check if this compression method is available
	// auto compression_availability_result = CompressionTypeIsAvailable(compression_type, storage_manager);
	// if (!compression_availability_result.IsAvailable()) {
	//	throw InvalidInputException("The forced compression method (%s) is not available in the current storage
	// version", CompressionTypeToString(compression_type));
	//}

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

vector<unique_ptr<AnalyzeState>> ColumnDataCheckpointer::InitAnalyze(idx_t checkpoint_state_idx) {
	auto &functions = compression_functions[checkpoint_state_idx];
	auto &checkpoint_state = checkpoint_states[checkpoint_state_idx].get();
	auto &coldata = checkpoint_state.GetResultColumn();
	vector<unique_ptr<AnalyzeState>> states(functions.size());
	for (idx_t j = 0; j < functions.size(); j++) {
		auto &func = functions[j];
		if (func) {
			states[j] = func->init_analyze(coldata, coldata.type.InternalType());
		}
	}
	return states;
}

void ColumnDataCheckpointer::AnalyzeVector(idx_t checkpoint_state_idx, vector<unique_ptr<AnalyzeState>> &states,
                                           Vector &scan_vector) {
	auto &functions = compression_functions[checkpoint_state_idx];
	for (idx_t j = 0; j < functions.size(); j++) {
		auto &state = states[j];
		auto &func = functions[j];
		if (!state) {
			continue;
		}
		if (!func->analyze(*state, scan_vector)) {
			// Analyze states are range-local; keep the function available for later ranges.
			state = nullptr;
		}
	}
}

CheckpointAnalyzeResult ColumnDataCheckpointer::FinalizeAnalyze(idx_t checkpoint_state_idx,
                                                                vector<unique_ptr<AnalyzeState>> states,
                                                                CompressionType forced_method) {
	auto &functions = compression_functions[checkpoint_state_idx];
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

		bool forced_method_found = function->type == forced_method;
		auto score = function->final_analyze(*state);
		if (score == DConstants::INVALID_INDEX) {
			continue;
		}
		if (score < best_score || forced_method_found) {
			compression_idx = j;
			best_score = score;
			chosen_state = std::move(state);
		}
		if (forced_method_found) {
			break;
		}
	}

	auto &col_data = checkpoint_states[checkpoint_state_idx].get().GetResultColumn();
	if (!chosen_state) {
		throw FatalException("No suitable compression/storage method found to store column of type %s",
		                     col_data.type.ToString());
	}
	D_ASSERT(compression_idx != DConstants::INVALID_INDEX);

	auto &best_function = *functions[compression_idx];
	auto &db = storage_manager.GetDatabase();
	DUCKDB_LOG_TRACE(db, "ColumnDataCheckpointer FinalAnalyze(%s) result for %s.%s.%d(%s): %d",
	                 EnumUtil::ToString(best_function.type), col_data.info.GetSchemaName(),
	                 col_data.info.GetTableName(), col_data.column_index, col_data.type.ToString(), best_score);
	return CheckpointAnalyzeResult(std::move(chosen_state), best_function);
}

unique_ptr<ColumnDataCheckpointer::BaseSegmentCheckpointPlan> ColumnDataCheckpointer::TryBuildBaseSegmentPlan() {
	auto compression_type = checkpoint_info.GetCompressionType();
	if (checkpoint_states.size() != 2) {
		return nullptr;
	}
	auto auto_compression = compression_type == CompressionType::COMPRESSION_AUTO;
	if (auto_compression) {
		auto &config = DBConfig::GetConfig(storage_manager.GetDatabase());
		if (Settings::Get<ForceCompressionSetting>(config) != CompressionType::COMPRESSION_AUTO) {
			return nullptr;
		}
	}

	auto &base_data = checkpoint_states[0].get().original_column;
	auto &validity_data = checkpoint_states[1].get().original_column;
	if (base_data.HasParent() || base_data.type.IsNested() || validity_data.type.InternalType() != PhysicalType::BIT ||
	    !validity_data.data.GetRootSegment()) {
		return nullptr;
	}
	auto total_tuple_count = base_data.count.load();
	bool requested_method_available = auto_compression;
	for (auto &function : compression_functions[0]) {
		if (!function) {
			continue;
		}
		if (!auto_compression && function->validity != CompressionValidity::REQUIRES_VALIDITY) {
			return nullptr;
		}
		requested_method_available |= function->type == compression_type;
	}
	if (!requested_method_available) {
		return nullptr;
	}
	for (auto &segment : validity_data.data.Segments()) {
		if (segment.GetSegmentType() != ColumnSegmentType::PERSISTENT ||
		    segment.GetCompressionFunction().type == CompressionType::COMPRESSION_EMPTY) {
			return nullptr;
		}
	}
	// Row-remapping checkpoints cannot reuse source Segment ranges.
	if (total_tuple_count != row_group.count.load()) {
		return nullptr;
	}

	idx_t current_row = 0;
	vector<BaseSegmentCheckpointPlan::SegmentPlan> segment_plans;
	for (auto &segment_node : base_data.data.SegmentNodes()) {
		auto segment_start = segment_node.GetRowStart();
		auto segment_end = segment_node.GetRowEnd();
		auto segment_tuple_count = segment_node.GetNode().count.load();
		if (segment_start != current_row || segment_tuple_count == 0) {
			return nullptr;
		}
		auto &segment = segment_node.GetNode();
		auto reuse = CanReuseBaseSegment(segment) && !base_data.HasChanges(segment_start, segment_end);
		segment_plans.emplace_back(segment_start, segment_tuple_count, reuse);
		current_row = segment_end;
	}
	if (current_row != total_tuple_count) {
		return nullptr;
	}

	auto plan = make_uniq<BaseSegmentCheckpointPlan>(std::move(segment_plans));
	if (!plan->IsProfitable()) {
		return nullptr;
	}
	return plan;
}

vector<CheckpointAnalyzeResult> ColumnDataCheckpointer::DetectBestCompressionMethod() {
	D_ASSERT(!compression_functions.empty());
	auto &db = storage_manager.GetDatabase();
	auto &config = DBConfig::GetConfig(db);
	vector<CompressionType> forced_methods(checkpoint_states.size(), CompressionType::COMPRESSION_AUTO);

	auto compression_type = checkpoint_info.GetCompressionType();
	for (idx_t i = 0; i < checkpoint_states.size(); i++) {
		auto &functions = compression_functions[i];
		if (compression_type != CompressionType::COMPRESSION_AUTO) {
			forced_methods[i] = ForceCompression(storage_manager, functions, compression_type);
		}
		if (compression_type == CompressionType::COMPRESSION_AUTO) {
			auto force_compression = Settings::Get<ForceCompressionSetting>(config);
			if (force_compression != CompressionType::COMPRESSION_AUTO) {
				forced_methods[i] = ForceCompression(storage_manager, functions, force_compression);
			}
		}
	}

	base_segment_plan = TryBuildBaseSegmentPlan();
	vector<CheckpointAnalyzeResult> result(checkpoint_states.size());
	auto &base_data = checkpoint_states[0].get().original_column;

	if (base_segment_plan) {
		optional_ptr<BaseSegmentCheckpointPlan::SegmentPlan> rewrite_range_start;
		vector<unique_ptr<AnalyzeState>> states;
		auto finalize_rewrite = [&]() {
			if (!rewrite_range_start) {
				return;
			}
			rewrite_range_start->rewrite_range_result = FinalizeAnalyze(0, std::move(states), forced_methods[0]);
			rewrite_range_start = nullptr;
		};
		base_segment_plan->ForEachSegment(base_data, [&](auto &segment_plan, auto &segment_node) {
			if (segment_plan.reuse) {
				finalize_rewrite();
				return;
			}
			if (!rewrite_range_start) {
				rewrite_range_start = &segment_plan;
				states = InitAnalyze(0);
			}
			ScanSegment(base_data, segment_node, [&](Vector &scan_vector) { AnalyzeVector(0, states, scan_vector); });
		});
		finalize_rewrite();
		auto &validity_data = checkpoint_states[1].get().original_column;
		if (validity_data.HasChanges()) {
			auto states = InitAnalyze(1);
			ScanSegments(validity_data, [&](Vector &scan_vector) { AnalyzeVector(1, states, scan_vector); });
			result[1] = FinalizeAnalyze(1, std::move(states), forced_methods[1]);
		}
		return result;
	}
	vector<vector<unique_ptr<AnalyzeState>>> analyze_states(checkpoint_states.size());
	for (idx_t i = 0; i < checkpoint_states.size(); i++) {
		analyze_states[i] = InitAnalyze(i);
	}
	ScanSegments(base_data, [&](Vector &scan_vector) {
		for (idx_t i = 0; i < checkpoint_states.size(); i++) {
			AnalyzeVector(i, analyze_states[i], scan_vector);
		}
	});
	for (idx_t i = 0; i < checkpoint_states.size(); i++) {
		result[i] = FinalizeAnalyze(i, std::move(analyze_states[i]), forced_methods[i]);
	}
	return result;
}

struct CheckpointBlockIdDropper : public BlockIdVisitor {
	explicit CheckpointBlockIdDropper(BlockManager &manager) : manager(manager) {
	}

	void Visit(block_id_t block_id) override {
		manager.MarkBlockAsModified(block_id);
	}

	BlockManager &manager;
};

void ColumnDataCheckpointer::DropSegments() {
	// Mark blocks owned by replaced segments as modified
	for (idx_t i = 0; i < checkpoint_states.size(); i++) {
		auto &state = checkpoint_states[i];
		auto &col_data = state.get().original_column;
		if (base_segment_plan && i > 0 && !col_data.HasChanges()) {
			continue;
		}

		CheckpointBlockIdDropper dropper(storage_manager.GetBlockManager());
		if (i == 0 && base_segment_plan) {
			base_segment_plan->ForEachSegment(col_data, [&](auto &segment_plan, auto &segment_node) {
				if (segment_plan.reuse) {
					return;
				}
				segment_node.GetNode().VisitBlockIds(dropper);
			});
			continue;
		}
		for (auto &segment : col_data.data.Segments()) {
			segment.VisitBlockIds(dropper);
		}
	}
}

bool ColumnDataCheckpointer::ValidityCoveredByBasedata(vector<CheckpointAnalyzeResult> &result) {
	if (result.size() != 2) {
		return false;
	}
	auto &base = result[0];
	D_ASSERT(base.function);
	return base.function->validity == CompressionValidity::NO_VALIDITY_REQUIRED;
}

void ColumnDataCheckpointer::WriteToDisk() {
	auto analyze_result = DetectBestCompressionMethod();
	if (base_segment_plan) {
		WriteBaseSegments();
		auto &validity_data = checkpoint_states[1].get().original_column;
		if (validity_data.HasChanges()) {
			WriteValidity(analyze_result[1]);
		} else {
			PreserveUnchangedValidity();
		}
		DropSegments();
		return;
	}
	if (ValidityCoveredByBasedata(analyze_result)) {
		D_ASSERT(analyze_result.size() == 2);
		auto &validity = analyze_result[1];
		auto &db = storage_manager.GetDatabase();
		auto &config = DBConfig::GetConfig(db);
		// Override the function to the COMPRESSION_EMPTY
		// turning the compression+final compress steps into a no-op, saving a single empty segment
		validity.function = config.GetCompressionFunction(CompressionType::COMPRESSION_EMPTY, PhysicalType::BIT).get();
	}
	// Initialize the compression for the selected function
	D_ASSERT(analyze_result.size() == checkpoint_states.size());
	vector<ColumnDataCheckpointData> checkpoint_data(checkpoint_states.size());
	vector<unique_ptr<CompressionState>> compression_states(checkpoint_states.size());
	for (idx_t i = 0; i < analyze_result.size(); i++) {
		auto &analyze_state = analyze_result[i].analyze_state;
		auto &function = analyze_result[i].function;

		auto &checkpoint_state = checkpoint_states[i];
		auto &col_data = checkpoint_state.get().GetResultColumn();

		checkpoint_data[i] =
		    ColumnDataCheckpointData(checkpoint_state, col_data, col_data.GetDatabase(), row_group, storage_manager);
		compression_states[i] = function->init_compression(checkpoint_data[i], std::move(analyze_state));
	}

	// Scan over the existing segment + changes and compress the data
	auto &base_data = checkpoint_states[0].get().original_column;
	ScanSegments(base_data, [&](Vector &scan_vector) {
		for (idx_t i = 0; i < checkpoint_states.size(); i++) {
			auto &function = analyze_result[i].function;
			auto &compression_state = compression_states[i];
			function->compress(*compression_state, scan_vector);
		}
	});

	// Finalize the compression
	for (idx_t i = 0; i < checkpoint_states.size(); i++) {
		auto &function = analyze_result[i].function;
		auto &compression_state = compression_states[i];
		function->compress_finalize(*compression_state);
	}
	// after we finish checkpointing we can drop this segment
	DropSegments();
}

void ColumnDataCheckpointer::WriteBaseSegments() {
	D_ASSERT(base_segment_plan);
	auto &checkpoint_state = checkpoint_states[0].get();
	auto &original_column = checkpoint_state.original_column;
	auto &result_column = checkpoint_state.GetResultColumn();
	ColumnDataCheckpointData checkpoint_data(checkpoint_state, result_column, result_column.GetDatabase(), row_group,
	                                         storage_manager);
	optional_ptr<const CompressionFunction> rewrite_function;
	unique_ptr<CompressionState> compression_state;
	auto finalize_rewrite = [&]() {
		if (!compression_state) {
			return;
		}
		rewrite_function->compress_finalize(*compression_state);
		compression_state.reset();
		rewrite_function = nullptr;
	};

	base_segment_plan->ForEachSegment(original_column, [&](auto &segment_plan, auto &segment_node) {
		if (segment_plan.reuse) {
			finalize_rewrite();
			checkpoint_state.AppendPersistentSegmentReference(segment_node.ReferenceNode(), segment_node.GetRowStart());
			return;
		}

		if (!compression_state) {
			auto &analyze_result = segment_plan.rewrite_range_result;
			if (!analyze_result.function) {
				throw InternalException("Missing compression method for a checkpoint rewrite range");
			}
			rewrite_function = analyze_result.function;
			compression_state =
			    rewrite_function->init_compression(checkpoint_data, std::move(analyze_result.analyze_state));
		}
		ScanSegment(original_column, segment_node,
		            [&](Vector &scan_vector) { rewrite_function->compress(*compression_state, scan_vector); });
	});
	finalize_rewrite();
}

void ColumnDataCheckpointer::WriteValidity(CheckpointAnalyzeResult &analyze_result) {
	D_ASSERT(analyze_result.function);
	auto &checkpoint_state = checkpoint_states[1].get();
	auto &original_column = checkpoint_state.original_column;
	auto &result_column = checkpoint_state.GetResultColumn();
	ColumnDataCheckpointData checkpoint_data(checkpoint_state, result_column, result_column.GetDatabase(), row_group,
	                                         storage_manager);
	auto &function = *analyze_result.function;
	auto compression_state = function.init_compression(checkpoint_data, std::move(analyze_result.analyze_state));
	ScanSegments(original_column, [&](Vector &scan_vector) { function.compress(*compression_state, scan_vector); });
	function.compress_finalize(*compression_state);
}

void ColumnDataCheckpointer::WritePersistentSegments(ColumnCheckpointState &state) {
	// all segments are persistent and there are no updates
	// we only need to write the metadata

	auto &col_data = state.original_column;

	optional_idx error_segment_start;
	idx_t current_row = 0;
	for (auto &segment_node : col_data.data.SegmentNodes()) {
		auto &segment = segment_node.GetNode();
		auto segment_start = segment_node.GetRowStart();
		if (segment_start != current_row) {
			error_segment_start = segment_start;
			break;
		}
		auto pointer = segment.GetDataPointer(current_row);
		current_row += segment.count;

		// merge the persistent stats into the global column stats
		state.global_stats->Merge(segment.GetStats());
		state.data_pointers.push_back(std::move(pointer));
	}
	if (error_segment_start.IsValid()) {
		string extra_info;
		for (auto &s : col_data.data.SegmentNodes()) {
			extra_info += "\n";
			extra_info += StringUtil::Format("Start %d, count %d", s.GetRowStart(), s.GetNode().count.load());
		}
		throw InternalException(
		    "Failure in RowGroup::Checkpoint - column data pointer is unaligned with row group "
		    "start\nRow group start: %d\nRow group count %d\nCurrent row: %d\nSegment start: %d\nColumn index: "
		    "%d\nColumn type: %s\nRoot type: %s\nTable: %s.%s\nAll segments:%s",
		    row_group.count.load(), current_row, error_segment_start.GetIndex(), col_data.column_index, col_data.type,
		    col_data.type, col_data.info.GetSchemaName(), col_data.info.GetTableName(), extra_info);
	}
}

struct CheckpointBlockIdMarker : public BlockIdVisitor {
	explicit CheckpointBlockIdMarker(BlockManager &manager) : manager(manager) {
	}

	void Visit(block_id_t block_id) override {
		manager.MarkBlockAsCheckpointed(block_id);
	}

	BlockManager &manager;
};

void ColumnDataCheckpointer::PreserveUnchangedValidity() {
	auto &state = checkpoint_states[1].get();
	D_ASSERT(!state.original_column.HasChanges());
	CheckpointBlockIdMarker marker(storage_manager.GetBlockManager());
	state.original_column.VisitBlockIds(marker);
	WritePersistentSegments(state);
}

void ColumnDataCheckpointer::Checkpoint() {
	for (idx_t i = 0; i < checkpoint_states.size(); i++) {
		auto &state = checkpoint_states[i];
		auto &col_data = state.get().original_column;
		if (col_data.HasChanges()) {
			has_changes = true;
			break;
		}
	}

	if (!has_changes) {
		// Nothing has undergone any changes, no need to checkpoint
		// just move on to finalizing
		// mark block ids as checkpointed
		CheckpointBlockIdMarker marker(storage_manager.GetBlockManager());
		for (idx_t i = 0; i < checkpoint_states.size(); i++) {
			auto &state = checkpoint_states[i];
			auto &col_data = state.get().original_column;
			col_data.VisitBlockIds(marker);
		}
		return;
	}

	WriteToDisk();
}

void ColumnDataCheckpointer::FinalizeCheckpoint() {
	if (has_changes) {
		// Changed columns were finalized by WriteToDisk
		return;
	}
	// no changes - copy over the original columns
	for (idx_t i = 0; i < checkpoint_states.size(); i++) {
		auto &state = checkpoint_states[i].get();
		WritePersistentSegments(state);
	}
}

} // namespace duckdb
