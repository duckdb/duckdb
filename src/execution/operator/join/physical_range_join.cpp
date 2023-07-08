#include "duckdb/execution/operator/join/physical_range_join.hpp"

#include "duckdb/common/fast_mem.hpp"
#include "duckdb/common/operator/comparison_operators.hpp"
#include "duckdb/common/row_operations/row_operations.hpp"
#include "duckdb/common/sort/comparators.hpp"
#include "duckdb/common/sort/sort.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parallel/base_pipeline_event.hpp"
#include "duckdb/parallel/thread_context.hpp"

#include <thread>

namespace duckdb {

PhysicalRangeJoin::LocalSortedTable::LocalSortedTable(ClientContext &context, const PhysicalRangeJoin &op,
                                                      const idx_t child)
    : op(op), executor(context), has_null(0), count(0) {
	// Initialize order clause expression executor and key DataChunk
	vector<LogicalType> types;
	for (const auto &cond : op.conditions) {
		const auto &expr = child ? cond.right : cond.left;
		executor.AddExpression(*expr);

		types.push_back(expr->return_type);
	}
	auto &allocator = Allocator::Get(context);
	keys.Initialize(allocator, types);
}

void PhysicalRangeJoin::LocalSortedTable::Sink(DataChunk &input, GlobalSortState &global_sort_state) {
	// Initialize local state (if necessary)
	if (!local_sort_state.initialized) {
		local_sort_state.Initialize(global_sort_state, global_sort_state.buffer_manager);
	}

	// Obtain sorting columns
	keys.Reset();
	executor.Execute(input, keys);

	// Count the NULLs so we can exclude them later
	has_null += MergeNulls(op.conditions);
	count += keys.size();

	//	Only sort the primary key
	DataChunk join_head;
	join_head.data.emplace_back(keys.data[0]);
	join_head.SetCardinality(keys.size());

	// Sink the data into the local sort state
	local_sort_state.SinkChunk(join_head, input);
}

PhysicalRangeJoin::GlobalSortedTable::GlobalSortedTable(ClientContext &context, const vector<BoundOrderByNode> &orders,
                                                        RowLayout &payload_layout)
    : global_sort_state(BufferManager::GetBufferManager(context), orders, payload_layout), has_null(0), count(0),
      memory_per_thread(0) {
	D_ASSERT(orders.size() == 1);

	// Set external (can be forced with the PRAGMA)
	auto &config = ClientConfig::GetConfig(context);
	global_sort_state.external = config.force_external;
	memory_per_thread = PhysicalRangeJoin::GetMaxThreadMemory(context);
}

void PhysicalRangeJoin::GlobalSortedTable::Combine(LocalSortedTable &ltable) {
	global_sort_state.AddLocalState(ltable.local_sort_state);
	has_null += ltable.has_null;
	count += ltable.count;
}

void PhysicalRangeJoin::GlobalSortedTable::IntializeMatches() {
	found_match = make_unsafe_uniq_array<bool>(Count());
	memset(found_match.get(), 0, sizeof(bool) * Count());
}

void PhysicalRangeJoin::GlobalSortedTable::Print() {
	global_sort_state.Print();
}

class RangeJoinMergeTask : public ExecutorTask {
public:
	using GlobalSortedTable = PhysicalRangeJoin::GlobalSortedTable;

public:
	RangeJoinMergeTask(shared_ptr<Event> event_p, ClientContext &context, GlobalSortedTable &table)
	    : ExecutorTask(context), event(std::move(event_p)), context(context), table(table) {
	}

	TaskExecutionResult ExecuteTask(TaskExecutionMode mode) override {
		// Initialize iejoin sorted and iterate until done
		auto &global_sort_state = table.global_sort_state;
		MergeSorter merge_sorter(global_sort_state, BufferManager::GetBufferManager(context));
		merge_sorter.PerformInMergeRound();
		event->FinishTask();

		return TaskExecutionResult::TASK_FINISHED;
	}

private:
	shared_ptr<Event> event;
	ClientContext &context;
	GlobalSortedTable &table;
};

class RangeJoinMergeEvent : public BasePipelineEvent {
public:
	using GlobalSortedTable = PhysicalRangeJoin::GlobalSortedTable;

public:
	RangeJoinMergeEvent(GlobalSortedTable &table_p, Pipeline &pipeline_p)
	    : BasePipelineEvent(pipeline_p), table(table_p) {
	}

	GlobalSortedTable &table;

public:
	void Schedule() override {
		auto &context = pipeline->GetClientContext();

		// Schedule tasks equal to the number of threads, which will each merge multiple partitions
		auto &ts = TaskScheduler::GetScheduler(context);
		idx_t num_threads = ts.NumberOfThreads();

		vector<shared_ptr<Task>> iejoin_tasks;
		for (idx_t tnum = 0; tnum < num_threads; tnum++) {
			iejoin_tasks.push_back(make_uniq<RangeJoinMergeTask>(shared_from_this(), context, table));
		}
		SetTasks(std::move(iejoin_tasks));
	}

	void FinishEvent() override {
		auto &global_sort_state = table.global_sort_state;

		global_sort_state.CompleteMergeRound(true);
		if (global_sort_state.sorted_blocks.size() > 1) {
			// Multiple blocks remaining: Schedule the next round
			table.ScheduleMergeTasks(*pipeline, *this);
		}
	}
};

void PhysicalRangeJoin::GlobalSortedTable::ScheduleMergeTasks(Pipeline &pipeline, Event &event) {
	// Initialize global sort state for a round of merging
	global_sort_state.InitializeMergeRound();
	auto new_event = make_shared<RangeJoinMergeEvent>(*this, pipeline);
	event.InsertEvent(std::move(new_event));
}

void PhysicalRangeJoin::GlobalSortedTable::Finalize(Pipeline &pipeline, Event &event) {
	// Prepare for merge sort phase
	global_sort_state.PrepareMergePhase();

	// Start the merge phase or finish if a merge is not necessary
	if (global_sort_state.sorted_blocks.size() > 1) {
		ScheduleMergeTasks(pipeline, event);
	}
}

PhysicalRangeJoin::PhysicalRangeJoin(LogicalComparisonJoin &op, PhysicalOperatorType type,
                                     unique_ptr<PhysicalOperator> left, unique_ptr<PhysicalOperator> right,
                                     vector<JoinCondition> cond, JoinType join_type, idx_t estimated_cardinality)
    : PhysicalComparisonJoin(op, type, std::move(cond), join_type, estimated_cardinality) {
	// Reorder the conditions so that ranges are at the front.
	// TODO: use stats to improve the choice?
	// TODO: Prefer fixed length types?
	if (conditions.size() > 1) {
		vector<JoinCondition> conditions_p(conditions.size());
		std::swap(conditions_p, conditions);
		idx_t range_position = 0;
		idx_t other_position = conditions_p.size();
		for (idx_t i = 0; i < conditions_p.size(); ++i) {
			switch (conditions_p[i].comparison) {
			case ExpressionType::COMPARE_LESSTHAN:
			case ExpressionType::COMPARE_LESSTHANOREQUALTO:
			case ExpressionType::COMPARE_GREATERTHAN:
			case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
				conditions[range_position++] = std::move(conditions_p[i]);
				break;
			default:
				conditions[--other_position] = std::move(conditions_p[i]);
				break;
			}
		}
	}

	children.push_back(std::move(left));
	children.push_back(std::move(right));

	//	Fill out the left projection map.
	left_projection_map = op.left_projection_map;
	if (left_projection_map.empty()) {
		const auto left_count = children[0]->types.size();
		left_projection_map.reserve(left_count);
		for (column_t i = 0; i < left_count; ++i) {
			left_projection_map.emplace_back(i);
		}
	}
	//	Fill out the right projection map.
	right_projection_map = op.right_projection_map;
	if (right_projection_map.empty()) {
		const auto right_count = children[1]->types.size();
		right_projection_map.reserve(right_count);
		for (column_t i = 0; i < right_count; ++i) {
			right_projection_map.emplace_back(i);
		}
	}

	//	Construct the unprojected type layout from the children's types
	unprojected_types = children[0]->GetTypes();
	auto &types = children[1]->GetTypes();
	unprojected_types.insert(unprojected_types.end(), types.begin(), types.end());
}

idx_t PhysicalRangeJoin::LocalSortedTable::MergeNulls(const vector<JoinCondition> &conditions) {
	// Merge the validity masks of the comparison keys into the primary
	// Return the number of NULLs in the resulting chunk
	D_ASSERT(keys.ColumnCount() > 0);
	const auto count = keys.size();

	size_t all_constant = 0;
	for (auto &v : keys.data) {
		if (v.GetVectorType() == VectorType::CONSTANT_VECTOR) {
			++all_constant;
		}
	}

	auto &primary = keys.data[0];
	if (all_constant == keys.data.size()) {
		//	Either all NULL or no NULLs
		for (auto &v : keys.data) {
			if (ConstantVector::IsNull(v)) {
				ConstantVector::SetNull(primary, true);
				return count;
			}
		}
		return 0;
	} else if (keys.ColumnCount() > 1) {
		//	Flatten the primary, as it will need to merge arbitrary validity masks
		primary.Flatten(count);
		auto &pvalidity = FlatVector::Validity(primary);
		D_ASSERT(keys.ColumnCount() == conditions.size());
		for (size_t c = 1; c < keys.data.size(); ++c) {
			// Skip comparisons that accept NULLs
			if (conditions[c].comparison == ExpressionType::COMPARE_DISTINCT_FROM) {
				continue;
			}
			//	ToUnifiedFormat the rest, as the sort code will do this anyway.
			auto &v = keys.data[c];
			UnifiedVectorFormat vdata;
			v.ToUnifiedFormat(count, vdata);
			auto &vvalidity = vdata.validity;
			if (vvalidity.AllValid()) {
				continue;
			}
			pvalidity.EnsureWritable();
			switch (v.GetVectorType()) {
			case VectorType::FLAT_VECTOR: {
				// Merge entire entries
				auto pmask = pvalidity.GetData();
				const auto entry_count = pvalidity.EntryCount(count);
				for (idx_t entry_idx = 0; entry_idx < entry_count; ++entry_idx) {
					pmask[entry_idx] &= vvalidity.GetValidityEntry(entry_idx);
				}
				break;
			}
			case VectorType::CONSTANT_VECTOR:
				// All or nothing
				if (ConstantVector::IsNull(v)) {
					pvalidity.SetAllInvalid(count);
					return count;
				}
				break;
			default:
				// One by one
				for (idx_t i = 0; i < count; ++i) {
					const auto idx = vdata.sel->get_index(i);
					if (!vvalidity.RowIsValidUnsafe(idx)) {
						pvalidity.SetInvalidUnsafe(i);
					}
				}
				break;
			}
		}
		return count - pvalidity.CountValid(count);
	} else {
		return count - VectorOperations::CountNotNull(primary, count);
	}
}

void PhysicalRangeJoin::ProjectResult(DataChunk &chunk, DataChunk &result) const {
	const auto left_projected = left_projection_map.size();
	for (idx_t i = 0; i < left_projected; ++i) {
		result.data[i].Reference(chunk.data[left_projection_map[i]]);
	}
	const auto left_width = children[0]->types.size();
	for (idx_t i = 0; i < right_projection_map.size(); ++i) {
		result.data[left_projected + i].Reference(chunk.data[left_width + right_projection_map[i]]);
	}
	result.SetCardinality(chunk);
}

BufferHandle PhysicalRangeJoin::SliceSortedPayload(DataChunk &payload, GlobalSortState &state, const idx_t block_idx,
                                                   const SelectionVector &result, const idx_t result_count,
                                                   const idx_t left_cols) {
	// There should only be one sorted block if they have been sorted
	D_ASSERT(state.sorted_blocks.size() == 1);
	SBScanState read_state(state.buffer_manager, state);
	read_state.sb = state.sorted_blocks[0].get();
	auto &sorted_data = *read_state.sb->payload_data;

	read_state.SetIndices(block_idx, 0);
	read_state.PinData(sorted_data);
	const auto data_ptr = read_state.DataPtr(sorted_data);
	data_ptr_t heap_ptr = nullptr;

	// Set up a batch of pointers to scan data from
	Vector addresses(LogicalType::POINTER, result_count);
	auto data_pointers = FlatVector::GetData<data_ptr_t>(addresses);

	// Set up the data pointers for the values that are actually referenced
	const idx_t &row_width = sorted_data.layout.GetRowWidth();

	auto prev_idx = result.get_index(0);
	SelectionVector gsel(result_count);
	idx_t addr_count = 0;
	gsel.set_index(0, addr_count);
	data_pointers[addr_count] = data_ptr + prev_idx * row_width;
	for (idx_t i = 1; i < result_count; ++i) {
		const auto row_idx = result.get_index(i);
		if (row_idx != prev_idx) {
			data_pointers[++addr_count] = data_ptr + row_idx * row_width;
			prev_idx = row_idx;
		}
		gsel.set_index(i, addr_count);
	}
	++addr_count;

	// Unswizzle the offsets back to pointers (if needed)
	if (!sorted_data.layout.AllConstant() && state.external) {
		heap_ptr = read_state.payload_heap_handle.Ptr();
	}

	// Deserialize the payload data
	auto sel = FlatVector::IncrementalSelectionVector();
	for (idx_t col_no = 0; col_no < sorted_data.layout.ColumnCount(); col_no++) {
		auto &col = payload.data[left_cols + col_no];
		RowOperations::Gather(addresses, *sel, col, *sel, addr_count, sorted_data.layout, col_no, 0, heap_ptr);
		col.Slice(gsel, result_count);
	}

	return std::move(read_state.payload_heap_handle);
}

idx_t PhysicalRangeJoin::SelectJoinTail(const ExpressionType &condition, Vector &left, Vector &right,
                                        const SelectionVector *sel, idx_t count, SelectionVector *true_sel) {
	switch (condition) {
	case ExpressionType::COMPARE_NOTEQUAL:
		return VectorOperations::NotEquals(left, right, sel, count, true_sel, nullptr);
	case ExpressionType::COMPARE_LESSTHAN:
		return VectorOperations::LessThan(left, right, sel, count, true_sel, nullptr);
	case ExpressionType::COMPARE_GREATERTHAN:
		return VectorOperations::GreaterThan(left, right, sel, count, true_sel, nullptr);
	case ExpressionType::COMPARE_LESSTHANOREQUALTO:
		return VectorOperations::LessThanEquals(left, right, sel, count, true_sel, nullptr);
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
		return VectorOperations::GreaterThanEquals(left, right, sel, count, true_sel, nullptr);
	case ExpressionType::COMPARE_DISTINCT_FROM:
		return VectorOperations::DistinctFrom(left, right, sel, count, true_sel, nullptr);
	case ExpressionType::COMPARE_NOT_DISTINCT_FROM:
		return VectorOperations::NotDistinctFrom(left, right, sel, count, true_sel, nullptr);
	case ExpressionType::COMPARE_EQUAL:
		return VectorOperations::Equals(left, right, sel, count, true_sel, nullptr);
	default:
		throw InternalException("Unsupported comparison type for PhysicalRangeJoin");
	}

	return count;
}

} // namespace duckdb
