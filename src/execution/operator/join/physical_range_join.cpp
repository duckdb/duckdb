#include "duckdb/execution/operator/join/physical_range_join.hpp"

#include "duckdb/common/row_operations/row_operations.hpp"
#include "duckdb/common/sorting/sort_key.hpp"
#include "duckdb/common/types/validity_mask.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parallel/base_pipeline_event.hpp"
#include "duckdb/parallel/thread_context.hpp"
#include "duckdb/parallel/executor_task.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"

namespace duckdb {

PhysicalRangeJoin::LocalSortedTable::LocalSortedTable(ExecutionContext &context, GlobalSortedTable &global_table,
                                                      const idx_t child)
    : global_table(global_table), executor(context.client), has_null(0), count(0) {
	// Initialize order clause expression executor and key DataChunk
	const auto &op = global_table.op;
	vector<LogicalType> types;
	for (const auto &cond : op.conditions) {
		const auto &expr = child ? cond.right : cond.left;
		executor.AddExpression(*expr);

		types.push_back(expr->return_type);
	}
	auto &allocator = Allocator::Get(context.client);
	keys.Initialize(allocator, types);

	local_sink = global_table.sort->GetLocalSinkState(context);

	//	Only sort the primary key
	types.resize(1);
	const auto &payload_types = op.children[child].get().types;
	types.insert(types.end(), payload_types.begin(), payload_types.end());
	sort_chunk.InitializeEmpty(types);
}

void PhysicalRangeJoin::LocalSortedTable::Sink(ExecutionContext &context, DataChunk &input) {
	// Obtain sorting columns
	keys.Reset();
	executor.Execute(input, keys);

	// Do not operate on primary key directly to avoid modifying the input chunk
	Vector primary = keys.data[0];
	// Count the NULLs so we can exclude them later
	has_null += MergeNulls(primary, global_table.op.conditions);
	count += keys.size();

	//	Only sort the primary key
	sort_chunk.data[0].Reference(primary);
	for (column_t col_idx = 0; col_idx < input.ColumnCount(); ++col_idx) {
		sort_chunk.data[col_idx + 1].Reference(input.data[col_idx]);
	}
	sort_chunk.SetCardinality(input);

	// Sink the data into the local sort state
	InterruptState interrupt;
	OperatorSinkInput sink {*global_table.global_sink, *local_sink, interrupt};
	global_table.sort->Sink(context, sort_chunk, sink);
}

PhysicalRangeJoin::GlobalSortedTable::GlobalSortedTable(ClientContext &client,
                                                        const vector<BoundOrderByNode> &order_bys,
                                                        const vector<LogicalType> &payload_types,
                                                        const PhysicalRangeJoin &op)
    : op(op), has_null(0), count(0), tasks_completed(0) {
	// Set up the sort. We will materialize keys ourselves, so just set up references.
	vector<BoundOrderByNode> orders;
	vector<LogicalType> input_types;
	for (const auto &order_by : order_bys) {
		auto order = order_by.Copy();
		const auto type = order.expression->return_type;
		input_types.emplace_back(type);
		order.expression = make_uniq<BoundReferenceExpression>(type, orders.size());
		orders.emplace_back(std::move(order));
	}

	vector<idx_t> projection_map;
	for (const auto &type : payload_types) {
		projection_map.emplace_back(input_types.size());
		input_types.emplace_back(type);
	}

	sort = make_uniq<Sort>(client, orders, input_types, projection_map);

	global_sink = sort->GetGlobalSinkState(client);
}

void PhysicalRangeJoin::GlobalSortedTable::Combine(ExecutionContext &context, LocalSortedTable &ltable) {
	InterruptState interrupt;
	OperatorSinkCombineInput combine {*global_sink, *ltable.local_sink, interrupt};
	sort->Combine(context, combine);
	has_null += ltable.has_null;
	count += ltable.count;
}

void PhysicalRangeJoin::GlobalSortedTable::Finalize(ClientContext &client, InterruptState &interrupt) {
	OperatorSinkFinalizeInput finalize {*global_sink, interrupt};
	sort->Finalize(client, finalize);
}

void PhysicalRangeJoin::GlobalSortedTable::IntializeMatches() {
	found_match = make_unsafe_uniq_array_uninitialized<bool>(Count());
	memset(found_match.get(), 0, sizeof(bool) * Count());
}

void PhysicalRangeJoin::GlobalSortedTable::MaterializeEmpty(ClientContext &client) {
	D_ASSERT(!sorted);
	sorted = make_uniq<SortedRun>(client, *sort, false);
}

void PhysicalRangeJoin::GlobalSortedTable::Print() {
	D_ASSERT(sorted);
	auto &collection = *sorted->payload_data;
	TupleDataScanState scanner;
	collection.InitializeScan(scanner);

	DataChunk payload;
	collection.InitializeScanChunk(scanner, payload);

	while (collection.Scan(scanner, payload)) {
		payload.Print();
	}
}

//===--------------------------------------------------------------------===//
// RangeJoinMaterializeTask
//===--------------------------------------------------------------------===//
class RangeJoinMaterializeTask : public ExecutorTask {
public:
	using GlobalSortedTable = PhysicalRangeJoin::GlobalSortedTable;

public:
	RangeJoinMaterializeTask(Pipeline &pipeline, shared_ptr<Event> event, ClientContext &client,
	                         GlobalSortedTable &table, idx_t tasks_scheduled)
	    : ExecutorTask(client, std::move(event), table.op), pipeline(pipeline), table(table),
	      tasks_scheduled(tasks_scheduled) {
	}

	TaskExecutionResult ExecuteTask(TaskExecutionMode mode) override {
		ExecutionContext execution(pipeline.GetClientContext(), *thread_context, &pipeline);
		auto &sort = *table.sort;
		auto &sort_global = *table.global_source;
		auto sort_local = sort.GetLocalSourceState(execution, sort_global);
		InterruptState interrupt((weak_ptr<Task>(shared_from_this())));
		OperatorSourceInput input {sort_global, *sort_local, interrupt};
		sort.MaterializeSortedRun(execution, input);
		if (++table.tasks_completed == tasks_scheduled) {
			table.sorted = sort.GetSortedRun(sort_global);
			if (!table.sorted) {
				table.MaterializeEmpty(execution.client);
			}
		}

		event->FinishTask();
		return TaskExecutionResult::TASK_FINISHED;
	}

	string TaskType() const override {
		return "RangeJoinMaterializeTask";
	}

private:
	Pipeline &pipeline;
	GlobalSortedTable &table;
	const idx_t tasks_scheduled;
};

//===--------------------------------------------------------------------===//
// RangeJoinMaterializeEvent
//===--------------------------------------------------------------------===//
class RangeJoinMaterializeEvent : public BasePipelineEvent {
public:
	using GlobalSortedTable = PhysicalRangeJoin::GlobalSortedTable;

public:
	RangeJoinMaterializeEvent(GlobalSortedTable &table, Pipeline &pipeline)
	    : BasePipelineEvent(pipeline), table(table) {
	}

	GlobalSortedTable &table;

public:
	void Schedule() override {
		auto &client = pipeline->GetClientContext();

		// Schedule as many tasks as the sort will allow
		auto &ts = TaskScheduler::GetScheduler(client);
		auto num_threads = NumericCast<idx_t>(ts.NumberOfThreads());
		vector<shared_ptr<Task>> tasks;

		auto &sort = *table.sort;
		auto &global_sink = *table.global_sink;
		table.global_source = sort.GetGlobalSourceState(client, global_sink);
		const auto tasks_scheduled = MinValue<idx_t>(num_threads, table.global_source->MaxThreads());
		for (idx_t tnum = 0; tnum < tasks_scheduled; ++tnum) {
			tasks.push_back(
			    make_uniq<RangeJoinMaterializeTask>(*pipeline, shared_from_this(), client, table, tasks_scheduled));
		}

		SetTasks(std::move(tasks));
	}
};

void PhysicalRangeJoin::GlobalSortedTable::Materialize(Pipeline &pipeline, Event &event) {
	// Schedule all the sorts for maximum thread utilisation
	auto sort_event = make_shared_ptr<RangeJoinMaterializeEvent>(*this, pipeline);
	event.InsertEvent(std::move(sort_event));
}

void PhysicalRangeJoin::GlobalSortedTable::Materialize(ExecutionContext &context, InterruptState &interrupt) {
	global_source = sort->GetGlobalSourceState(context.client, *global_sink);
	auto local_source = sort->GetLocalSourceState(context, *global_source);
	OperatorSourceInput source {*global_source, *local_source, interrupt};
	sort->MaterializeSortedRun(context, source);
	sorted = sort->GetSortedRun(*global_source);
	if (!sorted) {
		MaterializeEmpty(context.client);
	}
}

PhysicalRangeJoin::PhysicalRangeJoin(PhysicalPlan &physical_plan, LogicalComparisonJoin &op, PhysicalOperatorType type,
                                     PhysicalOperator &left, PhysicalOperator &right, vector<JoinCondition> cond,
                                     JoinType join_type, idx_t estimated_cardinality,
                                     unique_ptr<JoinFilterPushdownInfo> pushdown_info)
    : PhysicalComparisonJoin(physical_plan, op, type, std::move(cond), join_type, estimated_cardinality) {
	filter_pushdown = std::move(pushdown_info);
	// Reorder the conditions so that ranges are at the front.
	// TODO: use stats to improve the choice?
	// TODO: Prefer fixed length types?
	if (conditions.size() > 1) {
		unordered_map<idx_t, idx_t> cond_idx;
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
				cond_idx[i] = range_position - 1;
				break;
			default:
				conditions[--other_position] = std::move(conditions_p[i]);
				cond_idx[i] = other_position;
				break;
			}
		}
		if (filter_pushdown) {
			for (auto &idx : filter_pushdown->join_condition) {
				if (cond_idx.find(idx) != cond_idx.end()) {
					idx = cond_idx[idx];
				}
			}
		}
	}

	children.push_back(left);
	children.push_back(right);

	//	Fill out the left projection map.
	left_projection_map = op.left_projection_map;
	if (left_projection_map.empty()) {
		const auto left_count = children[0].get().GetTypes().size();
		left_projection_map.reserve(left_count);
		for (column_t i = 0; i < left_count; ++i) {
			left_projection_map.emplace_back(i);
		}
	}
	//	Fill out the right projection map.
	right_projection_map = op.right_projection_map;
	if (right_projection_map.empty()) {
		const auto right_count = children[1].get().GetTypes().size();
		right_projection_map.reserve(right_count);
		for (column_t i = 0; i < right_count; ++i) {
			right_projection_map.emplace_back(i);
		}
	}

	//	Construct the unprojected type layout from the children's types
	unprojected_types = children[0].get().GetTypes();
	auto &types = children[1].get().GetTypes();
	unprojected_types.insert(unprojected_types.end(), types.begin(), types.end());
}

idx_t PhysicalRangeJoin::LocalSortedTable::MergeNulls(Vector &primary, const vector<JoinCondition> &conditions) {
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

	if (all_constant == keys.data.size()) {
		//	Either all NULL or no NULLs
		if (ConstantVector::IsNull(primary)) {
			// Primary is already NULL
			return count;
		}
		for (size_t c = 1; c < keys.data.size(); ++c) {
			// Skip comparisons that accept NULLs
			if (conditions[c].comparison == ExpressionType::COMPARE_DISTINCT_FROM) {
				continue;
			}
			auto &v = keys.data[c];
			if (ConstantVector::IsNull(v)) {
				// Create a new validity mask to avoid modifying original mask
				auto &pvalidity = ConstantVector::Validity(primary);
				ValidityMask pvalidity_copy = ConstantVector::Validity(primary);
				pvalidity.Copy(pvalidity_copy, count);
				ConstantVector::SetNull(primary, true);
				return count;
			}
		}
		return 0;
	} else if (keys.ColumnCount() > 1) {
		//	Flatten the primary, as it will need to merge arbitrary validity masks
		primary.Flatten(count);
		auto &pvalidity = FlatVector::Validity(primary);
		// Make a copy of validity to avoid modifying original mask
		ValidityMask pvalidity_copy = FlatVector::Validity(primary);
		pvalidity.Copy(pvalidity_copy, count);

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
	const auto left_width = children[0].get().GetTypes().size();
	for (idx_t i = 0; i < right_projection_map.size(); ++i) {
		result.data[left_projected + i].Reference(chunk.data[left_width + right_projection_map[i]]);
	}
	result.SetCardinality(chunk);
}

template <SortKeyType SORT_KEY_TYPE>
static void TemplatedSliceSortedPayload(DataChunk &chunk, const SortedRun &sorted_run,
                                        ExternalBlockIteratorState &state, Vector &sort_key_pointers,
                                        SortedRunScanState &scan_state, const idx_t chunk_idx, SelectionVector &result,
                                        const idx_t result_count) {
	using SORT_KEY = SortKey<SORT_KEY_TYPE>;
	using BLOCK_ITERATOR = block_iterator_t<ExternalBlockIteratorState, SORT_KEY>;
	BLOCK_ITERATOR itr(state, chunk_idx, 0);

	const auto sort_keys = FlatVector::GetData<SORT_KEY *>(sort_key_pointers);
	for (idx_t i = 0; i < result_count; ++i) {
		const auto idx = state.GetIndex(chunk_idx, result.get_index(i));
		sort_keys[i] = &itr[idx];
	}

	// Scan
	chunk.Reset();
	scan_state.Scan(sorted_run, sort_key_pointers, result_count, chunk);
}

void PhysicalRangeJoin::SliceSortedPayload(DataChunk &chunk, GlobalSortedTable &table,
                                           ExternalBlockIteratorState &state, TupleDataChunkState &chunk_state,
                                           const idx_t chunk_idx, SelectionVector &result, const idx_t result_count,
                                           SortedRunScanState &scan_state) {
	auto &sorted = *table.sorted;
	auto &sort_keys = chunk_state.row_locations;
	const auto sort_key_type = table.GetSortKeyType();

	switch (sort_key_type) {
	case SortKeyType::NO_PAYLOAD_FIXED_8:
		TemplatedSliceSortedPayload<SortKeyType::NO_PAYLOAD_FIXED_8>(chunk, sorted, state, sort_keys, scan_state,
		                                                             chunk_idx, result, result_count);
		break;
	case SortKeyType::NO_PAYLOAD_FIXED_16:
		TemplatedSliceSortedPayload<SortKeyType::NO_PAYLOAD_FIXED_16>(chunk, sorted, state, sort_keys, scan_state,
		                                                              chunk_idx, result, result_count);
		break;
	case SortKeyType::NO_PAYLOAD_FIXED_24:
		TemplatedSliceSortedPayload<SortKeyType::NO_PAYLOAD_FIXED_24>(chunk, sorted, state, sort_keys, scan_state,
		                                                              chunk_idx, result, result_count);
		break;
	case SortKeyType::NO_PAYLOAD_FIXED_32:
		TemplatedSliceSortedPayload<SortKeyType::NO_PAYLOAD_FIXED_32>(chunk, sorted, state, sort_keys, scan_state,
		                                                              chunk_idx, result, result_count);
		break;
	case SortKeyType::NO_PAYLOAD_VARIABLE_32:
		TemplatedSliceSortedPayload<SortKeyType::NO_PAYLOAD_VARIABLE_32>(chunk, sorted, state, sort_keys, scan_state,
		                                                                 chunk_idx, result, result_count);
		break;
	case SortKeyType::PAYLOAD_FIXED_16:
		TemplatedSliceSortedPayload<SortKeyType::PAYLOAD_FIXED_16>(chunk, sorted, state, sort_keys, scan_state,
		                                                           chunk_idx, result, result_count);
		break;
	case SortKeyType::PAYLOAD_FIXED_24:
		TemplatedSliceSortedPayload<SortKeyType::PAYLOAD_FIXED_24>(chunk, sorted, state, sort_keys, scan_state,
		                                                           chunk_idx, result, result_count);
		break;
	case SortKeyType::PAYLOAD_FIXED_32:
		TemplatedSliceSortedPayload<SortKeyType::PAYLOAD_FIXED_32>(chunk, sorted, state, sort_keys, scan_state,
		                                                           chunk_idx, result, result_count);
		break;
	case SortKeyType::PAYLOAD_VARIABLE_32:
		TemplatedSliceSortedPayload<SortKeyType::PAYLOAD_VARIABLE_32>(chunk, sorted, state, sort_keys, scan_state,
		                                                              chunk_idx, result, result_count);
		break;
	default:
		throw NotImplementedException("MergeJoinSimpleBlocks for %s", EnumUtil::ToString(sort_key_type));
	}
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
