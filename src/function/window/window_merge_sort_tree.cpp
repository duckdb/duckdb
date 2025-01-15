#include "duckdb/function/window/window_merge_sort_tree.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"

#include <thread>
#include <utility>

namespace duckdb {

WindowMergeSortTree::WindowMergeSortTree(ClientContext &context, const vector<BoundOrderByNode> &orders,
                                         const vector<column_t> &sort_idx, const idx_t count, bool unique)
    : context(context), memory_per_thread(PhysicalOperator::GetMaxThreadMemory(context)), sort_idx(sort_idx),
      build_stage(PartitionSortStage::INIT), tasks_completed(0) {
	// Sort the unfiltered indices by the orders
	const auto force_external = ClientConfig::GetConfig(context).force_external;
	LogicalType index_type;
	if (count < std::numeric_limits<uint32_t>::max() && !force_external) {
		index_type = LogicalType::INTEGER;
		mst32 = make_uniq<MergeSortTree32>();
	} else {
		index_type = LogicalType::BIGINT;
		mst64 = make_uniq<MergeSortTree64>();
	}

	vector<LogicalType> payload_types;
	payload_types.emplace_back(index_type);

	RowLayout payload_layout;
	payload_layout.Initialize(payload_types);

	auto &buffer_manager = BufferManager::GetBufferManager(context);
	if (unique) {
		vector<BoundOrderByNode> unique_orders;
		for (const auto &order : orders) {
			unique_orders.emplace_back(order.Copy());
		}
		auto unique_expr = make_uniq<BoundConstantExpression>(Value(index_type));
		const auto order_type = OrderType::ASCENDING;
		const auto order_by_type = OrderByNullType::NULLS_LAST;
		unique_orders.emplace_back(BoundOrderByNode(order_type, order_by_type, std::move(unique_expr)));
		global_sort = make_uniq<GlobalSortState>(buffer_manager, unique_orders, payload_layout);
	} else {
		global_sort = make_uniq<GlobalSortState>(buffer_manager, orders, payload_layout);
	}
	global_sort->external = force_external;
}

optional_ptr<LocalSortState> WindowMergeSortTree::AddLocalSort() {
	lock_guard<mutex> local_sort_guard(lock);
	auto local_sort = make_uniq<LocalSortState>();
	local_sort->Initialize(*global_sort, global_sort->buffer_manager);
	local_sorts.emplace_back(std::move(local_sort));

	return local_sorts.back().get();
}

WindowMergeSortTreeLocalState::WindowMergeSortTreeLocalState(WindowMergeSortTree &window_tree)
    : window_tree(window_tree) {
	sort_chunk.Initialize(window_tree.context, window_tree.global_sort->sort_layout.logical_types);
	payload_chunk.Initialize(window_tree.context, window_tree.global_sort->payload_layout.GetTypes());
	local_sort = window_tree.AddLocalSort();
}

void WindowMergeSortTreeLocalState::SinkChunk(DataChunk &chunk, const idx_t row_idx,
                                              optional_ptr<SelectionVector> filter_sel, idx_t filtered) {
	//	Sequence the payload column
	auto &indices = payload_chunk.data[0];
	payload_chunk.SetCardinality(chunk);
	indices.Sequence(int64_t(row_idx), 1, payload_chunk.size());

	//	Reference the sort columns
	auto &sort_idx = window_tree.sort_idx;
	for (column_t c = 0; c < sort_idx.size(); ++c) {
		sort_chunk.data[c].Reference(chunk.data[sort_idx[c]]);
	}
	// Add the row numbers if we are uniquifying
	if (sort_idx.size() < sort_chunk.ColumnCount()) {
		sort_chunk.data[sort_idx.size()].Reference(indices);
	}
	sort_chunk.SetCardinality(chunk);

	//	Apply FILTER clause, if any
	if (filter_sel) {
		sort_chunk.Slice(*filter_sel, filtered);
		payload_chunk.Slice(*filter_sel, filtered);
	}

	local_sort->SinkChunk(sort_chunk, payload_chunk);

	//	Flush if we have too much data
	if (local_sort->SizeInBytes() > window_tree.memory_per_thread) {
		local_sort->Sort(*window_tree.global_sort, true);
	}
}

void WindowMergeSortTreeLocalState::ExecuteSortTask() {
	switch (build_stage) {
	case PartitionSortStage::SCAN:
		window_tree.global_sort->AddLocalState(*window_tree.local_sorts[build_task]);
		break;
	case PartitionSortStage::MERGE: {
		auto &global_sort = *window_tree.global_sort;
		MergeSorter merge_sorter(global_sort, global_sort.buffer_manager);
		merge_sorter.PerformInMergeRound();
		break;
	}
	case PartitionSortStage::SORTED:
		BuildLeaves();
		break;
	default:
		break;
	}

	++window_tree.tasks_completed;
}

idx_t WindowMergeSortTree::MeasurePayloadBlocks() {
	const auto &blocks = global_sort->sorted_blocks[0]->payload_data->data_blocks;
	idx_t count = 0;
	for (const auto &block : blocks) {
		block_starts.emplace_back(count);
		count += block->count;
	}
	block_starts.emplace_back(count);

	// Allocate the leaves.
	if (mst32) {
		mst32->Allocate(count);
		mst32->LowestLevel().resize(count);
	} else if (mst64) {
		mst64->Allocate(count);
		mst64->LowestLevel().resize(count);
	}

	return count;
}

void WindowMergeSortTreeLocalState::BuildLeaves() {
	auto &global_sort = *window_tree.global_sort;
	if (global_sort.sorted_blocks.empty()) {
		return;
	}

	PayloadScanner scanner(global_sort, build_task);
	idx_t row_idx = window_tree.block_starts[build_task];
	for (;;) {
		payload_chunk.Reset();
		scanner.Scan(payload_chunk);
		const auto count = payload_chunk.size();
		if (count == 0) {
			break;
		}
		auto &indices = payload_chunk.data[0];
		if (window_tree.mst32) {
			auto &sorted = window_tree.mst32->LowestLevel();
			auto data = FlatVector::GetData<uint32_t>(indices);
			std::copy(data, data + count, sorted.data() + row_idx);
		} else {
			auto &sorted = window_tree.mst64->LowestLevel();
			auto data = FlatVector::GetData<uint64_t>(indices);
			std::copy(data, data + count, sorted.data() + row_idx);
		}
		row_idx += count;
	}
}

void WindowMergeSortTree::CleanupSort() {
	global_sort.reset();
	local_sorts.clear();
}

bool WindowMergeSortTree::TryPrepareSortStage(WindowMergeSortTreeLocalState &lstate) {
	lock_guard<mutex> stage_guard(lock);

	switch (build_stage.load()) {
	case PartitionSortStage::INIT:
		total_tasks = local_sorts.size();
		tasks_assigned = 0;
		tasks_completed = 0;
		lstate.build_stage = build_stage = PartitionSortStage::SCAN;
		lstate.build_task = tasks_assigned++;
		return true;
	case PartitionSortStage::SCAN:
		// Process all the local sorts
		if (tasks_assigned < total_tasks) {
			lstate.build_stage = PartitionSortStage::SCAN;
			lstate.build_task = tasks_assigned++;
			return true;
		} else if (tasks_completed < tasks_assigned) {
			return false;
		}
		global_sort->PrepareMergePhase();
		if (!(global_sort->sorted_blocks.size() / 2)) {
			if (global_sort->sorted_blocks.empty()) {
				lstate.build_stage = build_stage = PartitionSortStage::FINISHED;
				return true;
			}
			MeasurePayloadBlocks();
			total_tasks = block_starts.size() - 1;
			tasks_completed = 0;
			tasks_assigned = 0;
			lstate.build_stage = build_stage = PartitionSortStage::SORTED;
			lstate.build_task = tasks_assigned++;
			return true;
		}
		global_sort->InitializeMergeRound();
		lstate.build_stage = build_stage = PartitionSortStage::MERGE;
		total_tasks = local_sorts.size();
		tasks_assigned = 1;
		tasks_completed = 0;
		return true;
	case PartitionSortStage::MERGE:
		if (tasks_assigned < total_tasks) {
			lstate.build_stage = PartitionSortStage::MERGE;
			++tasks_assigned;
			return true;
		} else if (tasks_completed < tasks_assigned) {
			return false;
		}
		global_sort->CompleteMergeRound(true);
		if (!(global_sort->sorted_blocks.size() / 2)) {
			MeasurePayloadBlocks();
			total_tasks = block_starts.size() - 1;
			tasks_completed = 0;
			tasks_assigned = 0;
			lstate.build_stage = build_stage = PartitionSortStage::SORTED;
			lstate.build_task = tasks_assigned++;
			return true;
		}
		global_sort->InitializeMergeRound();
		lstate.build_stage = PartitionSortStage::MERGE;
		total_tasks = local_sorts.size();
		tasks_assigned = 1;
		tasks_completed = 0;
		return true;
	case PartitionSortStage::SORTED:
		if (tasks_assigned < total_tasks) {
			lstate.build_stage = PartitionSortStage::SORTED;
			lstate.build_task = tasks_assigned++;
			return true;
		} else if (tasks_completed < tasks_assigned) {
			lstate.build_stage = PartitionSortStage::FINISHED;
			// Sleep while other tasks finish
			return false;
		}
		CleanupSort();
		break;
	default:
		break;
	}

	lstate.build_stage = build_stage = PartitionSortStage::FINISHED;

	return true;
}

void WindowMergeSortTreeLocalState::Sort() {
	// Sort, merge and build the tree in parallel
	while (window_tree.build_stage.load() != PartitionSortStage::FINISHED) {
		if (window_tree.TryPrepareSortStage(*this)) {
			ExecuteSortTask();
		} else {
			std::this_thread::yield();
		}
	}
}

void WindowMergeSortTree::Build() {
	if (mst32) {
		mst32->Build();
	} else {
		mst64->Build();
	}
}

} // namespace duckdb
