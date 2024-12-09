#include "duckdb/function/window/window_index_tree.hpp"

#include <thread>
#include <utility>

namespace duckdb {

WindowIndexTree::WindowIndexTree(ClientContext &context, const BoundOrderModifier &order_bys, vector<column_t> sort_idx,
                                 const idx_t count)
    : context(context), memory_per_thread(PhysicalOperator::GetMaxThreadMemory(context)),
      sort_idx(std::move(sort_idx)) {
	// Sort the unfiltered indices by the orders
	LogicalType index_type;
	if (count < std::numeric_limits<uint32_t>::max()) {
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
	global_sort = make_uniq<GlobalSortState>(buffer_manager, order_bys.orders, payload_layout);
	global_sort->external = ClientConfig::GetConfig(context).force_external;
}

optional_ptr<LocalSortState> WindowIndexTree::AddLocalSort() {
	lock_guard<mutex> local_sort_guard(lock);
	auto local_sort = make_uniq<LocalSortState>();
	local_sort->Initialize(*global_sort, global_sort->buffer_manager);
	local_sorts.emplace_back(std::move(local_sort));

	return local_sorts.back().get();
}

WindowIndexTreeLocalState::WindowIndexTreeLocalState(WindowIndexTree &index_tree) : index_tree(index_tree) {
	payload_chunk.Initialize(index_tree.context, index_tree.global_sort->payload_layout.GetTypes());
	local_sort = index_tree.AddLocalSort();
}

void WindowIndexTreeLocalState::SinkChunk(DataChunk &chunk, const idx_t row_idx,
                                          optional_ptr<SelectionVector> filter_sel, idx_t filtered) {
	//	Reference the sort columns
	auto &sort_idx = index_tree.sort_idx;
	for (column_t c = 0; c < sort_idx.size(); ++c) {
		sort_chunk.data[c].Reference(chunk.data[sort_idx[c]]);
	}

	//	Sequence the payload column
	auto &indices = payload_chunk.data[0];
	payload_chunk.SetCardinality(sort_chunk);
	indices.Sequence(int64_t(row_idx), 1, payload_chunk.size());

	//	Apply FILTER clause, if any
	if (filter_sel) {
		sort_chunk.Slice(*filter_sel, filtered);
		payload_chunk.Slice(*filter_sel, filtered);
	}

	local_sort->SinkChunk(sort_chunk, payload_chunk);

	//	Flush if we have too much data
	if (local_sort->SizeInBytes() > index_tree.memory_per_thread) {
		local_sort->Sort(*index_tree.global_sort, true);
	}
}

void WindowIndexTreeLocalState::ExecuteSortTask() {
	auto &global_sort = *index_tree.global_sort;
	switch (build_stage) {
	case PartitionSortStage::SCAN:
		global_sort.AddLocalState(*index_tree.local_sorts[build_task]);
		break;
	case PartitionSortStage::MERGE: {
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

	++index_tree.tasks_completed;
}

void WindowIndexTree::MeasurePayloadBlocks() {
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
}

void WindowIndexTreeLocalState::BuildLeaves() {
	auto &global_sort = *index_tree.global_sort;
	if (global_sort.sorted_blocks.empty()) {
		return;
	}

	PayloadScanner scanner(global_sort, build_task, true);
	for (;;) {
		idx_t row_idx = scanner.Scanned();
		payload_chunk.Reset();
		scanner.Scan(payload_chunk);
		if (payload_chunk.size() == 0) {
			break;
		}
		auto &indices = payload_chunk.data[0];
		if (index_tree.mst32) {
			auto &sorted = index_tree.mst32->LowestLevel();
			auto data = FlatVector::GetData<uint32_t>(indices);
			std::copy(data, data + payload_chunk.size(), sorted.data() + row_idx);
		} else {
			auto &sorted = index_tree.mst64->LowestLevel();
			auto data = FlatVector::GetData<uint64_t>(indices);
			std::copy(data, data + payload_chunk.size(), sorted.data() + row_idx);
		}
	}
}

void WindowIndexTree::CleanupSort() {
	global_sort.reset();
	local_sorts.clear();
}

bool WindowIndexTree::TryPrepareSortStage(WindowIndexTreeLocalState &lstate) {
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

void WindowIndexTreeLocalState::Build() {
	// Sort, merge and build the tree in parallel
	while (index_tree.build_stage.load() != PartitionSortStage::FINISHED) {
		if (index_tree.TryPrepareSortStage(*this)) {
			ExecuteSortTask();
		} else {
			std::this_thread::yield();
		}
	}

	//	Now build the tree in parallel
	if (index_tree.mst32) {
		index_tree.mst32->Build();
	} else {
		index_tree.mst64->Build();
	}
}

idx_t WindowIndexTree::SelectNth(const SubFrames &frames, idx_t n) const {
	if (mst32) {
		return mst32->SelectNth(frames, n);
	} else {
		return mst64->SelectNth(frames, n);
	}
}

} // namespace duckdb
