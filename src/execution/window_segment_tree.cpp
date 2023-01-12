#include "duckdb/execution/window_segment_tree.hpp"

#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/common/algorithm.hpp"
#include "duckdb/common/helper.hpp"

namespace duckdb {

WindowSegmentTree::WindowSegmentTree(AggregateFunction &aggregate, FunctionData *bind_info,
                                     const LogicalType &result_type_p, DataChunk *input,
                                     const ValidityMask &filter_mask_p, WindowAggregationMode mode_p)
    : aggregate(aggregate), bind_info(bind_info), result_type(result_type_p), state(aggregate.state_size()),
      statep(Value::POINTER((idx_t)state.data())), frame(0, 0), statev(Value::POINTER((idx_t)state.data())),
      internal_nodes(0), input_ref(input), filter_mask(filter_mask_p), mode(mode_p) {
	statep.Flatten(input->size());
	statev.SetVectorType(VectorType::FLAT_VECTOR); // Prevent conversion of results to constants

	if (input_ref && input_ref->ColumnCount() > 0) {
		filter_sel.Initialize(input->size());
		inputs.Initialize(Allocator::DefaultAllocator(), input_ref->GetTypes());
		// if we have a frame-by-frame method, share the single state
		if (aggregate.window && UseWindowAPI()) {
			AggregateInit();
			inputs.Reference(*input_ref);
		} else {
			inputs.SetCapacity(*input_ref);
			if (aggregate.combine && UseCombineAPI()) {
				ConstructTree();
			}
		}
	}
}

WindowSegmentTree::~WindowSegmentTree() {
	if (!aggregate.destructor) {
		// nothing to destroy
		return;
	}
	// call the destructor for all the intermediate states
	data_ptr_t address_data[STANDARD_VECTOR_SIZE];
	Vector addresses(LogicalType::POINTER, (data_ptr_t)address_data);
	idx_t count = 0;
	for (idx_t i = 0; i < internal_nodes; i++) {
		address_data[count++] = data_ptr_t(levels_flat_native.get() + i * state.size());
		if (count == STANDARD_VECTOR_SIZE) {
			aggregate.destructor(addresses, count);
			count = 0;
		}
	}
	if (count > 0) {
		aggregate.destructor(addresses, count);
	}

	if (aggregate.window && UseWindowAPI()) {
		aggregate.destructor(statev, 1);
	}
}

void WindowSegmentTree::AggregateInit() {
	aggregate.initialize(state.data());
}

void WindowSegmentTree::AggegateFinal(Vector &result, idx_t rid) {
	AggregateInputData aggr_input_data(bind_info, Allocator::DefaultAllocator());
	aggregate.finalize(statev, aggr_input_data, result, 1, rid);

	if (aggregate.destructor) {
		aggregate.destructor(statev, 1);
	}
}

void WindowSegmentTree::ExtractFrame(idx_t begin, idx_t end) {
	const auto size = end - begin;

	auto &chunk = *input_ref;
	const auto input_count = input_ref->ColumnCount();
	inputs.SetCardinality(size);
	for (idx_t i = 0; i < input_count; ++i) {
		auto &v = inputs.data[i];
		auto &vec = chunk.data[i];
		v.Slice(vec, begin, end);
		v.Verify(size);
	}

	// Slice to any filtered rows
	if (!filter_mask.AllValid()) {
		idx_t filtered = 0;
		for (idx_t i = begin; i < end; ++i) {
			if (filter_mask.RowIsValid(i)) {
				filter_sel.set_index(filtered++, i - begin);
			}
		}
		if (filtered != inputs.size()) {
			inputs.Slice(filter_sel, filtered);
		}
	}
}

void WindowSegmentTree::WindowSegmentValue(idx_t l_idx, idx_t begin, idx_t end) {
	D_ASSERT(begin <= end);
	if (begin == end) {
		return;
	}

	const auto count = end - begin;
	Vector s(statep, 0, count);
	if (l_idx == 0) {
		ExtractFrame(begin, end);
		AggregateInputData aggr_input_data(bind_info, Allocator::DefaultAllocator());
		aggregate.update(&inputs.data[0], aggr_input_data, input_ref->ColumnCount(), s, inputs.size());
	} else {
		// find out where the states begin
		data_ptr_t begin_ptr = levels_flat_native.get() + state.size() * (begin + levels_flat_start[l_idx - 1]);
		// set up a vector of pointers that point towards the set of states
		Vector v(LogicalType::POINTER, count);
		auto pdata = FlatVector::GetData<data_ptr_t>(v);
		for (idx_t i = 0; i < count; i++) {
			pdata[i] = begin_ptr + i * state.size();
		}
		v.Verify(count);
		AggregateInputData aggr_input_data(bind_info, Allocator::DefaultAllocator());
		aggregate.combine(v, s, aggr_input_data, count);
	}
}

void WindowSegmentTree::ConstructTree() {
	D_ASSERT(input_ref);
	D_ASSERT(inputs.ColumnCount() > 0);

	// compute space required to store internal nodes of segment tree
	internal_nodes = 0;
	idx_t level_nodes = input_ref->size();
	do {
		level_nodes = (level_nodes + (TREE_FANOUT - 1)) / TREE_FANOUT;
		internal_nodes += level_nodes;
	} while (level_nodes > 1);
	levels_flat_native = unique_ptr<data_t[]>(new data_t[internal_nodes * state.size()]);
	levels_flat_start.push_back(0);

	idx_t levels_flat_offset = 0;
	idx_t level_current = 0;
	// level 0 is data itself
	idx_t level_size;
	// iterate over the levels of the segment tree
	while ((level_size = (level_current == 0 ? input_ref->size()
	                                         : levels_flat_offset - levels_flat_start[level_current - 1])) > 1) {
		for (idx_t pos = 0; pos < level_size; pos += TREE_FANOUT) {
			// compute the aggregate for this entry in the segment tree
			AggregateInit();
			WindowSegmentValue(level_current, pos, MinValue(level_size, pos + TREE_FANOUT));

			memcpy(levels_flat_native.get() + (levels_flat_offset * state.size()), state.data(), state.size());

			levels_flat_offset++;
		}

		levels_flat_start.push_back(levels_flat_offset);
		level_current++;
	}

	// Corner case: single element in the window
	if (levels_flat_offset == 0) {
		aggregate.initialize(levels_flat_native.get());
	}
}

void WindowSegmentTree::Compute(Vector &result, idx_t rid, idx_t begin, idx_t end) {
	D_ASSERT(input_ref);

	// If we have a window function, use that
	if (aggregate.window && UseWindowAPI()) {
		// Frame boundaries
		auto prev = frame;
		frame = FrameBounds(begin, end);

		// Extract the range
		AggregateInputData aggr_input_data(bind_info, Allocator::DefaultAllocator());
		aggregate.window(input_ref->data.data(), filter_mask, aggr_input_data, inputs.ColumnCount(), state.data(),
		                 frame, prev, result, rid, 0);
		return;
	}

	AggregateInit();

	// Aggregate everything at once if we can't combine states
	if (!aggregate.combine || !UseCombineAPI()) {
		WindowSegmentValue(0, begin, end);
		AggegateFinal(result, rid);
		return;
	}

	for (idx_t l_idx = 0; l_idx < levels_flat_start.size() + 1; l_idx++) {
		idx_t parent_begin = begin / TREE_FANOUT;
		idx_t parent_end = end / TREE_FANOUT;
		if (parent_begin == parent_end) {
			WindowSegmentValue(l_idx, begin, end);
			break;
		}
		idx_t group_begin = parent_begin * TREE_FANOUT;
		if (begin != group_begin) {
			WindowSegmentValue(l_idx, begin, group_begin + TREE_FANOUT);
			parent_begin++;
		}
		idx_t group_end = parent_end * TREE_FANOUT;
		if (end != group_end) {
			WindowSegmentValue(l_idx, group_end, end);
		}
		begin = parent_begin;
		end = parent_end;
	}

	AggegateFinal(result, rid);
}

} // namespace duckdb
