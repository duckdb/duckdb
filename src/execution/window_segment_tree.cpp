#include "duckdb/execution/window_segment_tree.hpp"

#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/common/algorithm.hpp"
#include "duckdb/common/helper.hpp"

namespace duckdb {

WindowSegmentTree::WindowSegmentTree(AggregateFunction &aggregate, FunctionData *bind_info,
                                     const LogicalType &result_type_p, ChunkCollection *input,
                                     const ValidityMask &filter_mask_p, WindowAggregationMode mode_p)
    : aggregate(aggregate), bind_info(bind_info), result_type(result_type_p), state(aggregate.state_size()),
      statep(Value::POINTER((idx_t)state.data())), frame(0, 0), active(0, 1),
      statev(Value::POINTER((idx_t)state.data())), internal_nodes(0), input_ref(input), filter_mask(filter_mask_p),
      mode(mode_p) {
#if STANDARD_VECTOR_SIZE < 512
	throw NotImplementedException("Window functions are not supported for vector sizes < 512");
#endif
	statep.Normalify(STANDARD_VECTOR_SIZE);
	statev.SetVectorType(VectorType::FLAT_VECTOR); // Prevent conversion of results to constants

	if (input_ref && input_ref->ColumnCount() > 0) {
		filter_sel.Initialize(STANDARD_VECTOR_SIZE);
		inputs.Initialize(input_ref->Types());
		// if we have a frame-by-frame method, share the single state
		if (aggregate.window && UseWindowAPI()) {
			AggregateInit();
			inputs.Reference(input_ref->GetChunk(0));
		} else if (aggregate.combine && UseCombineAPI()) {
			ConstructTree();
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
	aggregate.finalize(statev, bind_info, result, 1, rid);

	if (aggregate.destructor) {
		aggregate.destructor(statev, 1);
	}
}

void WindowSegmentTree::ExtractFrame(idx_t begin, idx_t end) {
	const auto size = end - begin;
	if (size >= STANDARD_VECTOR_SIZE) {
		throw InternalException("Cannot compute window aggregation: bounds are too large");
	}

	const idx_t start_in_vector = begin % STANDARD_VECTOR_SIZE;
	const auto input_count = input_ref->ColumnCount();
	if (start_in_vector + size <= STANDARD_VECTOR_SIZE) {
		inputs.SetCardinality(size);
		auto &chunk = input_ref->GetChunkForRow(begin);
		for (idx_t i = 0; i < input_count; ++i) {
			auto &v = inputs.data[i];
			auto &vec = chunk.data[i];
			v.Slice(vec, start_in_vector);
			v.Verify(size);
		}
	} else {
		inputs.Reset();
		inputs.SetCardinality(size);

		// we cannot just slice the individual vector!
		auto &chunk_a = input_ref->GetChunkForRow(begin);
		auto &chunk_b = input_ref->GetChunkForRow(end);
		idx_t chunk_a_count = chunk_a.size() - start_in_vector;
		idx_t chunk_b_count = inputs.size() - chunk_a_count;
		for (idx_t i = 0; i < input_count; ++i) {
			auto &v = inputs.data[i];
			VectorOperations::Copy(chunk_a.data[i], v, chunk_a.size(), start_in_vector, 0);
			VectorOperations::Copy(chunk_b.data[i], v, chunk_b_count, 0, chunk_a_count);
		}
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

	if (end - begin >= STANDARD_VECTOR_SIZE) {
		throw InternalException("Cannot compute window aggregation: bounds are too large");
	}

	Vector s(statep, 0);
	if (l_idx == 0) {
		ExtractFrame(begin, end);
		aggregate.update(&inputs.data[0], bind_info, input_ref->ColumnCount(), s, inputs.size());
	} else {
		inputs.Reset();
		inputs.SetCardinality(end - begin);
		// find out where the states begin
		data_ptr_t begin_ptr = levels_flat_native.get() + state.size() * (begin + levels_flat_start[l_idx - 1]);
		// set up a vector of pointers that point towards the set of states
		Vector v(LogicalType::POINTER);
		auto pdata = FlatVector::GetData<data_ptr_t>(v);
		for (idx_t i = 0; i < inputs.size(); i++) {
			pdata[i] = begin_ptr + i * state.size();
		}
		v.Verify(inputs.size());
		aggregate.combine(v, s, bind_info, inputs.size());
	}
}

void WindowSegmentTree::ConstructTree() {
	D_ASSERT(input_ref);
	D_ASSERT(inputs.ColumnCount() > 0);

	// compute space required to store internal nodes of segment tree
	internal_nodes = 0;
	idx_t level_nodes = input_ref->Count();
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
	while ((level_size = (level_current == 0 ? input_ref->Count()
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

	// No arguments, so just count
	if (inputs.ColumnCount() == 0) {
		D_ASSERT(GetTypeIdSize(result_type.InternalType()) == sizeof(idx_t));
		auto data = FlatVector::GetData<idx_t>(result);
		// Slice to any filtered rows
		if (!filter_mask.AllValid()) {
			idx_t filtered = 0;
			for (idx_t i = begin; i < end; ++i) {
				filtered += filter_mask.RowIsValid(i);
			}
			data[rid] = filtered;
		} else {
			data[rid] = end - begin;
		}
		return;
	}

	// If we have a window function, use that
	if (aggregate.window && UseWindowAPI()) {
		// Frame boundaries
		auto prev = frame;
		frame = FrameBounds(begin, end);

		// Extract the range
		auto &coll = *input_ref;
		const auto prev_active = active;
		const FrameBounds combined(MinValue(frame.first, prev.first), MaxValue(frame.second, prev.second));

		// The chunk bounds are the range that includes the begin and end - 1
		const FrameBounds prev_chunks(coll.LocateChunk(prev_active.first), coll.LocateChunk(prev_active.second - 1));
		const FrameBounds active_chunks(coll.LocateChunk(combined.first), coll.LocateChunk(combined.second - 1));

		// Extract the range
		if (active_chunks.first == active_chunks.second) {
			// If all the data is in a single chunk, then just reference it
			if (prev_chunks != active_chunks || (!prev.first && !prev.second)) {
				inputs.Reference(coll.GetChunk(active_chunks.first));
			}
		} else if (active_chunks.first == prev_chunks.first && prev_chunks.first != prev_chunks.second) {
			// If the start chunk did not change, and we are not just a reference, then extend if necessary
			for (auto chunk_idx = prev_chunks.second + 1; chunk_idx <= active_chunks.second; ++chunk_idx) {
				inputs.Append(coll.GetChunk(chunk_idx), true);
			}
		} else {
			// If the first chunk changed, start over
			inputs.Reset();
			for (auto chunk_idx = active_chunks.first; chunk_idx <= active_chunks.second; ++chunk_idx) {
				inputs.Append(coll.GetChunk(chunk_idx), true);
			}
		}

		active = FrameBounds(active_chunks.first * STANDARD_VECTOR_SIZE,
		                     MinValue((active_chunks.second + 1) * STANDARD_VECTOR_SIZE, coll.Count()));

		aggregate.window(inputs.data.data(), filter_mask, bind_info, inputs.ColumnCount(), state.data(), frame, prev,
		                 result, rid, active.first);
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
