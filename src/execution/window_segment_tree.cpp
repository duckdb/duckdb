#include "duckdb/execution/window_segment_tree.hpp"

#include "duckdb/common/vector_operations/vector_operations.hpp"

#include <cmath>

using namespace duckdb;
using namespace std;

WindowSegmentTree::WindowSegmentTree(AggregateFunction &aggregate, TypeId result_type, ChunkCollection *input)
    : aggregate(aggregate), state(aggregate.state_size()), statep(TypeId::POINTER), result_type(result_type),
      input_ref(input) {
	statep.SetCount(STANDARD_VECTOR_SIZE);
	Value ptr_val = Value::POINTER((idx_t)state.data());
	statep.Reference(ptr_val);
	statep.Normalify(STANDARD_VECTOR_SIZE);

	if (input_ref && input_ref->column_count() > 0) {
		inputs.Initialize(input_ref->types);
		if (aggregate.combine) {
			ConstructTree();
		}
	}
}

void WindowSegmentTree::AggregateInit() {
	aggregate.initialize(state.data());
}

Value WindowSegmentTree::AggegateFinal() {
	Vector statev(Value::POINTER((idx_t)state.data()));
	Vector result(result_type);
	result.vector_type = VectorType::CONSTANT_VECTOR;
	ConstantVector::SetNull(result, false);
	aggregate.finalize(statev, result, 1);

	return result.GetValue(0);
}

void WindowSegmentTree::WindowSegmentValue(idx_t l_idx, idx_t begin, idx_t end) {
	assert(begin <= end);
	if (begin == end) {
		return;
	}
	inputs.SetCardinality(end - begin);

	idx_t start_in_vector = begin % STANDARD_VECTOR_SIZE;
	Vector s;
	s.Slice(statep, start_in_vector);
	if (l_idx == 0) {
		const auto input_count = input_ref->column_count();
		auto &chunk = input_ref->GetChunk(begin);
		for (idx_t i = 0; i < input_count; ++i) {
			auto &v = inputs.data[i];
			auto &vec = chunk.data[i];
			v.Slice(vec, start_in_vector);
			v.Verify(inputs.size());
		}
		aggregate.update(&inputs.data[0], input_count, s, inputs.size());
	} else {
		assert(end - begin <= STANDARD_VECTOR_SIZE);
		data_ptr_t ptr = levels_flat_native.get() + state.size() * (begin + levels_flat_start[l_idx - 1]);
		Vector v(result_type, ptr);
		v.Verify(inputs.size());
		aggregate.combine(v, s, inputs.size());
	}
}

void WindowSegmentTree::ConstructTree() {
	assert(input_ref);
	assert(inputs.column_count() > 0);

	// compute space required to store internal nodes of segment tree
	idx_t internal_nodes = 0;
	idx_t level_nodes = input_ref->count;
	do {
		level_nodes = (idx_t)ceil((double)level_nodes / TREE_FANOUT);
		internal_nodes += level_nodes;
	} while (level_nodes > 1);
	levels_flat_native = unique_ptr<data_t[]>(new data_t[internal_nodes * state.size()]);
	levels_flat_start.push_back(0);

	idx_t levels_flat_offset = 0;
	idx_t level_current = 0;
	// level 0 is data itself
	idx_t level_size;
	while ((level_size = (level_current == 0 ? input_ref->count
	                                         : levels_flat_offset - levels_flat_start[level_current - 1])) > 1) {
		for (idx_t pos = 0; pos < level_size; pos += TREE_FANOUT) {
			AggregateInit();
			WindowSegmentValue(level_current, pos, min(level_size, pos + TREE_FANOUT));

			memcpy(levels_flat_native.get() + (levels_flat_offset * state.size()), state.data(), state.size());

			levels_flat_offset++;
		}

		levels_flat_start.push_back(levels_flat_offset);
		level_current++;
	}
}

Value WindowSegmentTree::Compute(idx_t begin, idx_t end) {
	assert(input_ref);

	// No arguments, so just count
	if (inputs.column_count() == 0) {
		return Value::Numeric(result_type, end - begin);
	}

	AggregateInit();

	// Aggregate everything at once if we can't combine states
	if (!aggregate.combine) {
		WindowSegmentValue(0, begin, end);
		return AggegateFinal();
	}

	for (idx_t l_idx = 0; l_idx < levels_flat_start.size() + 1; l_idx++) {
		idx_t parent_begin = begin / TREE_FANOUT;
		idx_t parent_end = end / TREE_FANOUT;
		if (parent_begin == parent_end) {
			WindowSegmentValue(l_idx, begin, end);
			return AggegateFinal();
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

	return AggegateFinal();
}
