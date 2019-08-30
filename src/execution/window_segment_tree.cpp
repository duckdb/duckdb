#include "execution/window_segment_tree.hpp"

#include "common/types/constant_vector.hpp"
#include "common/vector_operations/vector_operations.hpp"

#include <cmath>

using namespace duckdb;
using namespace std;

WindowSegmentTree::WindowSegmentTree(AggregateFunction& aggregate, TypeId result_type, ChunkCollection *input)
	: aggregate(aggregate), state(aggregate.state_size(result_type)), statep(TypeId::POINTER, true, false), result_type(result_type),
	input_ref(input) {
	assert(aggregate.initialize);
	assert(aggregate.update);
	assert(aggregate.finalize);
	statep.count = STANDARD_VECTOR_SIZE;
	VectorOperations::Set(statep, Value::POINTER((index_t) state.data()));

	if (aggregate.combine && (input_ref->column_count() == 1)) {
		ConstructTree();
	} else {
		inputs = unique_ptr<Vector[]>(new Vector[input_ref->column_count()]);
	}
}

void WindowSegmentTree::AggregateInit() {
	aggregate.initialize(state.data(), result_type);
}

Value WindowSegmentTree::AggegateFinal() {
	ConstantVector statev(Value::POINTER((index_t) state.data()));

	Value r(result_type);
	ConstantVector result(r);
	result.SetNull(0, false);
	aggregate.finalize(statev, result);

	return result.GetValue(0);
}

void WindowSegmentTree::WindowSegmentValue(index_t l_idx, index_t begin, index_t end) {
	assert(begin <= end);
	if (begin == end) {
		return;
	}
	Vector s;
	s.Reference(statep);
	s.count = end - begin;
	Vector v;
	if (l_idx == 0) {
		auto &vec = input_ref->GetChunk(begin).data[0];
		v.Reference(vec);
		index_t start_in_vector = begin % STANDARD_VECTOR_SIZE;
		v.data = v.data + GetTypeIdSize(v.type) * start_in_vector;
		v.count = end - begin;
		v.nullmask <<= start_in_vector;
		assert(v.count + start_in_vector <=
			   vec.count); // if STANDARD_VECTOR_SIZE is not divisible by fanout this will trip
		assert(!v.sel_vector);
		v.Verify();
		aggregate.update(&v, 1, s);
	} else {
		assert(end - begin < STANDARD_VECTOR_SIZE);
		v.data = levels_flat_native.get() + state.size() * (begin + levels_flat_start[l_idx - 1]);
		v.count = end - begin;
		v.type = result_type;
		assert(!v.sel_vector);
		v.Verify();
		aggregate.combine(v, s);
	}
}

void WindowSegmentTree::ConstructTree() {
	assert(input_ref);
	assert(input_ref->column_count() == 1);

	// compute space required to store internal nodes of segment tree
	index_t internal_nodes = 0;
	index_t level_nodes = input_ref->count;
	do {
		level_nodes = (index_t)ceil((double)level_nodes / TREE_FANOUT);
		internal_nodes += level_nodes;
	} while (level_nodes > 1);
	levels_flat_native = unique_ptr<data_t[]>(new data_t[internal_nodes * state.size()]);
	levels_flat_start.push_back(0);

	index_t levels_flat_offset = 0;
	index_t level_current = 0;
	// level 0 is data itself
	index_t level_size;
	while ((level_size = (level_current == 0 ? input_ref->count
	                                         : levels_flat_offset - levels_flat_start[level_current - 1])) > 1) {
		for (index_t pos = 0; pos < level_size; pos += TREE_FANOUT) {
			AggregateInit();
			WindowSegmentValue(level_current, pos, min(level_size, pos + TREE_FANOUT));

			memcpy(levels_flat_native.get() + (levels_flat_offset * state.size()), state.data(), state.size());

			levels_flat_offset++;
		}

		levels_flat_start.push_back(levels_flat_offset);
		level_current++;
	}
}

Value WindowSegmentTree::Aggregate(index_t begin, index_t end) {
	assert(input_ref);
	const auto input_count = input_ref->column_count();
	if (!input_count) {
		return Value::Numeric(result_type, end - begin);
	}

	Vector s;
	s.Reference(statep);

	AggregateInit();

	auto &chunk = input_ref->GetChunk(begin);
	for (index_t i = 0; i < input_count; ++i) {
		auto &v = inputs[i];
		auto &vec = chunk.data[i];
		v.Reference(vec);
		index_t start_in_vector = begin % STANDARD_VECTOR_SIZE;
		v.data = v.data + GetTypeIdSize(v.type) * start_in_vector;
		v.count = end - begin;
		v.nullmask <<= start_in_vector;
		assert(!v.sel_vector);
		v.Verify();
	}
	s.count = end - begin;
	aggregate.update(&inputs[0], input_count, s);

	return AggegateFinal();
}

Value WindowSegmentTree::Compute(index_t begin, index_t end) {
	assert(input_ref);
	if (inputs) {
		return Aggregate(begin, end);
	}

	AggregateInit();
	for (index_t l_idx = 0; l_idx < levels_flat_start.size() + 1; l_idx++) {
		index_t parent_begin = begin / TREE_FANOUT;
		index_t parent_end = end / TREE_FANOUT;
		if (parent_begin == parent_end) {
			WindowSegmentValue(l_idx, begin, end);
			return AggegateFinal();
		}
		index_t group_begin = parent_begin * TREE_FANOUT;
		if (begin != group_begin) {
			WindowSegmentValue(l_idx, begin, group_begin + TREE_FANOUT);
			parent_begin++;
		}
		index_t group_end = parent_end * TREE_FANOUT;
		if (end != group_end) {
			WindowSegmentValue(l_idx, group_end, end);
		}
		begin = parent_begin;
		end = parent_end;
	}

	return AggegateFinal();
}
