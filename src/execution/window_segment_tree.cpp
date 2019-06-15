#include "execution/window_segment_tree.hpp"

#include "common/types/constant_vector.hpp"
#include "common/vector_operations/vector_operations.hpp"

#include <cmath>

using namespace duckdb;
using namespace std;

void WindowSegmentTree::AggregateInit() {
	switch (window_type) {
	case ExpressionType::WINDOW_SUM:
	case ExpressionType::WINDOW_AVG:
		aggregate = Value::Numeric(payload_type, 0);
		break;
	case ExpressionType::WINDOW_MIN:
		aggregate = Value::MaximumValue(payload_type);
		break;
	case ExpressionType::WINDOW_MAX:
		aggregate = Value::MinimumValue(payload_type);
		break;
	default:
		throw NotImplementedException("Window Type");
	}
	assert(aggregate.type == payload_type);
	n_aggregated = 0;
}

Value WindowSegmentTree::AggegateFinal() {
	if (n_aggregated == 0) {
		return Value(payload_type);
	}
	switch (window_type) {
	case ExpressionType::WINDOW_AVG:
		return aggregate / Value::Numeric(payload_type, n_aggregated);
	default:
		return aggregate;
	}

	return aggregate;
}

void WindowSegmentTree::WindowSegmentValue(index_t l_idx, index_t begin, index_t end) {
	assert(begin <= end);
	if (begin == end) {
		return;
	}
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
	} else {
		assert(end - begin < STANDARD_VECTOR_SIZE);
		v.data = levels_flat_native.get() + GetTypeIdSize(payload_type) * (begin + levels_flat_start[l_idx - 1]);
		v.count = end - begin;
		v.type = payload_type;
	}

	assert(!v.sel_vector);
	v.Verify();

	switch (window_type) {
	case ExpressionType::WINDOW_SUM:
	case ExpressionType::WINDOW_AVG:
		aggregate = aggregate + VectorOperations::Sum(v);
		break;
	case ExpressionType::WINDOW_MIN: {
		auto val = VectorOperations::Min(v);
		aggregate = aggregate > val ? val : aggregate;
		break;
	}
	case ExpressionType::WINDOW_MAX: {
		auto val = VectorOperations::Max(v);
		aggregate = aggregate < val ? val : aggregate;
		break;
	}
	default:
		throw NotImplementedException("Window Type");
	}

	n_aggregated += end - begin;
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
	levels_flat_native = unique_ptr<data_t[]>(new data_t[internal_nodes * GetTypeIdSize(payload_type)]);
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

			ConstantVector res_vec(AggegateFinal());
			assert(res_vec.type == payload_type);
			ConstantVector ptr_vec(Value::POINTER(
			    (index_t)(levels_flat_native.get() + (levels_flat_offset * GetTypeIdSize(payload_type)))));
			VectorOperations::Scatter::Set(res_vec, ptr_vec);

			levels_flat_offset++;
		}

		levels_flat_start.push_back(levels_flat_offset);
		level_current++;
	}
}

Value WindowSegmentTree::Compute(index_t begin, index_t end) {
	assert(input_ref);
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
