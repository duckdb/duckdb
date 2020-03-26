#include "duckdb/common/operator/comparison_operators.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/nested_loop_join.hpp"

using namespace duckdb;
using namespace std;

template <class T, class OP>
static void mark_join_templated(Vector &left, Vector &right, idx_t lcount, idx_t rcount, bool found_match[]) {
	VectorData left_data, right_data;
	left.Orrify(lcount, left_data);
	right.Orrify(rcount, right_data);

	auto ldata = (T *)left_data.data;
	auto rdata = (T *)right_data.data;
	for (idx_t i = 0; i < lcount; i++) {
		if (found_match[i]) {
			continue;
		}
		auto lidx = left_data.sel->get_index(i);
		if ((*left_data.nullmask)[lidx]) {
			continue;
		}
		for (idx_t j = 0; j < rcount; j++) {
			auto ridx = right_data.sel->get_index(j);
			if ((*right_data.nullmask)[ridx]) {
				continue;
			}
			if (OP::Operation(ldata[lidx], rdata[ridx])) {
				found_match[i] = true;
				break;
			}
		}
	}
}

template <class OP>
static void mark_join_operator(Vector &left, Vector &right, idx_t lcount, idx_t rcount, bool found_match[]) {
	switch (left.type) {
	case TypeId::BOOL:
	case TypeId::INT8:
		return mark_join_templated<int8_t, OP>(left, right, lcount, rcount, found_match);
	case TypeId::INT16:
		return mark_join_templated<int16_t, OP>(left, right, lcount, rcount, found_match);
	case TypeId::INT32:
		return mark_join_templated<int32_t, OP>(left, right, lcount, rcount, found_match);
	case TypeId::INT64:
		return mark_join_templated<int64_t, OP>(left, right, lcount, rcount, found_match);
	case TypeId::FLOAT:
		return mark_join_templated<float, OP>(left, right, lcount, rcount, found_match);
	case TypeId::DOUBLE:
		return mark_join_templated<double, OP>(left, right, lcount, rcount, found_match);
	case TypeId::VARCHAR:
		return mark_join_templated<string_t, OP>(left, right, lcount, rcount, found_match);
	default:
		throw NotImplementedException("Unimplemented type for join!");
	}
}

static void mark_join(Vector &left, Vector &right, idx_t lcount, idx_t rcount, bool found_match[],
                      ExpressionType comparison_type) {
	assert(left.type == right.type);
	switch (comparison_type) {
	case ExpressionType::COMPARE_EQUAL:
		return mark_join_operator<duckdb::Equals>(left, right, lcount, rcount, found_match);
	case ExpressionType::COMPARE_NOTEQUAL:
		return mark_join_operator<duckdb::NotEquals>(left, right, lcount, rcount, found_match);
	case ExpressionType::COMPARE_LESSTHAN:
		return mark_join_operator<duckdb::LessThan>(left, right, lcount, rcount, found_match);
	case ExpressionType::COMPARE_GREATERTHAN:
		return mark_join_operator<duckdb::GreaterThan>(left, right, lcount, rcount, found_match);
	case ExpressionType::COMPARE_LESSTHANOREQUALTO:
		return mark_join_operator<duckdb::LessThanEquals>(left, right, lcount, rcount, found_match);
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
		return mark_join_operator<duckdb::GreaterThanEquals>(left, right, lcount, rcount, found_match);
	default:
		throw NotImplementedException("Unimplemented comparison type for join!");
	}
}

void NestedLoopJoinMark::Perform(DataChunk &left, ChunkCollection &right, bool found_match[],
                                 vector<JoinCondition> &conditions) {
	// initialize a new temporary selection vector for the left chunk
	// loop over all chunks in the RHS
	for (idx_t chunk_idx = 0; chunk_idx < right.chunks.size(); chunk_idx++) {
		DataChunk &right_chunk = *right.chunks[chunk_idx];
		for (idx_t i = 0; i < conditions.size(); i++) {
			mark_join(left.data[i], right_chunk.data[i], left.size(), right_chunk.size(), found_match,
			          conditions[i].comparison);
		}
	}
}
