#include "duckdb/execution/merge_join.hpp"

#include "duckdb/parser/expression/comparison_expression.hpp"

using namespace duckdb;
using namespace std;

template <class MJ, class L_ARG, class R_ARG> static idx_t merge_join(L_ARG &l, R_ARG &r) {
	switch (l.type) {
	case TypeId::BOOL:
	case TypeId::INT8:
		return MJ::template Operation<int8_t>(l, r);
	case TypeId::INT16:
		return MJ::template Operation<int16_t>(l, r);
	case TypeId::INT32:
		return MJ::template Operation<int32_t>(l, r);
	case TypeId::INT64:
		return MJ::template Operation<int64_t>(l, r);
	case TypeId::FLOAT:
		return MJ::template Operation<float>(l, r);
	case TypeId::DOUBLE:
		return MJ::template Operation<double>(l, r);
	case TypeId::VARCHAR:
		return MJ::template Operation<string_t>(l, r);
	default:
		throw NotImplementedException("Type not implemented for merge join!");
	}
}

template <class T, class L_ARG, class R_ARG>
static idx_t perform_merge_join(L_ARG &l, R_ARG &r, ExpressionType comparison_type) {
	switch (comparison_type) {
	case ExpressionType::COMPARE_EQUAL:
		return merge_join<typename T::Equality, L_ARG, R_ARG>(l, r);
	case ExpressionType::COMPARE_LESSTHAN:
		return merge_join<typename T::LessThan, L_ARG, R_ARG>(l, r);
	case ExpressionType::COMPARE_LESSTHANOREQUALTO:
		return merge_join<typename T::LessThanEquals, L_ARG, R_ARG>(l, r);
	case ExpressionType::COMPARE_GREATERTHAN:
		return merge_join<typename T::GreaterThan, L_ARG, R_ARG>(l, r);
	default:
		// "Unimplemented comparison type for merge join!"
		assert(comparison_type == ExpressionType::COMPARE_GREATERTHANOREQUALTO);
		return merge_join<typename T::GreaterThanEquals, L_ARG, R_ARG>(l, r);
	}
}

idx_t MergeJoinInner::Perform(MergeInfo &l, MergeInfo &r, ExpressionType comparison_type) {
	assert(l.info_type == MergeInfoType::SCALAR_MERGE_INFO && r.info_type == MergeInfoType::SCALAR_MERGE_INFO);
	auto &left = (ScalarMergeInfo &)l;
	auto &right = (ScalarMergeInfo &)r;
	assert(left.type == right.type);
	if (left.order.count == 0 || right.order.count == 0) {
		return 0;
	}
	return perform_merge_join<MergeJoinInner, ScalarMergeInfo, ScalarMergeInfo>(left, right, comparison_type);
}

idx_t MergeJoinMark::Perform(MergeInfo &l, MergeInfo &r, ExpressionType comparison_type) {
	assert(l.info_type == MergeInfoType::SCALAR_MERGE_INFO && r.info_type == MergeInfoType::CHUNK_MERGE_INFO);
	auto &left = (ScalarMergeInfo &)l;
	auto &right = (ChunkMergeInfo &)r;
	assert(left.type == right.type);
	if (left.order.count == 0 || right.data_chunks.count == 0) {
		return 0;
	}
	return perform_merge_join<MergeJoinMark, ScalarMergeInfo, ChunkMergeInfo>(left, right, comparison_type);
}
