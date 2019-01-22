#include "execution/merge_join.hpp"
#include "parser/expression/comparison_expression.hpp"

using namespace duckdb;
using namespace std;

template <class MJ> static size_t merge_join(MergeInfo &l, MergeInfo &r) {
	assert(l.v.type == r.v.type);
	if (l.count == 0 || r.count == 0) {
		return 0;
	}
	switch (l.v.type) {
	case TypeId::TINYINT:
		return MJ::template Operation<int8_t>(l, r);
	case TypeId::SMALLINT:
		return MJ::template Operation<int16_t>(l, r);
	case TypeId::DATE:
	case TypeId::INTEGER:
		return MJ::template Operation<int32_t>(l, r);
	case TypeId::TIMESTAMP:
	case TypeId::BIGINT:
		return MJ::template Operation<int64_t>(l, r);
	case TypeId::DECIMAL:
		return MJ::template Operation<double>(l, r);
	case TypeId::VARCHAR:
		return MJ::template Operation<const char *>(l, r);
	default:
		throw NotImplementedException("Type not for merge join implemented!");
	}
}

size_t MergeJoinInner::Perform(MergeInfo &l, MergeInfo &r, ExpressionType comparison_type) {
	if (comparison_type == ExpressionType::COMPARE_EQUAL) {
		return merge_join<MergeJoinInner::Equality>(l, r);
	} else if (comparison_type == ExpressionType::COMPARE_LESSTHAN) {
		return merge_join<MergeJoinInner::LessThan>(l, r);
	} else if (comparison_type == ExpressionType::COMPARE_LESSTHANOREQUALTO) {
		return merge_join<MergeJoinInner::LessThanEquals>(l, r);
	} else if (comparison_type == ExpressionType::COMPARE_GREATERTHAN) {
		return merge_join<MergeJoinInner::GreaterThan>(l, r);
	} else if (comparison_type == ExpressionType::COMPARE_GREATERTHANOREQUALTO) {
		return merge_join<MergeJoinInner::GreaterThanEquals>(l, r);
	} else {
		throw Exception("Unimplemented comparison type for merge join!");
	}
}
