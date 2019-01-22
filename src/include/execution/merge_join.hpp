//===----------------------------------------------------------------------===//
//                         DuckDB
//
// execution/merge_join.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/common.hpp"
#include "common/types/vector.hpp"

namespace duckdb {

struct MergeInfo {
	Vector &v;
	size_t count;
	sel_t *sel_vector;
	size_t &pos;
	sel_t result[STANDARD_VECTOR_SIZE];

	MergeInfo(Vector &v, size_t count, sel_t *sel_vector, size_t &pos)
	    : v(v), count(count), sel_vector(sel_vector), pos(pos) {
	}
};

struct MergeJoinInner {
	struct Equality {
		template <class T>
		static size_t Operation(MergeInfo &l, MergeInfo &r);
	};
	struct LessThan {
		template <class T>
		static size_t Operation(MergeInfo &l, MergeInfo &r);
	};
	struct LessThanEquals {
		template <class T>
		static size_t Operation(MergeInfo &l, MergeInfo &r);
	};
	struct GreaterThan {
		template <class T>
		static size_t Operation(MergeInfo &l, MergeInfo &r) {
			return LessThan::Operation<T>(r, l);
		}
	};
	struct GreaterThanEquals {
		template <class T>
		static size_t Operation(MergeInfo &l, MergeInfo &r) {
			return LessThanEquals::Operation<T>(r, l);
		}
	};
};

size_t MergeJoin(MergeInfo &l, MergeInfo &r, ExpressionType comparison_type, JoinType type);

#define INSTANTIATE_MERGEJOIN_TEMPLATES(MJCLASS, OPNAME)                             \
template size_t MJCLASS::OPNAME::Operation<int8_t>(MergeInfo &l, MergeInfo &r);      \
template size_t MJCLASS::OPNAME::Operation<int16_t>(MergeInfo &l, MergeInfo &r);     \
template size_t MJCLASS::OPNAME::Operation<int32_t>(MergeInfo &l, MergeInfo &r);     \
template size_t MJCLASS::OPNAME::Operation<int64_t>(MergeInfo &l, MergeInfo &r);     \
template size_t MJCLASS::OPNAME::Operation<double>(MergeInfo &l, MergeInfo &r);      \
template size_t MJCLASS::OPNAME::Operation<const char*>(MergeInfo &l, MergeInfo &r);

}
