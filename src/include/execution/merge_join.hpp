//===----------------------------------------------------------------------===//
//                         DuckDB
//
// execution/merge_join.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/common.hpp"
#include "common/enums/expression_type.hpp"
#include "common/types/chunk_collection.hpp"
#include "common/types/vector.hpp"

namespace duckdb {

struct MergeOrder {
	sel_t order[STANDARD_VECTOR_SIZE];
	index_t count;
};

enum MergeInfoType : uint8_t { SCALAR_MERGE_INFO = 1, CHUNK_MERGE_INFO = 2 };

struct MergeInfo {
	MergeInfo(MergeInfoType info_type, TypeId type) : info_type(info_type), type(type) {
	}
	MergeInfoType info_type;
	TypeId type;
};

struct ScalarMergeInfo : public MergeInfo {
	Vector &v;
	index_t count;
	sel_t *sel_vector;
	index_t &pos;
	sel_t result[STANDARD_VECTOR_SIZE];

	ScalarMergeInfo(Vector &v, index_t count, sel_t *sel_vector, index_t &pos)
	    : MergeInfo(MergeInfoType::SCALAR_MERGE_INFO, v.type), v(v), count(count), sel_vector(sel_vector), pos(pos) {
	}
};

struct ChunkMergeInfo : public MergeInfo {
	ChunkCollection &data_chunks;
	vector<MergeOrder> &order_info;
	bool found_match[STANDARD_VECTOR_SIZE];

	ChunkMergeInfo(ChunkCollection &data_chunks, vector<MergeOrder> &order_info)
	    : MergeInfo(MergeInfoType::CHUNK_MERGE_INFO, data_chunks.types[0]), data_chunks(data_chunks),
	      order_info(order_info) {
		memset(found_match, 0, sizeof(found_match));
	}
};

struct MergeJoinInner {
	struct Equality {
		template <class T> static index_t Operation(ScalarMergeInfo &l, ScalarMergeInfo &r);
	};
	struct LessThan {
		template <class T> static index_t Operation(ScalarMergeInfo &l, ScalarMergeInfo &r);
	};
	struct LessThanEquals {
		template <class T> static index_t Operation(ScalarMergeInfo &l, ScalarMergeInfo &r);
	};
	struct GreaterThan {
		template <class T> static index_t Operation(ScalarMergeInfo &l, ScalarMergeInfo &r) {
			return LessThan::Operation<T>(r, l);
		}
	};
	struct GreaterThanEquals {
		template <class T> static index_t Operation(ScalarMergeInfo &l, ScalarMergeInfo &r) {
			return LessThanEquals::Operation<T>(r, l);
		}
	};

	static index_t Perform(MergeInfo &l, MergeInfo &r, ExpressionType comparison_type);
};

struct MergeJoinMark {
	struct Equality {
		template <class T> static index_t Operation(ScalarMergeInfo &l, ChunkMergeInfo &r);
	};
	struct LessThan {
		template <class T> static index_t Operation(ScalarMergeInfo &l, ChunkMergeInfo &r);
	};
	struct LessThanEquals {
		template <class T> static index_t Operation(ScalarMergeInfo &l, ChunkMergeInfo &r);
	};
	struct GreaterThan {
		template <class T> static index_t Operation(ScalarMergeInfo &l, ChunkMergeInfo &r);
	};
	struct GreaterThanEquals {
		template <class T> static index_t Operation(ScalarMergeInfo &l, ChunkMergeInfo &r);
	};

	static index_t Perform(MergeInfo &l, MergeInfo &r, ExpressionType comparison);
};

#define INSTANTIATE_MERGEJOIN_TEMPLATES(MJCLASS, OPNAME, L, R)                                                         \
	template index_t MJCLASS::OPNAME::Operation<int8_t>(L & l, R & r);                                                 \
	template index_t MJCLASS::OPNAME::Operation<int16_t>(L & l, R & r);                                                \
	template index_t MJCLASS::OPNAME::Operation<int32_t>(L & l, R & r);                                                \
	template index_t MJCLASS::OPNAME::Operation<int64_t>(L & l, R & r);                                                \
	template index_t MJCLASS::OPNAME::Operation<double>(L & l, R & r);                                                 \
	template index_t MJCLASS::OPNAME::Operation<const char *>(L & l, R & r);

} // namespace duckdb
