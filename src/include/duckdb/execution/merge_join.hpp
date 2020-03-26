//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/merge_join.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/enums/expression_type.hpp"
#include "duckdb/common/types/chunk_collection.hpp"
#include "duckdb/common/types/vector.hpp"

namespace duckdb {

struct MergeOrder {
	SelectionVector order;
	idx_t count;
	VectorData vdata;
};

enum MergeInfoType : uint8_t { SCALAR_MERGE_INFO = 1, CHUNK_MERGE_INFO = 2 };

struct MergeInfo {
	MergeInfo(MergeInfoType info_type, TypeId type) : info_type(info_type), type(type) {
	}
	MergeInfoType info_type;
	TypeId type;
};

struct ScalarMergeInfo : public MergeInfo {
	MergeOrder &order;
	idx_t &pos;
	SelectionVector result;

	ScalarMergeInfo(MergeOrder &order, TypeId type, idx_t &pos)
	    : MergeInfo(MergeInfoType::SCALAR_MERGE_INFO, type), order(order), pos(pos), result(STANDARD_VECTOR_SIZE) {
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
		template <class T> static idx_t Operation(ScalarMergeInfo &l, ScalarMergeInfo &r);
	};
	struct LessThan {
		template <class T> static idx_t Operation(ScalarMergeInfo &l, ScalarMergeInfo &r);
	};
	struct LessThanEquals {
		template <class T> static idx_t Operation(ScalarMergeInfo &l, ScalarMergeInfo &r);
	};
	struct GreaterThan {
		template <class T> static idx_t Operation(ScalarMergeInfo &l, ScalarMergeInfo &r) {
			return LessThan::Operation<T>(r, l);
		}
	};
	struct GreaterThanEquals {
		template <class T> static idx_t Operation(ScalarMergeInfo &l, ScalarMergeInfo &r) {
			return LessThanEquals::Operation<T>(r, l);
		}
	};

	static idx_t Perform(MergeInfo &l, MergeInfo &r, ExpressionType comparison_type);
};

struct MergeJoinMark {
	struct Equality {
		template <class T> static idx_t Operation(ScalarMergeInfo &l, ChunkMergeInfo &r);
	};
	struct LessThan {
		template <class T> static idx_t Operation(ScalarMergeInfo &l, ChunkMergeInfo &r);
	};
	struct LessThanEquals {
		template <class T> static idx_t Operation(ScalarMergeInfo &l, ChunkMergeInfo &r);
	};
	struct GreaterThan {
		template <class T> static idx_t Operation(ScalarMergeInfo &l, ChunkMergeInfo &r);
	};
	struct GreaterThanEquals {
		template <class T> static idx_t Operation(ScalarMergeInfo &l, ChunkMergeInfo &r);
	};

	static idx_t Perform(MergeInfo &l, MergeInfo &r, ExpressionType comparison);
};

#define INSTANTIATE_MERGEJOIN_TEMPLATES(MJCLASS, OPNAME, L, R)                                                         \
	template idx_t MJCLASS::OPNAME::Operation<int8_t>(L & l, R & r);                                                   \
	template idx_t MJCLASS::OPNAME::Operation<int16_t>(L & l, R & r);                                                  \
	template idx_t MJCLASS::OPNAME::Operation<int32_t>(L & l, R & r);                                                  \
	template idx_t MJCLASS::OPNAME::Operation<int64_t>(L & l, R & r);                                                  \
	template idx_t MJCLASS::OPNAME::Operation<float>(L & l, R & r);                                                    \
	template idx_t MJCLASS::OPNAME::Operation<double>(L & l, R & r);                                                   \
	template idx_t MJCLASS::OPNAME::Operation<string_t>(L & l, R & r);

} // namespace duckdb
