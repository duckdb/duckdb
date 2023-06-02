//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/statistics/node_statistics.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/serializer.hpp"

namespace duckdb {

class NodeStatistics {
public:
	NodeStatistics() : has_estimated_cardinality(false), has_max_cardinality(false) {
	}
	explicit NodeStatistics(idx_t estimated_cardinality)
	    : has_estimated_cardinality(true), estimated_cardinality(estimated_cardinality), has_max_cardinality(false) {
	}
	NodeStatistics(idx_t estimated_cardinality, idx_t max_cardinality)
	    : has_estimated_cardinality(true), estimated_cardinality(estimated_cardinality), has_max_cardinality(true),
	      max_cardinality(max_cardinality) {
	}
	void Serialize(Serializer &serializer) const {
		serializer.Write(has_estimated_cardinality);
		if (has_estimated_cardinality) {
			serializer.Write(estimated_cardinality);
			serializer.Write(has_max_cardinality);
			if (has_max_cardinality) {
				serializer.Write(max_cardinality);
			}
		} else {
			D_ASSERT(!has_max_cardinality);
		}
	}
	static unique_ptr<NodeStatistics> Deserialize(Deserializer &source) {
		bool has_estimated_cardinality = source.Read<bool>();
		if (!has_estimated_cardinality) {
			return make_uniq<NodeStatistics>();
		}
		idx_t estimated_cardinality = source.Read<idx_t>();
		bool has_max_cardinality = source.Read<bool>();
		if (!has_max_cardinality) {
			return make_uniq<NodeStatistics>(estimated_cardinality);
		}
		idx_t max_cardinality = source.Read<idx_t>();
		return make_uniq<NodeStatistics>(estimated_cardinality, max_cardinality);
	}

	//! Whether or not the node has an estimated cardinality specified
	bool has_estimated_cardinality;
	//! The estimated cardinality at the specified node
	idx_t estimated_cardinality;
	//! Whether or not the node has a maximum cardinality specified
	bool has_max_cardinality;
	//! The max possible cardinality at the specified node
	idx_t max_cardinality;
};

} // namespace duckdb
