//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/hive_partitioning.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types/column/partitioned_column_data.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/optimizer/filter_combiner.hpp"
#include "duckdb/optimizer/statistics_propagator.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/table_filter.hpp"
#include "re2/re2.h"

#include <iostream>
#include <sstream>

namespace duckdb {

class HivePartitioning {
public:
	//! Parse a filename that follows the hive partitioning scheme
	DUCKDB_API static std::map<string, string> Parse(const string &filename);
	DUCKDB_API static std::map<string, string> Parse(const string &filename, duckdb_re2::RE2 &regex);
	//! Prunes a list of filenames based on a set of filters, can be used by TableFunctions in the
	//! pushdown_complex_filter function to skip files with filename-based filters. Also removes the filters that always
	//! evaluate to true.
	DUCKDB_API static void ApplyFiltersToFileList(ClientContext &context, vector<string> &files,
	                                              vector<unique_ptr<Expression>> &filters,
	                                              unordered_map<string, column_t> &column_map, LogicalGet &get,
	                                              bool hive_enabled, bool filename_enabled);

	//! Returns the compiled regex pattern to match hive partitions
	DUCKDB_API static const string REGEX_STRING;
};

struct HivePartitionKey {
	//! Columns by which we want to partition
	vector<Value> values;
	//! Precomputed hash of values
	hash_t hash;

	struct Hash {
		std::size_t operator()(const HivePartitionKey &k) const {
			return k.hash;
		}
	};

	struct Equality {
		bool operator()(const HivePartitionKey &a, const HivePartitionKey &b) const {
			if (a.values.size() != b.values.size()) {
				return false;
			}
			for (idx_t i = 0; i < a.values.size(); i++) {
				if (!Value::NotDistinctFrom(a.values[i], b.values[i])) {
					return false;
				}
			}
			return true;
		}
	};
};

//! Maps hive partitions to partition_ids
typedef unordered_map<HivePartitionKey, idx_t, HivePartitionKey::Hash, HivePartitionKey::Equality> hive_partition_map_t;

//! class shared between HivePartitionColumnData classes that synchronizes partition discovery between threads.
//! each HivePartitionedColumnData will hold a local copy of the key->partition map
class GlobalHivePartitionState {
public:
	mutex lock;
	hive_partition_map_t partition_map;
	//! Used for incremental updating local copies of the partition map;
	vector<hive_partition_map_t::const_iterator> partitions;
};

class HivePartitionedColumnData : public PartitionedColumnData {
public:
	HivePartitionedColumnData(ClientContext &context, vector<LogicalType> types, vector<idx_t> partition_by_cols,
	                          shared_ptr<GlobalHivePartitionState> global_state = nullptr)
	    : PartitionedColumnData(PartitionedColumnDataType::HIVE, context, std::move(types)),
	      global_state(std::move(global_state)), group_by_columns(std::move(partition_by_cols)),
	      hashes_v(LogicalType::HASH) {
		InitializeKeys();
	}
	HivePartitionedColumnData(const HivePartitionedColumnData &other);
	void ComputePartitionIndices(PartitionedColumnDataAppendState &state, DataChunk &input) override;

	//! Reverse lookup map to reconstruct keys from a partition id
	std::map<idx_t, const HivePartitionKey *> GetReverseMap();

protected:
	//! Create allocators for all currently registered partitions
	void GrowAllocators();
	//! Create append states for all currently registered partitions
	void GrowAppendState(PartitionedColumnDataAppendState &state);
	//! Create and initialize partitions for all currently registered partitions
	void GrowPartitions(PartitionedColumnDataAppendState &state);
	//! Register a newly discovered partition
	idx_t RegisterNewPartition(HivePartitionKey key, PartitionedColumnDataAppendState &state);
	//! Copy the newly added entries in the global_state.map to the local_partition_map (requires lock!)
	void SynchronizeLocalMap();

private:
	void InitializeKeys();

protected:
	//! Shared HivePartitionedColumnData should always have a global state to allow parallel key discovery
	shared_ptr<GlobalHivePartitionState> global_state;
	//! Thread-local copy of the partition map
	hive_partition_map_t local_partition_map;
	//! The columns that make up the key
	vector<idx_t> group_by_columns;
	//! Thread-local pre-allocated vector for hashes
	Vector hashes_v;
	//! Thread-local pre-allocated HivePartitionKeys
	vector<HivePartitionKey> keys;
};

} // namespace duckdb
