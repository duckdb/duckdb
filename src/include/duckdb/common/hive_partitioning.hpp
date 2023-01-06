//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/hive_partitioning.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types/partitioned_column_data.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/optimizer/filter_combiner.hpp"
#include "duckdb/optimizer/statistics_propagator.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/table_filter.hpp"
#include "re2/re2.h"

namespace duckdb {

class HivePartitioning {
public:
	//! Parse a filename that follows the hive partitioning scheme
	DUCKDB_API static std::map<string, string> Parse(string &filename);
	DUCKDB_API static std::map<string, string> Parse(string &filename, duckdb_re2::RE2 &regex);
	//! Prunes a list of filenames based on a set of filters, can be used by TableFunctions in the
	//! pushdown_complex_filter function to skip files with filename-based filters. Also removes the filters that always
	//! evaluate to true.
	DUCKDB_API static void ApplyFiltersToFileList(ClientContext &context, vector<string> &files,
	                                              vector<unique_ptr<Expression>> &filters,
	                                              unordered_map<string, column_t> &column_map, idx_t table_index,
	                                              bool hive_enabled, bool filename_enabled);

	//! Returns the compiled regex pattern to match hive partitions
	DUCKDB_API static const string REGEX_STRING;
};

struct HivePartitionKey {
	//! Columns by which we want to partition, last column containing the combined hash
	DataChunk& partition_columns;
	//! offset of this specific key
	idx_t offset;
};

struct HivePartitionKeyHash {
	std::size_t operator()(const HivePartitionKey &k) const {
		// Essentially a NOP, because the hash is already available here
		return k.partition_columns.data[k.partition_columns.ColumnCount()-1].GetData()[k.offset];
	}
};

// TODO: this is probably slow and wrong in many cases, simple INT types should work
struct HivePartitionKeyEquality {
	bool operator()(const HivePartitionKey &a, const HivePartitionKey &b) const {
		for(idx_t i = 0; i < a.partition_columns.ColumnCount()-1; i++){
			if (a.partition_columns.data[i].GetValue(a.offset) != b.partition_columns.data[i].GetValue(b.offset)) {
				return false;
			}
		}
		return true;
	}
};

// Maps hive partitions to partition_ids
typedef unordered_map<HivePartitionKey, idx_t, HivePartitionKeyHash, HivePartitionKeyEquality> hive_partition_map_t;

//! Class containing the mapping from hive partition to partition id
class HivePartitionMap {
public:
	hive_partition_map_t map;
	//! partition columns for each key
	DataChunk key_values;

	idx_t Lookup(HivePartitionKey& key) {
		auto lookup = map.find(key);
		if (lookup != map.end()) {
			return lookup->second;
		}
		return DConstants::INVALID_INDEX;
	}

	// Assumes key is not present!
	void Insert(HivePartitionKey& key) {
		SelectionVector updated(key.offset, 1);
		key_values.Append(key.partition_columns, true, &updated, 1);
		map[{key_values, map.size()}] = map.size();
	}

	void InsertIfNew(HivePartitionKey& key) {
		auto partition_id = Lookup(key);
		if (partition_id == DConstants::INVALID_INDEX) {
			Insert(key);
		}
	}

	void InsertBulk(DataChunk& keys) {
		// TODO
	}

	//! Update this HivePartitionMap with the keys that are present in the other maps
	//! Note: caller should acquire appropriate locks!
	void Synchronise(HivePartitionMap& other) {
		idx_t start_from = map.size();
		idx_t to_copy = other.map.size() - start_from;

		// TODO: this can be improved. We now need a local copy since DataChunk can resize, however we could make a shared
		//       global hivePartitionMapState which has a linked list to guarantee pointer stability
		SelectionVector updated(start_from, to_copy);
		key_values.Append(other.key_values, true, &updated, to_copy);

		// insert new keys into local map
		for (idx_t i = start_from; i < other.map.size(); i++) {
			map[{key_values, i}] = i;
		}
	}
 };

//! class shared between HivePartitionColumnData classes that synchronizes lazy partition discovery.
//! each HivePartitionedColumnData will hold a local copy of the key->partition map
class GlobalHivePartitionState {
public:
	mutex lock;
	HivePartitionMap partition_map;
};

class HivePartitionedColumnData : public PartitionedColumnData {
public:
	HivePartitionedColumnData(ClientContext &context, vector<LogicalType> types)
	    : PartitionedColumnData(PartitionedColumnDataType::HIVE, context, types) {

	}

	void ComputePartitionIndices(PartitionedColumnDataAppendState &state, DataChunk &input) override {
		throw NotImplementedException("TODO");

		// TODO:
		// generate Datachunk for key columns
		// add hash column to datachunk
		// Generate PartitionedColumnDataAppendState
	}

	idx_t RegisterNewPartition(HivePartitionKey key) {
		unique_lock<mutex> lck(global_state->lock);
		// Insert into global
		global_state->partition_map.InsertIfNew(key);
		// Synchronise global map into local, may contain changes from other threads too
		local_partition_map.Synchronise(global_state->partition_map);
	}

protected:
	//! Shared HivePartitionedColumnData should always have a global state to keep partition map sync
	shared_ptr<GlobalHivePartitionState> global_state;
	HivePartitionMap local_partition_map;
};

} // namespace duckdb
