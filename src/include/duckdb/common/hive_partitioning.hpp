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

#include <sstream>
#include <iostream>

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


static void HashDataChunk2(DataChunk& chunk, Vector &result, vector<idx_t> column_ids) {
	D_ASSERT(result.GetType().id() == LogicalType::HASH);
	D_ASSERT(column_ids.size() > 0);

	VectorOperations::Hash(chunk.data[column_ids[0]], result, chunk.size());
	for (idx_t i = 1; i < column_ids.size(); i++) {
		VectorOperations::CombineHash(result, chunk.data[column_ids[i]], chunk.size());
	}
}

// The idea
struct HivePartitionKey {
	//! Columns by which we want to partition
	vector<Value> values;
	//! Hash of this key
	hash_t hash;

	string ToString() {
		std::stringstream ss;

		ss << "[";
		for (const auto& val: values) {
			ss << val.ToString();
			ss << ", ";
		}
		ss << "] with hash: " << hash;

		return ss.str();
	}
};

struct HivePartitionKeyHash {
	std::size_t operator()(const HivePartitionKey &k) const {
		return k.hash;
	}
};

struct HivePartitionKeyEquality {
	bool operator()(const HivePartitionKey &a, const HivePartitionKey &b) const {
		return a.values == b.values;
	}
};

// Maps hive partitions to partition_ids
typedef unordered_map<HivePartitionKey, idx_t, HivePartitionKeyHash, HivePartitionKeyEquality> hive_partition_map_t;

//! Class containing the mapping from hive partition to partition id
class HivePartitionMap {
public:
	hive_partition_map_t map;

	idx_t Lookup(HivePartitionKey &key) {
		auto lookup = map.find(key);
		if (lookup != map.end()) {
			return lookup->second;
		}
		return DConstants::INVALID_INDEX;
	}

	// Assumes key is not present!
	idx_t Insert(HivePartitionKey key) {
		auto new_idx = map.size();
		map.insert({std::move(key), new_idx});
		return new_idx;
	}

	idx_t InsertIfNew(HivePartitionKey key) {
		auto partition_id = Lookup(key);
		if (partition_id == DConstants::INVALID_INDEX) {
			return Insert(std::move(key));
		} else {
			return partition_id;
		}
	}

	void InsertBulk(DataChunk &keys) {
		// TODO
	}

	std::map<idx_t, const HivePartitionKey*> GetReverseMap() {
		std::map<idx_t, const HivePartitionKey*> ret;
		for (const auto& pair : map) {
			ret[pair.second] = &(pair.first);
		}
		return ret;
	}

	//! Update this HivePartitionMap with the keys that are present in the other maps
	//! Note: caller should acquire appropriate locks!
	void Synchronise(HivePartitionMap &other) {
		//		idx_t start_from = map.size();
		//		idx_t to_copy = other.map.size() - start_from;
		//
		// Todo: incremental instead of copy all over
		map = other.map;
	}
};

//! class shared between HivePartitionColumnData classes that synchronizes partition discovery between threads.
//! each HivePartitionedColumnData will hold a local copy of the key->partition map
class GlobalHivePartitionState {
public:
	mutex lock;
	HivePartitionMap partition_map;
};

class HivePartitionedColumnData : public PartitionedColumnData {
public:
	HivePartitionedColumnData(ClientContext &context, vector<LogicalType> types, vector<idx_t> partition_by_cols,
	                          shared_ptr<GlobalHivePartitionState> global_state = nullptr)
	    : PartitionedColumnData(PartitionedColumnDataType::HIVE, context, std::move(types)),
	      global_state(std::move(global_state)),group_by_columns(partition_by_cols) {
	}
	HivePartitionedColumnData(const HivePartitionedColumnData &other) : PartitionedColumnData(other) {

		// Synchronize to ensure consistency of partition map
		if (other.global_state) {
			global_state = other.global_state;
			unique_lock<mutex> lck(global_state->lock);
			local_partition_map.Synchronise(global_state->partition_map);
		}
	}

	void ComputePartitionIndices(PartitionedColumnDataAppendState &state, DataChunk &input) override {

		Vector hashes(LogicalType::HASH, input.size());
		input.Hash(hashes);
		HashDataChunk2(input, hashes, group_by_columns);

		for (idx_t i = 0; i < input.size(); i++) {
			HivePartitionKey key;
			key.hash = FlatVector::GetData<hash_t>(hashes)[i];
			for (auto &col : group_by_columns) {
				key.values.emplace_back(input.GetValue(col, i));
			}

			idx_t partition_id = local_partition_map.Lookup(key);
			const auto partition_indices = FlatVector::GetData<idx_t>(state.partition_indices);
			if (partition_id == DConstants::INVALID_INDEX) {
				idx_t new_partition_id = RegisterNewPartition(key,state);
				partition_indices[i] = new_partition_id;
			} else {
				partition_indices[i] = partition_id;
			}
		}
	}

	//! Create allocators for all currently registered partitions (requires lock!)
	void GrowAllocators() {
		idx_t current_allocator_size = allocators->allocators.size();
		idx_t required_allocators = local_partition_map.map.size();

		allocators->allocators.reserve(current_allocator_size);
		for (idx_t i = current_allocator_size; i < required_allocators; i++) {
			CreateAllocator();
		}

		D_ASSERT(allocators->allocators.size() == local_partition_map.map.size());
	}

	//! Create append states for all currently registered partitions (requires lock!)
	void GrowAppendState(PartitionedColumnDataAppendState &state) {
		idx_t current_append_state_size = state.partition_append_states.size();
		idx_t required_append_state_size = local_partition_map.map.size();

		for (idx_t i = current_append_state_size; i < required_append_state_size; i++) {
			state.partition_append_states.emplace_back(make_unique<ColumnDataAppendState>());
			state.partition_buffers.emplace_back(CreatePartitionBuffer());
		}

	}

	void GrowPartitions(PartitionedColumnDataAppendState &state) {
		idx_t current_partitions = partitions.size();
		idx_t required_partitions = local_partition_map.map.size();

		D_ASSERT(allocators->allocators.size() == required_partitions);

		for (idx_t i = current_partitions; i < required_partitions; i++) {
			partitions.emplace_back(CreatePartitionCollection(i));
			partitions[i]->InitializeAppend(*state.partition_append_states[i]);
		}
		D_ASSERT(partitions.size() == local_partition_map.map.size());
	}

	idx_t RegisterNewPartition(HivePartitionKey key, PartitionedColumnDataAppendState &state) {

		if (global_state) {
			// We need to lock both the GlobalHivePartitionState and the allocators while adding a partition
			unique_lock<mutex> lck_gstate(global_state->lock);
			unique_lock<mutex> lck_alloc(allocators->lock);

			// Insert into global map
			idx_t partition_id = global_state->partition_map.InsertIfNew(std::move(key));
			// Synchronise global map into local, may contain changes from other threads too
			local_partition_map.Synchronise(global_state->partition_map);

			// Grow all the things!
			GrowAllocators();
			GrowAppendState(state);
			GrowPartitions(state);

			return partition_id;
		} else {
			return local_partition_map.Insert(std::move(key));
		}
	}

	std::map<idx_t, const HivePartitionKey*> GetReverseMap() {
		return local_partition_map.GetReverseMap();
	}

protected:
	//! Shared HivePartitionedColumnData should always have a global state to keep partition map sync
	shared_ptr<GlobalHivePartitionState> global_state;
	HivePartitionMap local_partition_map;
	vector<idx_t> group_by_columns;
};

} // namespace duckdb
