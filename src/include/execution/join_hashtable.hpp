//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// execution/join_hashtable.hpp
//
// Author: Mark Raasveldt
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/internal_types.hpp"
#include "common/types/data_chunk.hpp"
#include "common/types/tuple.hpp"
#include "common/types/vector.hpp"

#include <mutex>

namespace duckdb {

//! JoinHashTable is a linear probing HT that is used for computing joins
/*!
   The JoinHashTable concatenates incoming chunks inside a linked list of
   data ptrs. The storage looks like this internally.
   [SERIALIZED ROW][NEXT POINTER]
   [SERIALIZED ROW][NEXT POINTER]
   There is a separate hash map of pointers that point into this table.
   This is what is used to resolve the hashes.
   [POINTER]
   [POINTER]
   [POINTER]
   The pointers are either NULL
*/
class JoinHashTable {
  public:
	//! Scan structure that can be used to resume scans, as a single probe can
	//! return 1024*N values (where N is the size of the HT). This is
	//! returned by the JoinHashTable::Scan function and can be used to resume a
	//! probe.
	struct ScanStructure {
		Vector pointers;
		Vector build_pointer_vector;
		sel_t sel_vector[STANDARD_VECTOR_SIZE];
		Tuple serialized_keys[STANDARD_VECTOR_SIZE];
		JoinHashTable &ht;

		ScanStructure(JoinHashTable &ht);
		//! Get the next batch of data from the scan structure
		void Next(DataChunk &left, DataChunk &result);

	  private:
		//! Next operator for the inner join
		void NextInnerJoin(DataChunk &left, DataChunk &result);
		//! Next operator for the semi join
		void NextSemiJoin(DataChunk &left, DataChunk &result);
		//! Next operator for the anti join
		void NextAntiJoin(DataChunk &left, DataChunk &result);
	};

  private:
	//! Nodes store the actual data of the tuples inside the HT as a linked list
	struct Node {
		size_t count;
		size_t capacity;
		std::unique_ptr<uint8_t[]> data;
		std::unique_ptr<Node> prev;

		Node(size_t tuple_size, size_t capacity)
		    : count(0), capacity(capacity) {
			data =
			    std::unique_ptr<uint8_t[]>(new uint8_t[tuple_size * capacity]);
			memset(data.get(), 0, tuple_size * capacity);
		}
	};

  public:
	JoinHashTable(std::vector<TypeId> key_types,
	              std::vector<TypeId> build_types, JoinType type,
	              size_t initial_capacity = 32768, bool parallel = false);
	//! Resize the HT to the specified size. Must be larger than the current
	//! size.
	void Resize(size_t size);
	//! Add the given data to the HT
	void Build(DataChunk &keys, DataChunk &input);
	//! Probe the HT with the given input chunk, resulting in the given result
	std::unique_ptr<ScanStructure> Probe(DataChunk &keys);

	//! The stringheap of the JoinHashTable
	StringHeap string_heap;

	size_t size() {
		return count;
	}

	//! Serializer for the keys
	TupleSerializer key_serializer;
	//! Serializer for the build side
	TupleSerializer build_serializer;
	//! The total tuple size
	size_t tuple_size;

  private:
	//! Insert the given set of locations into the HT with the given set of
	//! hashes. Caller should hold lock in parallel HT.
	void InsertHashes(Vector &hashes, uint8_t *key_locations[]);
	//! The types of the build side
	std::vector<TypeId> key_types;
	//! The types of the build side
	std::vector<TypeId> build_types;
	//! Size of key tuple
	size_t key_size;
	//! Size of build tuple
	size_t build_size;
	//! The size of an entry as stored in the HashTable
	size_t entry_size;
	//! The capacity of the HT. This can be increased using
	//! JoinHashTable::Resize
	size_t capacity;
	//! The amount of entries stored in the HT currently
	size_t count;
	//! The data of the HT
	std::unique_ptr<Node> head;
	//! The hash map of the HT
	std::unique_ptr<uint8_t *[]> hashed_pointers;
	//! Whether or not the HT has to support parallel build
	bool parallel = false;
	//! Mutex used for parallelism
	std::mutex parallel_lock;
	//! The join type of the HT
	JoinType join_type;

	//! Copying not allowed
	JoinHashTable(const JoinHashTable &) = delete;
};

} // namespace duckdb
