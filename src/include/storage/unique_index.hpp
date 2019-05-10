//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/unique_index.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/types/data_chunk.hpp"
#include "common/types/tuple.hpp"
#include "common/unordered_set.hpp"

#include <mutex>

namespace duckdb {

class DataTable;
class StorageChunk;
class Transaction;

struct UniqueIndexNode {
	Tuple tuple;
	uint64_t row_identifier;

	UniqueIndexNode *parent;
	unique_ptr<UniqueIndexNode> left;
	unique_ptr<UniqueIndexNode> right;

	UniqueIndexNode(Tuple tuple, uint64_t row_identifier)
	    : tuple(std::move(tuple)), row_identifier(row_identifier), parent(nullptr) {
	}
};

//! The unique index is used to lookup whether or not multiple values have the
//! same value. It is used to efficiently enforce PRIMARY KEY, FOREIGN KEY and
//! UNIQUE constraints.
class UniqueIndex {
	//! This structure contains a set of entries that were added to a unique index. In case of a conflict these might
	//! have to be removed again.
	struct UniqueIndexAddedEntries {
		UniqueIndex &index;
		UniqueIndexNode *nodes[STANDARD_VECTOR_SIZE];
		uint64_t count;
		unique_ptr<UniqueIndexAddedEntries> next;

		UniqueIndexAddedEntries(UniqueIndex &index) : index(index), count(0) {
		}
		~UniqueIndexAddedEntries() {
			for (uint64_t j = count; j > 0; j--) {
				if (nodes[j - 1]) {
					index.RemoveEntry(nodes[j - 1]);
				}
			}
		}
		void AddEntry(UniqueIndexNode *entry) {
			nodes[count++] = entry;
		}
		void Flush() {
			count = 0;
			if (next) {
				next->Flush();
			}
		}
	};

public:
	UniqueIndex(DataTable &table, vector<TypeId> types, vector<uint64_t> keys, bool allow_nulls);

	static void Append(Transaction &transaction, vector<unique_ptr<UniqueIndex>> &indexes, DataChunk &chunk,
	                   uint64_t row_identifier_start);

	static void Update(Transaction &transaction, StorageChunk *storage, vector<unique_ptr<UniqueIndex>> &indexes,
	                   vector<column_t> &column_ids, DataChunk &update_chunk, Vector &row_identifiers);

private:
	void AddEntries(Transaction &transaction, UniqueIndexAddedEntries &nodes, Tuple tuples[], bool has_null[],
	                Vector &row_identifiers, unordered_set<uint64_t> &ignored_identifiers);
	UniqueIndexNode *AddEntry(Transaction &transaction, Tuple tuple, uint64_t row_identifier,
	                          unordered_set<uint64_t> &ignored_identifiers);
	void RemoveEntry(UniqueIndexNode *entry);

	//! The tuple serializer
	TupleSerializer serializer;
	//! The comparer used to compare tuples stored in the index to tuples of the
	//! base table
	TupleComparer comparer;
	//! A reference to the table this Unique constraint relates to
	DataTable &table;
	//! Types of the UniqueIndex
	vector<TypeId> types;
	//! The set of keys that must be collectively unique
	vector<uint64_t> keys;
	//! Lock on the index
	std::mutex index_lock;
	//! Whether or not NULL values are allowed by the constraint (false for
	//! PRIMARY KEY, true for UNIQUE)
	bool allow_nulls;

	//! Root node of the index
	unique_ptr<UniqueIndexNode> root;
};

} // namespace duckdb
