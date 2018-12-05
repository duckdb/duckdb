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

#include <mutex>
#include <unordered_set>
#include <vector>

namespace duckdb {

class DataTable;
class StorageChunk;
class Transaction;

struct UniqueIndexNode {
	Tuple tuple;
	size_t row_identifier;

	UniqueIndexNode *parent;
	unique_ptr<UniqueIndexNode> left;
	unique_ptr<UniqueIndexNode> right;

	UniqueIndexNode(Tuple tuple, size_t row_identifier)
	    : tuple(std::move(tuple)), row_identifier(row_identifier), parent(nullptr) {
	}
};

//! The unique index is used to lookup whether or not multiple values have the
//! same value. It is used to efficiently enforce PRIMARY KEY, FOREIGN KEY and
//! UNIQUE constraints.
class UniqueIndex {
public:
	UniqueIndex(DataTable &table, vector<TypeId> types, vector<size_t> keys, bool allow_nulls);

	static std::string Append(Transaction &transaction, vector<unique_ptr<UniqueIndex>> &indexes, DataChunk &chunk,
	                          size_t row_identifier_start);

	static std::string Update(Transaction &transaction, StorageChunk *storage, vector<unique_ptr<UniqueIndex>> &indexes,
	                          vector<column_t> &column_ids, DataChunk &update_chunk, Vector &row_identifiers);

private:
	string AddEntries(Transaction &transaction, UniqueIndexNode *added_nodes[], Tuple tuples[], bool has_null[],
	                  Vector &row_identifiers, std::unordered_set<size_t> &ignored_identifiers);
	UniqueIndexNode *AddEntry(Transaction &transaction, Tuple tuple, size_t row_identifier,
	                          std::unordered_set<size_t> &ignored_identifiers);
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
	vector<size_t> keys;
	//! Lock on the index
	std::mutex index_lock;
	//! Whether or not NULL values are allowed by the constraint (false for
	//! PRIMARY KEY, true for UNIQUE)
	bool allow_nulls;

	//! Root node of the index
	unique_ptr<UniqueIndexNode> root;
};

} // namespace duckdb
