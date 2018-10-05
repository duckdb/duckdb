//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// storage/unique_index.hpp
//
// Author: Mark Raasveldt
//
//===----------------------------------------------------------------------===//

#pragma once

#include <mutex>
#include <vector>

#include "common/types/data_chunk.hpp"

namespace duckdb {

class DataTable;
class Transaction;

struct UniqueIndexNode {
	size_t size;
	size_t row_identifier;
	std::unique_ptr<uint8_t[]> key_data;

	UniqueIndexNode *parent;
	std::unique_ptr<UniqueIndexNode> left;
	std::unique_ptr<UniqueIndexNode> right;

	UniqueIndexNode(size_t size, std::unique_ptr<uint8_t[]> key_data,
	                size_t row_identifier)
	    : size(size), row_identifier(row_identifier), key_data(move(key_data)),
	      parent(nullptr) {}
};

//! The unique index is used to lookup whether or not multiple values have the
//! same value. It is used to efficiently enforce PRIMARY KEY, FOREIGN KEY and
//! UNIQUE constraints.
class UniqueIndex {
  public:
	UniqueIndex(DataTable &table, std::vector<TypeId> types,
	            std::vector<size_t> keys, bool allow_nulls);

	static std::string
	Append(Transaction &transaction,
	       std::vector<std::unique_ptr<UniqueIndex>> &indexes, DataChunk &chunk,
	       size_t row_identifier_start);

	// void Delete(DataChunk &chunk);
	// void Update(DataChunk &chunk);

  private:
	UniqueIndexNode *AddEntry(Transaction &transaction, size_t size,
	                          std::unique_ptr<uint8_t[]> data,
	                          size_t row_identifier);
	void RemoveEntry(UniqueIndexNode *entry);

	DataTable &table;
	//! Types of the UniqueIndex
	std::vector<TypeId> types;
	//! The set of keys that must be collectively unique
	std::vector<size_t> keys;
	//! Base size of tuples
	size_t base_size = 0;
	//! Set of variable-length columns included in the key (if any)
	std::vector<size_t> variable_columns;
	//! Lock on the index
	std::mutex index_lock;
	//! Whether or not NULL values are allowed by the constraint (false for
	//! PRIMARY KEY, true for UNIQUE)
	bool allow_nulls;

	//! Root node of the index
	std::unique_ptr<UniqueIndexNode> root;
};

} // namespace duckdb
