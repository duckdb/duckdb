//===----------------------------------------------------------------------===//
//                         DuckDB
//
// execution/index/art/art.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/common.hpp"
#include "common/types/data_chunk.hpp"
#include "common/types/tuple.hpp"
#include "common/types/vector.hpp"
#include "parser/parsed_expression.hpp"
#include "storage/data_table.hpp"
#include "storage/index.hpp"
#include "art_key.hpp"
#include "node.hpp"
#include "node4.hpp"
#include "node16.hpp"
#include "node48.hpp"
#include "node256.hpp"

namespace duckdb {
struct ARTIndexScanState : public IndexScanState {
	Value values[2];
	ExpressionType expressions[2];
	bool checked;

	ARTIndexScanState(vector<column_t> column_ids) : IndexScanState(column_ids) {
	}
};

class ART : public Index {
public:
	ART(DataTable &table, vector<column_t> column_ids, vector<TypeId> types, vector<TypeId> expression_types,
	    vector<unique_ptr<Expression>> expressions, vector<unique_ptr<Expression>> unbound_expressions);
	//! Insert data into the index
	void Insert(DataChunk &data, Vector &row_ids);
	//! Print the index to the console
	void Print(){

	};

	void BulkLoad(DataChunk &data, Vector &row_ids);

	//! Initialize a scan on the index with the given expression and column ids
	//! to fetch from the base table for a single predicate
	unique_ptr<IndexScanState> InitializeScanSinglePredicate(Transaction &transaction, vector<column_t> column_ids,
	                                                         Value value, ExpressionType expressionType) override;

	//! Initialize a scan on the index with the given expression and column ids
	//! to fetch from the base table for two predicates
	unique_ptr<IndexScanState> InitializeScanTwoPredicates(Transaction &transaction, vector<column_t> column_ids,
	                                                       Value low_value, ExpressionType low_expression_type,
	                                                       Value high_value,
	                                                       ExpressionType high_expression_type) override;
	//! Perform a lookup on the index
	void Scan(Transaction &transaction, IndexScanState *ss, DataChunk &result) override;

	//! Append entries to the index
	void Append(ClientContext &context, DataChunk &entries, size_t row_identifier_start) override;
	//! Update entries in the index
	void Update(ClientContext &context, vector<column_t> &column_ids, DataChunk &update_data,
	            Vector &row_identifiers) override;

	//! Delete entries in the index
	void Delete(DataChunk &entries, Vector &row_identifiers) override;
	//! Lock used for updating the index
	std::mutex lock;
	//! Root of the tree
	Node *tree;
	//! The table
	DataTable &table;
	//! Column identifiers to extract from the base table
	vector<column_t> column_ids;
	//! Types of the column identifiers
	vector<TypeId> types;
	//! True if machine is little endian
	bool is_little_endian;
	//! The maximum prefix length for compressed paths stored in the
	//! header, if the path is longer it is loaded from the database on demand
	uint8_t maxPrefix;

private:
	//! Insert the leaf value into the tree
	void insert(bool isLittleEndian, Node *node, Node **nodeRef, Key &key, unsigned depth, uintptr_t value,
	            unsigned maxKeyLength, TypeId type, uint64_t row_id);

	void erase(bool isLittleEndian, Node *node, Node **nodeRef, Key &key, unsigned depth, unsigned maxKeyLength,
	           TypeId type, uint64_t row_id);

	//! Check if the key of the leaf is equal to the searched key
	bool leafMatches(bool is_little_endian, Node *node, Key &key, unsigned keyLength, unsigned depth);

	//! Find the node with a matching key, optimistic version
	Node *lookup(Node *node, Key &key, unsigned keyLength, unsigned depth);

	template <class T> void templated_insert(DataChunk &input, Vector &row_ids) {
		auto input_data = (T *)input.data[0].data;
		auto row_identifiers = (uint64_t *)row_ids.data;
		for (size_t i = 0; i < row_ids.count; i++) {
			Key &key = *new Key(this->is_little_endian, input.data[0].type, input_data[i]);
			insert(this->is_little_endian, tree, &tree, key, 0, input_data[i], 8, input.data[0].type,
			       row_identifiers[i]);
		}
	}

	template <class T> void templated_delete(DataChunk &input, Vector &row_ids) {
		auto input_data = (T *)input.data[0].data;
		auto row_identifiers = (uint64_t *)row_ids.data;
		for (size_t i = 0; i < row_ids.count; i++) {
			Key &key = *new Key(this->is_little_endian, input.data[0].type, input_data[i]);
			erase(this->is_little_endian, tree, &tree, key, 0, 8, input.data[0].type, row_identifiers[i]);
		}
	}

	template <class T> size_t templated_lookup(TypeId type, T data, uint64_t *result_ids) {
		Key &key = *new Key(this->is_little_endian, type, data);
		size_t result_count = 0;
		auto leaf = static_cast<Leaf *>(lookup(tree, key, this->maxPrefix, 0));
		if (leaf) {
			for (size_t i = 0; i < leaf->num_elements; i++) {
				result_ids[result_count++] = leaf->row_id[i];
			}
		}
		return result_count;
	}

	DataChunk expression_result;
	//! Scan the index starting from the position, updating the position.
	//! Returns the amount of tuples scanned.
	void Scan(size_t &position_from, size_t &position_to, Value value, Vector &result_identifiers){};
};

} // namespace duckdb
