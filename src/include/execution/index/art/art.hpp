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
#include "common/types/static_vector.hpp"
#include "art_key.hpp"
#include "node.hpp"
#include "node4.hpp"
#include "node16.hpp"
#include "node48.hpp"
#include "node256.hpp"

namespace duckdb {
    struct IteratorEntry {
        Node *node;
        int pos;
    };

    struct Iterator {
        //! The current Leaf Node, valid if depth>0
        Leaf *node;
        //! The current depth
        int32_t depth = 0;
        //! Stack, actually the size is determined at runtime
        IteratorEntry stack[9];

        bool start = false;
    };

    struct ARTIndexScanState : public IndexScanState {
	ARTIndexScanState(vector<column_t> column_ids) : IndexScanState(column_ids), checked(false) {
	}

	Value values[2];
	ExpressionType expressions[2];
	bool checked;
    uint64_t pointquery_tuple = 0;
    Iterator iterator;

};


class ART : public Index {
public:
	ART(DataTable &table, vector<column_t> column_ids, vector<TypeId> types, vector<TypeId> expression_types,
	    vector<unique_ptr<Expression>> expressions, vector<unique_ptr<Expression>> unbound_expressions);
	~ART();
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
	void Append(ClientContext &context, DataChunk &entries, uint64_t row_identifier_start) override;
	//! Update entries in the index
	void Update(ClientContext &context, vector<column_t> &column_ids, DataChunk &update_data,
	            Vector &row_identifiers) override;

	//! Delete entries in the index
	void Delete(DataChunk &entries, Vector &row_identifiers) override;
	//! Lock used for updating the index
	std::mutex lock;
	//! Root of the tree
	unique_ptr<Node> tree;
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
	void insert(bool isLittleEndian, unique_ptr<Node> &node, Key &key, unsigned depth, uintptr_t value,
	            unsigned maxKeyLength, TypeId type, uint64_t row_id);

	//! Erase element from leaf (if leaf has more than one value) or eliminate the leaf itself
	void erase(bool isLittleEndian, unique_ptr<Node> &node, Key &key, unsigned depth, unsigned maxKeyLength,
	           TypeId type, uint64_t row_id);

	//! Check if the key of the leaf is equal to the searched key
	bool leafMatches(bool is_little_endian, Node *node, Key &key, unsigned keyLength, unsigned depth);

	//! Find the node with a matching key, optimistic version
	Node *lookup(unique_ptr<Node> &node, Key &key, unsigned keyLength, unsigned depth);

	//! Find the iterator position for bound queries
	bool bound(unique_ptr<Node> &node, Key &key, unsigned keyLength, Iterator &iterator, unsigned maxKeyLength,
	           bool inclusive, bool isLittleEndian);

	//! Gets next node for range queries
	bool iteratorNext(Iterator &iter);

	template <class T> void templated_insert(DataChunk &input, Vector &row_ids) {
		auto input_data = (T *)input.data[0].data;
		auto row_identifiers = (int64_t *)row_ids.data;
		for (uint64_t i = 0; i < row_ids.count; i++) {
			auto key =
			    make_unique<Key>(this->is_little_endian, input.data[0].type, input_data[i], sizeof(input_data[i]));
			insert(this->is_little_endian, tree, *key, 0, input_data[i], sizeof(input_data[i]), input.data[0].type,
			       row_identifiers[i]);
		}
	}

	template <class T> void templated_delete(DataChunk &input, Vector &row_ids) {
		auto input_data = (T *)input.data[0].data;
		auto row_identifiers = (int64_t *)row_ids.data;
		for (uint64_t i = 0; i < row_ids.count; i++) {
			auto key =
			    make_unique<Key>(this->is_little_endian, input.data[0].type, input_data[i], sizeof(input_data[i]));
			erase(this->is_little_endian, tree, *key, 0, sizeof(input_data[i]), input.data[0].type, row_identifiers[i]);
		}
	}

	template <class T> uint64_t templated_lookup(TypeId type, T data, int64_t *result_ids, ARTIndexScanState *state) {
		auto key = make_unique<Key>(this->is_little_endian, type, data, sizeof(data));
		uint64_t result_count = 0;
		auto leaf = static_cast<Leaf *>(lookup(tree, *key, this->maxPrefix, 0));
		if (leaf) {
			for (; state->pointquery_tuple < leaf->num_elements; state->pointquery_tuple++) {
				result_ids[result_count++] = leaf->row_id[state->pointquery_tuple];
                if (result_count == STANDARD_VECTOR_SIZE) {
                    state->pointquery_tuple++;
                    return result_count;
                }
			}
            state->checked = true;

        }
		return result_count;
	}

	template <class T> uint64_t templated_greater_scan(TypeId type, T data, int64_t *result_ids, bool inclusive,ARTIndexScanState *state) {
        Iterator *it = &state->iterator;
		auto key = make_unique<Key>(this->is_little_endian, type, data, sizeof(data));

		uint64_t result_count = 0;
        bool found;
        if (!it->start){
            found = ART::bound(tree, *key, sizeof(data), *it, sizeof(data), inclusive, is_little_endian);
            it->start = true;
        }
        else{
            found = true;
        }
		if (found) {
			bool hasNext;
			do {
                if (state->pointquery_tuple >= it->node->num_elements){
                    state->pointquery_tuple=0;
                }
                for (;state->pointquery_tuple < it->node->num_elements; state->pointquery_tuple++) {
                    result_ids[result_count++] = it->node->row_id[state->pointquery_tuple];
                    if (result_count == STANDARD_VECTOR_SIZE) {
                        state->pointquery_tuple++;
                        return result_count;
                    }
                }
				hasNext = ART::iteratorNext(*it);
			} while (hasNext && it->node->value >= (uint64_t)data);
		}
        state->checked = true;
        return result_count;
	}

	template <class T> uint64_t templated_less_scan(TypeId type, T data, int64_t *result_ids, bool inclusive,ARTIndexScanState *state) {
		Iterator *it = &state->iterator;
		uint64_t result_count = 0;
		auto min_value = Node::minimum(tree)->get();
		auto key = make_unique<Key>(this->is_little_endian, type, data, sizeof(data));
		Leaf *minimum = static_cast<Leaf *>(min_value);
		auto min_key = make_unique<Key>(this->is_little_endian, type, minimum->value, sizeof(data));

		// early out min value higher than upper bound query
		if (*min_key > *key)
			return result_count;
		bool found;
		if (!it->start){
            found = ART::bound(tree, *min_key, sizeof(data), *it, sizeof(data), true, is_little_endian);
            it->start = true;
		}
		else{
            found = true;
        }
		if (found) {
			bool hasNext;
			do {
			    if (state->pointquery_tuple >= it->node->num_elements){
                    state->pointquery_tuple=0;
			    }
				for (;state->pointquery_tuple < it->node->num_elements; state->pointquery_tuple++) {
					result_ids[result_count++] = it->node->row_id[state->pointquery_tuple];
                    if (result_count == STANDARD_VECTOR_SIZE) {
                        state->pointquery_tuple++;
                        return result_count;
                    }
				}
				if (it->node->value == (uint64_t)data)
					break;
				hasNext = ART::iteratorNext(*it);
				if (!inclusive && it->node->value == (uint64_t)data)
					break;
			} while (hasNext);
		}
        state->checked = true;
		return result_count;
	}

	template <class T>
	uint64_t templated_close_range(TypeId type, T left_query, T right_query, int64_t *result_ids, bool left_inclusive,
	                               bool right_inclusive,ARTIndexScanState *state) {
        Iterator *it = &state->iterator;
		auto key = make_unique<Key>(this->is_little_endian, type, left_query, sizeof(left_query));
		uint64_t result_count = 0;
        bool found;
        if (!it->start){
            found = ART::bound(tree, *key, sizeof(left_query), *it, sizeof(left_query), left_inclusive, is_little_endian);
            it->start = true;
        }
        else{
            found = true;
        }
		if (found) {
			bool hasNext;
			do {
                if (state->pointquery_tuple >= it->node->num_elements){
                    state->pointquery_tuple=0;
                }
                for (;state->pointquery_tuple < it->node->num_elements; state->pointquery_tuple++) {
                    result_ids[result_count++] = it->node->row_id[state->pointquery_tuple];
                    if (result_count == STANDARD_VECTOR_SIZE) {
                        state->pointquery_tuple++;
                        return result_count;
                    }
                }
				if (it->node->value == (uint64_t)right_query)
					break;
				hasNext = ART::iteratorNext(*it);
				if (!right_inclusive && it->node->value == (uint64_t)right_query)
					break;

			} while (hasNext);
		}
        state->checked = true;
        return result_count;
	}

	DataChunk expression_result;

	void SearchEqual(StaticVector<int64_t> *result_identifiers, ARTIndexScanState *state);
	void SearchGreater(StaticVector<int64_t> *result_identifiers, ARTIndexScanState *state, bool inclusive);
	void SearchLess(StaticVector<int64_t> *result_identifiers, ARTIndexScanState *state, bool inclusive);
	void SearchCloseRange(StaticVector<int64_t> *result_identifiers, ARTIndexScanState *state, bool left_inclusive,
	                      bool right_inclusive);
};

} // namespace duckdb
