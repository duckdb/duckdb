#include "execution/index/art/art.hpp"
#include "execution/expression_executor.hpp"
#include "common/vector_operations/vector_operations.hpp"
#include "main/client_context.hpp"
#include <algorithm>

using namespace duckdb;
using namespace std;

ART::ART(DataTable &table, vector<column_t> column_ids, vector<unique_ptr<Expression>> unbound_expressions, bool is_unique)
    : Index(IndexType::ART, table, column_ids, move(unbound_expressions)), is_unique(is_unique) {
	if (this->unbound_expressions.size() > 1) {
		throw NotImplementedException("Multiple columns in ART index not supported");
	}
	tree = nullptr;
	expression_result.Initialize(types);
	int n = 1;
	// little endian if true
	if (*(char *)&n == 1) {
		is_little_endian = true;
	} else {
		is_little_endian = false;
	}
	switch (types[0]) {
	case TypeId::TINYINT:
		maxPrefix = sizeof(int8_t);
		break;
	case TypeId::SMALLINT:
		maxPrefix = sizeof(int16_t);
		break;
	case TypeId::INTEGER:
		maxPrefix = sizeof(int32_t);
		break;
	case TypeId::BIGINT:
		maxPrefix = sizeof(int64_t);
		break;
	default:
		throw InvalidTypeException(types[0], "Invalid type for index");
	}
}

ART::~ART() {
}

bool ART::LeafMatches(Node *node, Key &key, unsigned depth) {
	if (depth != maxPrefix) {
		auto leaf = static_cast<Leaf *>(node);
		auto leafKey = make_unique<Key>(*this, types[0], leaf->value);
		Key &key_ref = *leafKey;
		for (index_t i = depth; i < maxPrefix; i++) {
			if (key_ref[i] != key[i]) {
				return false;
			}
		}
	}
	return true;
}

unique_ptr<IndexScanState> ART::InitializeScanSinglePredicate(Transaction &transaction, vector<column_t> column_ids,
                                                              Value value, ExpressionType expression_type) {
	auto result = make_unique<ARTIndexScanState>(column_ids);
	result->values[0] = value;
	result->expressions[0] = expression_type;
	return move(result);
}

unique_ptr<IndexScanState> ART::InitializeScanTwoPredicates(Transaction &transaction, vector<column_t> column_ids,
                                                            Value low_value, ExpressionType low_expression_type,
                                                            Value high_value, ExpressionType high_expression_type) {
	auto result = make_unique<ARTIndexScanState>(column_ids);
	result->values[0] = low_value;
	result->expressions[0] = low_expression_type;
	result->values[1] = high_value;
	result->expressions[1] = high_expression_type;
	return move(result);
}

//===--------------------------------------------------------------------===//
// Insert
//===--------------------------------------------------------------------===//
template <class T>
void ART::templated_insert(ClientContext &context, DataChunk &input, Vector &row_ids) {
	auto input_data = (T *)input.data[0].data;
	auto row_identifiers = (int64_t *)row_ids.data;
	VectorOperations::Exec(row_ids, [&](index_t i, index_t k) {
		auto key = make_unique<Key>(*this, input.data[0].type, input_data[i]);
		Insert(context, tree, *key, 0, input_data[i], input.data[0].type, row_identifiers[i]);
	});
}

void ART::Insert(ClientContext &context, DataChunk &input, Vector &row_ids) {
	assert(row_ids.type == TypeId::BIGINT);
	assert(input.size() == row_ids.count);
	assert(types[0] == input.data[0].type);
	switch (input.data[0].type) {
	case TypeId::TINYINT:
		templated_insert<int8_t>(context, input, row_ids);
		break;
	case TypeId::SMALLINT:
		templated_insert<int16_t>(context, input, row_ids);
		break;
	case TypeId::INTEGER:
		templated_insert<int32_t>(context, input, row_ids);
		break;
	case TypeId::BIGINT:
		templated_insert<int64_t>(context, input, row_ids);
		break;
	default:
		throw InvalidTypeException(input.data[0].type, "Invalid type for index");
	}
}

void ART::Append(ClientContext &context, DataChunk &appended_data, uint64_t row_identifier_start) {
	lock_guard<mutex> l(lock);

	// first resolve the expressions for the index
	ExecuteExpressions(appended_data, expression_result);

	// create the row identifiers
	StaticVector<int64_t> row_identifiers;
	auto row_ids = (int64_t *)row_identifiers.data;
	row_identifiers.count = appended_data.size();
	for (index_t i = 0; i < row_identifiers.count; i++) {
		row_ids[i] = row_identifier_start + i;
	}
	Insert(context, expression_result, row_identifiers);
}

void ART::InsertToLeaf(ClientContext &context, Leaf &leaf, row_t row_id) {
	if (is_unique && leaf.num_elements > 0) {
		throw CatalogException("duplicate key value violates primary key or unique constraint");
	}
	leaf.Insert(row_id);
}

void ART::Insert(ClientContext &context, unique_ptr<Node> &node, Key &key, unsigned depth, uintptr_t value, TypeId type, row_t row_id) {
	if (!node) {
		// node is currently empty, create a leaf here with the key
		node = make_unique<Leaf>(*this, value, row_id);
		return;
	}

	if (node->type == NodeType::NLeaf) {
		// Replace leaf with Node4 and store both leaves in it
		auto leaf = static_cast<Leaf *>(node.get());
		auto auxKey = make_unique<Key>(*this, type, leaf->value);

		Key &existingKey = *auxKey;
		uint32_t newPrefixLength = 0;
		// Leaf node is already there, update row_id vector
		if (depth + newPrefixLength == maxPrefix) {
			InsertToLeaf(context, *leaf, row_id);
			return;
		}
		while (existingKey[depth + newPrefixLength] == key[depth + newPrefixLength]) {
			newPrefixLength++;
			// Leaf node is already there, update row_id vector
			if (depth + newPrefixLength == maxPrefix) {
				InsertToLeaf(context, *leaf, row_id);
				return;
			}
		}
		unique_ptr<Node> newNode = make_unique<Node4>(*this);
		newNode->prefix_length = newPrefixLength;
		memcpy(newNode->prefix.get(), &key[depth], std::min(newPrefixLength, maxPrefix));
		Node4::insert(*this, newNode, existingKey[depth + newPrefixLength], node);
		unique_ptr<Node> leaf_node = make_unique<Leaf>(*this, value, row_id);
		Node4::insert(*this, newNode, key[depth + newPrefixLength], leaf_node);
		node = move(newNode);
		return;
	}

	// Handle prefix of inner node
	if (node->prefix_length) {
		uint32_t mismatchPos = Node::PrefixMismatch(*this, node.get(), key, depth, type);
		if (mismatchPos != node->prefix_length) {
			// Prefix differs, create new node
			unique_ptr<Node> newNode = make_unique<Node4>(*this);
			newNode->prefix_length = mismatchPos;
			memcpy(newNode->prefix.get(), node->prefix.get(), std::min(mismatchPos, maxPrefix));
			// Break up prefix
			if (node->prefix_length < maxPrefix) {
				auto node_ptr = node.get();
				Node4::insert(*this, newNode, node->prefix[mismatchPos], node);
				node_ptr->prefix_length -= (mismatchPos + 1);
				memmove(node_ptr->prefix.get(), node_ptr->prefix.get() + mismatchPos + 1,
				        std::min(node_ptr->prefix_length, maxPrefix));
			} else {
				throw NotImplementedException("PrefixLength > MaxPrefixLength");
			}
			unique_ptr<Node> leaf_node = make_unique<Leaf>(*this, value, row_id);

			Node4::insert(*this, newNode, key[depth + mismatchPos], leaf_node);
			node = move(newNode);
			return;
		}
		depth += node->prefix_length;
	}

	// Recurse
	auto child = Node::findChild(key[depth], node);
	if (child) {
		Insert(context, *child, key, depth + 1, value, type, row_id);
		return;
	}

	unique_ptr<Node> newNode = make_unique<Leaf>(*this, value, row_id);
	Node::InsertLeaf(*this, node, key[depth], newNode);
}

//===--------------------------------------------------------------------===//
// Delete
//===--------------------------------------------------------------------===//
template <class T>
void ART::templated_delete(DataChunk &input, Vector &row_ids) {
	auto input_data = (T *)input.data[0].data;
	auto row_identifiers = (int64_t *)row_ids.data;
	VectorOperations::Exec(row_ids, [&](index_t i, index_t k) {
		auto key = make_unique<Key>(*this, input.data[0].type, input_data[i]);
		Erase(tree, *key, 0, input.data[0].type, row_identifiers[i]);
	});
}

void ART::Delete(DataChunk &input, Vector &row_ids) {
	lock_guard<mutex> l(lock);

	// first resolve the expressions
	ExecuteExpressions(input, expression_result);

	assert(row_ids.type == TypeId::BIGINT);
	assert(expression_result.size() == row_ids.count);
	assert(types[0] == expression_result.data[0].type);
	switch (expression_result.data[0].type) {
	case TypeId::TINYINT:
		templated_delete<int8_t>(expression_result, row_ids);
		break;
	case TypeId::SMALLINT:
		templated_delete<int16_t>(expression_result, row_ids);
		break;
	case TypeId::INTEGER:
		templated_delete<int32_t>(expression_result, row_ids);
		break;
	case TypeId::BIGINT:
		templated_delete<int64_t>(expression_result, row_ids);
		break;
	default:
		throw InvalidTypeException(expression_result.data[0].type, "Invalid type for index");
	}
}

void ART::Erase(unique_ptr<Node> &node, Key &key, unsigned depth, TypeId type, row_t row_id) {
	if (!node)
		return;
	// Delete a leaf from a tree
	if (node->type == NodeType::NLeaf) {
		// Make sure we have the right leaf
		if (ART::LeafMatches(node.get(), key, depth)) {
			node.reset();
		}
		return;
	}

	// Handle prefix
	if (node->prefix_length) {
		if (Node::PrefixMismatch(*this, node.get(), key, depth, type) != node->prefix_length) {
			return;
		}
		depth += node->prefix_length;
	}
	int pos = Node::findKeyPos(key[depth], &(*node));
	auto child = Node::findChild(key[depth], node);
    if(child){
        unique_ptr<Node> &child_ref = *child;
        if (child_ref->type == NodeType::NLeaf && LeafMatches(child_ref.get(), key, depth)) {
            // Leaf found, remove entry
            auto leaf = static_cast<Leaf *>(child_ref.get());
            if (leaf->num_elements > 1) {
                leaf->Remove(row_id);
                return;
            }
            // Leaf only has one element, delete leaf, decrement node counter and maybe shrink node
            else {
                switch (node->type) {
                case NodeType::N4: {
                    Node4::erase(*this, node, pos);
                    break;
                }
                case NodeType::N16: {
                    Node16::erase(*this, node, pos);
                    break;
                }
                case NodeType::N48: {
                    Node48::erase(*this, node, pos);
                    break;
                }
                case NodeType::N256:
                    Node256::erase(*this, node, pos);
                    break;
                default:
                    assert(0);
                    break;
                }
            }

        } else {
            // Recurse
            Erase(*child, key, depth + 1, type, row_id);
        }
    }
}

void ART::Update(ClientContext &context, vector<column_t> &update_columns, DataChunk &update_data,
                 Vector &row_identifiers) {
	// first check if the columns we use here are updated
	bool index_is_updated = false;
	for (auto &column : update_columns) {
		if (find(column_ids.begin(), column_ids.end(), column) != column_ids.end()) {
			index_is_updated = true;
			break;
		}
	}
	if (!index_is_updated) {
		// none of the indexed columns are updated
		// we can ignore the update
		return;
	}
	// otherwise we need to change the data inside the index
	lock_guard<mutex> l(lock);

	DataChunk temp_chunk;
	temp_chunk.Initialize(table.types);
	temp_chunk.data[0].count = update_data.size();
	for (index_t col_idx = 0; col_idx < column_ids.size(); col_idx++) {
		if (column_ids[col_idx] == COLUMN_IDENTIFIER_ROW_ID) {
			continue;
		}
		bool found_column = false;
		for (index_t update_idx = 0; update_idx < update_columns.size(); update_idx++) {
			if (column_ids[col_idx] == update_columns[update_idx]) {
				temp_chunk.data[column_ids[col_idx]].Reference(update_data.data[update_idx]);
				temp_chunk.sel_vector = temp_chunk.data[column_ids[col_idx]].sel_vector;
				found_column = true;
				break;
			}
		}
		assert(found_column);
	}

	// now resolve the expressions on the temp_chunk
	ExecuteExpressions(temp_chunk, expression_result);

	// insert the expression result
	Insert(context, expression_result, row_identifiers);
}

//===--------------------------------------------------------------------===//
// Point Query
//===--------------------------------------------------------------------===//
template <class T>
index_t ART::templated_lookup(TypeId type, T data, int64_t *result_ids, ARTIndexScanState *state) {
	auto key = make_unique<Key>(*this, type, data);
	index_t result_count = 0;
	auto leaf = static_cast<Leaf *>(Lookup(tree, *key, 0));
	if (leaf) {
		for (; state->pointquery_tuple < leaf->num_elements; state->pointquery_tuple++) {
			result_ids[result_count++] = leaf->GetRowId(state->pointquery_tuple);
			if (result_count == STANDARD_VECTOR_SIZE) {
				state->pointquery_tuple++;
				return result_count;
			}
		}
		state->checked = true;
	}
	return result_count;
}

void ART::SearchEqual(StaticVector<int64_t> *result_identifiers, ARTIndexScanState *state) {
	auto row_ids = (int64_t *)result_identifiers->data;
	switch (types[0]) {
	case TypeId::TINYINT:
		result_identifiers->count = templated_lookup<int8_t>(types[0], state->values[0].value_.tinyint, row_ids, state);
		break;
	case TypeId::SMALLINT:
		result_identifiers->count =
		    templated_lookup<int16_t>(types[0], state->values[0].value_.smallint, row_ids, state);
		break;
	case TypeId::INTEGER:
		result_identifiers->count =
		    templated_lookup<int32_t>(types[0], state->values[0].value_.integer, row_ids, state);
		break;
	case TypeId::BIGINT:
		result_identifiers->count = templated_lookup<int64_t>(types[0], state->values[0].value_.bigint, row_ids, state);
		break;
	default:
		throw InvalidTypeException(types[0], "Invalid type for index");
	}
}

Node *ART::Lookup(unique_ptr<Node> &node, Key &key, unsigned depth) {
	bool skippedPrefix = false; // Did we optimistically skip some prefix without checking it?
	auto node_val = node.get();

	while (node_val) {
		if (node_val->type == NodeType::NLeaf) {
			if (!skippedPrefix && depth == maxPrefix)  {// No check required
				return node_val;
			}

			if (depth != maxPrefix) {
				// Check leaf
				auto leaf = static_cast<Leaf *>(node_val);
				auto auxKey = make_unique<Key>(*this, types[0], leaf->value);
				Key &leafKey = *auxKey;
				for (index_t i = (skippedPrefix ? 0 : depth); i < maxPrefix; i++) {
					if (leafKey[i] != key[i]) {
						return nullptr;
					}
				}
			}
			return node_val;
		}

		if (node_val->prefix_length) {
			if (node_val->prefix_length < maxPrefix) {
				for (index_t pos = 0; pos < node_val->prefix_length; pos++) {
					if (key[depth + pos] != node_val->prefix[pos]) {
						return nullptr;
					}
				}
			} else {
				skippedPrefix = true;
			}
			depth += node_val->prefix_length;
		}

		node_val = Node::findChild(key[depth], node_val);
		if (!node_val) {
			return nullptr;
		}
		depth++;
	}

	return nullptr;
}

//===--------------------------------------------------------------------===//
// Iterator scans
//===--------------------------------------------------------------------===//
template<class T, bool HAS_BOUND, bool INCLUSIVE>
index_t ART::iterator_scan(ARTIndexScanState *state, Iterator *it, int64_t *result_ids, T bound) {
	index_t result_count = 0;
	bool has_next;
	do {
		if (HAS_BOUND) {
			if (INCLUSIVE) {
				if (it->node->value > (uint64_t) bound) {
					break;
				}
			} else {
				if (it->node->value >= (uint64_t) bound) {
					break;
				}
			}
		}
		for (; state->pointquery_tuple < it->node->num_elements; state->pointquery_tuple++) {
			result_ids[result_count++] = it->node->GetRowId(state->pointquery_tuple);
			if (result_count == STANDARD_VECTOR_SIZE) {
				state->pointquery_tuple++;
				return result_count;
			}
		}
		state->pointquery_tuple = 0;
		has_next = ART::IteratorNext(*it);
	} while (has_next);
	state->checked = true;
	return result_count;
}

bool ART::IteratorNext(Iterator &iter) {
	// Skip leaf
	if ((iter.depth) && ((iter.stack[iter.depth - 1].node)->type == NodeType::NLeaf)) {
		iter.depth--;
	}

	// Look for next leaf
	while (iter.depth) {
		Node *node = iter.stack[iter.depth - 1].node;

		// Leaf found
		if (node->type == NodeType::NLeaf) {
			auto leaf = static_cast<Leaf *>(node);
			iter.node = leaf;
			return true;
		}

		// Find next node
		Node *next = nullptr;
		switch (node->type) {
		case NodeType::N4: {
			Node4 *n = static_cast<Node4 *>(node);
			if (iter.stack[iter.depth - 1].pos < node->count) {
				next = n->child[iter.stack[iter.depth - 1].pos++].get();
			}
			break;
		}
		case NodeType::N16: {
			Node16 *n = static_cast<Node16 *>(node);
			if (iter.stack[iter.depth - 1].pos < node->count) {
				next = n->child[iter.stack[iter.depth - 1].pos++].get();
			}
			break;
		}
		case NodeType::N48: {
			Node48 *n = static_cast<Node48 *>(node);
			unsigned depth = iter.depth - 1;
			for (; iter.stack[depth].pos < 256; iter.stack[depth].pos++) {
				if (n->childIndex[iter.stack[depth].pos] != 48) {
					next = n->child[n->childIndex[iter.stack[depth].pos++]].get();
					break;
				}
			}
			break;
		}
		case NodeType::N256: {
			Node256 *n = static_cast<Node256 *>(node);
			unsigned depth = iter.depth - 1;
			for (; iter.stack[depth].pos < 256; iter.stack[depth].pos++) {
				if (n->child[iter.stack[depth].pos]) {
					next = n->child[iter.stack[depth].pos++].get();
					break;
				}
			}
			break;
		}
		default:
			assert(0);
			break;
		}

		if (next) {
			iter.stack[iter.depth].pos = 0;
			iter.stack[iter.depth].node = next;
			iter.depth++;
		} else {
			iter.depth--;
		}
	}
	return false;
}

bool ART::Bound(unique_ptr<Node> &n, Key &key, Iterator &iterator, bool inclusive) {
	iterator.depth = 0;
	if (!n) {
		return false;
	}
	Node *node = n.get();

	index_t depth = 0;
	while (true) {
		iterator.stack[iterator.depth].node = node;
		auto &pos = iterator.stack[iterator.depth].pos;
		iterator.depth++;

		if (node->type == NodeType::NLeaf) {
			auto leaf = static_cast<Leaf *>(node);
			iterator.node = leaf;
			if (depth == maxPrefix) {
				// Equal
				if (inclusive) {
					return true;
				} else {
					return IteratorNext(iterator);
				}
			}
			auto auxKey = make_unique<Key>(*this, types[0], leaf->value);
			Key &leafKey = *auxKey;
			for (index_t i = depth; i < maxPrefix; i++) {
				if (leafKey[i] != key[i]) {
					if (leafKey[i] < key[i]) {
						// Less
						iterator.depth--;
						return IteratorNext(iterator);
					}
					// Greater
					return true;
				}
			}

			// Equal
			if (inclusive) {
				return true;
			} else {
				return IteratorNext(iterator);
			}
		}
		uint32_t mismatchPos = Node::PrefixMismatch(*this, node, key, depth, types[0]);
		if (mismatchPos != node->prefix_length) {
			if (node->prefix[mismatchPos] < key[depth + mismatchPos]) {
				// Less
				iterator.depth--;
				return IteratorNext(iterator);
			} else {
				// Greater
				pos = 0;
				return IteratorNext(iterator);
			}
		}
		// prefix matches, search inside the child for the key
		depth += node->prefix_length;
		Node *next = Node::findChild(key[depth], node);
		pos = Node::findKeyPos(key[depth], node);
		if (!next) {
			return IteratorNext(iterator);
		}

		pos++;
		node = next;
		depth++;
	}
}

//===--------------------------------------------------------------------===//
// Greater Than
//===--------------------------------------------------------------------===//
template <class T>
index_t ART::templated_greater_scan(TypeId type, T data, int64_t *result_ids, bool inclusive,
								ARTIndexScanState *state) {
	Iterator *it = &state->iterator;
	auto key = make_unique<Key>(*this, type, data);

	// greater than scan: first set the iterator to the node at which we will start our scan by finding the lowest node that satisfies our requirement
	if (!it->start) {
		bool found = ART::Bound(tree, *key, *it, inclusive);
		if (!found) {
			return 0;
		}
		it->start = true;
	}
	// after that we continue the scan; we don't need to check the bounds as any value following this value is automatically bigger and hence satisfies our predicate
	return iterator_scan<T, false, false>(state, it, result_ids, 0);
}

void ART::SearchGreater(StaticVector<int64_t> *result_identifiers, ARTIndexScanState *state, bool inclusive) {
	auto row_ids = (int64_t *)result_identifiers->data;
	switch (types[0]) {
	case TypeId::TINYINT:
		result_identifiers->count =
		    templated_greater_scan<int8_t>(types[0], state->values[0].value_.tinyint, row_ids, inclusive, state);
		break;
	case TypeId::SMALLINT:
		result_identifiers->count =
		    templated_greater_scan<int16_t>(types[0], state->values[0].value_.smallint, row_ids, inclusive, state);
		break;
	case TypeId::INTEGER:
		result_identifiers->count =
		    templated_greater_scan<int32_t>(types[0], state->values[0].value_.integer, row_ids, inclusive, state);
		break;
	case TypeId::BIGINT:
		result_identifiers->count =
		    templated_greater_scan<int64_t>(types[0], state->values[0].value_.bigint, row_ids, inclusive, state);
		break;
	default:
		throw InvalidTypeException(types[0], "Invalid type for index");
	}
}

//===--------------------------------------------------------------------===//
// Less Than
//===--------------------------------------------------------------------===//
template <class T>
index_t ART::templated_less_scan(TypeId type, T data, int64_t *result_ids, bool inclusive, ARTIndexScanState *state) {
	Iterator *it = &state->iterator;
	auto min_value = Node::minimum(tree)->get();
	auto key = make_unique<Key>(*this, type, data);
	Leaf *minimum = static_cast<Leaf *>(min_value);
	auto min_key = make_unique<Key>(*this, type, minimum->value);

	// early out min value higher than upper bound query
	if (*min_key > *key) {
		return 0;
	}
	// find the minimum value in the ART: we start scanning from this value
	if (!it->start) {
		bool found = ART::Bound(tree, *min_key, *it, true);
		if (!found) {
			return 0;
		}
		it->start = true;
	}
	// now continue the scan until we reach the upper bound
	if (inclusive) {
		return iterator_scan<T, true, true>(state, it, result_ids, data);
	} else {
		return iterator_scan<T, true, false>(state, it, result_ids, data);
	}
}

void ART::SearchLess(StaticVector<int64_t> *result_identifiers, ARTIndexScanState *state, bool inclusive) {
	auto row_ids = (int64_t *)result_identifiers->data;
	switch (types[0]) {
	case TypeId::TINYINT:
		result_identifiers->count =
		    templated_less_scan<int8_t>(types[0], state->values[0].value_.tinyint, row_ids, inclusive, state);
		break;
	case TypeId::SMALLINT:
		result_identifiers->count =
		    templated_less_scan<int16_t>(types[0], state->values[0].value_.smallint, row_ids, inclusive, state);
		break;
	case TypeId::INTEGER:
		result_identifiers->count =
		    templated_less_scan<int32_t>(types[0], state->values[0].value_.integer, row_ids, inclusive, state);
		break;
	case TypeId::BIGINT:
		result_identifiers->count =
		    templated_less_scan<int64_t>(types[0], state->values[0].value_.bigint, row_ids, inclusive, state);
		break;
	default:
		throw InvalidTypeException(types[0], "Invalid type for index");
	}
}

//===--------------------------------------------------------------------===//
// Closed Range Query
//===--------------------------------------------------------------------===//
template <class T>
index_t ART::templated_close_range(TypeId type, T left_query, T right_query, int64_t *result_ids, bool left_inclusive,
								bool right_inclusive, ARTIndexScanState *state) {
	Iterator *it = &state->iterator;
	auto key = make_unique<Key>(*this, type, left_query);
	// first find the first node that satisfies the left predicate
	if (!it->start) {
		bool found = ART::Bound(tree, *key, *it, left_inclusive);
		if (!found) {
			return 0;
		}
		it->start = true;
	}
	// now continue the scan until we reach the upper bound
	if (right_inclusive) {
		return iterator_scan<T, true, true>(state, it, result_ids, right_query);
	} else {
		return iterator_scan<T, true, false>(state, it, result_ids, right_query);
	}
}

void ART::SearchCloseRange(StaticVector<int64_t> *result_identifiers, ARTIndexScanState *state, bool left_inclusive,
                           bool right_inclusive) {
	auto row_ids = (int64_t *)result_identifiers->data;
	switch (types[0]) {
	case TypeId::TINYINT:
		result_identifiers->count =
		    templated_close_range<int8_t>(types[0], state->values[0].value_.tinyint, state->values[1].value_.tinyint,
		                                  row_ids, left_inclusive, right_inclusive, state);
		break;
	case TypeId::SMALLINT:
		result_identifiers->count =
		    templated_close_range<int16_t>(types[0], state->values[0].value_.smallint, state->values[1].value_.smallint,
		                                   row_ids, left_inclusive, right_inclusive, state);
		break;
	case TypeId::INTEGER:
		result_identifiers->count =
		    templated_close_range<int32_t>(types[0], state->values[0].value_.integer, state->values[1].value_.integer,
		                                   row_ids, left_inclusive, right_inclusive, state);
		break;
	case TypeId::BIGINT:
		result_identifiers->count =
		    templated_close_range<int64_t>(types[0], state->values[0].value_.bigint, state->values[1].value_.bigint,
		                                   row_ids, left_inclusive, right_inclusive, state);
		break;
	default:
		throw InvalidTypeException(types[0], "Invalid type for index");
	}
}

// FIXME: Returning one tuple per time so deletes in different chunks do not break.
void ART::Scan(Transaction &transaction, IndexScanState *ss, DataChunk &result) {
	auto state = (ARTIndexScanState *)ss;
	StaticVector<int64_t> result_identifiers;

	// scan the index
	if (!state->checked) {
		assert(state->values[0].type == types[0]);

		// single predicate
		if (state->values[1].is_null) {
			switch (state->expressions[0]) {
			case ExpressionType::COMPARE_EQUAL:
				SearchEqual(&result_identifiers, state);
				break;
			case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
				SearchGreater(&result_identifiers, state, true);
				break;
			case ExpressionType::COMPARE_GREATERTHAN:
				SearchGreater(&result_identifiers, state, false);
				break;
			case ExpressionType::COMPARE_LESSTHANOREQUALTO:
				SearchLess(&result_identifiers, state, true);
				break;
			case ExpressionType::COMPARE_LESSTHAN:
				SearchLess(&result_identifiers, state, false);
				break;
			default:
				throw NotImplementedException("Operation not implemented");
			}
		}
		// two predicates
		else {
			assert(state->values[1].type == types[0]);
			bool left_inclusive = state->expressions[0] == ExpressionType ::COMPARE_GREATERTHANOREQUALTO;
			bool right_inclusive = state->expressions[1] == ExpressionType ::COMPARE_LESSTHANOREQUALTO;
			SearchCloseRange(&result_identifiers, state, left_inclusive, right_inclusive);
		}
	}

	// scan the index
	if (result_identifiers.count == 0) {
		return;
	}

	table.Fetch(transaction, result, state->column_ids, result_identifiers);
}
