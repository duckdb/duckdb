#include "duckdb/execution/index/art/art.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/main/client_context.hpp"
#include <algorithm>
#include <ctgmath>

using namespace duckdb;
using namespace std;

ART::ART(DataTable &table, vector<column_t> column_ids, vector<unique_ptr<Expression>> unbound_expressions,
         bool is_unique)
    : Index(IndexType::ART, table, column_ids, move(unbound_expressions)), is_unique(is_unique) {
	if (this->unbound_expressions.size() > 1) {
		throw NotImplementedException("Multiple columns in ART index not supported");
	}
	tree = nullptr;
	expression_result.Initialize(types);
	int n = 1;
	//! little endian if true
	if (*(char *)&n == 1) {
		is_little_endian = true;
	} else {
		is_little_endian = false;
	}
	switch (types[0]) {
	case TypeId::INT8:
		maxPrefix = sizeof(int8_t);
		break;
	case TypeId::INT16:
		maxPrefix = sizeof(int16_t);
		break;
	case TypeId::INT32:
		maxPrefix = sizeof(int32_t);
		break;
	case TypeId::INT64:
		maxPrefix = sizeof(int64_t);
		break;
	case TypeId::FLOAT:
		maxPrefix = sizeof(float);
		break;
	case TypeId::DOUBLE:
		maxPrefix = sizeof(double);
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
		Key &leaf_key = *leaf->value;
		for (index_t i = depth; i < maxPrefix; i++) {
			if (leaf_key[i] != key[i]) {
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
template <class T> static void generate_keys(DataChunk &input, vector<unique_ptr<Key>> &keys, bool is_little_endian) {
	auto input_data = (T *)input.data[0].GetData();
	VectorOperations::Exec(input.data[0], [&](index_t i, index_t k) {
		if (input.data[0].nullmask[i]) {
			keys.push_back(nullptr);
		} else {
			keys.push_back(Key::CreateKey<T>(input_data[i], is_little_endian));
		}
	});
}

void ART::GenerateKeys(DataChunk &input, vector<unique_ptr<Key>> &keys) {
	keys.reserve(STANDARD_VECTOR_SIZE);

	switch (input.data[0].type) {
	case TypeId::INT8:
		generate_keys<int8_t>(input, keys, is_little_endian);
		break;
	case TypeId::INT16:
		generate_keys<int16_t>(input, keys, is_little_endian);
		break;
	case TypeId::INT32:
		generate_keys<int32_t>(input, keys, is_little_endian);
		break;
	case TypeId::INT64:
		generate_keys<int64_t>(input, keys, is_little_endian);
		break;
	case TypeId::FLOAT:
		generate_keys<float>(input, keys, is_little_endian);
		break;
	case TypeId::DOUBLE:
		generate_keys<double>(input, keys, is_little_endian);
		break;
	default:
		throw InvalidTypeException(input.data[0].type, "Invalid type for index");
	}
}

bool ART::Insert(IndexLock &lock, DataChunk &input, Vector &row_ids) {
	assert(row_ids.type == ROW_TYPE);
	assert(input.size() == row_ids.count);
	assert(types[0] == input.data[0].type);

	// generate the keys for the given input
	vector<unique_ptr<Key>> keys;
	GenerateKeys(input, keys);

	// now insert the elements into the index
	row_ids.Normalify();
	auto row_identifiers = (row_t *)row_ids.GetData();
	index_t failed_index = INVALID_INDEX;
	for (index_t i = 0; i < row_ids.count; i++) {
		if (!keys[i]) {
			continue;
		}
		row_t row_id = row_identifiers[row_ids.sel_vector ? row_ids.sel_vector[i] : i];
		if (!Insert(tree, move(keys[i]), 0, row_id)) {
			// failed to insert because of constraint violation
			failed_index = i;
			break;
		}
	}
	if (failed_index != INVALID_INDEX) {
		// failed to insert because of constraint violation: remove previously inserted entries
		// generate keys again
		keys.clear();
		GenerateKeys(input, keys);
		// now erase the entries
		for (index_t i = 0; i < failed_index; i++) {
			if (!keys[i]) {
				continue;
			}
			index_t k = row_ids.sel_vector ? row_ids.sel_vector[i] : i;
			row_t row_id = row_identifiers[k];
			Erase(tree, *keys[i], 0, row_id);
		}
		return false;
	}
	return true;
}

bool ART::Append(IndexLock &lock, DataChunk &appended_data, Vector &row_identifiers) {
	// first resolve the expressions for the index
	ExecuteExpressions(appended_data, expression_result);

	// now insert into the index
	return Insert(lock, expression_result, row_identifiers);
}

void ART::VerifyAppend(DataChunk &chunk) {
	if (!is_unique) {
		return;
	}

	// unique index, check
	lock_guard<mutex> l(lock);

	// first resolve the expressions for the index
	ExecuteExpressions(chunk, expression_result);

	// generate the keys for the given input
	vector<unique_ptr<Key>> keys;
	GenerateKeys(expression_result, keys);

	// now insert the elements into the index
	for (index_t i = 0; i < expression_result.size(); i++) {
		if (!keys[i]) {
			continue;
		}
		if (Lookup(tree, *keys[i], 0) != nullptr) {
			// node already exists in tree
			throw ConstraintException("duplicate key value violates primary key or unique constraint");
		}
	}
}

bool ART::InsertToLeaf(Leaf &leaf, row_t row_id) {
	if (is_unique && leaf.num_elements != 0) {
		return false;
	}
	leaf.Insert(row_id);
	return true;
}

bool ART::Insert(unique_ptr<Node> &node, unique_ptr<Key> value, unsigned depth, row_t row_id) {
	Key &key = *value;
	if (!node) {
		// node is currently empty, create a leaf here with the key
		node = make_unique<Leaf>(*this, move(value), row_id);
		return true;
	}

	if (node->type == NodeType::NLeaf) {
		// Replace leaf with Node4 and store both leaves in it
		auto leaf = static_cast<Leaf *>(node.get());

		Key &existingKey = *leaf->value;
		uint32_t newPrefixLength = 0;
		// Leaf node is already there, update row_id vector
		if (depth + newPrefixLength == maxPrefix) {
			return InsertToLeaf(*leaf, row_id);
		}
		while (existingKey[depth + newPrefixLength] == key[depth + newPrefixLength]) {
			newPrefixLength++;
			// Leaf node is already there, update row_id vector
			if (depth + newPrefixLength == maxPrefix) {
				return InsertToLeaf(*leaf, row_id);
			}
		}
		unique_ptr<Node> newNode = make_unique<Node4>(*this);
		newNode->prefix_length = newPrefixLength;
		memcpy(newNode->prefix.get(), &key[depth], std::min(newPrefixLength, maxPrefix));
		Node4::insert(*this, newNode, existingKey[depth + newPrefixLength], node);
		unique_ptr<Node> leaf_node = make_unique<Leaf>(*this, move(value), row_id);
		Node4::insert(*this, newNode, key[depth + newPrefixLength], leaf_node);
		node = move(newNode);
		return true;
	}

	// Handle prefix of inner node
	if (node->prefix_length) {
		uint32_t mismatchPos = Node::PrefixMismatch(*this, node.get(), key, depth);
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
			unique_ptr<Node> leaf_node = make_unique<Leaf>(*this, move(value), row_id);

			Node4::insert(*this, newNode, key[depth + mismatchPos], leaf_node);
			node = move(newNode);
			return true;
		}
		depth += node->prefix_length;
	}

	// Recurse
	index_t pos = node->GetChildPos(key[depth]);
	if (pos != INVALID_INDEX) {
		auto child = node->GetChild(pos);
		return Insert(*child, move(value), depth + 1, row_id);
	}

	unique_ptr<Node> newNode = make_unique<Leaf>(*this, move(value), row_id);
	Node::InsertLeaf(*this, node, key[depth], newNode);
	return true;
}

//===--------------------------------------------------------------------===//
// Delete
//===--------------------------------------------------------------------===//
void ART::Delete(IndexLock &state, DataChunk &input, Vector &row_ids) {
	// first resolve the expressions
	ExecuteExpressions(input, expression_result);

	// then generate the keys for the given input
	vector<unique_ptr<Key>> keys;
	GenerateKeys(expression_result, keys);

	// now erase the elements from the database
	VectorOperations::ExecNumeric<row_t>(row_ids, [&](row_t row_id, index_t i, index_t k) {
		if (!keys[k]) {
			return;
		}
		Erase(tree, *keys[k], 0, row_id);
		// assert that the entry was erased properly
		assert(!is_unique || Lookup(tree, *keys[k], 0) == nullptr);
	});
}

void ART::Erase(unique_ptr<Node> &node, Key &key, unsigned depth, row_t row_id) {
	if (!node) {
		return;
	}
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
		if (Node::PrefixMismatch(*this, node.get(), key, depth) != node->prefix_length) {
			return;
		}
		depth += node->prefix_length;
	}
	index_t pos = node->GetChildPos(key[depth]);
	if (pos != INVALID_INDEX) {
		auto child = node->GetChild(pos);
		assert(child);

		unique_ptr<Node> &child_ref = *child;
		if (child_ref->type == NodeType::NLeaf && LeafMatches(child_ref.get(), key, depth)) {
			// Leaf found, remove entry
			auto leaf = static_cast<Leaf *>(child_ref.get());
			if (leaf->num_elements > 1) {
				// leaf has multiple rows: remove the row from the leaf
				leaf->Remove(row_id);
			} else {
				// Leaf only has one element, delete leaf, decrement node counter and maybe shrink node
				Node::Erase(*this, node, pos);
			}
		} else {
			// Recurse
			Erase(*child, key, depth + 1, row_id);
		}
	}
}

//===--------------------------------------------------------------------===//
// Point Query
//===--------------------------------------------------------------------===//
static unique_ptr<Key> CreateKey(ART &art, TypeId type, Value &value) {
	assert(type == value.type);
	switch (type) {
	case TypeId::INT8:
		return Key::CreateKey<int8_t>(value.value_.tinyint, art.is_little_endian);
	case TypeId::INT16:
		return Key::CreateKey<int16_t>(value.value_.smallint, art.is_little_endian);
	case TypeId::INT32:
		return Key::CreateKey<int32_t>(value.value_.integer, art.is_little_endian);
	case TypeId::INT64:
		return Key::CreateKey<int64_t>(value.value_.bigint, art.is_little_endian);
	case TypeId::FLOAT:
		return Key::CreateKey<float>(value.value_.float_, art.is_little_endian);
	case TypeId::DOUBLE:
		return Key::CreateKey<double>(value.value_.double_, art.is_little_endian);
	default:
		throw InvalidTypeException(type, "Invalid type for index");
	}
}

void ART::SearchEqual(vector<row_t> &result_ids, ARTIndexScanState *state) {
	unique_ptr<Key> key = CreateKey(*this, types[0], state->values[0]);
	auto leaf = static_cast<Leaf *>(Lookup(tree, *key, 0));
	if (!leaf) {
		return;
	}
	for (index_t i = 0; i < leaf->num_elements; i++) {
		row_t row_id = leaf->GetRowId(i);
		result_ids.push_back(row_id);
	}
}

Node *ART::Lookup(unique_ptr<Node> &node, Key &key, unsigned depth) {
	bool skippedPrefix = false; // Did we optimistically skip some prefix without checking it?
	auto node_val = node.get();

	while (node_val) {
		if (node_val->type == NodeType::NLeaf) {
			if (!skippedPrefix && depth == maxPrefix) {
				// No check required
				return node_val;
			}

			if (depth != maxPrefix) {
				// Check leaf
				auto leaf = static_cast<Leaf *>(node_val);
				Key &leafKey = *leaf->value;
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
		index_t pos = node_val->GetChildPos(key[depth]);
		if (pos == INVALID_INDEX) {
			return nullptr;
		}
		node_val = node_val->GetChild(pos)->get();
		assert(node_val);

		depth++;
	}

	return nullptr;
}

//===--------------------------------------------------------------------===//
// Iterator scans
//===--------------------------------------------------------------------===//
template <bool HAS_BOUND, bool INCLUSIVE>
void ART::IteratorScan(ARTIndexScanState *state, Iterator *it, vector<row_t> &result_ids, Key *bound) {
	bool has_next;
	do {
		if (HAS_BOUND) {
			assert(bound);
			if (INCLUSIVE) {
				if (*it->node->value > *bound) {
					break;
				}
			} else {
				if (*it->node->value >= *bound) {
					break;
				}
			}
		}
		for (index_t i = 0; i < it->node->num_elements; i++) {
			row_t row_id = it->node->GetRowId(i);
			result_ids.push_back(row_id);
		}
		has_next = ART::IteratorNext(*it);
	} while (has_next);
}

bool ART::IteratorNext(Iterator &it) {
	// Skip leaf
	if ((it.depth) && ((it.stack[it.depth - 1].node)->type == NodeType::NLeaf)) {
		it.depth--;
	}

	// Look for the next leaf
	while (it.depth > 0) {
		auto &top = it.stack[it.depth - 1];
		Node *node = top.node;

		if (node->type == NodeType::NLeaf) {
			// found a leaf: move to next node
			it.node = (Leaf *)node;
			return true;
		}

		// Find next node
		top.pos = node->GetNextPos(top.pos);
		if (top.pos != INVALID_INDEX) {
			// next node found: go there
			it.stack[it.depth].node = node->GetChild(top.pos)->get();
			it.stack[it.depth].pos = INVALID_INDEX;
			it.depth++;
		} else {
			// no node found: move up the tree
			it.depth--;
		}
	}
	return false;
}

//===--------------------------------------------------------------------===//
// Greater Than
// Returns: True (If found leaf >= key)
//          False (Otherwise)
//===--------------------------------------------------------------------===//
bool ART::Bound(unique_ptr<Node> &n, Key &key, Iterator &it, bool inclusive) {
	it.depth = 0;
	if (!n) {
		return false;
	}
	Node *node = n.get();

	index_t depth = 0;
	while (true) {
		auto &top = it.stack[it.depth];
		top.node = node;
		it.depth++;

		if (node->type == NodeType::NLeaf) {
			// found a leaf node: check if it is bigger or equal than the current key
			auto leaf = static_cast<Leaf *>(node);
			it.node = leaf;
			// if the search is not inclusive the leaf node could still be equal to the current value
			// check if leaf is equal to the current key
			if (*leaf->value == key) {
				// if its not inclusive check if there is a next leaf
				if (!inclusive && !IteratorNext(it)) {
					return false;
				} else {
					return true;
				}
			}

			if (*leaf->value > key) {
				return true;
			}
			// Leaf is lower than key
			// Check if next leaf is still lower than key
			while (IteratorNext(it)) {
				if (*it.node->value == key) {
					// if its not inclusive check if there is a next leaf
					if (!inclusive && !IteratorNext(it)) {
						return false;
					} else {
						return true;
					}
				} else if (*it.node->value > key) {
					// if its not inclusive check if there is a next leaf
					return true;
				}
			}
			return false;
		}
		uint32_t mismatchPos = Node::PrefixMismatch(*this, node, key, depth);
		if (mismatchPos != node->prefix_length) {
			if (node->prefix[mismatchPos] < key[depth + mismatchPos]) {
				// Less
				it.depth--;
				return IteratorNext(it);
			} else {
				// Greater
				top.pos = INVALID_INDEX;
				return IteratorNext(it);
			}
		}
		// prefix matches, search inside the child for the key
		depth += node->prefix_length;

		top.pos = node->GetChildGreaterEqual(key[depth]);

		if (top.pos == INVALID_INDEX) {
			// Find min leaf
			top.pos = node->GetMin();
		}
		node = node->GetChild(top.pos)->get();
		depth++;
	}
}

void ART::SearchGreater(vector<row_t> &result_ids, ARTIndexScanState *state, bool inclusive) {
	Iterator *it = &state->iterator;
	auto key = CreateKey(*this, types[0], state->values[0]);

	// greater than scan: first set the iterator to the node at which we will start our scan by finding the lowest node
	// that satisfies our requirement
	if (!it->start) {
		bool found = ART::Bound(tree, *key, *it, inclusive);
		if (!found) {
			return;
		}
		it->start = true;
	}
	// after that we continue the scan; we don't need to check the bounds as any value following this value is
	// automatically bigger and hence satisfies our predicate
	IteratorScan<false, false>(state, it, result_ids, nullptr);
}

//===--------------------------------------------------------------------===//
// Less Than
//===--------------------------------------------------------------------===//
static Leaf &FindMinimum(Iterator &it, Node &node) {
	Node *next = nullptr;
	index_t pos = 0;
	switch (node.type) {
	case NodeType::NLeaf:
		it.node = (Leaf *)&node;
		return (Leaf &)node;
	case NodeType::N4:
		next = ((Node4 &)node).child[0].get();
		break;
	case NodeType::N16:
		next = ((Node16 &)node).child[0].get();
		break;
	case NodeType::N48: {
		auto &n48 = (Node48 &)node;
		while (n48.childIndex[pos] == Node::EMPTY_MARKER) {
			pos++;
		}
		next = n48.child[n48.childIndex[pos]].get();
		break;
	}
	case NodeType::N256: {
		auto &n256 = (Node256 &)node;
		while (!n256.child[pos]) {
			pos++;
		}
		next = n256.child[pos].get();
		break;
	}
	}
	it.stack[it.depth].node = &node;
	it.stack[it.depth].pos = pos;
	it.depth++;
	return FindMinimum(it, *next);
}

void ART::SearchLess(vector<row_t> &result_ids, ARTIndexScanState *state, bool inclusive) {
	if (!tree) {
		return;
	}

	Iterator *it = &state->iterator;
	auto upper_bound = CreateKey(*this, types[0], state->values[0]);

	if (!it->start) {
		// first find the minimum value in the ART: we start scanning from this value
		auto &minimum = FindMinimum(state->iterator, *tree);
		// early out min value higher than upper bound query
		if (*minimum.value > *upper_bound) {
			return;
		}
		it->start = true;
	}
	// now continue the scan until we reach the upper bound
	if (inclusive) {
		IteratorScan<true, true>(state, it, result_ids, upper_bound.get());
	} else {
		IteratorScan<true, false>(state, it, result_ids, upper_bound.get());
	}
}

//===--------------------------------------------------------------------===//
// Closed Range Query
//===--------------------------------------------------------------------===//
void ART::SearchCloseRange(vector<row_t> &result_ids, ARTIndexScanState *state, bool left_inclusive,
                           bool right_inclusive) {
	auto lower_bound = CreateKey(*this, types[0], state->values[0]);
	auto upper_bound = CreateKey(*this, types[0], state->values[1]);
	Iterator *it = &state->iterator;
	// first find the first node that satisfies the left predicate
	if (!it->start) {
		bool found = ART::Bound(tree, *lower_bound, *it, left_inclusive);
		if (!found) {
			return;
		}
		it->start = true;
	}
	// now continue the scan until we reach the upper bound
	if (right_inclusive) {
		IteratorScan<true, true>(state, it, result_ids, upper_bound.get());
	} else {
		IteratorScan<true, false>(state, it, result_ids, upper_bound.get());
	}
}

void ART::Scan(Transaction &transaction, TableIndexScanState &table_state, DataChunk &result) {
	auto state = (ARTIndexScanState *)table_state.index_state.get();

	// scan the index
	if (!state->checked) {
		vector<row_t> result_ids;
		assert(state->values[0].type == types[0]);

		if (state->values[1].is_null) {
			lock_guard<mutex> l(lock);
			// single predicate
			switch (state->expressions[0]) {
			case ExpressionType::COMPARE_EQUAL:
				SearchEqual(result_ids, state);
				break;
			case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
				SearchGreater(result_ids, state, true);
				break;
			case ExpressionType::COMPARE_GREATERTHAN:
				SearchGreater(result_ids, state, false);
				break;
			case ExpressionType::COMPARE_LESSTHANOREQUALTO:
				SearchLess(result_ids, state, true);
				break;
			case ExpressionType::COMPARE_LESSTHAN:
				SearchLess(result_ids, state, false);
				break;
			default:
				throw NotImplementedException("Operation not implemented");
			}
		} else {
			lock_guard<mutex> l(lock);
			// two predicates
			assert(state->values[1].type == types[0]);
			bool left_inclusive = state->expressions[0] == ExpressionType ::COMPARE_GREATERTHANOREQUALTO;
			bool right_inclusive = state->expressions[1] == ExpressionType ::COMPARE_LESSTHANOREQUALTO;
			SearchCloseRange(result_ids, state, left_inclusive, right_inclusive);
		}
		state->checked = true;

		if (result_ids.size() == 0) {
			return;
		}

		// sort the row ids
		sort(result_ids.begin(), result_ids.end());
		// duplicate eliminate the row ids and append them to the row ids of the state
		state->result_ids.reserve(result_ids.size());

		state->result_ids.push_back(result_ids[0]);
		for (index_t i = 1; i < result_ids.size(); i++) {
			if (result_ids[i] != result_ids[i - 1]) {
				state->result_ids.push_back(result_ids[i]);
			}
		}
	}

	if (state->result_index >= state->result_ids.size()) {
		// exhausted all row ids
		return;
	}

	// create a vector pointing to the current set of row ids
	Vector row_identifiers(ROW_TYPE, (data_ptr_t)&state->result_ids[state->result_index]);
	row_identifiers.count =
	    std::min((index_t)STANDARD_VECTOR_SIZE, (index_t)state->result_ids.size() - state->result_index);

	// fetch the actual values from the base table
	table.Fetch(transaction, result, state->column_ids, row_identifiers, table_state);

	// move to the next set of row ids
	state->result_index += row_identifiers.count;
}
