#include "duckdb/execution/index/art/art.hpp"

#include "duckdb/common/radix.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/expression_executor.hpp"

#include <algorithm>
#include <cstring>
#include <ctgmath>

namespace duckdb {

ART::ART(const vector<column_t> &column_ids, const vector<unique_ptr<Expression>> &unbound_expressions,
         IndexConstraintType constraint_type, DatabaseInstance &db, idx_t block_id, idx_t block_offset)
    : Index(IndexType::ART, column_ids, unbound_expressions, constraint_type), db(db) {
	if (block_id != DConstants::INVALID_INDEX) {
		tree = Node::Deserialize(*this, block_id, block_offset);
	} else {
		tree = nullptr;
	}
	for (idx_t i = 0; i < types.size(); i++) {
		switch (types[i]) {
		case PhysicalType::BOOL:
		case PhysicalType::INT8:
		case PhysicalType::INT16:
		case PhysicalType::INT32:
		case PhysicalType::INT64:
		case PhysicalType::INT128:
		case PhysicalType::UINT8:
		case PhysicalType::UINT16:
		case PhysicalType::UINT32:
		case PhysicalType::UINT64:
		case PhysicalType::FLOAT:
		case PhysicalType::DOUBLE:
		case PhysicalType::VARCHAR:
			break;
		default:
			throw InvalidTypeException(logical_types[i], "Invalid type for index");
		}
	}
}

ART::~ART() {
	if (tree) {
		delete tree;
		tree = nullptr;
	}
}

unique_ptr<IndexScanState> ART::InitializeScanSinglePredicate(Transaction &transaction, Value value,
                                                              ExpressionType expression_type) {
	auto result = make_unique<ARTIndexScanState>();
	result->values[0] = value;
	result->expressions[0] = expression_type;
	return move(result);
}

unique_ptr<IndexScanState> ART::InitializeScanTwoPredicates(Transaction &transaction, Value low_value,
                                                            ExpressionType low_expression_type, Value high_value,
                                                            ExpressionType high_expression_type) {
	auto result = make_unique<ARTIndexScanState>();
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
static void TemplatedGenerateKeys(Vector &input, idx_t count, vector<unique_ptr<Key>> &keys) {
	UnifiedVectorFormat idata;
	input.ToUnifiedFormat(count, idata);

	auto input_data = (T *)idata.data;
	for (idx_t i = 0; i < count; i++) {
		auto idx = idata.sel->get_index(i);
		if (idata.validity.RowIsValid(idx)) {
			keys.push_back(Key::CreateKey<T>(input_data[idx]));
		} else {
			keys.push_back(nullptr);
		}
	}
}

template <class T>
static void ConcatenateKeys(Vector &input, idx_t count, vector<unique_ptr<Key>> &keys) {
	UnifiedVectorFormat idata;
	input.ToUnifiedFormat(count, idata);

	auto input_data = (T *)idata.data;
	for (idx_t i = 0; i < count; i++) {
		auto idx = idata.sel->get_index(i);
		if (!idata.validity.RowIsValid(idx) || !keys[i]) {
			// either this column is NULL, or the previous column is NULL!
			keys[i] = nullptr;
		} else {
			// concatenate the keys
			auto old_key = move(keys[i]);
			auto new_key = Key::CreateKey<T>(input_data[idx]);
			auto key_len = old_key->len + new_key->len;
			auto compound_data = unique_ptr<data_t[]>(new data_t[key_len]);
			memcpy(compound_data.get(), old_key->data.get(), old_key->len);
			memcpy(compound_data.get() + old_key->len, new_key->data.get(), new_key->len);
			keys[i] = make_unique<Key>(move(compound_data), key_len);
		}
	}
}

void ART::GenerateKeys(DataChunk &input, vector<unique_ptr<Key>> &keys) {
	keys.reserve(STANDARD_VECTOR_SIZE);
	// generate keys for the first input column
	switch (input.data[0].GetType().InternalType()) {
	case PhysicalType::BOOL:
		TemplatedGenerateKeys<bool>(input.data[0], input.size(), keys);
		break;
	case PhysicalType::INT8:
		TemplatedGenerateKeys<int8_t>(input.data[0], input.size(), keys);
		break;
	case PhysicalType::INT16:
		TemplatedGenerateKeys<int16_t>(input.data[0], input.size(), keys);
		break;
	case PhysicalType::INT32:
		TemplatedGenerateKeys<int32_t>(input.data[0], input.size(), keys);
		break;
	case PhysicalType::INT64:
		TemplatedGenerateKeys<int64_t>(input.data[0], input.size(), keys);
		break;
	case PhysicalType::INT128:
		TemplatedGenerateKeys<hugeint_t>(input.data[0], input.size(), keys);
		break;
	case PhysicalType::UINT8:
		TemplatedGenerateKeys<uint8_t>(input.data[0], input.size(), keys);
		break;
	case PhysicalType::UINT16:
		TemplatedGenerateKeys<uint16_t>(input.data[0], input.size(), keys);
		break;
	case PhysicalType::UINT32:
		TemplatedGenerateKeys<uint32_t>(input.data[0], input.size(), keys);
		break;
	case PhysicalType::UINT64:
		TemplatedGenerateKeys<uint64_t>(input.data[0], input.size(), keys);
		break;
	case PhysicalType::FLOAT:
		TemplatedGenerateKeys<float>(input.data[0], input.size(), keys);
		break;
	case PhysicalType::DOUBLE:
		TemplatedGenerateKeys<double>(input.data[0], input.size(), keys);
		break;
	case PhysicalType::VARCHAR:
		TemplatedGenerateKeys<string_t>(input.data[0], input.size(), keys);
		break;
	default:
		throw InternalException("Invalid type for index");
	}

	for (idx_t i = 1; i < input.ColumnCount(); i++) {
		// for each of the remaining columns, concatenate
		switch (input.data[i].GetType().InternalType()) {
		case PhysicalType::BOOL:
			ConcatenateKeys<bool>(input.data[i], input.size(), keys);
			break;
		case PhysicalType::INT8:
			ConcatenateKeys<int8_t>(input.data[i], input.size(), keys);
			break;
		case PhysicalType::INT16:
			ConcatenateKeys<int16_t>(input.data[i], input.size(), keys);
			break;
		case PhysicalType::INT32:
			ConcatenateKeys<int32_t>(input.data[i], input.size(), keys);
			break;
		case PhysicalType::INT64:
			ConcatenateKeys<int64_t>(input.data[i], input.size(), keys);
			break;
		case PhysicalType::INT128:
			ConcatenateKeys<hugeint_t>(input.data[i], input.size(), keys);
			break;
		case PhysicalType::UINT8:
			ConcatenateKeys<uint8_t>(input.data[i], input.size(), keys);
			break;
		case PhysicalType::UINT16:
			ConcatenateKeys<uint16_t>(input.data[i], input.size(), keys);
			break;
		case PhysicalType::UINT32:
			ConcatenateKeys<uint32_t>(input.data[i], input.size(), keys);
			break;
		case PhysicalType::UINT64:
			ConcatenateKeys<uint64_t>(input.data[i], input.size(), keys);
			break;
		case PhysicalType::FLOAT:
			ConcatenateKeys<float>(input.data[i], input.size(), keys);
			break;
		case PhysicalType::DOUBLE:
			ConcatenateKeys<double>(input.data[i], input.size(), keys);
			break;
		case PhysicalType::VARCHAR:
			ConcatenateKeys<string_t>(input.data[i], input.size(), keys);
			break;
		default:
			throw InternalException("Invalid type for index");
		}
	}
}

struct KeySection {
	KeySection(idx_t start_p, idx_t end_p, idx_t depth_p, data_t key_byte_p)
	    : start(start_p), end(end_p), depth(depth_p), key_byte(key_byte_p) {};
	KeySection(idx_t start_p, idx_t end_p, vector<unique_ptr<Key>> &keys, KeySection &key_section)
	    : start(start_p), end(end_p), depth(key_section.depth + 1), key_byte(keys[end_p]->data[key_section.depth]) {};
	idx_t start;
	idx_t end;
	idx_t depth;
	data_t key_byte;
};

void GetChildSections(vector<KeySection> &child_sections, vector<unique_ptr<Key>> &keys, KeySection &key_section) {

	idx_t child_start_idx = key_section.start;
	for (idx_t i = key_section.start + 1; i <= key_section.end; i++) {
		if (keys[i - 1]->data[key_section.depth] != keys[i]->data[key_section.depth]) {
			child_sections.emplace_back(child_start_idx, i - 1, keys, key_section);
			child_start_idx = i;
		}
	}
	child_sections.emplace_back(child_start_idx, key_section.end, keys, key_section);
}

void Construct(vector<unique_ptr<Key>> &keys, row_t *row_ids, Node *&node, KeySection &key_section,
               bool &has_constraint) {

	D_ASSERT(key_section.start < keys.size());
	D_ASSERT(key_section.end < keys.size());
	D_ASSERT(key_section.start <= key_section.end);

	auto &start_key = *keys[key_section.start];
	auto &end_key = *keys[key_section.end];

	// increment the depth until we reach a leaf or find a mismatching byte
	auto prefix_start = key_section.depth;
	while (start_key.len != key_section.depth && start_key.ByteMatches(end_key, key_section.depth)) {
		key_section.depth++;
	}

	// we reached a leaf, i.e. all the bytes of start_key and end_key match
	if (start_key.len == key_section.depth) {

		// end_idx is inclusive
		auto num_row_ids = key_section.end - key_section.start + 1;

		// check for possible constraint violation
		if (has_constraint && num_row_ids != 1) {
			throw ConstraintException("New data contains duplicates on indexed column(s)");
		}

		// new row ids of this leaf
		auto new_row_ids = unique_ptr<row_t[]>(new row_t[num_row_ids]);
		for (idx_t i = 0; i < num_row_ids; i++) {
			new_row_ids[i] = row_ids[key_section.start + i];
		}

		node = new Leaf(start_key, prefix_start, move(new_row_ids), num_row_ids);

	} else { // create a new node and recurse

		// we will find at least two child entries of this node, otherwise we'd have reached a leaf
		vector<KeySection> child_sections;
		GetChildSections(child_sections, keys, key_section);

		auto node_type = Node::GetTypeBySize(child_sections.size());
		Node::New(node_type, node);

		auto prefix_length = key_section.depth - prefix_start;
		node->prefix = Prefix(start_key, prefix_start, prefix_length);

		// recurse on each child section
		for (auto &child_section : child_sections) {
			Node *new_child = nullptr;
			Construct(keys, row_ids, new_child, child_section, has_constraint);
			Node::InsertChild(node, child_section.key_byte, new_child);
		}
	}
}

void FindFirstNotNullKey(vector<unique_ptr<Key>> &keys, bool &skipped_all_nulls, idx_t &start_idx) {

	if (!skipped_all_nulls) {
		for (idx_t i = 0; i < keys.size(); i++) {
			if (keys[i]) {
				start_idx = i;
				skipped_all_nulls = true;
				return;
			}
		}
	}
}

void ART::ConstructAndMerge(IndexLock &lock, PayloadScanner &scanner, Allocator &allocator) {

	auto payload_types = logical_types;
	payload_types.emplace_back(LogicalType::ROW_TYPE);

	auto skipped_all_nulls = false;
	auto temp_art = make_unique<ART>(this->column_ids, this->unbound_expressions, this->constraint_type, this->db);
	for (;;) {
		DataChunk ordered_chunk;
		ordered_chunk.Initialize(allocator, payload_types);
		ordered_chunk.SetCardinality(0);
		scanner.Scan(ordered_chunk);
		if (ordered_chunk.size() == 0) {
			break;
		}

		// get the key chunk and the row_identifiers vector
		DataChunk row_id_chunk;
		ordered_chunk.Split(row_id_chunk, ordered_chunk.ColumnCount() - 1);
		auto &row_identifiers = row_id_chunk.data[0];

		D_ASSERT(row_identifiers.GetType().InternalType() == ROW_TYPE);
		D_ASSERT(logical_types[0] == ordered_chunk.data[0].GetType());

		// generate the keys for the given input
		vector<unique_ptr<Key>> keys;
		GenerateKeys(ordered_chunk, keys);

		// we order NULLS FIRST, so we might have to skip nulls at the start of our sorted data
		idx_t start_idx = 0;
		FindFirstNotNullKey(keys, skipped_all_nulls, start_idx);

		if (start_idx != 0 && IsPrimary()) {
			throw ConstraintException("NULLs in new data violate the primary key constraint of the index");
		}

		if (!skipped_all_nulls) {
			if (IsPrimary()) {
				// chunk consists only of NULLs
				throw ConstraintException("NULLs in new data violate the primary key constraint of the index");
			}
			continue;
		}

		// prepare the row_identifiers
		row_identifiers.Flatten(ordered_chunk.size());
		auto row_ids = FlatVector::GetData<row_t>(row_identifiers);

		// construct the ART of this chunk
		auto art = make_unique<ART>(this->column_ids, this->unbound_expressions, this->constraint_type, this->db);
		auto key_section = KeySection(start_idx, ordered_chunk.size() - 1, 0, 0);
		auto has_constraint = IsPrimary() || IsUnique();
		Construct(keys, row_ids, art->tree, key_section, has_constraint);

		// merge art into temp_art
		ART::Merge(temp_art.get(), art.get());
	}

	// NOTE: currently this code is only used for index creation, so we can assume that there are no
	// duplicate violations between the existing index and the new data
	ART::Merge(this, temp_art.get());
}

bool ART::Insert(IndexLock &lock, DataChunk &input, Vector &row_ids) {
	D_ASSERT(row_ids.GetType().InternalType() == ROW_TYPE);
	D_ASSERT(logical_types[0] == input.data[0].GetType());

	// generate the keys for the given input
	vector<unique_ptr<Key>> keys;
	GenerateKeys(input, keys);

	// now insert the elements into the index
	row_ids.Flatten(input.size());
	auto row_identifiers = FlatVector::GetData<row_t>(row_ids);
	idx_t failed_index = DConstants::INVALID_INDEX;
	for (idx_t i = 0; i < input.size(); i++) {
		if (!keys[i]) {
			continue;
		}

		row_t row_id = row_identifiers[i];
		if (!Insert(tree, move(keys[i]), 0, row_id)) {
			// failed to insert because of constraint violation
			failed_index = i;
			break;
		}
	}
	if (failed_index != DConstants::INVALID_INDEX) {
		// failed to insert because of constraint violation: remove previously inserted entries
		// generate keys again
		keys.clear();
		GenerateKeys(input, keys);
		unique_ptr<Key> key;

		// now erase the entries
		for (idx_t i = 0; i < failed_index; i++) {
			if (!keys[i]) {
				continue;
			}
			row_t row_id = row_identifiers[i];
			Erase(tree, *keys[i], 0, row_id);
		}
		return false;
	}
	return true;
}

bool ART::Append(IndexLock &lock, DataChunk &appended_data, Vector &row_identifiers) {
	DataChunk expression_result;
	expression_result.Initialize(Allocator::DefaultAllocator(), logical_types);

	// first resolve the expressions for the index
	ExecuteExpressions(appended_data, expression_result);

	// now insert into the index
	return Insert(lock, expression_result, row_identifiers);
}

void ART::VerifyAppend(DataChunk &chunk) {
	VerifyExistence(chunk, VerifyExistenceType::APPEND);
}

void ART::VerifyAppendForeignKey(DataChunk &chunk, string *err_msg_ptr) {
	VerifyExistence(chunk, VerifyExistenceType::APPEND_FK, err_msg_ptr);
}

void ART::VerifyDeleteForeignKey(DataChunk &chunk, string *err_msg_ptr) {
	VerifyExistence(chunk, VerifyExistenceType::DELETE_FK, err_msg_ptr);
}

bool ART::InsertToLeaf(Leaf &leaf, row_t row_id) {
#ifdef DEBUG
	for (idx_t k = 0; k < leaf.count; k++) {
		D_ASSERT(leaf.GetRowId(k) != row_id);
	}
#endif
	if (IsUnique() && leaf.count != 0) {
		return false;
	}
	leaf.Insert(row_id);
	return true;
}

bool ART::Insert(Node *&node, unique_ptr<Key> value, unsigned depth, row_t row_id) {
	Key &key = *value;
	if (!node) {
		// node is currently empty, create a leaf here with the key
		node = new Leaf(*value, depth, row_id);
		return true;
	}

	if (node->type == NodeType::NLeaf) {
		// Replace leaf with Node4 and store both leaves in it
		auto leaf = (Leaf *)node;

		auto &leaf_prefix = leaf->prefix;
		uint32_t new_prefix_length = 0;

		// Leaf node is already there (its key matches the current key), update row_id vector
		if (new_prefix_length == leaf->prefix.Size() && depth + leaf->prefix.Size() == key.len) {
			return InsertToLeaf(*leaf, row_id);
		}
		while (leaf_prefix[new_prefix_length] == key[depth + new_prefix_length]) {
			new_prefix_length++;
			// Leaf node is already there (its key matches the current key), update row_id vector
			if (new_prefix_length == leaf->prefix.Size() && depth + leaf->prefix.Size() == key.len) {
				return InsertToLeaf(*leaf, row_id);
			}
		}

		Node *new_node = new Node4();
		new_node->prefix = Prefix(key, depth, new_prefix_length);
		auto key_byte = node->prefix.Reduce(new_prefix_length);
		Node4::InsertChild(new_node, key_byte, node);
		Node *leaf_node = new Leaf(*value, depth + new_prefix_length + 1, row_id);
		Node4::InsertChild(new_node, key[depth + new_prefix_length], leaf_node);
		node = new_node;
		return true;
	}

	// Handle prefix of inner node
	if (node->prefix.Size()) {
		uint32_t mismatch_pos = node->prefix.KeyMismatchPosition(key, depth);
		if (mismatch_pos != node->prefix.Size()) {
			// Prefix differs, create new node
			Node *new_node = new Node4();
			new_node->prefix = Prefix(key, depth, mismatch_pos);
			// Break up prefix
			auto key_byte = node->prefix.Reduce(mismatch_pos);
			Node4::InsertChild(new_node, key_byte, node);

			Node *leaf_node = new Leaf(*value, depth + mismatch_pos + 1, row_id);
			Node4::InsertChild(new_node, key[depth + mismatch_pos], leaf_node);
			node = new_node;
			return true;
		}
		depth += node->prefix.Size();
	}

	// Recurse
	D_ASSERT(depth < key.len);
	idx_t pos = node->GetChildPos(key[depth]);
	if (pos != DConstants::INVALID_INDEX) {
		auto child = node->GetChild(*this, pos);
		bool insertion_result = Insert(child, move(value), depth + 1, row_id);
		node->ReplaceChildPointer(pos, child);
		return insertion_result;
	}
	Node *new_node = new Leaf(*value, depth + 1, row_id);
	Node::InsertChild(node, key[depth], new_node);
	return true;
}

//===--------------------------------------------------------------------===//
// Delete
//===--------------------------------------------------------------------===//
void ART::Delete(IndexLock &state, DataChunk &input, Vector &row_ids) {
	DataChunk expression;
	expression.Initialize(Allocator::DefaultAllocator(), logical_types);

	// first resolve the expressions
	ExecuteExpressions(input, expression);

	// then generate the keys for the given input
	vector<unique_ptr<Key>> keys;
	GenerateKeys(expression, keys);

	// now erase the elements from the database
	row_ids.Flatten(input.size());
	auto row_identifiers = FlatVector::GetData<row_t>(row_ids);

	for (idx_t i = 0; i < input.size(); i++) {
		if (!keys[i]) {
			continue;
		}
		Erase(tree, *keys[i], 0, row_identifiers[i]);
#ifdef DEBUG
		auto node = Lookup(tree, *keys[i], 0);
		if (node) {
			auto leaf = static_cast<Leaf *>(node);
			for (idx_t k = 0; k < leaf->count; k++) {
				D_ASSERT(leaf->GetRowId(k) != row_identifiers[i]);
			}
		}
#endif
	}
}

void ART::Erase(Node *&node, Key &key, unsigned depth, row_t row_id) {
	if (!node) {
		return;
	}
	// Delete a leaf from a tree
	if (node->type == NodeType::NLeaf) {
		// Make sure we have the right leaf
		auto leaf = static_cast<Leaf *>(node);
		leaf->Remove(row_id);
		if (leaf->count == 0) {
			delete node;
			node = nullptr;
		}

		return;
	}

	// Handle prefix
	if (node->prefix.Size()) {
		if (node->prefix.KeyMismatchPosition(key, depth) != node->prefix.Size()) {
			return;
		}
		depth += node->prefix.Size();
	}
	idx_t pos = node->GetChildPos(key[depth]);
	if (pos != DConstants::INVALID_INDEX) {
		auto child = node->GetChild(*this, pos);
		D_ASSERT(child);

		if (child->type == NodeType::NLeaf) {
			// Leaf found, remove entry
			auto leaf = (Leaf *)child;
			leaf->Remove(row_id);
			if (leaf->count == 0) {
				// Leaf is empty, delete leaf, decrement node counter and maybe shrink node
				Node::EraseChild(node, pos, *this);
			}
		} else {
			// Recurse
			Erase(child, key, depth + 1, row_id);
			node->ReplaceChildPointer(pos, child);
		}
	}
}

//===--------------------------------------------------------------------===//
// Point Query
//===--------------------------------------------------------------------===//
static unique_ptr<Key> CreateKey(ART &art, PhysicalType type, Value &value) {
	D_ASSERT(type == value.type().InternalType());
	switch (type) {
	case PhysicalType::BOOL:
		return Key::CreateKey<bool>(value);
	case PhysicalType::INT8:
		return Key::CreateKey<int8_t>(value);
	case PhysicalType::INT16:
		return Key::CreateKey<int16_t>(value);
	case PhysicalType::INT32:
		return Key::CreateKey<int32_t>(value);
	case PhysicalType::INT64:
		return Key::CreateKey<int64_t>(value);
	case PhysicalType::UINT8:
		return Key::CreateKey<uint8_t>(value);
	case PhysicalType::UINT16:
		return Key::CreateKey<uint16_t>(value);
	case PhysicalType::UINT32:
		return Key::CreateKey<uint32_t>(value);
	case PhysicalType::UINT64:
		return Key::CreateKey<uint64_t>(value);
	case PhysicalType::INT128:
		return Key::CreateKey<hugeint_t>(value);
	case PhysicalType::FLOAT:
		return Key::CreateKey<float>(value);
	case PhysicalType::DOUBLE:
		return Key::CreateKey<double>(value);
	case PhysicalType::VARCHAR:
		return Key::CreateKey<string_t>(value);
	default:
		throw InternalException("Invalid type for index");
	}
}

bool ART::SearchEqual(ARTIndexScanState *state, idx_t max_count, vector<row_t> &result_ids) {
	auto key = CreateKey(*this, types[0], state->values[0]);
	auto leaf = static_cast<Leaf *>(Lookup(tree, *key, 0));
	if (!leaf) {
		return true;
	}
	if (leaf->count > max_count) {
		return false;
	}
	for (idx_t i = 0; i < leaf->count; i++) {
		row_t row_id = leaf->GetRowId(i);
		result_ids.push_back(row_id);
	}
	return true;
}

void ART::SearchEqualJoinNoFetch(Value &equal_value, idx_t &result_size) {
	//! We need to look for a leaf
	auto key = CreateKey(*this, types[0], equal_value);
	auto leaf = (Leaf *)(Lookup(tree, *key, 0));
	if (!leaf) {
		return;
	}
	result_size = leaf->count;
}

Node *ART::Lookup(Node *node, Key &key, unsigned depth) {
	while (node) {
		if (node->type == NodeType::NLeaf) {
			auto leaf = (Leaf *)node;
			auto &leaf_prefix = leaf->prefix;
			//! Check leaf
			for (idx_t i = 0; i < leaf->prefix.Size(); i++) {
				if (leaf_prefix[i] != key[i + depth]) {
					return nullptr;
				}
			}
			return node;
		}
		if (node->prefix.Size()) {
			for (idx_t pos = 0; pos < node->prefix.Size(); pos++) {
				if (key[depth + pos] != node->prefix[pos]) {
					return nullptr;
				}
			}
			depth += node->prefix.Size();
		}
		idx_t pos = node->GetChildPos(key[depth]);
		if (pos == DConstants::INVALID_INDEX) {
			return nullptr;
		}
		node = node->GetChild(*this, pos);
		D_ASSERT(node);
		depth++;
	}
	return nullptr;
}

//===--------------------------------------------------------------------===//
// Greater Than
// Returns: True (If found leaf >= key)
//          False (Otherwise)
//===--------------------------------------------------------------------===//
bool ART::SearchGreater(ARTIndexScanState *state, bool inclusive, idx_t max_count, vector<row_t> &result_ids) {
	Iterator *it = &state->iterator;
	auto key = CreateKey(*this, types[0], state->values[0]);

	// greater than scan: first set the iterator to the node at which we will start our scan by finding the lowest node
	// that satisfies our requirement
	if (!it->art) {
		it->art = this;
		bool found = it->LowerBound(tree, *key, inclusive);
		if (!found) {
			return true;
		}
	}
	// after that we continue the scan; we don't need to check the bounds as any value following this value is
	// automatically bigger and hence satisfies our predicate
	return it->Scan(nullptr, max_count, result_ids, false);
}

//===--------------------------------------------------------------------===//
// Less Than
//===--------------------------------------------------------------------===//
bool ART::SearchLess(ARTIndexScanState *state, bool inclusive, idx_t max_count, vector<row_t> &result_ids) {
	if (!tree) {
		return true;
	}

	Iterator *it = &state->iterator;
	auto upper_bound = CreateKey(*this, types[0], state->values[0]);

	if (!it->art) {
		it->art = this;
		// first find the minimum value in the ART: we start scanning from this value
		it->FindMinimum(*tree);
		// early out min value higher than upper bound query
		if (it->cur_key > *upper_bound) {
			return true;
		}
	}
	// now continue the scan until we reach the upper bound
	return it->Scan(upper_bound.get(), max_count, result_ids, inclusive);
}

//===--------------------------------------------------------------------===//
// Closed Range Query
//===--------------------------------------------------------------------===//
bool ART::SearchCloseRange(ARTIndexScanState *state, bool left_inclusive, bool right_inclusive, idx_t max_count,
                           vector<row_t> &result_ids) {
	auto lower_bound = CreateKey(*this, types[0], state->values[0]);
	auto upper_bound = CreateKey(*this, types[0], state->values[1]);
	Iterator *it = &state->iterator;
	// first find the first node that satisfies the left predicate
	if (!it->art) {
		it->art = this;
		bool found = it->LowerBound(tree, *lower_bound, left_inclusive);
		if (!found) {
			return true;
		}
	}
	// now continue the scan until we reach the upper bound
	return it->Scan(upper_bound.get(), max_count, result_ids, right_inclusive);
}

bool ART::Scan(Transaction &transaction, DataTable &table, IndexScanState &table_state, idx_t max_count,
               vector<row_t> &result_ids) {
	auto state = (ARTIndexScanState *)&table_state;

	D_ASSERT(state->values[0].type().InternalType() == types[0]);

	vector<row_t> row_ids;
	bool success;
	if (state->values[1].IsNull()) {
		lock_guard<mutex> l(lock);
		// single predicate
		switch (state->expressions[0]) {
		case ExpressionType::COMPARE_EQUAL:
			success = SearchEqual(state, max_count, row_ids);
			break;
		case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
			success = SearchGreater(state, true, max_count, row_ids);
			break;
		case ExpressionType::COMPARE_GREATERTHAN:
			success = SearchGreater(state, false, max_count, row_ids);
			break;
		case ExpressionType::COMPARE_LESSTHANOREQUALTO:
			success = SearchLess(state, true, max_count, row_ids);
			break;
		case ExpressionType::COMPARE_LESSTHAN:
			success = SearchLess(state, false, max_count, row_ids);
			break;
		default:
			throw InternalException("Operation not implemented");
		}
	} else {
		lock_guard<mutex> l(lock);
		// two predicates
		D_ASSERT(state->values[1].type().InternalType() == types[0]);
		bool left_inclusive = state->expressions[0] == ExpressionType ::COMPARE_GREATERTHANOREQUALTO;
		bool right_inclusive = state->expressions[1] == ExpressionType ::COMPARE_LESSTHANOREQUALTO;
		success = SearchCloseRange(state, left_inclusive, right_inclusive, max_count, row_ids);
	}
	if (!success) {
		return false;
	}
	if (row_ids.empty()) {
		return true;
	}
	// sort the row ids
	sort(row_ids.begin(), row_ids.end());
	// duplicate eliminate the row ids and append them to the row ids of the state
	result_ids.reserve(row_ids.size());

	result_ids.push_back(row_ids[0]);
	for (idx_t i = 1; i < row_ids.size(); i++) {
		if (row_ids[i] != row_ids[i - 1]) {
			result_ids.push_back(row_ids[i]);
		}
	}
	return true;
}

void ART::VerifyExistence(DataChunk &chunk, VerifyExistenceType verify_type, string *err_msg_ptr) {
	if (verify_type != VerifyExistenceType::DELETE_FK && !IsUnique()) {
		return;
	}

	DataChunk expression_chunk;
	expression_chunk.Initialize(Allocator::DefaultAllocator(), logical_types);

	// unique index, check
	lock_guard<mutex> l(lock);
	// first resolve the expressions for the index
	ExecuteExpressions(chunk, expression_chunk);

	// generate the keys for the given input
	vector<unique_ptr<Key>> keys;
	GenerateKeys(expression_chunk, keys);

	for (idx_t i = 0; i < chunk.size(); i++) {
		if (!keys[i]) {
			continue;
		}
		Node *node_ptr = Lookup(tree, *keys[i], 0);
		bool throw_exception =
		    verify_type == VerifyExistenceType::APPEND_FK ? node_ptr == nullptr : node_ptr != nullptr;
		if (!throw_exception) {
			continue;
		}
		string key_name;
		for (idx_t k = 0; k < expression_chunk.ColumnCount(); k++) {
			if (k > 0) {
				key_name += ", ";
			}
			key_name += unbound_expressions[k]->GetName() + ": " + expression_chunk.data[k].GetValue(i).ToString();
		}
		string exception_msg;
		switch (verify_type) {
		case VerifyExistenceType::APPEND: {
			// node already exists in tree
			string type = IsPrimary() ? "primary key" : "unique";
			exception_msg = "duplicate key \"" + key_name + "\" violates ";
			exception_msg += type + " constraint";
			break;
		}
		case VerifyExistenceType::APPEND_FK: {
			// found node no exists in tree
			exception_msg =
			    "violates foreign key constraint because key \"" + key_name + "\" does not exist in referenced table";
			break;
		}
		case VerifyExistenceType::DELETE_FK: {
			// found node exists in tree
			exception_msg =
			    "violates foreign key constraint because key \"" + key_name + "\" exist in table has foreign key";
			break;
		}
		}
		if (err_msg_ptr) {
			err_msg_ptr[i] = exception_msg;
		} else {
			throw ConstraintException(exception_msg);
		}
	}
}

//===--------------------------------------------------------------------===//
// Serialization
//===--------------------------------------------------------------------===//
BlockPointer ART::Serialize(duckdb::MetaBlockWriter &writer) {
	lock_guard<mutex> l(lock);
	if (tree) {
		return tree->Serialize(*this, writer);
	}
	return {(block_id_t)DConstants::INVALID_INDEX, (uint32_t)DConstants::INVALID_INDEX};
}

//===--------------------------------------------------------------------===//
// Merge ARTs
//===--------------------------------------------------------------------===//
void ART::Merge(ART *l_art, ART *r_art) {

	if (!l_art->tree) {
		l_art->tree = r_art->tree;
		r_art->tree = nullptr;
		return;
	}

	Node::MergeARTs(l_art, r_art);
}

} // namespace duckdb
