#include "duckdb/execution/index/art/art.hpp"

#include "duckdb/common/radix.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/storage/arena_allocator.hpp"
#include "duckdb/execution/index/art/art_key.hpp"

#include <algorithm>
#include <cstring>
#include <ctgmath>

namespace duckdb {

ART::ART(const vector<column_t> &column_ids, TableIOManager &table_io_manager,
         const vector<unique_ptr<Expression>> &unbound_expressions, IndexConstraintType constraint_type,
         AttachedDatabase &db, bool track_memory, idx_t block_id, idx_t block_offset)

    : Index(db, IndexType::ART, table_io_manager, column_ids, unbound_expressions, constraint_type, track_memory) {

	if (!Radix::IsLittleEndian()) {
		throw NotImplementedException("ART indexes are not supported on big endian architectures");
	}

	// set the root node of the tree
	if (block_id != DConstants::INVALID_INDEX) {
		tree = Node::Deserialize(*this, block_id, block_offset);
	} else {
		tree = nullptr;
	}

	// validate the types of the key columns
	serialized_data_pointer = BlockPointer(block_id, block_offset);
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
		if (track_memory) {
			buffer_manager.DecreaseUsedMemory(memory_size);
		}
		Node::Delete(tree);
		tree = nullptr;
	}
}

//===--------------------------------------------------------------------===//
// Initialize Predicate Scans
//===--------------------------------------------------------------------===//

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
// Keys
//===--------------------------------------------------------------------===//

template <class T>
static void TemplatedGenerateKeys(ArenaAllocator &allocator, Vector &input, idx_t count, vector<Key> &keys) {
	UnifiedVectorFormat idata;
	input.ToUnifiedFormat(count, idata);

	D_ASSERT(keys.size() >= count);
	auto input_data = (T *)idata.data;
	for (idx_t i = 0; i < count; i++) {
		auto idx = idata.sel->get_index(i);
		if (idata.validity.RowIsValid(idx)) {
			Key::CreateKey<T>(allocator, keys[i], input_data[idx]);
		}
	}
}

template <class T>
static void ConcatenateKeys(ArenaAllocator &allocator, Vector &input, idx_t count, vector<Key> &keys) {
	UnifiedVectorFormat idata;
	input.ToUnifiedFormat(count, idata);

	auto input_data = (T *)idata.data;
	for (idx_t i = 0; i < count; i++) {
		auto idx = idata.sel->get_index(i);

		// key is not NULL (no previous column entry was NULL)
		if (!keys[i].Empty()) {
			if (!idata.validity.RowIsValid(idx)) {
				// this column entry is NULL, set whole key to NULL
				keys[i] = Key();
			} else {
				auto other_key = Key::CreateKey<T>(allocator, input_data[idx]);
				keys[i].ConcatenateKey(allocator, other_key);
			}
		}
	}
}

void ART::GenerateKeys(ArenaAllocator &allocator, DataChunk &input, vector<Key> &keys) {
	// generate keys for the first input column
	switch (input.data[0].GetType().InternalType()) {
	case PhysicalType::BOOL:
		TemplatedGenerateKeys<bool>(allocator, input.data[0], input.size(), keys);
		break;
	case PhysicalType::INT8:
		TemplatedGenerateKeys<int8_t>(allocator, input.data[0], input.size(), keys);
		break;
	case PhysicalType::INT16:
		TemplatedGenerateKeys<int16_t>(allocator, input.data[0], input.size(), keys);
		break;
	case PhysicalType::INT32:
		TemplatedGenerateKeys<int32_t>(allocator, input.data[0], input.size(), keys);
		break;
	case PhysicalType::INT64:
		TemplatedGenerateKeys<int64_t>(allocator, input.data[0], input.size(), keys);
		break;
	case PhysicalType::INT128:
		TemplatedGenerateKeys<hugeint_t>(allocator, input.data[0], input.size(), keys);
		break;
	case PhysicalType::UINT8:
		TemplatedGenerateKeys<uint8_t>(allocator, input.data[0], input.size(), keys);
		break;
	case PhysicalType::UINT16:
		TemplatedGenerateKeys<uint16_t>(allocator, input.data[0], input.size(), keys);
		break;
	case PhysicalType::UINT32:
		TemplatedGenerateKeys<uint32_t>(allocator, input.data[0], input.size(), keys);
		break;
	case PhysicalType::UINT64:
		TemplatedGenerateKeys<uint64_t>(allocator, input.data[0], input.size(), keys);
		break;
	case PhysicalType::FLOAT:
		TemplatedGenerateKeys<float>(allocator, input.data[0], input.size(), keys);
		break;
	case PhysicalType::DOUBLE:
		TemplatedGenerateKeys<double>(allocator, input.data[0], input.size(), keys);
		break;
	case PhysicalType::VARCHAR:
		TemplatedGenerateKeys<string_t>(allocator, input.data[0], input.size(), keys);
		break;
	default:
		throw InternalException("Invalid type for index");
	}

	for (idx_t i = 1; i < input.ColumnCount(); i++) {
		// for each of the remaining columns, concatenate
		switch (input.data[i].GetType().InternalType()) {
		case PhysicalType::BOOL:
			ConcatenateKeys<bool>(allocator, input.data[i], input.size(), keys);
			break;
		case PhysicalType::INT8:
			ConcatenateKeys<int8_t>(allocator, input.data[i], input.size(), keys);
			break;
		case PhysicalType::INT16:
			ConcatenateKeys<int16_t>(allocator, input.data[i], input.size(), keys);
			break;
		case PhysicalType::INT32:
			ConcatenateKeys<int32_t>(allocator, input.data[i], input.size(), keys);
			break;
		case PhysicalType::INT64:
			ConcatenateKeys<int64_t>(allocator, input.data[i], input.size(), keys);
			break;
		case PhysicalType::INT128:
			ConcatenateKeys<hugeint_t>(allocator, input.data[i], input.size(), keys);
			break;
		case PhysicalType::UINT8:
			ConcatenateKeys<uint8_t>(allocator, input.data[i], input.size(), keys);
			break;
		case PhysicalType::UINT16:
			ConcatenateKeys<uint16_t>(allocator, input.data[i], input.size(), keys);
			break;
		case PhysicalType::UINT32:
			ConcatenateKeys<uint32_t>(allocator, input.data[i], input.size(), keys);
			break;
		case PhysicalType::UINT64:
			ConcatenateKeys<uint64_t>(allocator, input.data[i], input.size(), keys);
			break;
		case PhysicalType::FLOAT:
			ConcatenateKeys<float>(allocator, input.data[i], input.size(), keys);
			break;
		case PhysicalType::DOUBLE:
			ConcatenateKeys<double>(allocator, input.data[i], input.size(), keys);
			break;
		case PhysicalType::VARCHAR:
			ConcatenateKeys<string_t>(allocator, input.data[i], input.size(), keys);
			break;
		default:
			throw InternalException("Invalid type for index");
		}
	}
}

//===--------------------------------------------------------------------===//
// Construct from sorted data
//===--------------------------------------------------------------------===//

struct KeySection {
	KeySection(idx_t start_p, idx_t end_p, idx_t depth_p, data_t key_byte_p)
	    : start(start_p), end(end_p), depth(depth_p), key_byte(key_byte_p) {};
	KeySection(idx_t start_p, idx_t end_p, vector<Key> &keys, KeySection &key_section)
	    : start(start_p), end(end_p), depth(key_section.depth + 1), key_byte(keys[end_p].data[key_section.depth]) {};
	idx_t start;
	idx_t end;
	idx_t depth;
	data_t key_byte;
};

void GetChildSections(vector<KeySection> &child_sections, vector<Key> &keys, KeySection &key_section) {

	idx_t child_start_idx = key_section.start;
	for (idx_t i = key_section.start + 1; i <= key_section.end; i++) {
		if (keys[i - 1].data[key_section.depth] != keys[i].data[key_section.depth]) {
			child_sections.emplace_back(child_start_idx, i - 1, keys, key_section);
			child_start_idx = i;
		}
	}
	child_sections.emplace_back(child_start_idx, key_section.end, keys, key_section);
}

void Construct(ART &art, vector<Key> &keys, row_t *row_ids, Node *&node, KeySection &key_section,
               bool &has_constraint) {

	D_ASSERT(key_section.start < keys.size());
	D_ASSERT(key_section.end < keys.size());
	D_ASSERT(key_section.start <= key_section.end);

	auto &start_key = keys[key_section.start];
	auto &end_key = keys[key_section.end];

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
		auto single_row_id = num_row_ids == 1;
		if (has_constraint && !single_row_id) {
			throw ConstraintException("New data contains duplicates on indexed column(s)");
		}

		if (single_row_id) {
			node = Leaf::New(start_key, prefix_start, row_ids[key_section.start]);
			art.memory_size += node->MemorySize(art, false);
			return;
		}
		node = Leaf::New(start_key, prefix_start, row_ids + key_section.start, num_row_ids);
		art.memory_size += node->MemorySize(art, false);

	} else { // create a new node and recurse

		// we will find at least two child entries of this node, otherwise we'd have reached a leaf
		vector<KeySection> child_sections;
		GetChildSections(child_sections, keys, key_section);

		auto node_type = Node::GetTypeBySize(child_sections.size());
		Node::New(node_type, node);

		auto prefix_length = key_section.depth - prefix_start;
		node->prefix = Prefix(start_key, prefix_start, prefix_length);
		art.memory_size += node->MemorySize(art, false);

		// recurse on each child section
		for (auto &child_section : child_sections) {
			Node *new_child = nullptr;
			Construct(art, keys, row_ids, new_child, child_section, has_constraint);
			Node::InsertChild(art, node, child_section.key_byte, new_child);
		}
	}
}

void ART::ConstructFromSorted(idx_t count, vector<Key> &keys, Vector &row_identifiers) {

	// prepare the row_identifiers
	row_identifiers.Flatten(count);
	auto row_ids = FlatVector::GetData<row_t>(row_identifiers);

	auto key_section = KeySection(0, count - 1, 0, 0);
	auto has_constraint = IsUnique();
	Construct(*this, keys, row_ids, this->tree, key_section, has_constraint);
}

//===--------------------------------------------------------------------===//
// Insert
//===--------------------------------------------------------------------===//

bool ART::Insert(IndexLock &lock, DataChunk &input, Vector &row_ids) {

	D_ASSERT(row_ids.GetType().InternalType() == ROW_TYPE);
	D_ASSERT(logical_types[0] == input.data[0].GetType());

	// generate the keys for the given input
	ArenaAllocator arena_allocator(BufferAllocator::Get(db));
	vector<Key> keys(input.size());
	GenerateKeys(arena_allocator, input, keys);

	auto old_memory_size = this->memory_size;

	// get the corresponding row IDs
	row_ids.Flatten(input.size());
	auto row_identifiers = FlatVector::GetData<row_t>(row_ids);

	// now insert the elements into the index
	idx_t failed_index = DConstants::INVALID_INDEX;
	for (idx_t i = 0; i < input.size(); i++) {
		if (keys[i].Empty()) {
			continue;
		}

		row_t row_id = row_identifiers[i];
		if (!Insert(tree, keys[i], 0, row_id)) {
			// failed to insert because of constraint violation
			failed_index = i;
			break;
		}
	}
	if (failed_index != DConstants::INVALID_INDEX) {

		// failed to insert because of constraint violation: remove previously inserted entries
		for (idx_t i = 0; i < failed_index; i++) {
			if (keys[i].Empty()) {
				continue;
			}
			row_t row_id = row_identifiers[i];
			Erase(tree, keys[i], 0, row_id);
		}
		// nothing changed, no need to update the buffer memory size
		return false;
	}

	D_ASSERT(old_memory_size <= this->memory_size);
	if (this->track_memory) {
		this->buffer_manager.IncreaseUsedMemory(this->memory_size - old_memory_size);
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
	leaf.Insert(*this, row_id);
	return true;
}

bool ART::Insert(Node *&node, Key &key, idx_t depth, row_t row_id) {

	if (!node) {
		// node is currently empty, create a leaf here with the key
		node = Leaf::New(key, depth, row_id);
		this->memory_size += node->MemorySize(*this, false);
		return true;
	}

	if (node->type == NodeType::NLeaf) {
		// replace leaf with Node4 and store both leaves in it
		auto leaf = (Leaf *)node;
		uint32_t new_prefix_length = 0;

		// FIXME: this code (if and while) can be optimized, less branching, see Construct
		// leaf node is already there (its key matches the current key), update row_id vector
		if (new_prefix_length == leaf->prefix.Size() && depth + leaf->prefix.Size() == key.len) {
			return InsertToLeaf(*leaf, row_id);
		}
		while (leaf->prefix[new_prefix_length] == key[depth + new_prefix_length]) {
			new_prefix_length++;
			// leaf node is already there (its key matches the current key), update row_id vector
			if (new_prefix_length == leaf->prefix.Size() && depth + leaf->prefix.Size() == key.len) {
				return InsertToLeaf(*leaf, row_id);
			}
		}

		Node *new_node = Node4::New();
		new_node->prefix = Prefix(key, depth, new_prefix_length);
		this->memory_size += new_node->MemorySize(*this, false);

		auto key_byte = node->prefix.Reduce(*this, new_prefix_length);
		Node4::InsertChild(*this, new_node, key_byte, node);

		Node *leaf_node = Leaf::New(key, depth + new_prefix_length + 1, row_id);
		Node4::InsertChild(*this, new_node, key[depth + new_prefix_length], leaf_node);
		this->memory_size += leaf_node->MemorySize(*this, false);

		node = new_node;
		return true;
	}

	// handle prefix of inner node
	if (node->prefix.Size()) {

		uint32_t mismatch_pos = node->prefix.KeyMismatchPosition(key, depth);
		if (mismatch_pos != node->prefix.Size()) {
			// prefix differs, create new node
			Node *new_node = Node4::New();
			new_node->prefix = Prefix(key, depth, mismatch_pos);
			this->memory_size += new_node->MemorySize(*this, false);

			// break up prefix
			auto key_byte = node->prefix.Reduce(*this, mismatch_pos);
			Node4::InsertChild(*this, new_node, key_byte, node);

			Node *leaf_node = Leaf::New(key, depth + mismatch_pos + 1, row_id);
			Node4::InsertChild(*this, new_node, key[depth + mismatch_pos], leaf_node);
			this->memory_size += leaf_node->MemorySize(*this, false);

			node = new_node;
			return true;
		}
		depth += node->prefix.Size();
	}

	// recurse
	D_ASSERT(depth < key.len);
	idx_t pos = node->GetChildPos(key[depth]);
	if (pos != DConstants::INVALID_INDEX) {
		auto child = node->GetChild(*this, pos);
		bool insertion_result = Insert(child, key, depth + 1, row_id);
		node->ReplaceChildPointer(pos, child);
		return insertion_result;
	}

	Node *leaf_node = Leaf::New(key, depth + 1, row_id);
	Node::InsertChild(*this, node, key[depth], leaf_node);
	this->memory_size += leaf_node->MemorySize(*this, false);
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
	ArenaAllocator arena_allocator(BufferAllocator::Get(db));
	vector<Key> keys(expression.size());
	GenerateKeys(arena_allocator, expression, keys);

	auto old_memory_size = this->memory_size;

	// now erase the elements from the database
	row_ids.Flatten(input.size());
	auto row_identifiers = FlatVector::GetData<row_t>(row_ids);

	for (idx_t i = 0; i < input.size(); i++) {
		if (keys[i].Empty()) {
			continue;
		}
		Erase(tree, keys[i], 0, row_identifiers[i]);
#ifdef DEBUG
		auto node = Lookup(tree, keys[i], 0);
		if (node) {
			auto leaf = (Leaf *)node;
			for (idx_t k = 0; k < leaf->count; k++) {
				D_ASSERT(leaf->GetRowId(k) != row_identifiers[i]);
			}
		}
#endif
	}

	D_ASSERT(old_memory_size >= this->memory_size);
	if (this->track_memory) {
		this->buffer_manager.DecreaseUsedMemory(old_memory_size - this->memory_size);
	}
}

void ART::Erase(Node *&node, Key &key, idx_t depth, row_t row_id) {

	if (!node) {
		return;
	}

	// delete a leaf from a tree
	if (node->type == NodeType::NLeaf) {
		auto leaf = (Leaf *)node;
		leaf->Remove(*this, row_id);

		if (leaf->count == 0) {
			this->memory_size -= leaf->MemorySize(*this, false);
			Node::Delete(node);
			node = nullptr;
		}
		return;
	}

	// handle prefix
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
			// leaf found, remove entry
			auto leaf = (Leaf *)child;
			leaf->Remove(*this, row_id);

			if (leaf->count == 0) {
				// leaf is empty, delete leaf, decrement node counter and maybe shrink node
				Node::EraseChild(*this, node, pos);
			}

		} else {
			// recurse
			Erase(child, key, depth + 1, row_id);
			node->ReplaceChildPointer(pos, child);
		}
	}
}

//===--------------------------------------------------------------------===//
// Point Query
//===--------------------------------------------------------------------===//

static Key CreateKey(ArenaAllocator &allocator, PhysicalType type, Value &value) {
	D_ASSERT(type == value.type().InternalType());
	switch (type) {
	case PhysicalType::BOOL:
		return Key::CreateKey<bool>(allocator, value);
	case PhysicalType::INT8:
		return Key::CreateKey<int8_t>(allocator, value);
	case PhysicalType::INT16:
		return Key::CreateKey<int16_t>(allocator, value);
	case PhysicalType::INT32:
		return Key::CreateKey<int32_t>(allocator, value);
	case PhysicalType::INT64:
		return Key::CreateKey<int64_t>(allocator, value);
	case PhysicalType::UINT8:
		return Key::CreateKey<uint8_t>(allocator, value);
	case PhysicalType::UINT16:
		return Key::CreateKey<uint16_t>(allocator, value);
	case PhysicalType::UINT32:
		return Key::CreateKey<uint32_t>(allocator, value);
	case PhysicalType::UINT64:
		return Key::CreateKey<uint64_t>(allocator, value);
	case PhysicalType::INT128:
		return Key::CreateKey<hugeint_t>(allocator, value);
	case PhysicalType::FLOAT:
		return Key::CreateKey<float>(allocator, value);
	case PhysicalType::DOUBLE:
		return Key::CreateKey<double>(allocator, value);
	case PhysicalType::VARCHAR:
		return Key::CreateKey<string_t>(allocator, value);
	default:
		throw InternalException("Invalid type for index");
	}
}

bool ART::SearchEqual(Key &key, idx_t max_count, vector<row_t> &result_ids) {

	auto leaf = (Leaf *)(Lookup(tree, key, 0));
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

void ART::SearchEqualJoinNoFetch(Key &key, idx_t &result_size) {

	// we need to look for a leaf
	auto leaf = Lookup(tree, key, 0);
	if (!leaf) {
		return;
	}
	result_size = leaf->count;
}

Leaf *ART::Lookup(Node *node, Key &key, idx_t depth) {
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
			return (Leaf *)node;
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

bool ART::SearchGreater(ARTIndexScanState *state, Key &key, bool inclusive, idx_t max_count,
                        vector<row_t> &result_ids) {

	Iterator *it = &state->iterator;

	// greater than scan: first set the iterator to the node at which we will start our scan by finding the lowest node
	// that satisfies our requirement
	if (!it->art) {
		it->art = this;
		bool found = it->LowerBound(tree, key, inclusive);
		if (!found) {
			return true;
		}
	}
	// after that we continue the scan; we don't need to check the bounds as any value following this value is
	// automatically bigger and hence satisfies our predicate
	Key empty_key = Key();
	return it->Scan(empty_key, max_count, result_ids, false);
}

//===--------------------------------------------------------------------===//
// Less Than
//===--------------------------------------------------------------------===//

bool ART::SearchLess(ARTIndexScanState *state, Key &upper_bound, bool inclusive, idx_t max_count,
                     vector<row_t> &result_ids) {

	if (!tree) {
		return true;
	}

	Iterator *it = &state->iterator;

	if (!it->art) {
		it->art = this;
		// first find the minimum value in the ART: we start scanning from this value
		it->FindMinimum(*tree);
		// early out min value higher than upper bound query
		if (it->cur_key > upper_bound) {
			return true;
		}
	}
	// now continue the scan until we reach the upper bound
	return it->Scan(upper_bound, max_count, result_ids, inclusive);
}

//===--------------------------------------------------------------------===//
// Closed Range Query
//===--------------------------------------------------------------------===//

bool ART::SearchCloseRange(ARTIndexScanState *state, Key &lower_bound, Key &upper_bound, bool left_inclusive,
                           bool right_inclusive, idx_t max_count, vector<row_t> &result_ids) {

	Iterator *it = &state->iterator;

	// first find the first node that satisfies the left predicate
	if (!it->art) {
		it->art = this;
		bool found = it->LowerBound(tree, lower_bound, left_inclusive);
		if (!found) {
			return true;
		}
	}
	// now continue the scan until we reach the upper bound
	return it->Scan(upper_bound, max_count, result_ids, right_inclusive);
}

bool ART::Scan(Transaction &transaction, DataTable &table, IndexScanState &table_state, idx_t max_count,
               vector<row_t> &result_ids) {

	auto state = (ARTIndexScanState *)&table_state;
	vector<row_t> row_ids;
	bool success;

	// FIXME: the key directly owning the data for a single key might be more efficient
	D_ASSERT(state->values[0].type().InternalType() == types[0]);
	ArenaAllocator arena_allocator(Allocator::Get(db));
	auto key = CreateKey(arena_allocator, types[0], state->values[0]);

	if (state->values[1].IsNull()) {

		// single predicate
		lock_guard<mutex> l(lock);
		switch (state->expressions[0]) {
		case ExpressionType::COMPARE_EQUAL:
			success = SearchEqual(key, max_count, row_ids);
			break;
		case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
			success = SearchGreater(state, key, true, max_count, row_ids);
			break;
		case ExpressionType::COMPARE_GREATERTHAN:
			success = SearchGreater(state, key, false, max_count, row_ids);
			break;
		case ExpressionType::COMPARE_LESSTHANOREQUALTO:
			success = SearchLess(state, key, true, max_count, row_ids);
			break;
		case ExpressionType::COMPARE_LESSTHAN:
			success = SearchLess(state, key, false, max_count, row_ids);
			break;
		default:
			throw InternalException("Operation not implemented");
		}

	} else {

		// two predicates
		lock_guard<mutex> l(lock);

		D_ASSERT(state->values[1].type().InternalType() == types[0]);
		auto upper_bound = CreateKey(arena_allocator, types[0], state->values[1]);

		bool left_inclusive = state->expressions[0] == ExpressionType ::COMPARE_GREATERTHANOREQUALTO;
		bool right_inclusive = state->expressions[1] == ExpressionType ::COMPARE_LESSTHANOREQUALTO;
		success = SearchCloseRange(state, key, upper_bound, left_inclusive, right_inclusive, max_count, row_ids);
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
	ArenaAllocator arena_allocator(BufferAllocator::Get(db));
	vector<Key> keys(expression_chunk.size());
	GenerateKeys(arena_allocator, expression_chunk, keys);

	for (idx_t i = 0; i < chunk.size(); i++) {
		if (keys[i].Empty()) {
			continue;
		}
		Node *node_ptr = Lookup(tree, keys[i], 0);
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
		serialized_data_pointer = tree->Serialize(*this, writer);
	} else {
		serialized_data_pointer = {(block_id_t)DConstants::INVALID_INDEX, (uint32_t)DConstants::INVALID_INDEX};
	}
	return serialized_data_pointer;
}

//===--------------------------------------------------------------------===//
// Merging
//===--------------------------------------------------------------------===//

bool ART::MergeIndexes(IndexLock &state, Index *other_index) {

	auto other_art = (ART *)other_index;

	if (!this->tree) {
		this->memory_size += other_art->memory_size;
		this->tree = other_art->tree;
		other_art->tree = nullptr;
		return true;
	}

	return Node::MergeARTs(this, other_art);
}

//===--------------------------------------------------------------------===//
// Utility
//===--------------------------------------------------------------------===//

string ART::ToString() {
	if (tree) {
		return tree->ToString(*this);
	}
	return "[empty]";
}

} // namespace duckdb
