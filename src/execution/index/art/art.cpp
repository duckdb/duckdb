#include "duckdb/execution/index/art/art.hpp"

#include "duckdb/common/radix.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/storage/arena_allocator.hpp"
#include "duckdb/execution/index/art/art_key.hpp"
#include "duckdb/execution/index/art/prefix.hpp"
#include "duckdb/execution/index/art/leaf.hpp"
#include "duckdb/execution/index/art/node4.hpp"
#include "duckdb/execution/index/art/node16.hpp"
#include "duckdb/execution/index/art/node48.hpp"
#include "duckdb/execution/index/art/node256.hpp"
#include "duckdb/execution/index/art/iterator.hpp"
#include "duckdb/common/types/conflict_manager.hpp"
#include "duckdb/storage/table/scan_state.hpp"
#include "duckdb/storage/metadata/metadata_reader.hpp"
#include "duckdb/storage/table_io_manager.hpp"

#include <algorithm>

namespace duckdb {

struct ARTIndexScanState : public IndexScanState {

	//! Scan predicates (single predicate scan or range scan)
	Value values[2];
	//! Expressions of the scan predicates
	ExpressionType expressions[2];
	bool checked = false;
	//! All scanned row IDs
	vector<row_t> result_ids;
	Iterator iterator;
};

ART::ART(const vector<column_t> &column_ids, TableIOManager &table_io_manager,
         const vector<unique_ptr<Expression>> &unbound_expressions, const IndexConstraintType constraint_type,
         AttachedDatabase &db, const shared_ptr<array<unique_ptr<FixedSizeAllocator>, ALLOCATOR_COUNT>> &allocators_ptr,
         const BlockPointer &pointer)
    : Index(db, IndexType::ART, table_io_manager, column_ids, unbound_expressions, constraint_type),
      allocators(allocators_ptr), owns_data(false) {
	if (!Radix::IsLittleEndian()) {
		throw NotImplementedException("ART indexes are not supported on big endian architectures");
	}

	// initialize all allocators
	if (!allocators) {
		owns_data = true;
		auto &block_manager = table_io_manager.GetIndexBlockManager();

		array<unique_ptr<FixedSizeAllocator>, ALLOCATOR_COUNT> allocator_array = {
		    make_uniq<FixedSizeAllocator>(sizeof(Prefix), block_manager),
		    make_uniq<FixedSizeAllocator>(sizeof(Leaf), block_manager),
		    make_uniq<FixedSizeAllocator>(sizeof(Node4), block_manager),
		    make_uniq<FixedSizeAllocator>(sizeof(Node16), block_manager),
		    make_uniq<FixedSizeAllocator>(sizeof(Node48), block_manager),
		    make_uniq<FixedSizeAllocator>(sizeof(Node256), block_manager)};
		allocators = make_shared<array<unique_ptr<FixedSizeAllocator>, ALLOCATOR_COUNT>>(std::move(allocator_array));
	}

	if (pointer.IsValid()) {
		Deserialize(pointer);
	}

	// validate the types of the key columns
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
			throw InvalidTypeException(logical_types[i], "Invalid type for index key.");
		}
	}
}

//===--------------------------------------------------------------------===//
// Initialize Predicate Scans
//===--------------------------------------------------------------------===//

unique_ptr<IndexScanState> ART::InitializeScanSinglePredicate(const Transaction &transaction, const Value &value,
                                                              const ExpressionType expression_type) {
	// initialize point lookup
	auto result = make_uniq<ARTIndexScanState>();
	result->values[0] = value;
	result->expressions[0] = expression_type;
	return std::move(result);
}

unique_ptr<IndexScanState> ART::InitializeScanTwoPredicates(const Transaction &transaction, const Value &low_value,
                                                            const ExpressionType low_expression_type,
                                                            const Value &high_value,
                                                            const ExpressionType high_expression_type) {
	// initialize range lookup
	auto result = make_uniq<ARTIndexScanState>();
	result->values[0] = low_value;
	result->expressions[0] = low_expression_type;
	result->values[1] = high_value;
	result->expressions[1] = high_expression_type;
	return std::move(result);
}

//===--------------------------------------------------------------------===//
// Keys
//===--------------------------------------------------------------------===//

template <class T>
static void TemplatedGenerateKeys(ArenaAllocator &allocator, Vector &input, idx_t count, vector<ARTKey> &keys) {
	UnifiedVectorFormat idata;
	input.ToUnifiedFormat(count, idata);

	D_ASSERT(keys.size() >= count);
	auto input_data = UnifiedVectorFormat::GetData<T>(idata);
	for (idx_t i = 0; i < count; i++) {
		auto idx = idata.sel->get_index(i);
		if (idata.validity.RowIsValid(idx)) {
			ARTKey::CreateARTKey<T>(allocator, input.GetType(), keys[i], input_data[idx]);
		} else {
			// we need to possibly reset the former key value in the keys vector
			keys[i] = ARTKey();
		}
	}
}

template <class T>
static void ConcatenateKeys(ArenaAllocator &allocator, Vector &input, idx_t count, vector<ARTKey> &keys) {
	UnifiedVectorFormat idata;
	input.ToUnifiedFormat(count, idata);

	auto input_data = UnifiedVectorFormat::GetData<T>(idata);
	for (idx_t i = 0; i < count; i++) {
		auto idx = idata.sel->get_index(i);

		// key is not NULL (no previous column entry was NULL)
		if (!keys[i].Empty()) {
			if (!idata.validity.RowIsValid(idx)) {
				// this column entry is NULL, set whole key to NULL
				keys[i] = ARTKey();
			} else {
				auto other_key = ARTKey::CreateARTKey<T>(allocator, input.GetType(), input_data[idx]);
				keys[i].ConcatenateARTKey(allocator, other_key);
			}
		}
	}
}

void ART::GenerateKeys(ArenaAllocator &allocator, DataChunk &input, vector<ARTKey> &keys) {
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
// Construct from sorted data (only during CREATE (UNIQUE) INDEX statements)
//===--------------------------------------------------------------------===//

struct KeySection {
	KeySection(idx_t start_p, idx_t end_p, idx_t depth_p, data_t key_byte_p)
	    : start(start_p), end(end_p), depth(depth_p), key_byte(key_byte_p) {};
	KeySection(idx_t start_p, idx_t end_p, vector<ARTKey> &keys, KeySection &key_section)
	    : start(start_p), end(end_p), depth(key_section.depth + 1), key_byte(keys[end_p].data[key_section.depth]) {};
	idx_t start;
	idx_t end;
	idx_t depth;
	data_t key_byte;
};

void GetChildSections(vector<KeySection> &child_sections, vector<ARTKey> &keys, KeySection &key_section) {

	idx_t child_start_idx = key_section.start;
	for (idx_t i = key_section.start + 1; i <= key_section.end; i++) {
		if (keys[i - 1].data[key_section.depth] != keys[i].data[key_section.depth]) {
			child_sections.emplace_back(child_start_idx, i - 1, keys, key_section);
			child_start_idx = i;
		}
	}
	child_sections.emplace_back(child_start_idx, key_section.end, keys, key_section);
}

bool Construct(ART &art, vector<ARTKey> &keys, row_t *row_ids, Node &node, KeySection &key_section,
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
			return false;
		}

		reference<Node> ref_node(node);
		Prefix::New(art, ref_node, start_key, prefix_start, start_key.len - prefix_start);
		if (single_row_id) {
			Leaf::New(ref_node, row_ids[key_section.start]);
		} else {
			Leaf::New(art, ref_node, row_ids + key_section.start, num_row_ids);
		}
		return true;
	}

	// create a new node and recurse

	// we will find at least two child entries of this node, otherwise we'd have reached a leaf
	vector<KeySection> child_sections;
	GetChildSections(child_sections, keys, key_section);

	// set the prefix
	reference<Node> ref_node(node);
	auto prefix_length = key_section.depth - prefix_start;
	Prefix::New(art, ref_node, start_key, prefix_start, prefix_length);

	// set the node
	auto node_type = Node::GetARTNodeTypeByCount(child_sections.size());
	Node::New(art, ref_node, node_type);

	// recurse on each child section
	for (auto &child_section : child_sections) {
		Node new_child;
		auto no_violation = Construct(art, keys, row_ids, new_child, child_section, has_constraint);
		Node::InsertChild(art, ref_node, child_section.key_byte, new_child);
		if (!no_violation) {
			return false;
		}
	}
	return true;
}

bool ART::ConstructFromSorted(idx_t count, vector<ARTKey> &keys, Vector &row_identifiers) {

	// prepare the row_identifiers
	row_identifiers.Flatten(count);
	auto row_ids = FlatVector::GetData<row_t>(row_identifiers);

	auto key_section = KeySection(0, count - 1, 0, 0);
	auto has_constraint = IsUnique();
	if (!Construct(*this, keys, row_ids, tree, key_section, has_constraint)) {
		return false;
	}

#ifdef DEBUG
	D_ASSERT(!VerifyAndToStringInternal(true).empty());
	for (idx_t i = 0; i < count; i++) {
		D_ASSERT(!keys[i].Empty());
		auto leaf = Lookup(tree, keys[i], 0);
		D_ASSERT(Leaf::ContainsRowId(*this, *leaf, row_ids[i]));
	}
#endif

	return true;
}

//===--------------------------------------------------------------------===//
// Insert / Verification / Constraint Checking
//===--------------------------------------------------------------------===//
PreservedError ART::Insert(IndexLock &lock, DataChunk &input, Vector &row_ids) {

	D_ASSERT(row_ids.GetType().InternalType() == ROW_TYPE);
	D_ASSERT(logical_types[0] == input.data[0].GetType());

	// generate the keys for the given input
	ArenaAllocator arena_allocator(BufferAllocator::Get(db));
	vector<ARTKey> keys(input.size());
	GenerateKeys(arena_allocator, input, keys);

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

	// failed to insert because of constraint violation: remove previously inserted entries
	if (failed_index != DConstants::INVALID_INDEX) {
		for (idx_t i = 0; i < failed_index; i++) {
			if (keys[i].Empty()) {
				continue;
			}
			row_t row_id = row_identifiers[i];
			Erase(tree, keys[i], 0, row_id);
		}
	}

	if (failed_index != DConstants::INVALID_INDEX) {
		return PreservedError(ConstraintException("PRIMARY KEY or UNIQUE constraint violated: duplicate key \"%s\"",
		                                          AppendRowError(input, failed_index)));
	}

#ifdef DEBUG
	for (idx_t i = 0; i < input.size(); i++) {
		if (keys[i].Empty()) {
			continue;
		}

		auto leaf = Lookup(tree, keys[i], 0);
		D_ASSERT(Leaf::ContainsRowId(*this, *leaf, row_identifiers[i]));
	}
#endif

	return PreservedError();
}

PreservedError ART::Append(IndexLock &lock, DataChunk &appended_data, Vector &row_identifiers) {
	DataChunk expression_result;
	expression_result.Initialize(Allocator::DefaultAllocator(), logical_types);

	// first resolve the expressions for the index
	ExecuteExpressions(appended_data, expression_result);

	// now insert into the index
	return Insert(lock, expression_result, row_identifiers);
}

void ART::VerifyAppend(DataChunk &chunk) {
	ConflictManager conflict_manager(VerifyExistenceType::APPEND, chunk.size());
	CheckConstraintsForChunk(chunk, conflict_manager);
}

void ART::VerifyAppend(DataChunk &chunk, ConflictManager &conflict_manager) {
	D_ASSERT(conflict_manager.LookupType() == VerifyExistenceType::APPEND);
	CheckConstraintsForChunk(chunk, conflict_manager);
}

bool ART::InsertToLeaf(Node &leaf, const row_t &row_id) {

	if (IsUnique()) {
		return false;
	}

	Leaf::Insert(*this, leaf, row_id);
	return true;
}

bool ART::Insert(Node &node, const ARTKey &key, idx_t depth, const row_t &row_id) {

	// node is currently empty, create a leaf here with the key
	if (!node.HasMetadata()) {
		D_ASSERT(depth <= key.len);
		reference<Node> ref_node(node);
		Prefix::New(*this, ref_node, key, depth, key.len - depth);
		Leaf::New(ref_node, row_id);
		return true;
	}

	auto node_type = node.GetType();

	// insert the row ID into this leaf
	if (node_type == NType::LEAF || node_type == NType::LEAF_INLINED) {
		return InsertToLeaf(node, row_id);
	}

	if (node_type != NType::PREFIX) {
		D_ASSERT(depth < key.len);
		auto child = node.GetChildMutable(*this, key[depth]);

		// recurse, if a child exists at key[depth]
		if (child) {
			bool success = Insert(*child, key, depth + 1, row_id);
			node.ReplaceChild(*this, key[depth], *child);
			return success;
		}

		// insert a new leaf node at key[depth]
		Node leaf_node;
		reference<Node> ref_node(leaf_node);
		if (depth + 1 < key.len) {
			Prefix::New(*this, ref_node, key, depth + 1, key.len - depth - 1);
		}
		Leaf::New(ref_node, row_id);
		Node::InsertChild(*this, node, key[depth], leaf_node);
		return true;
	}

	// this is a prefix node, traverse
	reference<Node> next_node(node);
	auto mismatch_position = Prefix::TraverseMutable(*this, next_node, key, depth);

	// prefix matches key
	if (next_node.get().GetType() != NType::PREFIX) {
		return Insert(next_node, key, depth, row_id);
	}

	// prefix does not match the key, we need to create a new Node4; this new Node4 has two children,
	// the remaining part of the prefix, and the new leaf
	Node remaining_prefix;
	auto prefix_byte = Prefix::GetByte(*this, next_node, mismatch_position);
	Prefix::Split(*this, next_node, remaining_prefix, mismatch_position);
	Node4::New(*this, next_node);

	// insert remaining prefix
	Node4::InsertChild(*this, next_node, prefix_byte, remaining_prefix);

	// insert new leaf
	Node leaf_node;
	reference<Node> ref_node(leaf_node);
	if (depth + 1 < key.len) {
		Prefix::New(*this, ref_node, key, depth + 1, key.len - depth - 1);
	}
	Leaf::New(ref_node, row_id);
	Node4::InsertChild(*this, next_node, key[depth], leaf_node);
	return true;
}

//===--------------------------------------------------------------------===//
// Drop and Delete
//===--------------------------------------------------------------------===//

void ART::CommitDrop(IndexLock &index_lock) {
	for (auto &allocator : *allocators) {
		allocator->Reset();
	}
	tree.Clear();
}

void ART::Delete(IndexLock &state, DataChunk &input, Vector &row_ids) {

	DataChunk expression;
	expression.Initialize(Allocator::DefaultAllocator(), logical_types);

	// first resolve the expressions
	ExecuteExpressions(input, expression);

	// then generate the keys for the given input
	ArenaAllocator arena_allocator(BufferAllocator::Get(db));
	vector<ARTKey> keys(expression.size());
	GenerateKeys(arena_allocator, expression, keys);

	// now erase the elements from the database
	row_ids.Flatten(input.size());
	auto row_identifiers = FlatVector::GetData<row_t>(row_ids);

	for (idx_t i = 0; i < input.size(); i++) {
		if (keys[i].Empty()) {
			continue;
		}
		Erase(tree, keys[i], 0, row_identifiers[i]);
	}

#ifdef DEBUG
	// verify that we removed all row IDs
	for (idx_t i = 0; i < input.size(); i++) {
		if (keys[i].Empty()) {
			continue;
		}

		auto leaf = Lookup(tree, keys[i], 0);
		if (leaf) {
			D_ASSERT(!Leaf::ContainsRowId(*this, *leaf, row_identifiers[i]));
		}
	}
#endif
}

void ART::Erase(Node &node, const ARTKey &key, idx_t depth, const row_t &row_id) {

	if (!node.HasMetadata()) {
		return;
	}

	// handle prefix
	reference<Node> next_node(node);
	if (next_node.get().GetType() == NType::PREFIX) {
		Prefix::TraverseMutable(*this, next_node, key, depth);
		if (next_node.get().GetType() == NType::PREFIX) {
			return;
		}
	}

	// delete a row ID from a leaf (root is leaf with possible prefix nodes)
	if (next_node.get().GetType() == NType::LEAF || next_node.get().GetType() == NType::LEAF_INLINED) {
		if (Leaf::Remove(*this, next_node, row_id)) {
			Node::Free(*this, node);
		}
		return;
	}

	D_ASSERT(depth < key.len);
	auto child = next_node.get().GetChildMutable(*this, key[depth]);
	if (child) {
		D_ASSERT(child->HasMetadata());

		auto temp_depth = depth + 1;
		reference<Node> child_node(*child);
		if (child_node.get().GetType() == NType::PREFIX) {
			Prefix::TraverseMutable(*this, child_node, key, temp_depth);
			if (child_node.get().GetType() == NType::PREFIX) {
				return;
			}
		}

		if (child_node.get().GetType() == NType::LEAF || child_node.get().GetType() == NType::LEAF_INLINED) {
			// leaf found, remove entry
			if (Leaf::Remove(*this, child_node, row_id)) {
				Node::DeleteChild(*this, next_node, node, key[depth]);
			}
			return;
		}

		// recurse
		Erase(*child, key, depth + 1, row_id);
		next_node.get().ReplaceChild(*this, key[depth], *child);
	}
}

//===--------------------------------------------------------------------===//
// Point Query (Equal)
//===--------------------------------------------------------------------===//

static ARTKey CreateKey(ArenaAllocator &allocator, PhysicalType type, Value &value) {
	D_ASSERT(type == value.type().InternalType());
	switch (type) {
	case PhysicalType::BOOL:
		return ARTKey::CreateARTKey<bool>(allocator, value.type(), value);
	case PhysicalType::INT8:
		return ARTKey::CreateARTKey<int8_t>(allocator, value.type(), value);
	case PhysicalType::INT16:
		return ARTKey::CreateARTKey<int16_t>(allocator, value.type(), value);
	case PhysicalType::INT32:
		return ARTKey::CreateARTKey<int32_t>(allocator, value.type(), value);
	case PhysicalType::INT64:
		return ARTKey::CreateARTKey<int64_t>(allocator, value.type(), value);
	case PhysicalType::UINT8:
		return ARTKey::CreateARTKey<uint8_t>(allocator, value.type(), value);
	case PhysicalType::UINT16:
		return ARTKey::CreateARTKey<uint16_t>(allocator, value.type(), value);
	case PhysicalType::UINT32:
		return ARTKey::CreateARTKey<uint32_t>(allocator, value.type(), value);
	case PhysicalType::UINT64:
		return ARTKey::CreateARTKey<uint64_t>(allocator, value.type(), value);
	case PhysicalType::INT128:
		return ARTKey::CreateARTKey<hugeint_t>(allocator, value.type(), value);
	case PhysicalType::FLOAT:
		return ARTKey::CreateARTKey<float>(allocator, value.type(), value);
	case PhysicalType::DOUBLE:
		return ARTKey::CreateARTKey<double>(allocator, value.type(), value);
	case PhysicalType::VARCHAR:
		return ARTKey::CreateARTKey<string_t>(allocator, value.type(), value);
	default:
		throw InternalException("Invalid type for the ART key");
	}
}

bool ART::SearchEqual(ARTKey &key, idx_t max_count, vector<row_t> &result_ids) {

	auto leaf = Lookup(tree, key, 0);
	if (!leaf) {
		return true;
	}
	return Leaf::GetRowIds(*this, *leaf, result_ids, max_count);
}

void ART::SearchEqualJoinNoFetch(ARTKey &key, idx_t &result_size) {

	// we need to look for a leaf
	auto leaf_node = Lookup(tree, key, 0);
	if (!leaf_node) {
		result_size = 0;
		return;
	}

	// we only perform index joins on PK/FK columns
	D_ASSERT(leaf_node->GetType() == NType::LEAF_INLINED);
	result_size = 1;
	return;
}

//===--------------------------------------------------------------------===//
// Lookup
//===--------------------------------------------------------------------===//

optional_ptr<const Node> ART::Lookup(const Node &node, const ARTKey &key, idx_t depth) {

	reference<const Node> node_ref(node);
	while (node_ref.get().HasMetadata()) {

		// traverse prefix, if exists
		reference<const Node> next_node(node_ref.get());
		if (next_node.get().GetType() == NType::PREFIX) {
			Prefix::Traverse(*this, next_node, key, depth);
			if (next_node.get().GetType() == NType::PREFIX) {
				return nullptr;
			}
		}

		if (next_node.get().GetType() == NType::LEAF || next_node.get().GetType() == NType::LEAF_INLINED) {
			return &next_node.get();
		}

		D_ASSERT(depth < key.len);
		auto child = next_node.get().GetChild(*this, key[depth]);
		if (!child) {
			// prefix matches key, but no child at byte, ART/subtree does not contain key
			return nullptr;
		}

		// lookup in child node
		node_ref = *child;
		D_ASSERT(node_ref.get().HasMetadata());
		depth++;
	}

	return nullptr;
}

//===--------------------------------------------------------------------===//
// Greater Than and Less Than
//===--------------------------------------------------------------------===//

bool ART::SearchGreater(ARTIndexScanState &state, ARTKey &key, bool equal, idx_t max_count, vector<row_t> &result_ids) {

	if (!tree.HasMetadata()) {
		return true;
	}
	Iterator &it = state.iterator;

	// find the lowest value that satisfies the predicate
	if (!it.art) {
		it.art = this;
		if (!it.LowerBound(tree, key, equal, 0)) {
			// early-out, if the maximum value in the ART is lower than the lower bound
			return true;
		}
	}

	// after that we continue the scan; we don't need to check the bounds as any value following this value is
	// automatically bigger and hence satisfies our predicate
	ARTKey empty_key = ARTKey();
	return it.Scan(empty_key, max_count, result_ids, false);
}

bool ART::SearchLess(ARTIndexScanState &state, ARTKey &upper_bound, bool equal, idx_t max_count,
                     vector<row_t> &result_ids) {

	if (!tree.HasMetadata()) {
		return true;
	}
	Iterator &it = state.iterator;

	if (!it.art) {
		it.art = this;
		// find the minimum value in the ART: we start scanning from this value
		it.FindMinimum(tree);
		// early-out, if the minimum value is higher than the upper bound
		if (it.current_key > upper_bound) {
			return true;
		}
	}

	// now continue the scan until we reach the upper bound
	return it.Scan(upper_bound, max_count, result_ids, equal);
}

//===--------------------------------------------------------------------===//
// Closed Range Query
//===--------------------------------------------------------------------===//

bool ART::SearchCloseRange(ARTIndexScanState &state, ARTKey &lower_bound, ARTKey &upper_bound, bool left_equal,
                           bool right_equal, idx_t max_count, vector<row_t> &result_ids) {

	Iterator &it = state.iterator;

	// find the first node that satisfies the left predicate
	if (!it.art) {
		it.art = this;
		if (!it.LowerBound(tree, lower_bound, left_equal, 0)) {
			// early-out, if the maximum value in the ART is lower than the lower bound
			return true;
		}
	}

	// now continue the scan until we reach the upper bound
	return it.Scan(upper_bound, max_count, result_ids, right_equal);
}

bool ART::Scan(const Transaction &transaction, const DataTable &table, IndexScanState &state, const idx_t max_count,
               vector<row_t> &result_ids) {

	auto &scan_state = state.Cast<ARTIndexScanState>();
	vector<row_t> row_ids;
	bool success;

	// FIXME: the key directly owning the data for a single key might be more efficient
	D_ASSERT(scan_state.values[0].type().InternalType() == types[0]);
	ArenaAllocator arena_allocator(Allocator::Get(db));
	auto key = CreateKey(arena_allocator, types[0], scan_state.values[0]);

	if (scan_state.values[1].IsNull()) {

		// single predicate
		lock_guard<mutex> l(lock);
		switch (scan_state.expressions[0]) {
		case ExpressionType::COMPARE_EQUAL:
			success = SearchEqual(key, max_count, row_ids);
			break;
		case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
			success = SearchGreater(scan_state, key, true, max_count, row_ids);
			break;
		case ExpressionType::COMPARE_GREATERTHAN:
			success = SearchGreater(scan_state, key, false, max_count, row_ids);
			break;
		case ExpressionType::COMPARE_LESSTHANOREQUALTO:
			success = SearchLess(scan_state, key, true, max_count, row_ids);
			break;
		case ExpressionType::COMPARE_LESSTHAN:
			success = SearchLess(scan_state, key, false, max_count, row_ids);
			break;
		default:
			throw InternalException("Index scan type not implemented");
		}

	} else {

		// two predicates
		lock_guard<mutex> l(lock);

		D_ASSERT(scan_state.values[1].type().InternalType() == types[0]);
		auto upper_bound = CreateKey(arena_allocator, types[0], scan_state.values[1]);

		bool left_equal = scan_state.expressions[0] == ExpressionType ::COMPARE_GREATERTHANOREQUALTO;
		bool right_equal = scan_state.expressions[1] == ExpressionType ::COMPARE_LESSTHANOREQUALTO;
		success = SearchCloseRange(scan_state, key, upper_bound, left_equal, right_equal, max_count, row_ids);
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

//===--------------------------------------------------------------------===//
// More Verification / Constraint Checking
//===--------------------------------------------------------------------===//

string ART::GenerateErrorKeyName(DataChunk &input, idx_t row) {

	// FIXME: why exactly can we not pass the expression_chunk as an argument to this
	// FIXME: function instead of re-executing?
	// re-executing the expressions is not very fast, but we're going to throw, so we don't care
	DataChunk expression_chunk;
	expression_chunk.Initialize(Allocator::DefaultAllocator(), logical_types);
	ExecuteExpressions(input, expression_chunk);

	string key_name;
	for (idx_t k = 0; k < expression_chunk.ColumnCount(); k++) {
		if (k > 0) {
			key_name += ", ";
		}
		key_name += unbound_expressions[k]->GetName() + ": " + expression_chunk.data[k].GetValue(row).ToString();
	}
	return key_name;
}

string ART::GenerateConstraintErrorMessage(VerifyExistenceType verify_type, const string &key_name) {
	switch (verify_type) {
	case VerifyExistenceType::APPEND: {
		// APPEND to PK/UNIQUE table, but node/key already exists in PK/UNIQUE table
		string type = IsPrimary() ? "primary key" : "unique";
		return StringUtil::Format(
		    "Duplicate key \"%s\" violates %s constraint. "
		    "If this is an unexpected constraint violation please double "
		    "check with the known index limitations section in our documentation (docs - sql - indexes).",
		    key_name, type);
	}
	case VerifyExistenceType::APPEND_FK: {
		// APPEND_FK to FK table, node/key does not exist in PK/UNIQUE table
		return StringUtil::Format(
		    "Violates foreign key constraint because key \"%s\" does not exist in the referenced table", key_name);
	}
	case VerifyExistenceType::DELETE_FK: {
		// DELETE_FK that still exists in a FK table, i.e., not a valid delete
		return StringUtil::Format("Violates foreign key constraint because key \"%s\" is still referenced by a foreign "
		                          "key in a different table",
		                          key_name);
	}
	default:
		throw NotImplementedException("Type not implemented for VerifyExistenceType");
	}
}

void ART::CheckConstraintsForChunk(DataChunk &input, ConflictManager &conflict_manager) {

	// don't alter the index during constraint checking
	lock_guard<mutex> l(lock);

	// first resolve the expressions for the index
	DataChunk expression_chunk;
	expression_chunk.Initialize(Allocator::DefaultAllocator(), logical_types);
	ExecuteExpressions(input, expression_chunk);

	// generate the keys for the given input
	ArenaAllocator arena_allocator(BufferAllocator::Get(db));
	vector<ARTKey> keys(expression_chunk.size());
	GenerateKeys(arena_allocator, expression_chunk, keys);

	idx_t found_conflict = DConstants::INVALID_INDEX;
	for (idx_t i = 0; found_conflict == DConstants::INVALID_INDEX && i < input.size(); i++) {

		if (keys[i].Empty()) {
			if (conflict_manager.AddNull(i)) {
				found_conflict = i;
			}
			continue;
		}

		auto leaf = Lookup(tree, keys[i], 0);
		if (!leaf) {
			if (conflict_manager.AddMiss(i)) {
				found_conflict = i;
			}
			continue;
		}

		// when we find a node, we need to update the 'matches' and 'row_ids'
		// NOTE: leaves can have more than one row_id, but for UNIQUE/PRIMARY KEY they will only have one
		D_ASSERT(leaf->GetType() == NType::LEAF_INLINED);
		if (conflict_manager.AddHit(i, leaf->GetRowId())) {
			found_conflict = i;
		}
	}

	conflict_manager.FinishLookup();

	if (found_conflict == DConstants::INVALID_INDEX) {
		return;
	}

	auto key_name = GenerateErrorKeyName(input, found_conflict);
	auto exception_msg = GenerateConstraintErrorMessage(conflict_manager.LookupType(), key_name);
	throw ConstraintException(exception_msg);
}

//===--------------------------------------------------------------------===//
// Serialization
//===--------------------------------------------------------------------===//

BlockPointer ART::Serialize(MetadataWriter &writer) {

	D_ASSERT(owns_data);

	// early-out, if all allocators are empty
	if (!tree.HasMetadata()) {
		root_block_pointer = BlockPointer();
		return root_block_pointer;
	}

	lock_guard<mutex> l(lock);
	auto &block_manager = table_io_manager.GetIndexBlockManager();
	PartialBlockManager partial_block_manager(block_manager, CheckpointType::FULL_CHECKPOINT);

	vector<BlockPointer> allocator_pointers;
	for (auto &allocator : *allocators) {
		allocator_pointers.push_back(allocator->Serialize(partial_block_manager, writer));
	}
	partial_block_manager.FlushPartialBlocks();

	root_block_pointer = writer.GetBlockPointer();
	writer.Write(tree);
	for (auto &allocator_pointer : allocator_pointers) {
		writer.Write(allocator_pointer);
	}

	return root_block_pointer;
}

void ART::Deserialize(const BlockPointer &pointer) {

	D_ASSERT(pointer.IsValid());
	MetadataReader reader(table_io_manager.GetMetadataManager(), pointer);
	tree = reader.Read<Node>();

	for (idx_t i = 0; i < ALLOCATOR_COUNT; i++) {
		(*allocators)[i]->Deserialize(reader.Read<BlockPointer>());
	}
}

//===--------------------------------------------------------------------===//
// Vacuum
//===--------------------------------------------------------------------===//

void ART::InitializeVacuum(ARTFlags &flags) {

	flags.vacuum_flags.reserve(allocators->size());
	for (auto &allocator : *allocators) {
		flags.vacuum_flags.push_back(allocator->InitializeVacuum());
	}
}

void ART::FinalizeVacuum(const ARTFlags &flags) {

	for (idx_t i = 0; i < allocators->size(); i++) {
		if (flags.vacuum_flags[i]) {
			(*allocators)[i]->FinalizeVacuum();
		}
	}
}

void ART::Vacuum(IndexLock &state) {

	D_ASSERT(owns_data);

	if (!tree.HasMetadata()) {
		for (auto &allocator : *allocators) {
			allocator->Reset();
		}
		return;
	}

	// holds true, if an allocator needs a vacuum, and false otherwise
	ARTFlags flags;
	InitializeVacuum(flags);

	// skip vacuum if no allocators require it
	auto perform_vacuum = false;
	for (const auto &vacuum_flag : flags.vacuum_flags) {
		if (vacuum_flag) {
			perform_vacuum = true;
			break;
		}
	}
	if (!perform_vacuum) {
		return;
	}

	// traverse the allocated memory of the tree to perform a vacuum
	tree.Vacuum(*this, flags);

	// finalize the vacuum operation
	FinalizeVacuum(flags);
}

//===--------------------------------------------------------------------===//
// Merging
//===--------------------------------------------------------------------===//

void ART::InitializeMerge(ARTFlags &flags) {

	D_ASSERT(owns_data);

	flags.merge_buffer_counts.reserve(allocators->size());
	for (auto &allocator : *allocators) {
		flags.merge_buffer_counts.emplace_back(allocator->GetUpperBoundBufferId());
	}
}

bool ART::MergeIndexes(IndexLock &state, Index &other_index) {

	auto &other_art = other_index.Cast<ART>();
	if (!other_art.tree.HasMetadata()) {
		return true;
	}

	if (other_art.owns_data) {
		if (tree.HasMetadata()) {
			//  fully deserialize other_index, and traverse it to increment its buffer IDs
			ARTFlags flags;
			InitializeMerge(flags);
			other_art.tree.InitializeMerge(other_art, flags);
		}

		// merge the node storage
		for (idx_t i = 0; i < allocators->size(); i++) {
			(*allocators)[i]->Merge(*(*other_art.allocators)[i]);
		}
	}

	// merge the ARTs
	if (!tree.Merge(*this, other_art.tree)) {
		return false;
	}
	return true;
}

//===--------------------------------------------------------------------===//
// Utility
//===--------------------------------------------------------------------===//

string ART::VerifyAndToString(IndexLock &state, const bool only_verify) {
	// FIXME: this can be improved by counting the allocations of each node type,
	// FIXME: and by asserting that each fixed-size allocator lists an equal number of
	// FIXME: allocations of that type
	return VerifyAndToStringInternal(only_verify);
}

string ART::VerifyAndToStringInternal(const bool only_verify) {
	if (tree.HasMetadata()) {
		return "ART: " + tree.VerifyAndToString(*this, only_verify);
	}
	return "[empty]";
}

} // namespace duckdb
