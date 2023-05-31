#include "duckdb/execution/index/art/art.hpp"

#include "duckdb/common/radix.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/storage/arena_allocator.hpp"
#include "duckdb/execution/index/art/art_key.hpp"
#include "duckdb/execution/index/art/prefix_segment.hpp"
#include "duckdb/execution/index/art/leaf_segment.hpp"
#include "duckdb/execution/index/art/prefix.hpp"
#include "duckdb/execution/index/art/leaf.hpp"
#include "duckdb/execution/index/art/node4.hpp"
#include "duckdb/execution/index/art/node16.hpp"
#include "duckdb/execution/index/art/node48.hpp"
#include "duckdb/execution/index/art/node256.hpp"
#include "duckdb/execution/index/art/iterator.hpp"
#include "duckdb/common/types/conflict_manager.hpp"
#include "duckdb/storage/table/scan_state.hpp"

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
         AttachedDatabase &db, const idx_t block_id, const idx_t block_offset)

    : Index(db, IndexType::ART, table_io_manager, column_ids, unbound_expressions, constraint_type) {

	if (!Radix::IsLittleEndian()) {
		throw NotImplementedException("ART indexes are not supported on big endian architectures");
	}

	// initialize all allocators
	allocators.emplace_back(make_uniq<FixedSizeAllocator>(sizeof(PrefixSegment), buffer_manager.GetBufferAllocator()));
	allocators.emplace_back(make_uniq<FixedSizeAllocator>(sizeof(LeafSegment), buffer_manager.GetBufferAllocator()));
	allocators.emplace_back(make_uniq<FixedSizeAllocator>(sizeof(Leaf), buffer_manager.GetBufferAllocator()));
	allocators.emplace_back(make_uniq<FixedSizeAllocator>(sizeof(Node4), buffer_manager.GetBufferAllocator()));
	allocators.emplace_back(make_uniq<FixedSizeAllocator>(sizeof(Node16), buffer_manager.GetBufferAllocator()));
	allocators.emplace_back(make_uniq<FixedSizeAllocator>(sizeof(Node48), buffer_manager.GetBufferAllocator()));
	allocators.emplace_back(make_uniq<FixedSizeAllocator>(sizeof(Node256), buffer_manager.GetBufferAllocator()));

	// set the root node of the tree
	tree = make_uniq<Node>();
	if (block_id != DConstants::INVALID_INDEX) {
		tree->buffer_id = block_id;
		tree->offset = block_offset;
		tree->Deserialize(*this);
	}
	serialized_data_pointer = BlockPointer(block_id, block_offset);

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

ART::~ART() {
	tree->Reset();
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

		if (single_row_id) {
			Leaf::New(art, node, start_key, prefix_start, row_ids[key_section.start]);
		} else {
			Leaf::New(art, node, start_key, prefix_start, row_ids + key_section.start, num_row_ids);
		}
		return true;
	}

	// create a new node and recurse

	// we will find at least two child entries of this node, otherwise we'd have reached a leaf
	vector<KeySection> child_sections;
	GetChildSections(child_sections, keys, key_section);

	auto node_type = Node::GetARTNodeTypeByCount(child_sections.size());
	Node::New(art, node, node_type);

	auto prefix_length = key_section.depth - prefix_start;
	node.GetPrefix(art).Initialize(art, start_key, prefix_start, prefix_length);

	// recurse on each child section
	for (auto &child_section : child_sections) {
		Node new_child;
		auto no_violation = Construct(art, keys, row_ids, new_child, child_section, has_constraint);
		Node::InsertChild(art, node, child_section.key_byte, new_child);
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
	if (!Construct(*this, keys, row_ids, *this->tree, key_section, has_constraint)) {
		return false;
	}

#ifdef DEBUG
	D_ASSERT(!VerifyAndToStringInternal(true).empty());
	for (idx_t i = 0; i < count; i++) {
		D_ASSERT(!keys[i].Empty());
		auto leaf_node = Lookup(*tree, keys[i], 0);
		D_ASSERT(leaf_node.IsSet());
		auto &leaf = Leaf::Get(*this, leaf_node);

		if (leaf.IsInlined()) {
			D_ASSERT(row_ids[i] == leaf.row_ids.inlined);
			continue;
		}

		D_ASSERT(leaf.row_ids.ptr.IsSet());
		Node leaf_segment = leaf.row_ids.ptr;
		auto position = leaf.FindRowId(*this, leaf_segment, row_ids[i]);
		D_ASSERT(position != (uint32_t)DConstants::INVALID_INDEX);
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
		if (!Insert(*tree, keys[i], 0, row_id)) {
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
			Erase(*tree, keys[i], 0, row_id);
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

		auto leaf_node = Lookup(*tree, keys[i], 0);
		D_ASSERT(leaf_node.IsSet());
		auto &leaf = Leaf::Get(*this, leaf_node);

		if (leaf.IsInlined()) {
			D_ASSERT(row_identifiers[i] == leaf.row_ids.inlined);
			continue;
		}

		D_ASSERT(leaf.row_ids.ptr.IsSet());
		Node leaf_segment = leaf.row_ids.ptr;
		auto position = leaf.FindRowId(*this, leaf_segment, row_identifiers[i]);
		D_ASSERT(position != (uint32_t)DConstants::INVALID_INDEX);
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

bool ART::InsertToLeaf(Node &leaf_node, const row_t &row_id) {

	auto &leaf = Leaf::Get(*this, leaf_node);

#ifdef DEBUG
	for (idx_t k = 0; k < leaf.count; k++) {
		D_ASSERT(leaf.GetRowId(*this, k) != row_id);
	}
#endif
	if (IsUnique() && leaf.count != 0) {
		return false;
	}
	leaf.Insert(*this, row_id);
	return true;
}

bool ART::Insert(Node &node, const ARTKey &key, idx_t depth, const row_t &row_id) {

	if (!node.IsSet()) {
		// node is currently empty, create a leaf here with the key
		Leaf::New(*this, node, key, depth, row_id);
		return true;
	}

	if (node.DecodeARTNodeType() == NType::LEAF) {

		// add a row ID to a leaf, if they have the same key
		auto &leaf = Leaf::Get(*this, node);
		auto mismatch_position = leaf.prefix.KeyMismatchPosition(*this, key, depth);
		if (mismatch_position == leaf.prefix.count && depth + leaf.prefix.count == key.len) {
			return InsertToLeaf(node, row_id);
		}

		// replace leaf with Node4 and store both leaves in it
		auto old_node = node;
		auto &new_n4 = Node4::New(*this, node);
		new_n4.prefix.Initialize(*this, key, depth, mismatch_position);

		auto key_byte = old_node.GetPrefix(*this).Reduce(*this, mismatch_position);
		Node4::InsertChild(*this, node, key_byte, old_node);

		Node leaf_node;
		Leaf::New(*this, leaf_node, key, depth + mismatch_position + 1, row_id);
		Node4::InsertChild(*this, node, key[depth + mismatch_position], leaf_node);

		return true;
	}

	// handle prefix of inner node
	auto &old_node_prefix = node.GetPrefix(*this);
	if (old_node_prefix.count) {

		auto mismatch_position = old_node_prefix.KeyMismatchPosition(*this, key, depth);
		if (mismatch_position != old_node_prefix.count) {

			// prefix differs, create new node
			auto old_node = node;
			auto &new_n4 = Node4::New(*this, node);
			new_n4.prefix.Initialize(*this, key, depth, mismatch_position);

			auto key_byte = old_node_prefix.Reduce(*this, mismatch_position);
			Node4::InsertChild(*this, node, key_byte, old_node);

			Node leaf_node;
			Leaf::New(*this, leaf_node, key, depth + mismatch_position + 1, row_id);
			Node4::InsertChild(*this, node, key[depth + mismatch_position], leaf_node);

			return true;
		}
		depth += node.GetPrefix(*this).count;
	}

	// recurse
	D_ASSERT(depth < key.len);
	auto child = node.GetChild(*this, key[depth]);
	if (child) {
		bool success = Insert(*child, key, depth + 1, row_id);
		node.ReplaceChild(*this, key[depth], *child);
		return success;
	}

	// insert at position
	Node leaf_node;
	Leaf::New(*this, leaf_node, key, depth + 1, row_id);
	Node::InsertChild(*this, node, key[depth], leaf_node);
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
	vector<ARTKey> keys(expression.size());
	GenerateKeys(arena_allocator, expression, keys);

	// now erase the elements from the database
	row_ids.Flatten(input.size());
	auto row_identifiers = FlatVector::GetData<row_t>(row_ids);

	for (idx_t i = 0; i < input.size(); i++) {
		if (keys[i].Empty()) {
			continue;
		}
		Erase(*tree, keys[i], 0, row_identifiers[i]);
	}

#ifdef DEBUG
	// verify that we removed all row IDs
	for (idx_t i = 0; i < input.size(); i++) {
		if (keys[i].Empty()) {
			continue;
		}

		auto node = Lookup(*tree, keys[i], 0);
		if (node.IsSet()) {
			auto &leaf = Leaf::Get(*this, node);

			if (leaf.IsInlined()) {
				D_ASSERT(row_identifiers[i] != leaf.row_ids.inlined);
				continue;
			}

			D_ASSERT(leaf.row_ids.ptr.IsSet());
			Node leaf_segment = leaf.row_ids.ptr;
			auto position = leaf.FindRowId(*this, leaf_segment, row_identifiers[i]);
			D_ASSERT(position == (uint32_t)DConstants::INVALID_INDEX);
		}
	}
#endif
}

void ART::Erase(Node &node, const ARTKey &key, idx_t depth, const row_t &row_id) {

	if (!node.IsSet()) {
		return;
	}

	// delete a row ID from a leaf
	if (node.DecodeARTNodeType() == NType::LEAF) {
		auto &leaf = Leaf::Get(*this, node);
		leaf.Remove(*this, row_id);

		if (leaf.count == 0) {
			Node::Free(*this, node);
			node.Reset();
		}
		return;
	}

	// handle prefix
	auto &node_prefix = node.GetPrefix(*this);
	if (node_prefix.count) {
		if (node_prefix.KeyMismatchPosition(*this, key, depth) != node_prefix.count) {
			return;
		}
		depth += node_prefix.count;
	}

	auto child = node.GetChild(*this, key[depth]);
	if (child) {
		D_ASSERT(child->IsSet());

		if (child->DecodeARTNodeType() == NType::LEAF) {
			// leaf found, remove entry
			auto &leaf = Leaf::Get(*this, *child);
			leaf.Remove(*this, row_id);

			if (leaf.count == 0) {
				// leaf is empty, delete leaf, decrement node counter and maybe shrink node
				Node::DeleteChild(*this, node, key[depth]);
			}
			return;
		}

		// recurse
		Erase(*child, key, depth + 1, row_id);
		node.ReplaceChild(*this, key[depth], *child);
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

	auto leaf_node = Lookup(*tree, key, 0);
	if (!leaf_node.IsSet()) {
		return true;
	}

	auto &leaf = Leaf::Get(*this, leaf_node);
	if (leaf.count > max_count) {
		return false;
	}
	for (idx_t i = 0; i < leaf.count; i++) {
		row_t row_id = leaf.GetRowId(*this, i);
		result_ids.push_back(row_id);
	}
	return true;
}

void ART::SearchEqualJoinNoFetch(ARTKey &key, idx_t &result_size) {

	// we need to look for a leaf
	auto leaf_node = Lookup(*tree, key, 0);
	if (!leaf_node.IsSet()) {
		result_size = 0;
		return;
	}

	auto &leaf = Leaf::Get(*this, leaf_node);
	result_size = leaf.count;
}

//===--------------------------------------------------------------------===//
// Lookup
//===--------------------------------------------------------------------===//

Node ART::Lookup(Node node, const ARTKey &key, idx_t depth) {

	while (node.IsSet()) {
		if (node.DecodeARTNodeType() == NType::LEAF) {
			auto &leaf = Leaf::Get(*this, node);

			// check if leaf contains key
			for (idx_t i = 0; i < leaf.prefix.count; i++) {
				if (leaf.prefix.GetByte(*this, i) != key[i + depth]) {
					return Node();
				}
			}
			return node;
		}
		auto &node_prefix = node.GetPrefix(*this);
		if (node_prefix.count) {
			for (idx_t pos = 0; pos < node_prefix.count; pos++) {
				if (key[depth + pos] != node_prefix.GetByte(*this, pos)) {
					// prefix mismatch, subtree of node does not contain key
					return Node();
				}
			}
			depth += node_prefix.count;
		}

		// prefix matches key, but no child at byte, does not contain key
		auto child = node.GetChild(*this, key[depth]);
		if (!child) {
			return Node();
		}

		// recurse into child
		node = *child;
		D_ASSERT(node.IsSet());
		depth++;
	}

	return Node();
}

//===--------------------------------------------------------------------===//
// Greater Than
// Returns: True (If found leaf >= key)
//          False (Otherwise)
//===--------------------------------------------------------------------===//

bool ART::SearchGreater(ARTIndexScanState &state, ARTKey &key, bool inclusive, idx_t max_count,
                        vector<row_t> &result_ids) {

	auto &it = state.iterator;

	// greater than scan: first set the iterator to the node at which we will start our scan by finding the lowest node
	// that satisfies our requirement
	if (!it.art) {
		it.art = this;
		if (!it.LowerBound(*tree, key, inclusive)) {
			return true;
		}
	}

	// after that we continue the scan; we don't need to check the bounds as any value following this value is
	// automatically bigger and hence satisfies our predicate
	ARTKey empty_key = ARTKey();
	return it.Scan(empty_key, max_count, result_ids, false);
}

//===--------------------------------------------------------------------===//
// Less Than
//===--------------------------------------------------------------------===//

bool ART::SearchLess(ARTIndexScanState &state, ARTKey &upper_bound, bool inclusive, idx_t max_count,
                     vector<row_t> &result_ids) {

	if (!tree->IsSet()) {
		return true;
	}

	auto &it = state.iterator;

	if (!it.art) {
		it.art = this;
		// first find the minimum value in the ART: we start scanning from this value
		it.FindMinimum(*tree);
		// early out min value higher than upper bound query
		if (it.cur_key > upper_bound) {
			return true;
		}
	}

	// now continue the scan until we reach the upper bound
	return it.Scan(upper_bound, max_count, result_ids, inclusive);
}

//===--------------------------------------------------------------------===//
// Closed Range Query
//===--------------------------------------------------------------------===//

bool ART::SearchCloseRange(ARTIndexScanState &state, ARTKey &lower_bound, ARTKey &upper_bound, bool left_inclusive,
                           bool right_inclusive, idx_t max_count, vector<row_t> &result_ids) {
	auto &it = state.iterator;

	// first find the first node that satisfies the left predicate
	if (!it.art) {
		it.art = this;
		if (!it.LowerBound(*tree, lower_bound, left_inclusive)) {
			return true;
		}
	}

	// now continue the scan until we reach the upper bound
	return it.Scan(upper_bound, max_count, result_ids, right_inclusive);
}

bool ART::Scan(const Transaction &transaction, const DataTable &table, IndexScanState &table_state,
               const idx_t max_count, vector<row_t> &result_ids) {
	auto &state = table_state.Cast<ARTIndexScanState>();
	vector<row_t> row_ids;
	bool success;

	// FIXME: the key directly owning the data for a single key might be more efficient
	D_ASSERT(state.values[0].type().InternalType() == types[0]);
	ArenaAllocator arena_allocator(Allocator::Get(db));
	auto key = CreateKey(arena_allocator, types[0], state.values[0]);

	if (state.values[1].IsNull()) {

		// single predicate
		lock_guard<mutex> l(lock);
		switch (state.expressions[0]) {
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

		D_ASSERT(state.values[1].type().InternalType() == types[0]);
		auto upper_bound = CreateKey(arena_allocator, types[0], state.values[1]);

		bool left_inclusive = state.expressions[0] == ExpressionType ::COMPARE_GREATERTHANOREQUALTO;
		bool right_inclusive = state.expressions[1] == ExpressionType ::COMPARE_LESSTHANOREQUALTO;
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

		auto leaf_node = Lookup(*tree, keys[i], 0);
		if (!leaf_node.IsSet()) {
			if (conflict_manager.AddMiss(i)) {
				found_conflict = i;
			}
			continue;
		}

		// When we find a node, we need to update the 'matches' and 'row_ids'
		// NOTE: Leafs can have more than one row_id, but for UNIQUE/PRIMARY KEY they will only have one
		Leaf &leaf = Leaf::Get(*this, leaf_node);
		D_ASSERT(leaf.count == 1);
		auto row_id = leaf.GetRowId(*this, 0);
		if (conflict_manager.AddHit(i, row_id)) {
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

BlockPointer ART::Serialize(MetaBlockWriter &writer) {

	lock_guard<mutex> l(lock);
	if (tree->IsSet()) {
		serialized_data_pointer = tree->Serialize(*this, writer);
	} else {
		serialized_data_pointer = {(block_id_t)DConstants::INVALID_INDEX, (uint32_t)DConstants::INVALID_INDEX};
	}

	return serialized_data_pointer;
}

//===--------------------------------------------------------------------===//
// Vacuum
//===--------------------------------------------------------------------===//

void ART::InitializeVacuum(ARTFlags &flags) {

	flags.vacuum_flags.reserve(allocators.size());
	for (auto &allocator : allocators) {
		flags.vacuum_flags.push_back(allocator->InitializeVacuum());
	}
}

void ART::FinalizeVacuum(const ARTFlags &flags) {

	for (idx_t i = 0; i < allocators.size(); i++) {
		if (flags.vacuum_flags[i]) {
			allocators[i]->FinalizeVacuum();
		}
	}
}

void ART::Vacuum(IndexLock &state) {

	if (!tree->IsSet()) {
		for (auto &allocator : allocators) {
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
	Node::Vacuum(*this, *tree, flags);

	// finalize the vacuum operation
	FinalizeVacuum(flags);

	for (auto &allocator : allocators) {
		allocator->Verify();
	}
}

//===--------------------------------------------------------------------===//
// Merging
//===--------------------------------------------------------------------===//

void ART::InitializeMerge(ARTFlags &flags) {

	flags.merge_buffer_counts.reserve(allocators.size());
	for (auto &allocator : allocators) {
		flags.merge_buffer_counts.emplace_back(allocator->buffers.size());
	}
}

bool ART::MergeIndexes(IndexLock &state, Index &other_index) {

	auto &other_art = other_index.Cast<ART>();
	if (!other_art.tree->IsSet()) {
		return true;
	}

	if (tree->IsSet()) {
		//  fully deserialize other_index, and traverse it to increment its buffer IDs
		ARTFlags flags;
		InitializeMerge(flags);
		other_art.tree->InitializeMerge(other_art, flags);
	}

	// merge the node storage
	for (idx_t i = 0; i < allocators.size(); i++) {
		allocators[i]->Merge(*other_art.allocators[i]);
	}

	// merge the ARTs
	if (!tree->Merge(*this, *other_art.tree)) {
		return false;
	}

	for (auto &allocator : allocators) {
		allocator->Verify();
	}
	return true;
}

//===--------------------------------------------------------------------===//
// Utility
//===--------------------------------------------------------------------===//

string ART::VerifyAndToString(IndexLock &state, const bool only_verify) {
	return VerifyAndToStringInternal(only_verify);
}

string ART::VerifyAndToStringInternal(const bool only_verify) {
	if (tree->IsSet()) {
		return "ART: " + tree->VerifyAndToString(*this, only_verify);
	}
	return "[empty]";
}

} // namespace duckdb
