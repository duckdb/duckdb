#include "duckdb/execution/index/art/art.hpp"

#include "duckdb/common/types/conflict_manager.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/execution/index/art/art_key.hpp"
#include "duckdb/execution/index/art/iterator.hpp"
#include "duckdb/execution/index/art/leaf.hpp"
#include "duckdb/execution/index/art/node16.hpp"
#include "duckdb/execution/index/art/node256.hpp"
#include "duckdb/execution/index/art/node4.hpp"
#include "duckdb/execution/index/art/node48.hpp"
#include "duckdb/execution/index/art/prefix.hpp"
#include "duckdb/optimizer/matcher/expression_matcher.hpp"
#include "duckdb/planner/expression/bound_between_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/storage/arena_allocator.hpp"
#include "duckdb/storage/metadata/metadata_reader.hpp"
#include "duckdb/storage/table/scan_state.hpp"
#include "duckdb/storage/table_io_manager.hpp"

namespace duckdb {

struct ARTIndexScanState : public IndexScanState {
	//! The predicates to scan. A single predicate for point lookups, and two predicates for range scans.
	Value values[2];
	//! The expressions over the scan predicates.
	ExpressionType expressions[2];
	bool checked = false;
	//! All scanned row IDs.
	unsafe_vector<row_t> row_ids;
};

//===--------------------------------------------------------------------===//
// ART
//===--------------------------------------------------------------------===//

ART::ART(const string &name, const IndexConstraintType index_constraint_type, const vector<column_t> &column_ids,
         TableIOManager &table_io_manager, const vector<unique_ptr<Expression>> &unbound_expressions,
         AttachedDatabase &db, const shared_ptr<array<unique_ptr<FixedSizeAllocator>, ALLOCATOR_COUNT>> &allocators_ptr,
         const IndexStorageInfo &info)
    : BoundIndex(name, ART::TYPE_NAME, index_constraint_type, column_ids, table_io_manager, unbound_expressions, db),
      allocators(allocators_ptr), owns_data(false) {

	// Validate the key column types.
	// FIXME: Use the new byte representation function to support nested types.
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
		case PhysicalType::UINT128:
		case PhysicalType::FLOAT:
		case PhysicalType::DOUBLE:
		case PhysicalType::VARCHAR:
			break;
		default:
			throw InvalidTypeException(logical_types[i], "Invalid type for index key.");
		}
	}

	// Initialize the allocators.
	if (!allocators) {
		owns_data = true;
		auto &block_manager = table_io_manager.GetIndexBlockManager();
		array<unique_ptr<FixedSizeAllocator>, ALLOCATOR_COUNT> allocator_array = {
		    make_uniq<FixedSizeAllocator>(sizeof(Prefix), block_manager),
		    make_uniq<FixedSizeAllocator>(sizeof(Leaf), block_manager),
		    make_uniq<FixedSizeAllocator>(sizeof(Node4), block_manager),
		    make_uniq<FixedSizeAllocator>(sizeof(Node16), block_manager),
		    make_uniq<FixedSizeAllocator>(sizeof(Node48), block_manager),
		    make_uniq<FixedSizeAllocator>(sizeof(Node256), block_manager),
		    make_uniq<FixedSizeAllocator>(sizeof(Node7Leaf), block_manager),
		    make_uniq<FixedSizeAllocator>(sizeof(Node15Leaf), block_manager),
		    make_uniq<FixedSizeAllocator>(sizeof(Node256Leaf), block_manager),
		};
		allocators =
		    make_shared_ptr<array<unique_ptr<FixedSizeAllocator>, ALLOCATOR_COUNT>>(std::move(allocator_array));
	}

	if (!info.IsValid()) {
		// We are creating a new ART.
		return;
	}

	if (info.root_block_ptr.IsValid()) {
		// Backwards compatibility.
		Deserialize(info.root_block_ptr);
		return;
	}

	// Set the root node and initialize the allocators.
	tree.Set(info.root);
	InitAllocators(info);
}

//===--------------------------------------------------------------------===//
// Initialize Scans
//===--------------------------------------------------------------------===//

static unique_ptr<IndexScanState> InitializeScanSinglePredicate(const Value &value,
                                                                const ExpressionType expression_type) {
	auto result = make_uniq<ARTIndexScanState>();
	result->values[0] = value;
	result->expressions[0] = expression_type;
	return std::move(result);
}

static unique_ptr<IndexScanState> InitializeScanTwoPredicates(const Value &low_value,
                                                              const ExpressionType low_expression_type,
                                                              const Value &high_value,
                                                              const ExpressionType high_expression_type) {
	auto result = make_uniq<ARTIndexScanState>();
	result->values[0] = low_value;
	result->expressions[0] = low_expression_type;
	result->values[1] = high_value;
	result->expressions[1] = high_expression_type;
	return std::move(result);
}

unique_ptr<IndexScanState> ART::TryInitializeScan(const Expression &expr, const Expression &filter_expr) {
	Value low_value, high_value, equal_value;
	ExpressionType low_comparison_type = ExpressionType::INVALID, high_comparison_type = ExpressionType::INVALID;

	// Try to find a matching index for any of the filter expressions.
	ComparisonExpressionMatcher matcher;
	// Match on a comparison type.
	matcher.expr_type = make_uniq<ComparisonExpressionTypeMatcher>();
	// Match on a constant comparison with the indexed expression.
	matcher.matchers.push_back(make_uniq<ExpressionEqualityMatcher>(expr));
	matcher.matchers.push_back(make_uniq<ConstantExpressionMatcher>());
	matcher.policy = SetMatcher::Policy::UNORDERED;

	vector<reference<Expression>> bindings;
	auto filter_match =
	    matcher.Match(const_cast<Expression &>(filter_expr), bindings); // NOLINT: Match does not alter the expr.
	if (filter_match) {
		// This is a range or equality comparison with a constant value, so we can use the index.
		// 		bindings[0] = the expression
		// 		bindings[1] = the index expression
		// 		bindings[2] = the constant
		auto &comparison = bindings[0].get().Cast<BoundComparisonExpression>();
		auto constant_value = bindings[2].get().Cast<BoundConstantExpression>().value;
		auto comparison_type = comparison.type;

		if (comparison.left->type == ExpressionType::VALUE_CONSTANT) {
			// The expression is on the right side, we flip the comparison expression.
			comparison_type = FlipComparisonExpression(comparison_type);
		}

		if (comparison_type == ExpressionType::COMPARE_EQUAL) {
			// An equality value overrides any other bounds.
			equal_value = constant_value;
		} else if (comparison_type == ExpressionType::COMPARE_GREATERTHANOREQUALTO ||
		           comparison_type == ExpressionType::COMPARE_GREATERTHAN) {
			// This is a lower bound.
			low_value = constant_value;
			low_comparison_type = comparison_type;
		} else {
			// This is an upper bound.
			high_value = constant_value;
			high_comparison_type = comparison_type;
		}

	} else if (filter_expr.type == ExpressionType::COMPARE_BETWEEN) {
		auto &between = filter_expr.Cast<BoundBetweenExpression>();
		if (!between.input->Equals(expr)) {
			// The expression does not match the index expression.
			return nullptr;
		}

		if (between.lower->type != ExpressionType::VALUE_CONSTANT ||
		    between.upper->type != ExpressionType::VALUE_CONSTANT) {
			// Not a constant expression.
			return nullptr;
		}

		low_value = between.lower->Cast<BoundConstantExpression>().value;
		low_comparison_type = between.lower_inclusive ? ExpressionType::COMPARE_GREATERTHANOREQUALTO
		                                              : ExpressionType::COMPARE_GREATERTHAN;
		high_value = (between.upper->Cast<BoundConstantExpression>()).value;
		high_comparison_type =
		    between.upper_inclusive ? ExpressionType::COMPARE_LESSTHANOREQUALTO : ExpressionType::COMPARE_LESSTHAN;
	}

	// We cannot use an index scan.
	if (equal_value.IsNull() && low_value.IsNull() && high_value.IsNull()) {
		return nullptr;
	}

	// Initialize the index scan state and return it.
	if (!equal_value.IsNull()) {
		// Equality predicate.
		return InitializeScanSinglePredicate(equal_value, ExpressionType::COMPARE_EQUAL);
	}
	if (!low_value.IsNull() && !high_value.IsNull()) {
		// Two-sided predicate.
		return InitializeScanTwoPredicates(low_value, low_comparison_type, high_value, high_comparison_type);
	}
	if (!low_value.IsNull()) {
		// Less-than predicate.
		return InitializeScanSinglePredicate(low_value, low_comparison_type);
	}
	// Greater-than predicate.
	return InitializeScanSinglePredicate(high_value, high_comparison_type);
}

//===--------------------------------------------------------------------===//
// ART Keys
//===--------------------------------------------------------------------===//

template <class T, bool IS_NOT_NULL>
static void TemplatedGenerateKeys(ArenaAllocator &allocator, Vector &input, idx_t count, unsafe_vector<ARTKey> &keys) {

	D_ASSERT(keys.size() >= count);
	UnifiedVectorFormat idata;
	input.ToUnifiedFormat(count, idata);
	auto input_data = UnifiedVectorFormat::GetData<T>(idata);

	for (idx_t i = 0; i < count; i++) {
		auto idx = idata.sel->get_index(i);

		if (IS_NOT_NULL || idata.validity.RowIsValid(idx)) {
			ARTKey::CreateARTKey<T>(allocator, input.GetType(), keys[i], input_data[idx]);
			continue;
		}

		// We need to reset the key value in the reusable keys vector.
		keys[i] = ARTKey();
	}
}

template <class T, bool IS_NOT_NULL>
static void ConcatenateKeys(ArenaAllocator &allocator, Vector &input, idx_t count, unsafe_vector<ARTKey> &keys) {

	UnifiedVectorFormat idata;
	input.ToUnifiedFormat(count, idata);
	auto input_data = UnifiedVectorFormat::GetData<T>(idata);

	for (idx_t i = 0; i < count; i++) {
		auto idx = idata.sel->get_index(i);

		if (IS_NOT_NULL) {
			auto other_key = ARTKey::CreateARTKey<T>(allocator, input.GetType(), input_data[idx]);
			keys[i].ConcatenateARTKey(allocator, other_key);
			continue;
		}

		// A previous column entry was NULL.
		if (keys[i].Empty()) {
			continue;
		}

		// This column entry is NULL, so we set the whole key to NULL.
		if (!idata.validity.RowIsValid(idx)) {
			keys[i] = ARTKey();
			continue;
		}

		// Concatenate the keys.
		auto other_key = ARTKey::CreateARTKey<T>(allocator, input.GetType(), input_data[idx]);
		keys[i].ConcatenateARTKey(allocator, other_key);
	}
}

template <bool IS_NOT_NULL>
void GenerateKeysInternal(ArenaAllocator &allocator, DataChunk &input, unsafe_vector<ARTKey> &keys) {
	switch (input.data[0].GetType().InternalType()) {
	case PhysicalType::BOOL:
		TemplatedGenerateKeys<bool, IS_NOT_NULL>(allocator, input.data[0], input.size(), keys);
		break;
	case PhysicalType::INT8:
		TemplatedGenerateKeys<int8_t, IS_NOT_NULL>(allocator, input.data[0], input.size(), keys);
		break;
	case PhysicalType::INT16:
		TemplatedGenerateKeys<int16_t, IS_NOT_NULL>(allocator, input.data[0], input.size(), keys);
		break;
	case PhysicalType::INT32:
		TemplatedGenerateKeys<int32_t, IS_NOT_NULL>(allocator, input.data[0], input.size(), keys);
		break;
	case PhysicalType::INT64:
		TemplatedGenerateKeys<int64_t, IS_NOT_NULL>(allocator, input.data[0], input.size(), keys);
		break;
	case PhysicalType::INT128:
		TemplatedGenerateKeys<hugeint_t, IS_NOT_NULL>(allocator, input.data[0], input.size(), keys);
		break;
	case PhysicalType::UINT8:
		TemplatedGenerateKeys<uint8_t, IS_NOT_NULL>(allocator, input.data[0], input.size(), keys);
		break;
	case PhysicalType::UINT16:
		TemplatedGenerateKeys<uint16_t, IS_NOT_NULL>(allocator, input.data[0], input.size(), keys);
		break;
	case PhysicalType::UINT32:
		TemplatedGenerateKeys<uint32_t, IS_NOT_NULL>(allocator, input.data[0], input.size(), keys);
		break;
	case PhysicalType::UINT64:
		TemplatedGenerateKeys<uint64_t, IS_NOT_NULL>(allocator, input.data[0], input.size(), keys);
		break;
	case PhysicalType::UINT128:
		TemplatedGenerateKeys<uhugeint_t, IS_NOT_NULL>(allocator, input.data[0], input.size(), keys);
		break;
	case PhysicalType::FLOAT:
		TemplatedGenerateKeys<float, IS_NOT_NULL>(allocator, input.data[0], input.size(), keys);
		break;
	case PhysicalType::DOUBLE:
		TemplatedGenerateKeys<double, IS_NOT_NULL>(allocator, input.data[0], input.size(), keys);
		break;
	case PhysicalType::VARCHAR:
		TemplatedGenerateKeys<string_t, IS_NOT_NULL>(allocator, input.data[0], input.size(), keys);
		break;
	default:
		throw InternalException("Invalid type for index");
	}

	// We concatenate the keys for each remaining column of a compound key.
	for (idx_t i = 1; i < input.ColumnCount(); i++) {
		switch (input.data[i].GetType().InternalType()) {
		case PhysicalType::BOOL:
			ConcatenateKeys<bool, IS_NOT_NULL>(allocator, input.data[i], input.size(), keys);
			break;
		case PhysicalType::INT8:
			ConcatenateKeys<int8_t, IS_NOT_NULL>(allocator, input.data[i], input.size(), keys);
			break;
		case PhysicalType::INT16:
			ConcatenateKeys<int16_t, IS_NOT_NULL>(allocator, input.data[i], input.size(), keys);
			break;
		case PhysicalType::INT32:
			ConcatenateKeys<int32_t, IS_NOT_NULL>(allocator, input.data[i], input.size(), keys);
			break;
		case PhysicalType::INT64:
			ConcatenateKeys<int64_t, IS_NOT_NULL>(allocator, input.data[i], input.size(), keys);
			break;
		case PhysicalType::INT128:
			ConcatenateKeys<hugeint_t, IS_NOT_NULL>(allocator, input.data[i], input.size(), keys);
			break;
		case PhysicalType::UINT8:
			ConcatenateKeys<uint8_t, IS_NOT_NULL>(allocator, input.data[i], input.size(), keys);
			break;
		case PhysicalType::UINT16:
			ConcatenateKeys<uint16_t, IS_NOT_NULL>(allocator, input.data[i], input.size(), keys);
			break;
		case PhysicalType::UINT32:
			ConcatenateKeys<uint32_t, IS_NOT_NULL>(allocator, input.data[i], input.size(), keys);
			break;
		case PhysicalType::UINT64:
			ConcatenateKeys<uint64_t, IS_NOT_NULL>(allocator, input.data[i], input.size(), keys);
			break;
		case PhysicalType::UINT128:
			ConcatenateKeys<uhugeint_t, IS_NOT_NULL>(allocator, input.data[i], input.size(), keys);
			break;
		case PhysicalType::FLOAT:
			ConcatenateKeys<float, IS_NOT_NULL>(allocator, input.data[i], input.size(), keys);
			break;
		case PhysicalType::DOUBLE:
			ConcatenateKeys<double, IS_NOT_NULL>(allocator, input.data[i], input.size(), keys);
			break;
		case PhysicalType::VARCHAR:
			ConcatenateKeys<string_t, IS_NOT_NULL>(allocator, input.data[i], input.size(), keys);
			break;
		default:
			throw InternalException("Invalid type for index");
		}
	}
}

template <>
void ART::GenerateKeys<>(ArenaAllocator &allocator, DataChunk &input, unsafe_vector<ARTKey> &keys) {
	GenerateKeysInternal<false>(allocator, input, keys);
}

template <>
void ART::GenerateKeys<true>(ArenaAllocator &allocator, DataChunk &input, unsafe_vector<ARTKey> &keys) {
	GenerateKeysInternal<true>(allocator, input, keys);
}

void ART::GenerateKeyVectors(ArenaAllocator &allocator, DataChunk &input, Vector &row_ids, unsafe_vector<ARTKey> &keys,
                             unsafe_vector<ARTKey> &row_id_keys) {
	GenerateKeys<>(allocator, input, keys);

	DataChunk row_id_chunk;
	row_id_chunk.Initialize(Allocator::DefaultAllocator(), vector<LogicalType> {LogicalType::ROW_TYPE}, keys.size());
	row_id_chunk.data[0].Reference(row_ids);
	row_id_chunk.SetCardinality(keys.size());
	GenerateKeys<>(allocator, row_id_chunk, row_id_keys);
}

//===--------------------------------------------------------------------===//
// Construct from sorted data (only during CREATE (UNIQUE) INDEX statements)
//===--------------------------------------------------------------------===//

bool ART::Construct(const unsafe_vector<ARTKey> &keys, const unsafe_vector<ARTKey> &row_ids, Node &node,
                    ARTKeySection &section) {
	D_ASSERT(section.start < keys.size());
	D_ASSERT(section.end < keys.size());
	D_ASSERT(section.start <= section.end);

	auto &start_key = keys[section.start];
	auto &end_key = keys[section.end];

	// Increment the depth until we reach a leaf or find a mismatching byte.
	auto prefix_start = section.depth;
	while (start_key.len != section.depth &&
	       start_key.ByteMatches(end_key, UnsafeNumericCast<uint32_t>(section.depth))) {
		section.depth++;
	}

	// We reached a leaf, i.e. all the bytes of start_key and end_key match.
	if (start_key.len == section.depth) {
		// end_idx is inclusive.
		auto num_row_ids = section.end - section.start + 1;

		// Check for a possible constraint violation.
		if (IsUnique() && num_row_ids != 1) {
			return false;
		}

		// Create the prefix.
		reference<Node> ref_node(node);
		Prefix::New(*this, ref_node, start_key, UnsafeNumericCast<uint32_t>(prefix_start),
		            UnsafeNumericCast<uint32_t>(start_key.len - prefix_start));

		// Create the leaf.
		if (num_row_ids == 1) {
			Leaf::New(ref_node, row_ids[section.start].GetRowID());
		} else {
			Leaf::New(*this, ref_node, row_ids, section.start, num_row_ids);
		}
		return true;
	}

	// Create a new node and recurse.

	// There are at least two child entries for this node. Otherwise, we would have reached a leaf.
	unsafe_vector<ARTKeySection> child_sections;
	section.GetChildSections(child_sections, keys);

	// Create the prefix.
	reference<Node> ref_node(node);
	auto prefix_length = section.depth - prefix_start;
	Prefix::New(*this, ref_node, start_key, UnsafeNumericCast<uint32_t>(prefix_start),
	            UnsafeNumericCast<uint32_t>(prefix_length));

	// Create the node.
	auto node_type = Node::NodeTypeByCount(child_sections.size());
	Node::New(*this, ref_node, node_type);

	// Recurse on each child section.
	for (auto &child_section : child_sections) {
		Node new_child;
		auto success = Construct(keys, row_ids, new_child, child_section);
		Node::InsertChild(*this, ref_node, child_section.key_byte, new_child);
		if (!success) {
			return false;
		}
	}
	return true;
}

bool ART::ConstructFromSorted(const unsafe_vector<ARTKey> &keys, const unsafe_vector<ARTKey> &row_id_keys,
                              const idx_t row_count) {
	ARTKeySection section(0, row_count - 1, 0, 0);
	if (!Construct(keys, row_id_keys, tree, section)) {
		return false;
	}

#ifdef DEBUG
	D_ASSERT(!VerifyAndToStringInternal(true).empty());
	for (idx_t i = 0; i < row_count; i++) {
		D_ASSERT(!keys[i].Empty());
		auto leaf = Lookup(tree, keys[i], 0);
		D_ASSERT(Leaf::ContainsRowId(*this, *leaf, row_id_keys[i]));
	}
#endif

	return true;
}

//===--------------------------------------------------------------------===//
// Insert / Verification / Constraint Checking
//===--------------------------------------------------------------------===//

ErrorData ART::Insert(IndexLock &lock, DataChunk &input, Vector &row_ids) {
	D_ASSERT(row_ids.GetType().InternalType() == ROW_TYPE);
	auto row_count = input.size();

	ArenaAllocator allocator(BufferAllocator::Get(db));
	unsafe_vector<ARTKey> keys(row_count);
	unsafe_vector<ARTKey> row_id_keys(row_count);
	GenerateKeyVectors(allocator, input, row_ids, keys, row_id_keys);

	// Insert the entries into the index.
	idx_t failed_index = DConstants::INVALID_INDEX;
	for (idx_t i = 0; i < row_count; i++) {
		if (keys[i].Empty()) {
			continue;
		}
		if (!Insert(tree, keys[i], 0, row_id_keys[i], tree.IsGate())) {
			// Insertion failure due to a constraint violation.
			failed_index = i;
			break;
		}
	}

	// Remove any previously inserted entries.
	if (failed_index != DConstants::INVALID_INDEX) {
		for (idx_t i = 0; i < failed_index; i++) {
			if (keys[i].Empty()) {
				continue;
			}
			Erase(tree, keys[i], 0, row_id_keys[i], tree.IsGate());
		}
	}

	if (failed_index != DConstants::INVALID_INDEX) {
		auto msg = AppendRowError(input, failed_index);
		return ErrorData(ConstraintException("PRIMARY KEY or UNIQUE constraint violated: duplicate key \"%s\"", msg));
	}

#ifdef DEBUG
	for (idx_t i = 0; i < row_count; i++) {
		if (keys[i].Empty()) {
			continue;
		}
		auto leaf = Lookup(tree, keys[i], 0);
		D_ASSERT(Leaf::ContainsRowId(*this, *leaf, row_id_keys[i]));
	}
#endif

	return ErrorData();
}

ErrorData ART::Append(IndexLock &lock, DataChunk &input, Vector &row_ids) {
	// Execute all column expressions before inserting the data chunk.
	DataChunk expr_chunk;
	expr_chunk.Initialize(Allocator::DefaultAllocator(), logical_types);
	ExecuteExpressions(input, expr_chunk);
	return Insert(lock, expr_chunk, row_ids);
}

void ART::VerifyAppend(DataChunk &chunk) {
	ConflictManager conflict_manager(VerifyExistenceType::APPEND, chunk.size());
	CheckConstraintsForChunk(chunk, conflict_manager);
}

void ART::VerifyAppend(DataChunk &chunk, ConflictManager &conflict_manager) {
	D_ASSERT(conflict_manager.LookupType() == VerifyExistenceType::APPEND);
	CheckConstraintsForChunk(chunk, conflict_manager);
}

void ART::InsertIntoEmptyNode(Node &node, const ARTKey &key, idx_t depth, const ARTKey &row_id_key,
                              const bool inside_gate) {
	D_ASSERT(depth <= key.len);

	auto depth_byte = UnsafeNumericCast<uint32_t>(depth);
	idx_t count = key.len - depth;
	if (inside_gate) {
		PrefixInlined::New(*this, node, key, depth_byte, UnsafeNumericCast<uint8_t>(count));
		return;
	}

	reference<Node> ref_node(node);
	Prefix::New(*this, ref_node, key, depth_byte, UnsafeNumericCast<uint32_t>(count));
	Leaf::New(ref_node, row_id_key.GetRowID());
}

bool ART::Insert(Node &node, reference<const ARTKey> key, idx_t depth, reference<const ARTKey> row_id_key,
                 const bool inside_gate) {
	// If the node is empty, we create a new inlined leaf.
	if (!node.HasMetadata()) {
		InsertIntoEmptyNode(node, key, depth, row_id_key, inside_gate);
		return true;
	}

	// Enter a nested leaf.
	if (node.IsGate()) {
		key = row_id_key;
		depth = 0;
	}

	switch (node.GetType()) {
	case NType::LEAF_INLINED:
		// Insert the row ID into an existing inlined leaf.
		if (IsUnique()) {
			return false;
		}
		D_ASSERT(!inside_gate);
		Leaf::InsertIntoInlined(*this, node, row_id_key);
		return true;

	case NType::LEAF: {
		// Transform a deprecated leaf.
		Leaf::TransformToNested(*this, node);
		return Insert(node, key, depth, row_id_key, inside_gate);
	}

	case NType::NODE_7_LEAF:
	case NType::NODE_15_LEAF:
	case NType::NODE_256_LEAF:
		// TODO

	case NType::PREFIX_INLINED:
		// TODO

	case NType::NODE_4:
	case NType::NODE_16:
	case NType::NODE_48:
	case NType::NODE_256: {
		D_ASSERT(depth < key.get().len);
		auto child = node.GetChildMutable(*this, key.get()[depth]);

		// Recurse, if a child exists at key[depth].
		if (child) {
			bool success = Insert(*child, key, depth + 1, row_id_key, inside_gate);
			node.ReplaceChild(*this, key.get()[depth], *child);
			return success;
		}

		// Insert a new inlined leaf at key[depth].
		D_ASSERT(!inside_gate);
		Node leaf_node;
		reference<Node> ref_node(leaf_node);
		if (depth + 1 < key.get().len) {
			// Create the prefix.
			auto byte = UnsafeNumericCast<uint32_t>(depth + 1);
			auto count = UnsafeNumericCast<uint32_t>(key.get().len - depth - 1);
			Prefix::New(*this, ref_node, key, byte, count);
		}
		// Create the inlined leaf.
		Leaf::New(ref_node, row_id_key.get().GetRowID());
		Node::InsertChild(*this, node, key.get()[depth], leaf_node);
		return true;
	}

	case NType::PREFIX: {
		// If this is a prefix node, we traverse the prefix.
		reference<Node> next_node(node);
		auto mismatch_pos = Prefix::Traverse<Prefix, Node>(*this, next_node, key, depth, &Node::RefMutable<Prefix>);

		// We recurse into the next node, if
		// (1) the prefix matches the key, or
		// (2) we reach a gate (which can start with a PREFIX node).
		if (mismatch_pos == DConstants::INVALID_INDEX) {
			if (next_node.get().GetType() != NType::PREFIX || next_node.get().IsGate()) {
				return Insert(next_node, key, depth, row_id_key, inside_gate);
			}
		}

		// If the prefix does not match the key, we create a new Node4. It has two children, which are
		// the remaining part of the prefix, and the new inlined leaf.
		Node remaining_prefix;
		auto prefix_byte = Prefix::GetByte(*this, next_node, UnsafeNumericCast<uint8_t>(mismatch_pos));
		auto freed_gate = Prefix::Split(*this, next_node, remaining_prefix, UnsafeNumericCast<uint8_t>(mismatch_pos));
		Node4::New(*this, next_node);
		if (freed_gate) {
			next_node.get().SetGate();
		}

		// Insert the remaining prefix into the new Node4.
		Node4::InsertChild(*this, next_node, prefix_byte, remaining_prefix);

		// Insert the new inlined leaf.
		D_ASSERT(!inside_gate);
		Node leaf_node;
		reference<Node> ref_node(leaf_node);
		if (depth + 1 < key.get().len) {
			// Create the prefix.
			auto byte = UnsafeNumericCast<uint32_t>(depth + 1);
			auto count = UnsafeNumericCast<uint32_t>(key.get().len - depth - 1);
			Prefix::New(*this, ref_node, key, byte, count);
		}
		// Create the inlined leaf.
		Leaf::New(ref_node, row_id_key.get().GetRowID());
		Node4::InsertChild(*this, next_node, key.get()[depth], leaf_node);
		return true;
	}
	default:
		throw InternalException("Invalid node type for Insert.");
	}
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
	auto row_count = input.size();

	DataChunk expr_chunk;
	expr_chunk.Initialize(Allocator::DefaultAllocator(), logical_types);
	ExecuteExpressions(input, expr_chunk);

	ArenaAllocator allocator(BufferAllocator::Get(db));
	unsafe_vector<ARTKey> keys(row_count);
	unsafe_vector<ARTKey> row_id_keys(row_count);
	GenerateKeyVectors(allocator, expr_chunk, row_ids, keys, row_id_keys);

	for (idx_t i = 0; i < row_count; i++) {
		if (keys[i].Empty()) {
			continue;
		}
		Erase(tree, keys[i], 0, row_id_keys[i], tree.IsGate());
	}

#ifdef DEBUG
	for (idx_t i = 0; i < row_count; i++) {
		if (keys[i].Empty()) {
			continue;
		}
		auto leaf = Lookup(tree, keys[i], 0);
		D_ASSERT(!leaf || !Leaf::ContainsRowId(*this, *leaf, row_id_keys[i]));
	}
#endif
}

void ART::Erase(Node &node, reference<const ARTKey> key, idx_t depth, reference<const ARTKey> row_id_key,
                bool inside_gate) {
	// TODO: add description with "ahead looking for inlined leaf, otherwise recurse

	if (!node.HasMetadata()) {
		return;
	}

	// Traverse the prefix.
	reference<Node> next_node(node);
	if (next_node.get().GetType() == NType::PREFIX) {
		Prefix::Traverse<Prefix, Node>(*this, next_node, key, depth, &Node::RefMutable<Prefix>);
		if (next_node.get().GetType() == NType::PREFIX && !next_node.get().IsGate()) {
			return;
		}
	}

	//	Delete the row ID from the leaf.
	//	This is the root node, which can be a leaf with possible prefix nodes.
	if (next_node.get().GetType() == NType::LEAF_INLINED) {
		if (next_node.get().GetRowId() == row_id_key.get().GetRowID()) {
			Node::Free(*this, node);
		}
		return;
	}

	// Transform a deprecated leaf.
	if (next_node.get().GetType() == NType::LEAF) {
		D_ASSERT(!inside_gate);
		Leaf::TransformToNested(*this, next_node);
	}

	// Enter a nested leaf.
	if (!inside_gate && next_node.get().IsGate()) {
		return Leaf::EraseFromNested(*this, next_node, row_id_key);
	}

	D_ASSERT(depth < key.get().len);
	auto child = next_node.get().GetChildMutable(*this, key.get()[depth]);
	if (!child) {
		return;
	}

	// Transform a deprecated leaf.
	if (child->GetType() == NType::LEAF) {
		D_ASSERT(!inside_gate);
		Leaf::TransformToNested(*this, *child);
	}

	// Enter a nested leaf.
	if (!inside_gate && child->IsGate()) {
		return Leaf::EraseFromNested(*this, *child, row_id_key);
	}

	auto temp_depth = depth + 1;
	reference<Node> child_node(*child);

	if (child_node.get().GetType() == NType::PREFIX) {
		Prefix::Traverse<Prefix, Node>(*this, child_node, key, temp_depth, &Node::RefMutable<Prefix>);
		if (child_node.get().GetType() == NType::PREFIX && !child_node.get().IsGate()) {
			return;
		}
	}

	if (child_node.get().GetType() == NType::LEAF_INLINED) {
		if (child_node.get().GetRowId() == row_id_key.get().GetRowID()) {
			Node::DeleteChild(*this, next_node, node, key.get()[depth]);
		}
		return;
	}

	// Recurse.
	Erase(*child, key, depth + 1, row_id_key, inside_gate);
	next_node.get().ReplaceChild(*this, key.get()[depth], *child);
}

//===--------------------------------------------------------------------===//
// Point Query (Equal)
//===--------------------------------------------------------------------===//

bool ART::SearchEqual(ARTKey &key, idx_t max_count, unsafe_vector<row_t> &row_ids) {
	auto leaf = Lookup(tree, key, 0);
	if (!leaf) {
		return true;
	}

	Iterator it(*this);
	it.FindMinimum(*leaf);
	ARTKey empty_key = ARTKey();
	return it.Scan(empty_key, max_count, row_ids, false);
}

//===--------------------------------------------------------------------===//
// Lookup
//===--------------------------------------------------------------------===//

optional_ptr<const Node> ART::Lookup(const Node &node, const ARTKey &key, idx_t depth) {
	reference<const Node> node_ref(node);
	while (node_ref.get().HasMetadata()) {

		// Return the leaf.
		if (node_ref.get().IsAnyLeaf() || node_ref.get().IsGate()) {
			return &node_ref.get();
		}

		// Traverse the prefix.
		if (node_ref.get().GetType() == NType::PREFIX) {
			Prefix::Traverse<const Prefix, const Node>(*this, node_ref, key, depth, &Node::Ref<const Prefix>);
			if (node_ref.get().GetType() == NType::PREFIX && !node_ref.get().IsGate()) {
				// Prefix mismatch, return nullptr.
				return nullptr;
			}
			continue;
		}

		// Get the child node.
		D_ASSERT(depth < key.len);
		auto child = node_ref.get().GetChild(*this, key[depth]);

		// No child at the matching byte, return nullptr.
		if (!child) {
			return nullptr;
		}

		// Continue in the child.
		node_ref = *child;
		D_ASSERT(node_ref.get().HasMetadata());
		depth++;
	}

	return nullptr;
}

//===--------------------------------------------------------------------===//
// Greater Than and Less Than
//===--------------------------------------------------------------------===//

bool ART::SearchGreater(ARTKey &key, bool equal, idx_t max_count, unsafe_vector<row_t> &row_ids) {
	if (!tree.HasMetadata()) {
		return true;
	}

	// Find the lowest value that satisfies the predicate.
	Iterator it(*this);

	// Early-out, if the maximum value in the ART is lower than the lower bound.
	if (!it.LowerBound(tree, key, equal, 0)) {
		return true;
	}

	// We continue the scan. We do not check the bounds as any value following this value is
	// greater and satisfies our predicate.
	return it.Scan(ARTKey(), max_count, row_ids, false);
}

bool ART::SearchLess(ARTKey &upper_bound, bool equal, idx_t max_count, unsafe_vector<row_t> &row_ids) {
	if (!tree.HasMetadata()) {
		return true;
	}

	// Find the minimum value in the ART: we start scanning from this value.
	Iterator it(*this);
	it.FindMinimum(tree);

	// Early-out, if the minimum value is higher than the upper bound.
	if (it.current_key.GreaterThan(upper_bound, equal)) {
		return true;
	}

	// Continue the scan until we reach the upper bound.
	return it.Scan(upper_bound, max_count, row_ids, equal);
}

//===--------------------------------------------------------------------===//
// Closed Range Query
//===--------------------------------------------------------------------===//

bool ART::SearchCloseRange(ARTKey &lower_bound, ARTKey &upper_bound, bool left_equal, bool right_equal, idx_t max_count,
                           unsafe_vector<row_t> &row_ids) {
	// Find the first node that satisfies the left predicate.
	Iterator it(*this);

	// Early-out, if the maximum value in the ART is lower than the lower bound.
	if (!it.LowerBound(tree, lower_bound, left_equal, 0)) {
		return true;
	}

	// Continue the scan until we reach the upper bound.
	return it.Scan(upper_bound, max_count, row_ids, right_equal);
}

bool ART::Scan(IndexScanState &state, const idx_t max_count, unsafe_vector<row_t> &row_ids) {
	auto &scan_state = state.Cast<ARTIndexScanState>();
	D_ASSERT(scan_state.values[0].type().InternalType() == types[0]);
	ArenaAllocator arena_allocator(Allocator::Get(db));
	auto key = ARTKey::CreateKey(arena_allocator, types[0], scan_state.values[0]);

	if (scan_state.values[1].IsNull()) {
		// Single predicate.
		lock_guard<mutex> l(lock);
		switch (scan_state.expressions[0]) {
		case ExpressionType::COMPARE_EQUAL:
			return SearchEqual(key, max_count, row_ids);
		case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
			return SearchGreater(key, true, max_count, row_ids);
		case ExpressionType::COMPARE_GREATERTHAN:
			return SearchGreater(key, false, max_count, row_ids);
		case ExpressionType::COMPARE_LESSTHANOREQUALTO:
			return SearchLess(key, true, max_count, row_ids);
		case ExpressionType::COMPARE_LESSTHAN:
			return SearchLess(key, false, max_count, row_ids);
		default:
			throw InternalException("Index scan type not implemented");
		}
	}

	// Two predicates.
	lock_guard<mutex> l(lock);
	D_ASSERT(scan_state.values[1].type().InternalType() == types[0]);
	auto upper_bound = ARTKey::CreateKey(arena_allocator, types[0], scan_state.values[1]);
	bool left_equal = scan_state.expressions[0] == ExpressionType ::COMPARE_GREATERTHANOREQUALTO;
	bool right_equal = scan_state.expressions[1] == ExpressionType ::COMPARE_LESSTHANOREQUALTO;
	return SearchCloseRange(key, upper_bound, left_equal, right_equal, max_count, row_ids);
}

//===--------------------------------------------------------------------===//
// More Verification / Constraint Checking
//===--------------------------------------------------------------------===//

string ART::GenerateErrorKeyName(DataChunk &input, idx_t row_idx) {
	DataChunk expr_chunk;
	expr_chunk.Initialize(Allocator::DefaultAllocator(), logical_types);
	ExecuteExpressions(input, expr_chunk);

	string key_name;
	for (idx_t k = 0; k < expr_chunk.ColumnCount(); k++) {
		if (k > 0) {
			key_name += ", ";
		}
		key_name += unbound_expressions[k]->GetName() + ": " + expr_chunk.data[k].GetValue(row_idx).ToString();
	}
	return key_name;
}

string ART::GenerateConstraintErrorMessage(VerifyExistenceType verify_type, const string &key_name) {
	switch (verify_type) {
	case VerifyExistenceType::APPEND: {
		// APPEND to PK/UNIQUE table, but node/key already exists in PK/UNIQUE table.
		string type = IsPrimary() ? "primary key" : "unique";
		return StringUtil::Format("Duplicate key \"%s\" violates %s constraint. "
		                          "If this is an unexpected constraint violation please double "
		                          "check with the known index limitations section in our documentation "
		                          "(https://duckdb.org/docs/sql/indexes).",
		                          key_name, type);
	}
	case VerifyExistenceType::APPEND_FK: {
		// APPEND_FK to FK table, node/key does not exist in PK/UNIQUE table.
		return StringUtil::Format(
		    "Violates foreign key constraint because key \"%s\" does not exist in the referenced table", key_name);
	}
	case VerifyExistenceType::DELETE_FK: {
		// DELETE_FK that still exists in a FK table, i.e., not a valid delete.
		return StringUtil::Format("Violates foreign key constraint because key \"%s\" is still referenced by a foreign "
		                          "key in a different table",
		                          key_name);
	}
	default:
		throw NotImplementedException("Type not implemented for VerifyExistenceType");
	}
}

void ART::CheckConstraintsForChunk(DataChunk &input, ConflictManager &conflict_manager) {
	// Lock the index during constraint checking.
	lock_guard<mutex> l(lock);

	DataChunk expr_chunk;
	expr_chunk.Initialize(Allocator::DefaultAllocator(), logical_types);
	ExecuteExpressions(input, expr_chunk);

	ArenaAllocator arena_allocator(BufferAllocator::Get(db));
	unsafe_vector<ARTKey> keys(expr_chunk.size());
	GenerateKeys<>(arena_allocator, expr_chunk, keys);

	auto found_conflict = DConstants::INVALID_INDEX;
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

		// If we find a node, we need to update the 'matches' and 'row_ids'.
		// We only perform constraint checking on unique indexes, i.e., all leaves are inlined.
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

string ART::GetConstraintViolationMessage(VerifyExistenceType verify_type, idx_t failed_index, DataChunk &input) {
	auto key_name = GenerateErrorKeyName(input, failed_index);
	auto exception_msg = GenerateConstraintErrorMessage(verify_type, key_name);
	return exception_msg;
}

//===--------------------------------------------------------------------===//
// Helper functions for (de)serialization
//===--------------------------------------------------------------------===//

IndexStorageInfo ART::GetStorageInfo(const case_insensitive_map_t<Value> &options, const bool to_wal) {
	// If the storage format uses deprecated leaf storage,
	// then we need to transform all nested leaves before serialization.
	auto v1_0_0_option = options.find("v1_0_0_storage");
	bool v1_0_0_storage = v1_0_0_option == options.end() || v1_0_0_option->second != Value(false);
	if (v1_0_0_storage && tree.HasMetadata()) {
		Node::TransformToDeprecated(*this, tree);
	}

	IndexStorageInfo info(name);
	info.root = tree.Get();
	info.options = options;

	for (auto &allocator : *allocators) {
		allocator->RemoveEmptyBuffers();
	}

#ifdef DEBUG
	if (v1_0_0_storage) {
		D_ASSERT((*allocators)[ALLOCATOR_COUNT - 4]->IsEmpty());
		D_ASSERT((*allocators)[ALLOCATOR_COUNT - 3]->IsEmpty());
		D_ASSERT((*allocators)[ALLOCATOR_COUNT - 2]->IsEmpty());
		D_ASSERT((*allocators)[ALLOCATOR_COUNT - 1]->IsEmpty());
	}
#endif

	idx_t allocator_count = v1_0_0_storage ? ALLOCATOR_COUNT - 4 : ALLOCATOR_COUNT;
	if (!to_wal) {
		// Store the data on disk as partial blocks and set the block ids.
		WritePartialBlocks(v1_0_0_storage);

	} else {
		// Set the correct allocation sizes and get the map containing all buffers.
		for (idx_t i = 0; i < allocator_count; i++) {
			info.buffers.push_back((*allocators)[i]->InitSerializationToWAL());
		}
	}

	for (idx_t i = 0; i < allocator_count; i++) {
		info.allocator_infos.push_back((*allocators)[i]->GetInfo());
	}
	return info;
}

void ART::WritePartialBlocks(const bool v1_0_0_storage) {
	auto &block_manager = table_io_manager.GetIndexBlockManager();
	PartialBlockManager partial_block_manager(block_manager, PartialBlockType::FULL_CHECKPOINT);

	idx_t allocator_count = v1_0_0_storage ? ALLOCATOR_COUNT - 4 : ALLOCATOR_COUNT;
	for (idx_t i = 0; i < allocator_count; i++) {
		(*allocators)[i]->SerializeBuffers(partial_block_manager);
	}
	partial_block_manager.FlushPartialBlocks();
}

void ART::InitAllocators(const IndexStorageInfo &info) {
	for (idx_t i = 0; i < info.allocator_infos.size(); i++) {
		(*allocators)[i]->Init(info.allocator_infos[i]);
	}
}

void ART::Deserialize(const BlockPointer &pointer) {
	D_ASSERT(pointer.IsValid());

	auto &metadata_manager = table_io_manager.GetMetadataManager();
	MetadataReader reader(metadata_manager, pointer);
	tree = reader.Read<Node>();

	for (idx_t i = 0; i < ALLOCATOR_COUNT - 4; i++) {
		(*allocators)[i]->Deserialize(metadata_manager, reader.Read<BlockPointer>());
	}
}

//===--------------------------------------------------------------------===//
// Vacuum
//===--------------------------------------------------------------------===//

void ART::InitializeVacuum(ARTFlags &flags) {
	flags.vacuum_flags.reserve(flags.vacuum_flags.size() + allocators->size());
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

	// True, if an allocator needs a vacuum, false otherwise.
	ARTFlags flags;
	InitializeVacuum(flags);

	// Skip vacuum, if no allocators require it.
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

	// Traverse the allocated memory of the tree to perform a vacuum.
	tree.Vacuum(*this, flags);

	// Finalize the vacuum operation.
	FinalizeVacuum(flags);
}

//===--------------------------------------------------------------------===//
// Size
//===--------------------------------------------------------------------===//

idx_t ART::GetInMemorySize(IndexLock &index_lock) {
	D_ASSERT(owns_data);

	idx_t in_memory_size = 0;
	for (auto &allocator : *allocators) {
		in_memory_size += allocator->GetInMemorySize();
	}
	return in_memory_size;
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

bool ART::MergeIndexes(IndexLock &state, BoundIndex &other_index) {
	auto &other_art = other_index.Cast<ART>();
	if (!other_art.tree.HasMetadata()) {
		return true;
	}

	if (other_art.owns_data) {
		if (tree.HasMetadata()) {
			// Fully deserialize other_index, and traverse it to increment its buffer IDs.
			ARTFlags flags;
			InitializeMerge(flags);
			other_art.tree.InitializeMerge(other_art, flags);
		}

		// Merge the node storage.
		for (idx_t i = 0; i < allocators->size(); i++) {
			(*allocators)[i]->Merge(*(*other_art.allocators)[i]);
		}
	}

	// Merge the ARTs.
	D_ASSERT(tree.IsGate() == other_art.tree.IsGate());
	if (!tree.Merge(*this, other_art.tree, tree.IsGate())) {
		return false;
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
	if (tree.HasMetadata()) {
		return "ART: " + tree.VerifyAndToString(*this, only_verify);
	}
	return "[empty]";
}

constexpr const char *ART::TYPE_NAME;

} // namespace duckdb
