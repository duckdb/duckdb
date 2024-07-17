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
	//! All scanned row IDs
	vector<row_t> result_ids;
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
		    make_uniq<FixedSizeAllocator>(sizeof(Node256), block_manager)};
		allocators =
		    make_shared_ptr<array<unique_ptr<FixedSizeAllocator>, ALLOCATOR_COUNT>>(std::move(allocator_array));
	}

	if (!info.IsValid()) {
		// We are creating a new ART.
		has_nested_leaves = true;
		return;
	}

	if (info.root_block_ptr.IsValid()) {
		// Backwards compatibility.
		has_nested_leaves = false;
		Deserialize(info.root_block_ptr);
		return;
	}

	// Set the root node and initialize the allocators.
	tree.Set(info.root);
	has_nested_leaves = !info.deprecated_storage;
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
static void TemplatedGenerateKeys(ArenaAllocator &allocator, Vector &input, idx_t count, vector<ARTKey> &keys) {

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
static void ConcatenateKeys(ArenaAllocator &allocator, Vector &input, idx_t count, vector<ARTKey> &keys) {

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
void GenerateKeysInternal(ArenaAllocator &allocator, DataChunk &input, vector<ARTKey> &keys) {
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
void ART::GenerateKeys<>(ArenaAllocator &allocator, DataChunk &input, vector<ARTKey> &keys) {
	GenerateKeysInternal<false>(allocator, input, keys);
}

template <>
void ART::GenerateKeys<true>(ArenaAllocator &allocator, DataChunk &input, vector<ARTKey> &keys) {
	GenerateKeysInternal<true>(allocator, input, keys);
}

void ART::GenerateKeyVectors(ArenaAllocator &allocator, DataChunk &input, Vector &row_ids, vector<ARTKey> &keys,
                             vector<ARTKey> &row_id_keys) {
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

bool ART::Construct(const vector<ARTKey> &keys, const vector<ARTKey> &row_ids, Node &node, ARTKeySection &section) {

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
	vector<ARTKeySection> child_sections;
	section.GetChildSections(child_sections, keys);

	// Create the prefix.
	reference<Node> ref_node(node);
	auto prefix_length = section.depth - prefix_start;
	Prefix::New(*this, ref_node, start_key, UnsafeNumericCast<uint32_t>(prefix_start),
	            UnsafeNumericCast<uint32_t>(prefix_length));

	// Create the node.
	auto node_type = Node::GetARTNodeTypeByCount(child_sections.size());
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

bool ART::ConstructFromSorted(const vector<ARTKey> &keys, const vector<ARTKey> &row_id_keys, const idx_t row_count) {
	ARTKeySection section(0, row_count - 1, 0, 0);
	if (!Construct(keys, row_id_keys, tree, section)) {
		return false;
	}

#ifdef DEBUG
	D_ASSERT(!VerifyAndToStringInternal(true).empty());
	for (idx_t i = 0; i < row_count; i++) {
		D_ASSERT(!keys[i].Empty());
		auto leaf = Lookup(tree, keys[i], 0);
		auto contains_row_id = Leaf::ContainsRowId(*this, *leaf, row_id_keys[i]);
		D_ASSERT(contains_row_id);
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
	vector<ARTKey> keys(row_count);
	vector<ARTKey> row_id_keys(row_count);
	GenerateKeyVectors(allocator, input, row_ids, keys, row_id_keys);

	// Insert the entries into the index.
	idx_t failed_index = DConstants::INVALID_INDEX;
	for (idx_t i = 0; i < row_count; i++) {
		if (keys[i].Empty()) {
			continue;
		}
		if (!Insert(tree, keys[i], 0, row_id_keys[i])) {
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
			Erase(tree, keys[i], 0, row_id_keys[i]);
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

bool ART::InsertToLeaf(Node &leaf, const ARTKey &row_id_key) {
	// We're trying to insert another row ID into an existing leaf.
	if (IsUnique()) {
		return false;
	}

	Leaf::Insert(*this, leaf, row_id_key);
	return true;
}

bool ART::InsertToEmptyNode(Node &node, const ARTKey &key, idx_t depth, const ARTKey &row_id_key) {
	D_ASSERT(depth <= key.len);
	reference<Node> ref_node(node);

	// Create the prefix.
	auto byte = UnsafeNumericCast<uint32_t>(depth);
	auto count = UnsafeNumericCast<uint32_t>(key.len - depth);
	Prefix::New(*this, ref_node, key, byte, count);

	// Create the inlined leaf.
	Leaf::New(ref_node, row_id_key.GetRowID());
	return true;
}

bool ART::Insert(Node &node, const ARTKey &key, idx_t depth, const ARTKey &row_id_key) {
	// If the node is empty, we create a new inlined leaf.
	if (!node.HasMetadata()) {
		return InsertToEmptyNode(node, key, depth, row_id_key);
	}

	auto node_type = node.GetType();

	// Insert the row ID into an existing leaf.
	if (node_type == NType::LEAF || node_type == NType::LEAF_INLINED) {
		return InsertToLeaf(node, row_id_key);
	}

	if (node_type != NType::PREFIX) {
		D_ASSERT(depth < key.len);
		auto child = node.GetChildMutable(*this, key[depth]);

		// Recurse, if a child exists at key[depth].
		if (child) {
			bool success = Insert(*child, key, depth + 1, row_id_key);
			node.ReplaceChild(*this, key[depth], *child);
			return success;
		}

		// Insert a new inlined leaf at key[depth].
		Node leaf_node;
		reference<Node> ref_node(leaf_node);
		if (depth + 1 < key.len) {
			// Create the prefix.
			auto byte = UnsafeNumericCast<uint32_t>(depth + 1);
			auto count = UnsafeNumericCast<uint32_t>(key.len - depth - 1);
			Prefix::New(*this, ref_node, key, byte, count);
		}
		// Create the inlined leaf.
		Leaf::New(ref_node, row_id_key.GetRowID());
		Node::InsertChild(*this, node, key[depth], leaf_node);
		return true;
	}

	// If this is a prefix node, we traverse the prefix.
	reference<Node> next_node(node);
	auto mismatch_position = Prefix::TraverseMutable(*this, next_node, key, depth);

	// If the prefix matches the key, then we recurse into the next node.
	if (next_node.get().GetType() != NType::PREFIX) {
		return Insert(next_node, key, depth, row_id_key);
	}

	// If the prefix does not match the key, we create a new Node4. It has two children, which are
	// the remaining part of the prefix, and the new inlined leaf.
	Node remaining_prefix;
	auto prefix_byte = Prefix::GetByte(*this, next_node, mismatch_position);
	Prefix::Split(*this, next_node, remaining_prefix, mismatch_position);
	Node4::New(*this, next_node);

	// Insert the remaining prefix into the new Node4.
	Node4::InsertChild(*this, next_node, prefix_byte, remaining_prefix);

	// Insert the new inlined leaf.
	Node leaf_node;
	reference<Node> ref_node(leaf_node);
	if (depth + 1 < key.len) {
		// Create the prefix.
		auto byte = UnsafeNumericCast<uint32_t>(depth + 1);
		auto count = UnsafeNumericCast<uint32_t>(key.len - depth - 1);
		Prefix::New(*this, ref_node, key, byte, count);
	}
	// Create the inlined leaf.
	Leaf::New(ref_node, row_id_key.GetRowID());
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
	auto row_count = input.size();

	DataChunk expr_chunk;
	expr_chunk.Initialize(Allocator::DefaultAllocator(), logical_types);
	ExecuteExpressions(input, expr_chunk);

	ArenaAllocator allocator(BufferAllocator::Get(db));
	vector<ARTKey> keys(row_count);
	vector<ARTKey> row_id_keys(row_count);
	GenerateKeyVectors(allocator, expr_chunk, row_ids, keys, row_id_keys);

	for (idx_t i = 0; i < row_count; i++) {
		if (keys[i].Empty()) {
			continue;
		}
		Erase(tree, keys[i], 0, row_id_keys[i]);
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

void ART::Erase(Node &node, const ARTKey &key, idx_t depth, const ARTKey &row_id_key) {
	if (!node.HasMetadata()) {
		return;
	}

	// Prefix handling.
	reference<Node> next_node(node);
	if (next_node.get().GetType() == NType::PREFIX) {
		Prefix::TraverseMutable(*this, next_node, key, depth);
		if (next_node.get().GetType() == NType::PREFIX) {
			return;
		}
	}

	// Delete the row ID from the leaf.
	// This is the root node, which can be a leaf with possible prefix nodes.
	if (next_node.get().GetType() == NType::LEAF || next_node.get().GetType() == NType::LEAF_INLINED) {
		if (Leaf::Remove(*this, next_node, row_id_key)) {
			Node::Free(*this, node);
		}
		return;
	}

	D_ASSERT(depth < key.len);
	auto child = next_node.get().GetChildMutable(*this, key[depth]);
	if (!child) {
		return;
	}

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
		// We found a leaf from which to remove the row ID.
		if (Leaf::Remove(*this, child_node, row_id_key)) {
			Node::DeleteChild(*this, next_node, node, key[depth]);
		}
		return;
	}

	// Recurse.
	Erase(*child, key, depth + 1, row_id_key);
	next_node.get().ReplaceChild(*this, key[depth], *child);
}

//===--------------------------------------------------------------------===//
// Point Query (Equal)
//===--------------------------------------------------------------------===//

bool ART::SearchEqual(ARTKey &key, idx_t max_count, vector<row_t> &row_ids) {

	auto leaf = Lookup(tree, key, 0);
	if (!leaf) {
		return true;
	}
	return Leaf::GetRowIds(*this, *leaf, row_ids, max_count);
}

//===--------------------------------------------------------------------===//
// Lookup
//===--------------------------------------------------------------------===//

optional_ptr<const Node> ART::Lookup(const Node &node, const ARTKey &key, idx_t depth) {
	reference<const Node> node_ref(node);
	while (node_ref.get().HasMetadata()) {

		// Traverse the prefix, if it exists.
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
			// The prefix matches the key, but there is no child at the respective byte.
			return nullptr;
		}

		// Recurse (iteratively).
		node_ref = *child;
		D_ASSERT(node_ref.get().HasMetadata());
		depth++;
	}

	return nullptr;
}

//===--------------------------------------------------------------------===//
// Greater Than and Less Than
//===--------------------------------------------------------------------===//

bool ART::SearchGreater(ARTKey &key, bool equal, idx_t max_count, vector<row_t> &row_ids) {
	if (!tree.HasMetadata()) {
		return true;
	}

	// Find the lowest value that satisfies the predicate.
	Iterator it;
	if (!it.art) {
		it.art = this;
		if (!it.LowerBound(tree, key, equal, 0)) {
			// Early-out, if the maximum value in the ART is lower than the lower bound.
			return true;
		}
	}

	// We continue the scan. We do not check the bounds as any value following this value is
	// greater and satisfies our predicate.
	ARTKey empty_key = ARTKey();
	return it.Scan(empty_key, max_count, row_ids, false);
}

bool ART::SearchLess(ARTKey &upper_bound, bool equal, idx_t max_count, vector<row_t> &row_ids) {
	if (!tree.HasMetadata()) {
		return true;
	}

	Iterator it;
	if (!it.art) {
		it.art = this;
		// Find the minimum value in the ART: we start scanning from this value.
		it.FindMinimum(tree);
		// Early-out, if the minimum value is higher than the upper bound.
		if (it.current_key > upper_bound) {
			return true;
		}
	}

	// Continue the scan until we reach the upper bound.
	return it.Scan(upper_bound, max_count, row_ids, equal);
}

//===--------------------------------------------------------------------===//
// Closed Range Query
//===--------------------------------------------------------------------===//

bool ART::SearchCloseRange(ARTKey &lower_bound, ARTKey &upper_bound, bool left_equal, bool right_equal, idx_t max_count,
                           vector<row_t> &row_ids) {
	// Find the first node that satisfies the left predicate.
	Iterator it;
	if (!it.art) {
		it.art = this;
		if (!it.LowerBound(tree, lower_bound, left_equal, 0)) {
			// Early-out, if the maximum value in the ART is lower than the lower bound.
			return true;
		}
	}

	// Continue the scan until we reach the upper bound.
	return it.Scan(upper_bound, max_count, row_ids, right_equal);
}

bool ART::Scan(IndexScanState &state, const idx_t max_count, vector<row_t> &row_ids) {

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
	// Do not alter the index during constraint checking.
	lock_guard<mutex> l(lock);

	DataChunk expr_chunk;
	expr_chunk.Initialize(Allocator::DefaultAllocator(), logical_types);
	ExecuteExpressions(input, expr_chunk);

	ArenaAllocator arena_allocator(BufferAllocator::Get(db));
	vector<ARTKey> keys(expr_chunk.size());
	GenerateKeys<>(arena_allocator, expr_chunk, keys);

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

		// If we find a node, we need to update the 'matches' and 'row_ids'.
		// NOTE: We only perform constraint checking on unique indexes, i.e., leaves never contain more than a single
		// row ID.
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
// Helper functions for (de)serialization
//===--------------------------------------------------------------------===//

IndexStorageInfo ART::GetStorageInfo(const bool use_deprecated_storage, const bool to_wal) {
	// If the storage format uses deprecated leaf storage,
	// then we need to transform all nested leaves before serialization.
	if (use_deprecated_storage && has_nested_leaves && tree.HasMetadata()) {
		Node::TransformToDeprecated(*this, tree);
	}

	IndexStorageInfo info(name);
	info.root = tree.Get();
	info.deprecated_storage = use_deprecated_storage;

	if (!to_wal) {
		// Store the data on disk as partial blocks and set the block ids.
		WritePartialBlocks();
	} else {
		// Set the correct allocation sizes and get the map containing all buffers.
		for (const auto &allocator : *allocators) {
			info.buffers.push_back(allocator->InitSerializationToWAL());
		}
	}

	for (const auto &allocator : *allocators) {
		info.allocator_infos.push_back(allocator->GetInfo());
	}
	return info;
}

void ART::WritePartialBlocks() {
	// Use the partial block manager to serialize all allocator data.
	auto &block_manager = table_io_manager.GetIndexBlockManager();
	PartialBlockManager partial_block_manager(block_manager, PartialBlockType::FULL_CHECKPOINT);

	for (auto &allocator : *allocators) {
		allocator->SerializeBuffers(partial_block_manager);
	}
	partial_block_manager.FlushPartialBlocks();
}

void ART::InitAllocators(const IndexStorageInfo &info) {
	D_ASSERT(info.allocator_infos.size() == ALLOCATOR_COUNT);
	for (idx_t i = 0; i < info.allocator_infos.size(); i++) {
		(*allocators)[i]->Init(info.allocator_infos[i]);
	}
}

void ART::Deserialize(const BlockPointer &pointer) {
	D_ASSERT(pointer.IsValid());

	auto &metadata_manager = table_io_manager.GetMetadataManager();
	MetadataReader reader(metadata_manager, pointer);
	tree = reader.Read<Node>();

	for (idx_t i = 0; i < ALLOCATOR_COUNT; i++) {
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

string ART::GetConstraintViolationMessage(VerifyExistenceType verify_type, idx_t failed_index, DataChunk &input) {
	auto key_name = GenerateErrorKeyName(input, failed_index);
	auto exception_msg = GenerateConstraintErrorMessage(verify_type, key_name);
	return exception_msg;
}

constexpr const char *ART::TYPE_NAME;

} // namespace duckdb
