#include "duckdb/execution/index/art/art.hpp"

#include "duckdb/common/types/conflict_manager.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/execution/index/art/art_builder.hpp"
#include "duckdb/execution/index/art/art_key.hpp"
#include "duckdb/execution/index/art/art_merger.hpp"
#include "duckdb/execution/index/art/art_operator.hpp"
#include "duckdb/execution/index/art/art_scanner.hpp"
#include "duckdb/execution/index/art/base_leaf.hpp"
#include "duckdb/execution/index/art/base_node.hpp"
#include "duckdb/execution/index/art/iterator.hpp"
#include "duckdb/execution/index/art/leaf.hpp"
#include "duckdb/execution/index/art/node256.hpp"
#include "duckdb/execution/index/art/node256_leaf.hpp"
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
	//! The predicates to scan.
	//! A single predicate for point lookups, and two predicates for range scans.
	Value values[2];
	//! The expressions over the scan predicates.
	ExpressionType expressions[2];
	bool checked = false;
	//! All scanned row IDs.
	set<row_t> row_ids;
};

//===--------------------------------------------------------------------===//
// ART
//===--------------------------------------------------------------------===//

ART::ART(const string &name, const IndexConstraintType index_constraint_type, const vector<column_t> &column_ids,
         TableIOManager &table_io_manager, const vector<unique_ptr<Expression>> &unbound_expressions,
         AttachedDatabase &db,
         const shared_ptr<array<unsafe_unique_ptr<FixedSizeAllocator>, ALLOCATOR_COUNT>> &allocators_ptr,
         const IndexStorageInfo &info)
    : BoundIndex(name, ART::TYPE_NAME, index_constraint_type, column_ids, table_io_manager, unbound_expressions, db),
      allocators(allocators_ptr), owns_data(false), verify_max_key_len(false) {
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

	if (types.size() > 1) {
		verify_max_key_len = true;
	} else if (types[0] == PhysicalType::VARCHAR) {
		verify_max_key_len = true;
	}

	// Initialize the allocators.
	SetPrefixCount(info);
	if (!allocators) {
		owns_data = true;
		auto prefix_size = NumericCast<idx_t>(prefix_count) + NumericCast<idx_t>(Prefix::METADATA_SIZE);
		auto &block_manager = table_io_manager.GetIndexBlockManager();

		array<unsafe_unique_ptr<FixedSizeAllocator>, ALLOCATOR_COUNT> allocator_array = {
		    make_unsafe_uniq<FixedSizeAllocator>(prefix_size, block_manager),
		    make_unsafe_uniq<FixedSizeAllocator>(sizeof(Leaf), block_manager),
		    make_unsafe_uniq<FixedSizeAllocator>(sizeof(Node4), block_manager),
		    make_unsafe_uniq<FixedSizeAllocator>(sizeof(Node16), block_manager),
		    make_unsafe_uniq<FixedSizeAllocator>(sizeof(Node48), block_manager),
		    make_unsafe_uniq<FixedSizeAllocator>(sizeof(Node256), block_manager),
		    make_unsafe_uniq<FixedSizeAllocator>(sizeof(Node7Leaf), block_manager),
		    make_unsafe_uniq<FixedSizeAllocator>(sizeof(Node15Leaf), block_manager),
		    make_unsafe_uniq<FixedSizeAllocator>(sizeof(Node256Leaf), block_manager),
		};
		allocators =
		    make_shared_ptr<array<unsafe_unique_ptr<FixedSizeAllocator>, ALLOCATOR_COUNT>>(std::move(allocator_array));
	}

	if (!info.IsValid()) {
		// We create a new ART.
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
		auto comparison_type = comparison.GetExpressionType();

		if (comparison.left->GetExpressionType() == ExpressionType::VALUE_CONSTANT) {
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

	} else if (filter_expr.GetExpressionType() == ExpressionType::COMPARE_BETWEEN) {
		auto &between = filter_expr.Cast<BoundBetweenExpression>();
		if (!between.input->Equals(expr)) {
			// The expression does not match the index expression.
			return nullptr;
		}

		if (between.lower->GetExpressionType() != ExpressionType::VALUE_CONSTANT ||
		    between.upper->GetExpressionType() != ExpressionType::VALUE_CONSTANT) {
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
	// FIXME: add another if...else... to match rewritten BETWEEN,
	// i.e., WHERE i BETWEEN 50 AND 1502 is rewritten to CONJUNCTION_AND.

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

	UnifiedVectorFormat data;
	input.ToUnifiedFormat(count, data);
	auto input_data = UnifiedVectorFormat::GetData<T>(data);

	for (idx_t i = 0; i < count; i++) {
		auto idx = data.sel->get_index(i);
		if (IS_NOT_NULL || data.validity.RowIsValid(idx)) {
			ARTKey::CreateARTKey<T>(allocator, keys[i], input_data[idx]);
			continue;
		}

		// We need to reset the key value in the reusable keys vector.
		keys[i] = ARTKey();
	}
}

template <class T, bool IS_NOT_NULL>
static void ConcatenateKeys(ArenaAllocator &allocator, Vector &input, idx_t count, unsafe_vector<ARTKey> &keys) {
	UnifiedVectorFormat data;
	input.ToUnifiedFormat(count, data);
	auto input_data = UnifiedVectorFormat::GetData<T>(data);

	for (idx_t i = 0; i < count; i++) {
		auto idx = data.sel->get_index(i);

		if (IS_NOT_NULL) {
			auto other_key = ARTKey::CreateARTKey<T>(allocator, input_data[idx]);
			keys[i].Concat(allocator, other_key);
			continue;
		}

		// A previous column entry was NULL.
		if (keys[i].Empty()) {
			continue;
		}

		// This column entry is NULL, so we set the whole key to NULL.
		if (!data.validity.RowIsValid(idx)) {
			keys[i] = ARTKey();
			continue;
		}

		// Concatenate the keys.
		auto other_key = ARTKey::CreateARTKey<T>(allocator, input_data[idx]);
		keys[i].Concat(allocator, other_key);
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
	if (!verify_max_key_len) {
		return;
	}
	auto max_len = MAX_KEY_LEN * idx_t(prefix_count);
	for (idx_t i = 0; i < input.size(); i++) {
		keys[i].VerifyKeyLength(max_len);
	}
}

template <>
void ART::GenerateKeys<true>(ArenaAllocator &allocator, DataChunk &input, unsafe_vector<ARTKey> &keys) {
	GenerateKeysInternal<true>(allocator, input, keys);
	if (!verify_max_key_len) {
		return;
	}
	auto max_len = MAX_KEY_LEN * idx_t(prefix_count);
	for (idx_t i = 0; i < input.size(); i++) {
		keys[i].VerifyKeyLength(max_len);
	}
}

void ART::GenerateKeyVectors(ArenaAllocator &allocator, DataChunk &input, Vector &row_ids, unsafe_vector<ARTKey> &keys,
                             unsafe_vector<ARTKey> &row_id_keys) {
	GenerateKeys<>(allocator, input, keys);

	DataChunk row_id_chunk;
	row_id_chunk.Initialize(Allocator::DefaultAllocator(), vector<LogicalType> {LogicalType::ROW_TYPE}, input.size());
	row_id_chunk.data[0].Reference(row_ids);
	row_id_chunk.SetCardinality(input.size());
	GenerateKeys<>(allocator, row_id_chunk, row_id_keys);
}

//===--------------------------------------------------------------------===//
// Build from sorted data.
//===--------------------------------------------------------------------===//

ARTConflictType ART::Build(unsafe_vector<ARTKey> &keys, unsafe_vector<ARTKey> &row_ids, const idx_t row_count) {
	ArenaAllocator arena(BufferAllocator::Get(db));
	ARTBuilder builder(arena, *this, keys, row_ids);
	builder.Init(tree, row_count - 1);

	auto result = builder.Build();
	if (result != ARTConflictType::NO_CONFLICT) {
		return result;
	}

#ifdef DEBUG
	set<row_t> row_ids_debug;
	Iterator it(*this);
	it.FindMinimum(tree);
	ARTKey empty_key = ARTKey();
	it.Scan(empty_key, NumericLimits<row_t>().Maximum(), row_ids_debug, false);
	D_ASSERT(row_count == row_ids_debug.size());
#endif

	return ARTConflictType::NO_CONFLICT;
}

//===--------------------------------------------------------------------===//
// Insert and Constraint Checking
//===--------------------------------------------------------------------===//

ErrorData ART::Insert(IndexLock &l, DataChunk &chunk, Vector &row_ids) {
	IndexAppendInfo info;
	return Insert(l, chunk, row_ids, info);
}

ErrorData ART::Insert(IndexLock &l, DataChunk &chunk, Vector &row_ids, IndexAppendInfo &info) {
	D_ASSERT(row_ids.GetType().InternalType() == ROW_TYPE);
	auto row_count = chunk.size();

	ArenaAllocator arena(BufferAllocator::Get(db));
	unsafe_vector<ARTKey> keys(row_count);
	unsafe_vector<ARTKey> row_id_keys(row_count);
	GenerateKeyVectors(arena, chunk, row_ids, keys, row_id_keys);

	optional_ptr<ART> delete_art;
	if (info.delete_index) {
		delete_art = info.delete_index->Cast<ART>();
	}

	auto conflict_type = ARTConflictType::NO_CONFLICT;
	optional_idx conflict_idx;
	auto was_empty = !tree.HasMetadata();

	// Insert the entries into the index.
	for (idx_t i = 0; i < row_count; i++) {
		if (keys[i].Empty()) {
			continue;
		}
		conflict_type = ARTOperator::Insert(arena, *this, tree, keys[i], 0, row_id_keys[i], GateStatus::GATE_NOT_SET,
		                                    delete_art, info.append_mode);
		if (conflict_type != ARTConflictType::NO_CONFLICT) {
			conflict_idx = i;
			break;
		}
	}

	// Remove any previously inserted entries.
	if (conflict_type != ARTConflictType::NO_CONFLICT) {
		D_ASSERT(conflict_idx.IsValid());
		for (idx_t i = 0; i < conflict_idx.GetIndex(); i++) {
			if (keys[i].Empty()) {
				continue;
			}
			D_ASSERT(tree.GetGateStatus() == GateStatus::GATE_NOT_SET);
			ARTOperator::Delete(*this, tree, keys[i], row_id_keys[i]);
		}
	}

	if (was_empty) {
		// All nodes are in-memory.
		VerifyAllocationsInternal();
	}

	if (conflict_type == ARTConflictType::TRANSACTION) {
		auto msg = AppendRowError(chunk, conflict_idx.GetIndex());
		return ErrorData(TransactionException("write-write conflict on key: \"%s\"", msg));
	}

	if (conflict_type == ARTConflictType::CONSTRAINT) {
		auto msg = AppendRowError(chunk, conflict_idx.GetIndex());
		return ErrorData(ConstraintException("PRIMARY KEY or UNIQUE constraint violation: duplicate key \"%s\"", msg));
	}

#ifdef DEBUG
	for (idx_t i = 0; i < row_count; i++) {
		if (keys[i].Empty()) {
			continue;
		}
		auto leaf = ARTOperator::Lookup(*this, tree, keys[i], 0);
		D_ASSERT(leaf);
		D_ASSERT(ARTOperator::LookupInLeaf(*this, *leaf, row_id_keys[i]));
	}
#endif
	return ErrorData();
}

ErrorData ART::Append(IndexLock &l, DataChunk &chunk, Vector &row_ids) {
	// Execute all column expressions before inserting the data chunk.
	DataChunk expr_chunk;
	expr_chunk.Initialize(Allocator::DefaultAllocator(), logical_types);
	ExecuteExpressions(chunk, expr_chunk);

	// Now insert the data chunk.
	IndexAppendInfo info;
	return Insert(l, expr_chunk, row_ids, info);
}

ErrorData ART::Append(IndexLock &l, DataChunk &chunk, Vector &row_ids, IndexAppendInfo &info) {
	// Execute all column expressions before inserting the data chunk.
	DataChunk expr_chunk;
	expr_chunk.Initialize(Allocator::DefaultAllocator(), logical_types);
	ExecuteExpressions(chunk, expr_chunk);

	// Now insert the data chunk.
	return Insert(l, expr_chunk, row_ids, info);
}

void ART::VerifyAppend(DataChunk &chunk, IndexAppendInfo &info, optional_ptr<ConflictManager> manager) {
	if (manager) {
		D_ASSERT(manager->GetVerifyExistenceType() == VerifyExistenceType::APPEND);
		return VerifyConstraint(chunk, info, *manager);
	}
	ConflictManager local_manager(VerifyExistenceType::APPEND, chunk.size());
	VerifyConstraint(chunk, info, local_manager);
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
	// FIXME: We could pass a row_count in here, as we sometimes don't have to delete all row IDs in the chunk,
	// FIXME: but rather all row IDs up to the conflicting row.
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
		D_ASSERT(tree.GetGateStatus() == GateStatus::GATE_NOT_SET);
		ARTOperator::Delete(*this, tree, keys[i], row_id_keys[i]);
	}

	if (!tree.HasMetadata()) {
		// No more allocations.
		VerifyAllocationsInternal();
	}

#ifdef DEBUG
	for (idx_t i = 0; i < row_count; i++) {
		if (keys[i].Empty()) {
			continue;
		}
		auto leaf = ARTOperator::Lookup(*this, tree, keys[i], 0);
		if (leaf) {
			auto contains_row_id = ARTOperator::LookupInLeaf(*this, *leaf, row_id_keys[i]);
			D_ASSERT(!contains_row_id);
		}
	}
#endif
}

//===--------------------------------------------------------------------===//
// Point and range lookups
//===--------------------------------------------------------------------===//

bool ART::SearchEqual(ARTKey &key, idx_t max_count, set<row_t> &row_ids) {
	auto leaf = ARTOperator::Lookup(*this, tree, key, 0);
	if (!leaf) {
		return true;
	}

	Iterator it(*this);
	it.FindMinimum(*leaf);
	ARTKey empty_key = ARTKey();
	return it.Scan(empty_key, max_count, row_ids, false);
}

bool ART::SearchGreater(ARTKey &key, bool equal, idx_t max_count, set<row_t> &row_ids) {
	if (!tree.HasMetadata()) {
		return true;
	}

	// Find the lowest value that satisfies the predicate.
	Iterator it(*this);

	// Early-out, if the maximum value in the ART is lower than the lower bound.
	if (!it.LowerBound(tree, key, equal)) {
		return true;
	}

	// We continue the scan. We do not check the bounds as any value following this value is
	// greater and satisfies our predicate.
	return it.Scan(ARTKey(), max_count, row_ids, false);
}

bool ART::SearchLess(ARTKey &upper_bound, bool equal, idx_t max_count, set<row_t> &row_ids) {
	if (!tree.HasMetadata()) {
		return true;
	}

	// Find the minimum value in the ART: we start scanning from this value.
	Iterator it(*this);
	it.FindMinimum(tree);

	// Early-out, if the minimum value is higher than the upper bound.
	if (it.current_key.GreaterThan(upper_bound, equal, it.GetNestedDepth())) {
		return true;
	}

	// Continue the scan until we reach the upper bound.
	return it.Scan(upper_bound, max_count, row_ids, equal);
}

bool ART::SearchCloseRange(ARTKey &lower_bound, ARTKey &upper_bound, bool left_equal, bool right_equal, idx_t max_count,
                           set<row_t> &row_ids) {
	// Find the first node that satisfies the left predicate.
	Iterator it(*this);

	// Early-out, if the maximum value in the ART is lower than the lower bound.
	if (!it.LowerBound(tree, lower_bound, left_equal)) {
		return true;
	}

	// Continue the scan until we reach the upper bound.
	return it.Scan(upper_bound, max_count, row_ids, right_equal);
}

bool ART::Scan(IndexScanState &state, const idx_t max_count, set<row_t> &row_ids) {
	auto &scan_state = state.Cast<ARTIndexScanState>();
	D_ASSERT(scan_state.values[0].type().InternalType() == types[0]);
	ArenaAllocator arena_allocator(Allocator::Get(db));
	auto key = ARTKey::CreateKey(arena_allocator, types[0], scan_state.values[0]);
	auto max_len = MAX_KEY_LEN * prefix_count;
	key.VerifyKeyLength(max_len);

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
	upper_bound.VerifyKeyLength(max_len);

	bool left_equal = scan_state.expressions[0] == ExpressionType ::COMPARE_GREATERTHANOREQUALTO;
	bool right_equal = scan_state.expressions[1] == ExpressionType ::COMPARE_LESSTHANOREQUALTO;
	return SearchCloseRange(key, upper_bound, left_equal, right_equal, max_count, row_ids);
}

//===--------------------------------------------------------------------===//
// More Constraint Checking
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
		return StringUtil::Format("Duplicate key \"%s\" violates %s constraint.", key_name, type);
	}
	case VerifyExistenceType::APPEND_FK: {
		// APPEND_FK to FK table, node/key does not exist in PK/UNIQUE table.
		return StringUtil::Format(
		    "Violates foreign key constraint because key \"%s\" does not exist in the referenced table", key_name);
	}
	case VerifyExistenceType::DELETE_FK: {
		// DELETE_FK that still exists in a FK table, i.e., not a valid delete.
		return StringUtil::Format(
		    "Violates foreign key constraint because key \"%s\" is still referenced by a foreign "
		    "key in a different table. If this is an unexpected constraint violation, please refer to our "
		    "foreign key limitations in the documentation",
		    key_name);
	}
	default:
		throw NotImplementedException("Type not implemented for VerifyExistenceType");
	}
}

void ART::VerifyLeaf(const Node &leaf, const ARTKey &key, optional_ptr<ART> delete_art, ConflictManager &manager,
                     optional_idx &conflict_idx, idx_t i) {
	// Fast path, the leaf is inlined, and the delete ART does not exist.
	if (leaf.GetType() == NType::LEAF_INLINED && !delete_art) {
		if (manager.AddHit(i, leaf.GetRowId())) {
			conflict_idx = i;
		}
		return;
	}

	// Get the delete_leaf.
	// All leaves in the delete ART are inlined.
	unsafe_optional_ptr<const Node> deleted_leaf;
	if (delete_art) {
		deleted_leaf = ARTOperator::Lookup(*delete_art, delete_art->tree, key, 0);
	}

	// The leaf is inlined, and there is no deleted leaf with the same key.
	if (leaf.GetType() == NType::LEAF_INLINED && !deleted_leaf) {
		if (manager.AddHit(i, leaf.GetRowId())) {
			conflict_idx = i;
		}
		return;
	}

	// The leaf is inlined, and the same key exists in the delete ART.
	if (leaf.GetType() == NType::LEAF_INLINED && deleted_leaf) {
		D_ASSERT(deleted_leaf->GetType() == NType::LEAF_INLINED);
		auto deleted_row_id = deleted_leaf->GetRowId();
		auto this_row_id = leaf.GetRowId();

		if (deleted_row_id == this_row_id) {
			return;
		}

		if (manager.AddHit(i, this_row_id)) {
			conflict_idx = i;
		}
		return;
	}

	// Fast path for FOREIGN KEY constraints.
	// Up to here, the above code paths work implicitly for FKs, as the leaf is inlined.
	// FIXME: proper foreign key + delete ART support.
	if (index_constraint_type == IndexConstraintType::FOREIGN) {
		D_ASSERT(!deleted_leaf);
		// We don't handle FK conflicts in UPSERT, so the row ID should not matter.
		if (manager.AddHit(i, MAX_ROW_ID)) {
			conflict_idx = i;
		}
		return;
	}

	// Scan the two row IDs in the leaf.
	Iterator it(*this);
	it.FindMinimum(leaf);
	ARTKey empty_key = ARTKey();
	set<row_t> row_ids;
	auto success = it.Scan(empty_key, 2, row_ids, false);
	if (!success || row_ids.size() != 2) {
		throw InternalException("VerifyLeaf expects exactly two row IDs to be scanned");
	}

	if (deleted_leaf) {
		auto deleted_row_id = deleted_leaf->GetRowId();
		for (const auto row_id : row_ids) {
			if (deleted_row_id == row_id) {
				return;
			}
		}
	}

	auto row_id_it = row_ids.begin();
	if (manager.AddHit(i, *row_id_it)) {
		conflict_idx = i;
	}
	row_id_it++;
	manager.AddSecondHit(i, *row_id_it);
}

void ART::VerifyConstraint(DataChunk &chunk, IndexAppendInfo &info, ConflictManager &manager) {
	// Lock the index during constraint checking.
	lock_guard<mutex> l(lock);

	DataChunk expr_chunk;
	expr_chunk.Initialize(Allocator::DefaultAllocator(), logical_types);
	ExecuteExpressions(chunk, expr_chunk);

	ArenaAllocator arena_allocator(BufferAllocator::Get(db));
	unsafe_vector<ARTKey> keys(expr_chunk.size());
	GenerateKeys<>(arena_allocator, expr_chunk, keys);

	optional_ptr<ART> delete_art;
	if (info.delete_index) {
		delete_art = info.delete_index->Cast<ART>();
	}

	optional_idx conflict_idx;
	for (idx_t i = 0; !conflict_idx.IsValid() && i < chunk.size(); i++) {
		if (keys[i].Empty()) {
			if (manager.AddNull(i)) {
				conflict_idx = i;
			}
			continue;
		}

		auto leaf = ARTOperator::Lookup(*this, tree, keys[i], 0);
		if (!leaf) {
			continue;
		}
		VerifyLeaf(*leaf, keys[i], delete_art, manager, conflict_idx, i);
	}

	manager.FinishLookup();
	if (!conflict_idx.IsValid()) {
		return;
	}

	auto key_name = GenerateErrorKeyName(chunk, conflict_idx.GetIndex());
	auto exception_msg = GenerateConstraintErrorMessage(manager.GetVerifyExistenceType(), key_name);
	throw ConstraintException(exception_msg);
}

string ART::GetConstraintViolationMessage(VerifyExistenceType verify_type, idx_t failed_index, DataChunk &input) {
	auto key_name = GenerateErrorKeyName(input, failed_index);
	auto exception_msg = GenerateConstraintErrorMessage(verify_type, key_name);
	return exception_msg;
}

//===--------------------------------------------------------------------===//
// Storage and Memory
//===--------------------------------------------------------------------===//

void ART::TransformToDeprecated() {
	auto idx = Node::GetAllocatorIdx(NType::PREFIX);
	auto &block_manager = (*allocators)[idx]->block_manager;
	unsafe_unique_ptr<FixedSizeAllocator> deprecated_allocator;

	if (prefix_count != Prefix::DEPRECATED_COUNT) {
		auto prefix_size = NumericCast<idx_t>(Prefix::DEPRECATED_COUNT) + NumericCast<idx_t>(Prefix::METADATA_SIZE);
		deprecated_allocator = make_unsafe_uniq<FixedSizeAllocator>(prefix_size, block_manager);
	}

	// Transform all leaves, and possibly the prefixes.
	if (tree.HasMetadata()) {
		Node::TransformToDeprecated(*this, tree, deprecated_allocator);
	}

	// Replace the prefix allocator with the deprecated allocator.
	if (deprecated_allocator) {
		prefix_count = Prefix::DEPRECATED_COUNT;

		D_ASSERT((*allocators)[idx]->Empty());
		(*allocators)[idx]->Reset();
		(*allocators)[idx] = std::move(deprecated_allocator);
	}
}

IndexStorageInfo ART::PrepareSerialize(const case_insensitive_map_t<Value> &options, const bool v1_0_0_storage) {
	if (v1_0_0_storage) {
		TransformToDeprecated();
	}

	IndexStorageInfo info(name);
	info.root = tree.Get();
	info.options = options;

	for (auto &allocator : *allocators) {
		allocator->RemoveEmptyBuffers();
	}

#ifdef DEBUG
	if (v1_0_0_storage) {
		D_ASSERT((*allocators)[Node::GetAllocatorIdx(NType::NODE_7_LEAF)]->Empty());
		D_ASSERT((*allocators)[Node::GetAllocatorIdx(NType::NODE_15_LEAF)]->Empty());
		D_ASSERT((*allocators)[Node::GetAllocatorIdx(NType::NODE_256_LEAF)]->Empty());
		D_ASSERT((*allocators)[Node::GetAllocatorIdx(NType::PREFIX)]->GetSegmentSize() ==
		         Prefix::DEPRECATED_COUNT + Prefix::METADATA_SIZE);
	}
#endif

	return info;
}

IndexStorageInfo ART::SerializeToDisk(QueryContext context, const case_insensitive_map_t<Value> &options) {
	// If the storage format uses deprecated leaf storage,
	// then we need to transform all nested leaves before serialization.
	auto v1_0_0_option = options.find("v1_0_0_storage");
	bool v1_0_0_storage = v1_0_0_option == options.end() || v1_0_0_option->second != Value(false);
	auto info = PrepareSerialize(options, v1_0_0_storage);
	auto allocator_count = v1_0_0_storage ? DEPRECATED_ALLOCATOR_COUNT : ALLOCATOR_COUNT;

	// Store the data on disk as partial blocks and set the block ids.
	WritePartialBlocks(context, v1_0_0_storage);

	for (idx_t i = 0; i < allocator_count; i++) {
		info.allocator_infos.push_back((*allocators)[i]->GetInfo());
	}

	return info;
}

IndexStorageInfo ART::SerializeToWAL(const case_insensitive_map_t<Value> &options) {
	// If the storage format uses deprecated leaf storage,
	// then we need to transform all nested leaves before serialization.
	auto v1_0_0_option = options.find("v1_0_0_storage");
	bool v1_0_0_storage = v1_0_0_option == options.end() || v1_0_0_option->second != Value(false);
	auto info = PrepareSerialize(options, v1_0_0_storage);
	auto allocator_count = v1_0_0_storage ? DEPRECATED_ALLOCATOR_COUNT : ALLOCATOR_COUNT;

	// Set the correct allocation sizes and get the map containing all buffers.
	for (idx_t i = 0; i < allocator_count; i++) {
		info.buffers.push_back((*allocators)[i]->InitSerializationToWAL());
	}

	for (idx_t i = 0; i < allocator_count; i++) {
		info.allocator_infos.push_back((*allocators)[i]->GetInfo());
	}

	return info;
}

void ART::WritePartialBlocks(QueryContext context, const bool v1_0_0_storage) {
	auto &block_manager = table_io_manager.GetIndexBlockManager();
	PartialBlockManager partial_block_manager(context, block_manager, PartialBlockType::FULL_CHECKPOINT);

	idx_t allocator_count = v1_0_0_storage ? DEPRECATED_ALLOCATOR_COUNT : ALLOCATOR_COUNT;
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

	for (idx_t i = 0; i < DEPRECATED_ALLOCATOR_COUNT; i++) {
		(*allocators)[i]->Deserialize(metadata_manager, reader.Read<BlockPointer>());
	}
}

void ART::SetPrefixCount(const IndexStorageInfo &info) {
	auto numeric_max = NumericLimits<uint8_t>().Maximum();
	auto max_aligned = AlignValueFloor<uint8_t>(numeric_max - Prefix::METADATA_SIZE);

	if (info.IsValid() && info.root_block_ptr.IsValid()) {
		prefix_count = Prefix::DEPRECATED_COUNT;
		return;
	}

	if (info.IsValid()) {
		auto serialized_count = info.allocator_infos[0].segment_size - Prefix::METADATA_SIZE;
		prefix_count = NumericCast<uint8_t>(serialized_count);
		return;
	}

	idx_t compound_size = 0;
	for (const auto &type : types) {
		compound_size += GetTypeIdSize(type);
	}

	auto aligned = AlignValue(compound_size) - 1;
	if (aligned > NumericCast<idx_t>(max_aligned)) {
		prefix_count = max_aligned;
		return;
	}

	prefix_count = NumericCast<uint8_t>(aligned);
}

idx_t ART::GetInMemorySize(IndexLock &index_lock) {
	D_ASSERT(owns_data);

	idx_t in_memory_size = 0;
	for (auto &allocator : *allocators) {
		in_memory_size += allocator->GetInMemorySize();
	}
	return in_memory_size;
}

//===-------------------------------------------------------------------===//
// Vacuum
//===--------------------------------------------------------------------===//

void ART::InitializeVacuum(unordered_set<uint8_t> &indexes) {
	for (idx_t i = 0; i < allocators->size(); i++) {
		if ((*allocators)[i]->InitializeVacuum()) {
			indexes.insert(NumericCast<uint8_t>(i));
		}
	}
}

void ART::FinalizeVacuum(const unordered_set<uint8_t> &indexes) {
	for (const auto &idx : indexes) {
		(*allocators)[idx]->FinalizeVacuum();
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
	unordered_set<uint8_t> indexes;
	InitializeVacuum(indexes);

	// Skip vacuum, if no allocators require it.
	if (indexes.empty()) {
		return;
	}

	// Traverse the allocated memory of the tree to perform a vacuum.
	auto &art = *this;
	auto handler = [&art, &indexes](Node &node) {
		ARTHandlingResult result;
		const auto type = node.GetType();
		switch (type) {
		case NType::LEAF_INLINED:
			return ARTHandlingResult::SKIP;
		case NType::LEAF: {
			if (indexes.find(Node::GetAllocatorIdx(type)) == indexes.end()) {
				return ARTHandlingResult::SKIP;
			}
			Leaf::DeprecatedVacuum(art, node);
			return ARTHandlingResult::SKIP;
		}
		case NType::NODE_7_LEAF:
		case NType::NODE_15_LEAF:
		case NType::NODE_256_LEAF: {
			result = ARTHandlingResult::SKIP;
			break;
		}
		case NType::PREFIX:
		case NType::NODE_4:
		case NType::NODE_16:
		case NType::NODE_48:
		case NType::NODE_256: {
			result = ARTHandlingResult::CONTINUE;
			break;
		}
		default:
			throw InternalException("invalid node type for Vacuum: %d", type);
		}

		const auto idx = Node::GetAllocatorIdx(type);
		auto &allocator = Node::GetAllocator(art, type);
		const auto needs_vacuum = indexes.find(idx) != indexes.end() && allocator.NeedsVacuum(node);
		if (needs_vacuum) {
			const auto status = node.GetGateStatus();
			node = allocator.VacuumPointer(node);
			node.SetMetadata(static_cast<uint8_t>(type));
			node.SetGateStatus(status);
		}
		return result;
	};

	ARTScanner<ARTScanHandling::EMPLACE, Node> scanner(*this, handler, tree);
	scanner.Scan(handler);

	// Finalize the vacuum operation.
	FinalizeVacuum(indexes);
}

//===--------------------------------------------------------------------===//
// Merging
//===--------------------------------------------------------------------===//

void ART::InitializeMergeUpperBounds(unsafe_vector<idx_t> &upper_bounds) {
	D_ASSERT(owns_data);
	for (auto &allocator : *allocators) {
		upper_bounds.emplace_back(allocator->GetUpperBoundBufferId());
	}
}

void ART::InitializeMerge(Node &node, unsafe_vector<idx_t> &upper_bounds) {
	D_ASSERT(node.HasMetadata());

	auto handler = [&upper_bounds](Node &node) {
		const auto type = node.GetType();
		if (node.GetType() == NType::LEAF_INLINED) {
			return ARTHandlingResult::NONE;
		}
		if (type == NType::LEAF) {
			throw InternalException("deprecated ART storage in InitializeMerge");
		}
		const auto idx = Node::GetAllocatorIdx(type);
		node.IncreaseBufferId(upper_bounds[idx]);
		return ARTHandlingResult::NONE;
	};

	ARTScanner<ARTScanHandling::POP, Node> scanner(*this, handler, node);
	scanner.Scan(handler);
}

bool ART::MergeIndexes(IndexLock &state, BoundIndex &other_index) {
	auto &other_art = other_index.Cast<ART>();
	if (!other_art.tree.HasMetadata()) {
		return true;
	}

	if (other_art.owns_data) {
		if (tree.HasMetadata()) {
			// Fully deserialize other_index, and traverse it to increment its buffer IDs.
			unsafe_vector<idx_t> upper_bounds;
			InitializeMergeUpperBounds(upper_bounds);
			other_art.InitializeMerge(other_art.tree, upper_bounds);
		}

		// Merge the node storage.
		for (idx_t i = 0; i < allocators->size(); i++) {
			(*allocators)[i]->Merge(*(*other_art.allocators)[i]);
		}
	}

	// Merge the ARTs.
	D_ASSERT(tree.GetGateStatus() == other_art.tree.GetGateStatus());
	if (tree.HasMetadata()) {
		ArenaAllocator arena(Allocator::Get(db));
		ARTMerger merger(arena, *this);
		merger.Init(tree, other_art.tree);
		return merger.Merge() == ARTConflictType::NO_CONFLICT;
	}

	tree = other_art.tree;
	other_art.tree.Clear();
	return true;
}

//===--------------------------------------------------------------------===//
// Verification
//===--------------------------------------------------------------------===//

string ART::ToString(IndexLock &l, bool display_ascii) {
	return ToStringInternal(display_ascii);
}

string ART::ToStringInternal(bool display_ascii) {
	if (tree.HasMetadata()) {
		return "\nART: \n" + tree.ToString(*this, 0, false, display_ascii);
	}
	return "[empty]";
}

void ART::Verify(IndexLock &l) {
	VerifyInternal();
}

void ART::VerifyInternal() {
	if (tree.HasMetadata()) {
		tree.Verify(*this);
	}
}

void ART::VerifyAllocations(IndexLock &l) {
	return VerifyAllocationsInternal();
}

void ART::VerifyAllocationsInternal() {
#ifdef DEBUG
	unordered_map<uint8_t, idx_t> node_counts;
	for (idx_t i = 0; i < allocators->size(); i++) {
		node_counts[NumericCast<uint8_t>(i)] = 0;
	}

	if (tree.HasMetadata()) {
		tree.VerifyAllocations(*this, node_counts);
	}

	for (idx_t i = 0; i < allocators->size(); i++) {
		auto segment_count = (*allocators)[i]->GetSegmentCount();
		D_ASSERT(segment_count == node_counts[NumericCast<uint8_t>(i)]);
	}
#endif
}

void ART::VerifyBuffers(IndexLock &l) {
	for (auto &allocator : *allocators) {
		allocator->VerifyBuffers();
	}
}

constexpr const char *ART::TYPE_NAME;

} // namespace duckdb
