#include "duckdb/function/scalar/generic_functions.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/chunk_hasher.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_hash_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"

namespace duckdb {

static void HashFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.ColumnCount() > 0);
	VectorOperations::Hash(args.data[0], result, args.size());
	for (idx_t i = 1; i < args.ColumnCount(); i++) {
		VectorOperations::CombineHash(result, args.data[i], args.size());
	}
}

ScalarFunction HashFun::GetFunction() {
	auto hash_fun = ScalarFunction({}, LogicalType::HASH, HashFunction);
	hash_fun.SetName("hash");
	hash_fun.GetSignature().AddParameter("value", LogicalType::ANY);
	hash_fun.GetSignature().AddArgs("args", LogicalType::ANY);
	hash_fun.SetNullHandling(FunctionNullHandling::SPECIAL_HANDLING);
	// hash the arguments after their combinable collations
	hash_fun.SetCollationHandling(FunctionCollationHandling::PUSH_COMBINABLE_COLLATIONS);
	return hash_fun;
}

//===--------------------------------------------------------------------===//
// BoundHashExpression
//===--------------------------------------------------------------------===//
unique_ptr<Expression> BoundHashExpression::CreateOnColumns(ClientContext &context, const vector<LogicalType> &types,
                                                            const vector<idx_t> &column_ids) {
	(void)context;
	D_ASSERT(!column_ids.empty());
	vector<unique_ptr<Expression>> children;
	for (auto column_id : column_ids) {
		children.push_back(make_uniq<BoundReferenceExpression>(types[column_id], column_id));
	}
	return make_uniq<BoundFunctionExpression>(BoundScalarFunction(HashFun::GetFunction()), std::move(children),
	                                          nullptr);
}

unique_ptr<Expression> BoundHashExpression::CreateOnColumns(ClientContext &context, const vector<LogicalType> &types) {
	vector<idx_t> column_ids;
	for (idx_t i = 0; i < types.size(); i++) {
		column_ids.push_back(i);
	}
	return CreateOnColumns(context, types, column_ids);
}

void BoundHashExpression::Execute(ExpressionExecutor &executor, DataChunk &input, Vector &hashes, idx_t expr_idx) {
	executor.SetChunk(input);
	executor.ExecuteExpression(expr_idx, hashes);
	// the dictionary optimization of the executor can emit a dictionary vector
	if (hashes.GetVectorType() == VectorType::DICTIONARY_VECTOR) {
		hashes.Flatten();
	}
}

//===--------------------------------------------------------------------===//
// ChunkHasher
//===--------------------------------------------------------------------===//
ChunkHasher::ChunkHasher(ClientContext &context, const vector<LogicalType> &types)
    : expression(BoundHashExpression::CreateOnColumns(context, types)), executor(context, *expression) {
}

ChunkHasher::ChunkHasher(ClientContext &context, const vector<LogicalType> &types, const vector<idx_t> &column_ids)
    : expression(BoundHashExpression::CreateOnColumns(context, types, column_ids)), executor(context, *expression) {
}

void ChunkHasher::Hash(DataChunk &input, Vector &hashes) {
	BoundHashExpression::Execute(executor, input, hashes);
}

} // namespace duckdb
