#include "duckdb/function/scalar/sequence_functions.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/sequence_catalog_entry.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/transaction/transaction.hpp"
#include "duckdb/common/vector_operations/unary_executor.hpp"

namespace duckdb {

struct CurrvalBindData : public FunctionData {
	//! The client context for the function call
	ClientContext &context;
	SequenceCatalogEntry *sequence;

	CurrvalBindData(ClientContext &context, SequenceCatalogEntry *sequence) : context(context), sequence(sequence) {
	}

	unique_ptr<FunctionData> Copy() override {
		return make_unique<CurrvalBindData>(context, sequence);
	}
};

static int64_t CurrSequenceValue(Transaction &transaction, SequenceCatalogEntry *seq) {
	lock_guard<mutex> seqlock(seq->lock);
	int64_t result;
	if (seq->usage_count == 0u) {
		throw SequenceException("currval: sequence is not yet defined in this session");
	}
	result = seq->last_value;
	return result;
}

static void CurrValFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &func_expr = (BoundFunctionExpression &)state.expr;
	auto &info = (CurrvalBindData &)*func_expr.bind_info;
	auto &input = args.data[0];

	auto &transaction = Transaction::GetTransaction(info.context);
	if (info.sequence) {
		// sequence to use is hard coded
		result.SetVectorType(VectorType::FLAT_VECTOR);
		auto result_data = FlatVector::GetData<int64_t>(result);
		for (idx_t i = 0; i < args.size(); i++) {
			// get the next value from the sequence
			result_data[i] = CurrSequenceValue(transaction, info.sequence);
		}
	} else {
		// sequence to use comes from the input
		UnaryExecutor::Execute<string_t, int64_t>(input, result, args.size(), [&](string_t value) {
			auto qname = QualifiedName::Parse(value.GetString());
			// fetch the sequence from the catalog
			auto sequence = Catalog::GetCatalog(info.context)
			                    .GetEntry<SequenceCatalogEntry>(info.context, qname.schema, qname.name);
			// finally get the next value from the sequence
			return CurrSequenceValue(transaction, sequence);
		});
	}
}

static unique_ptr<FunctionData> CurrValBind(ClientContext &context, ScalarFunction &bound_function,
                                            vector<unique_ptr<Expression>> &arguments) {
	SequenceCatalogEntry *sequence = nullptr;
	if (arguments[0]->IsFoldable()) {
		// parameter to currval function is a foldable constant
		// evaluate the constant and perform the catalog lookup already
		Value seqname = ExpressionExecutor::EvaluateScalar(*arguments[0]);
		if (!seqname.is_null) {
			D_ASSERT(seqname.type().id() == LogicalTypeId::VARCHAR);
			auto qname = QualifiedName::Parse(seqname.str_value);
			sequence = Catalog::GetCatalog(context).GetEntry<SequenceCatalogEntry>(context, qname.schema, qname.name);
		}
	}
	return make_unique<CurrvalBindData>(context, sequence);
}

static void CurrValDependency(BoundFunctionExpression &expr, unordered_set<CatalogEntry *> &dependencies) {
	auto &info = (CurrvalBindData &)*expr.bind_info;
	if (info.sequence) {
		dependencies.insert(info.sequence);
	}
}

void CurrvalFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(ScalarFunction("currval", {LogicalType::VARCHAR}, LogicalType::BIGINT, CurrValFunction, true,
	                               CurrValBind, CurrValDependency));
}
} // namespace duckdb
