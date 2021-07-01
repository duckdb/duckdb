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

struct NextvalBindData : public FunctionData {
	//! The client context for the function call
	ClientContext &context;
	//! The sequence to use for the nextval computation; only if
	SequenceCatalogEntry *sequence;

	NextvalBindData(ClientContext &context, SequenceCatalogEntry *sequence) : context(context), sequence(sequence) {
	}

	unique_ptr<FunctionData> Copy() override {
		return make_unique<NextvalBindData>(context, sequence);
	}
};

static int64_t NextSequenceValue(Transaction &transaction, SequenceCatalogEntry *seq) {
	lock_guard<mutex> seqlock(seq->lock);
	int64_t result;
	if (seq->cycle) {
		result = seq->counter;
		seq->counter += seq->increment;
		if (result < seq->min_value) {
			result = seq->max_value;
			seq->counter = seq->max_value + seq->increment;
		} else if (result > seq->max_value) {
			result = seq->min_value;
			seq->counter = seq->min_value + seq->increment;
		}
	} else {
		result = seq->counter;
		seq->counter += seq->increment;
		if (result < seq->min_value) {
			throw SequenceException("nextval: reached minimum value of sequence \"%s\" (%lld)", seq->name,
			                        seq->min_value);
		}
		if (result > seq->max_value) {
			throw SequenceException("nextval: reached maximum value of sequence \"%s\" (%lld)", seq->name,
			                        seq->max_value);
		}
	}
	seq->last_value = result;
	seq->usage_count++;
	transaction.sequence_usage[seq] = SequenceValue(seq->usage_count, seq->counter);
	return result;
}

static void NextValFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &func_expr = (BoundFunctionExpression &)state.expr;
	auto &info = (NextvalBindData &)*func_expr.bind_info;
	auto &input = args.data[0];

	auto &transaction = Transaction::GetTransaction(info.context);
	if (info.sequence) {
		// sequence to use is hard coded
		// increment the sequence
		result.SetVectorType(VectorType::FLAT_VECTOR);
		auto result_data = FlatVector::GetData<int64_t>(result);
		for (idx_t i = 0; i < args.size(); i++) {
			// get the next value from the sequence
			result_data[i] = NextSequenceValue(transaction, info.sequence);
		}
	} else {
		// sequence to use comes from the input
		UnaryExecutor::Execute<string_t, int64_t>(input, result, args.size(), [&](string_t value) {
			auto qname = QualifiedName::Parse(value.GetString());
			// fetch the sequence from the catalog
			auto sequence = Catalog::GetCatalog(info.context)
			                    .GetEntry<SequenceCatalogEntry>(info.context, qname.schema, qname.name);
			// finally get the next value from the sequence
			return NextSequenceValue(transaction, sequence);
		});
	}
}

static unique_ptr<FunctionData> NextValBind(ClientContext &context, ScalarFunction &bound_function,
                                            vector<unique_ptr<Expression>> &arguments) {
	SequenceCatalogEntry *sequence = nullptr;
	if (arguments[0]->IsFoldable()) {
		// parameter to nextval function is a foldable constant
		// evaluate the constant and perform the catalog lookup already
		Value seqname = ExpressionExecutor::EvaluateScalar(*arguments[0]);
		if (!seqname.is_null) {
			D_ASSERT(seqname.type().id() == LogicalTypeId::VARCHAR);
			auto qname = QualifiedName::Parse(seqname.str_value);
			sequence = Catalog::GetCatalog(context).GetEntry<SequenceCatalogEntry>(context, qname.schema, qname.name);
		}
	}
	return make_unique<NextvalBindData>(context, sequence);
}

static void NextValDependency(BoundFunctionExpression &expr, unordered_set<CatalogEntry *> &dependencies) {
	auto &info = (NextvalBindData &)*expr.bind_info;
	if (info.sequence) {
		dependencies.insert(info.sequence);
	}
}

void NextvalFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(ScalarFunction("nextval", {LogicalType::VARCHAR}, LogicalType::BIGINT, NextValFunction, true,
	                               NextValBind, NextValDependency));
}

} // namespace duckdb
