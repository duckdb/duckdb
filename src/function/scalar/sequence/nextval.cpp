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

using namespace std;

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

static int64_t next_sequence_value(Transaction &transaction, SequenceCatalogEntry *seq) {
	lock_guard<mutex> seqlock(seq->lock);
	int64_t result;
	if (seq->cycle) {
		result = seq->counter;
		seq->counter += seq->increment;
		;
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
			throw SequenceException("nextval: reached minimum value of sequence \"%s\" (%lld)", seq->name.c_str(),
			                        seq->min_value);
		}
		if (result > seq->max_value) {
			throw SequenceException("nextval: reached maximum value of sequence \"%s\" (%lld)", seq->name.c_str(),
			                        seq->max_value);
		}
	}
	seq->usage_count++;
	transaction.sequence_usage[seq] = SequenceValue(seq->usage_count, seq->counter);
	return result;
}

static void nextval_function(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &func_expr = (BoundFunctionExpression &)state.expr;
	auto &info = (NextvalBindData &)*func_expr.bind_info;
	assert(args.column_count() == 1 && args.data[0].type == TypeId::VARCHAR);
	auto &input = args.data[0];

	auto &transaction = Transaction::GetTransaction(info.context);
	if (info.sequence) {
		// sequence to use is hard coded
		// increment the sequence
		result.vector_type = VectorType::FLAT_VECTOR;
		auto result_data = FlatVector::GetData<int64_t>(result);
		for (idx_t i = 0; i < args.size(); i++) {
			// get the next value from the sequence
			result_data[i] = next_sequence_value(transaction, info.sequence);
		}
	} else {
		// sequence to use comes from the input
		UnaryExecutor::Execute<string_t, int64_t, true>(input, result, args.size(), [&](string_t value) {
			string schema, seq;
			string seqname = value.GetString();
			Catalog::ParseRangeVar(seqname, schema, seq);
			// fetch the sequence from the catalog
			auto sequence = Catalog::GetCatalog(info.context).GetEntry<SequenceCatalogEntry>(info.context, schema, seq);
			// finally get the next value from the sequence
			return next_sequence_value(transaction, sequence);
		});
	}
}

static unique_ptr<FunctionData> nextval_bind(BoundFunctionExpression &expr, ClientContext &context) {
	SequenceCatalogEntry *sequence = nullptr;
	if (expr.children[0]->IsFoldable()) {
		string schema, seq;
		// parameter to nextval function is a foldable constant
		// evaluate the constant and perform the catalog lookup already
		Value seqname = ExpressionExecutor::EvaluateScalar(*expr.children[0]);
		if (!seqname.is_null) {
			assert(seqname.type == TypeId::VARCHAR);
			Catalog::ParseRangeVar(seqname.str_value, schema, seq);
			sequence = Catalog::GetCatalog(context).GetEntry<SequenceCatalogEntry>(context, schema, seq);
		}
	}
	return make_unique<NextvalBindData>(context, sequence);
}

static void nextval_dependency(BoundFunctionExpression &expr, unordered_set<CatalogEntry *> &dependencies) {
	auto &info = (NextvalBindData &)*expr.bind_info;
	if (info.sequence) {
		dependencies.insert(info.sequence);
	}
}

void NextvalFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(ScalarFunction("nextval", {SQLType::VARCHAR}, SQLType::BIGINT, nextval_function, true, nextval_bind,
	                               nextval_dependency));
}

} // namespace duckdb
