#include "duckdb/function/scalar/sequence_functions.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/dependency_list.hpp"
#include "duckdb/catalog/catalog_entry/sequence_catalog_entry.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/transaction/duck_transaction.hpp"
#include "duckdb/common/vector_operations/unary_executor.hpp"
#include "duckdb/common/operator/add.hpp"
#include "duckdb/planner/binder.hpp"

namespace duckdb {

struct NextvalBindData : public FunctionData {
	explicit NextvalBindData(optional_ptr<SequenceCatalogEntry> sequence) : sequence(sequence) {
	}

	//! The sequence to use for the nextval computation; only if the sequence is a constant
	optional_ptr<SequenceCatalogEntry> sequence;

	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<NextvalBindData>(sequence);
	}

	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<NextvalBindData>();
		return sequence == other.sequence;
	}
};

struct CurrentSequenceValueOperator {
	static int64_t Operation(DuckTransaction &transaction, SequenceCatalogEntry &seq) {
		lock_guard<mutex> seqlock(seq.lock);
		int64_t result;
		if (seq.usage_count == 0u) {
			throw SequenceException("currval: sequence is not yet defined in this session");
		}
		result = seq.last_value;
		return result;
	}
};

struct NextSequenceValueOperator {
	static int64_t Operation(DuckTransaction &transaction, SequenceCatalogEntry &seq) {
		lock_guard<mutex> seqlock(seq.lock);
		int64_t result;
		result = seq.counter;
		bool overflow = !TryAddOperator::Operation(seq.counter, seq.increment, seq.counter);
		if (seq.cycle) {
			if (overflow) {
				seq.counter = seq.increment < 0 ? seq.max_value : seq.min_value;
			} else if (seq.counter < seq.min_value) {
				seq.counter = seq.max_value;
			} else if (seq.counter > seq.max_value) {
				seq.counter = seq.min_value;
			}
		} else {
			if (result < seq.min_value || (overflow && seq.increment < 0)) {
				throw SequenceException("nextval: reached minimum value of sequence \"%s\" (%lld)", seq.name,
				                        seq.min_value);
			}
			if (result > seq.max_value || overflow) {
				throw SequenceException("nextval: reached maximum value of sequence \"%s\" (%lld)", seq.name,
				                        seq.max_value);
			}
		}
		seq.last_value = result;
		seq.usage_count++;
		if (!seq.temporary) {
			transaction.sequence_usage[&seq] = SequenceValue(seq.usage_count, seq.counter);
		}
		return result;
	}
};

SequenceCatalogEntry &BindSequence(ClientContext &context, const string &name) {
	auto qname = QualifiedName::Parse(name);
	// fetch the sequence from the catalog
	Binder::BindSchemaOrCatalog(context, qname.catalog, qname.schema);
	return Catalog::GetEntry<SequenceCatalogEntry>(context, qname.catalog, qname.schema, qname.name);
}

template <class OP>
static void NextValFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &func_expr = state.expr.Cast<BoundFunctionExpression>();
	auto &info = func_expr.bind_info->Cast<NextvalBindData>();
	auto &input = args.data[0];

	auto &context = state.GetContext();
	if (info.sequence) {
		auto &sequence = *info.sequence;
		auto &transaction = DuckTransaction::Get(context, sequence.catalog);
		// sequence to use is hard coded
		// increment the sequence
		result.SetVectorType(VectorType::FLAT_VECTOR);
		auto result_data = FlatVector::GetData<int64_t>(result);
		for (idx_t i = 0; i < args.size(); i++) {
			// get the next value from the sequence
			result_data[i] = OP::Operation(transaction, sequence);
		}
	} else {
		// sequence to use comes from the input
		UnaryExecutor::Execute<string_t, int64_t>(input, result, args.size(), [&](string_t value) {
			// fetch the sequence from the catalog
			auto &sequence = BindSequence(context, value.GetString());
			// finally get the next value from the sequence
			auto &transaction = DuckTransaction::Get(context, sequence.catalog);
			return OP::Operation(transaction, sequence);
		});
	}
}

static unique_ptr<FunctionData> NextValBind(ClientContext &context, ScalarFunction &bound_function,
                                            vector<unique_ptr<Expression>> &arguments) {
	optional_ptr<SequenceCatalogEntry> sequence;
	if (arguments[0]->IsFoldable()) {
		// parameter to nextval function is a foldable constant
		// evaluate the constant and perform the catalog lookup already
		auto seqname = ExpressionExecutor::EvaluateScalar(context, *arguments[0]);
		if (!seqname.IsNull()) {
			sequence = &BindSequence(context, seqname.ToString());
		}
	}
	return make_uniq<NextvalBindData>(sequence);
}

static void NextValDependency(BoundFunctionExpression &expr, DependencyList &dependencies) {
	auto &info = expr.bind_info->Cast<NextvalBindData>();
	if (info.sequence) {
		dependencies.AddDependency(*info.sequence);
	}
}

void NextvalFun::RegisterFunction(BuiltinFunctions &set) {
	ScalarFunction next_val("nextval", {LogicalType::VARCHAR}, LogicalType::BIGINT,
	                        NextValFunction<NextSequenceValueOperator>, NextValBind, NextValDependency);
	next_val.side_effects = FunctionSideEffects::HAS_SIDE_EFFECTS;
	set.AddFunction(next_val);
}

void CurrvalFun::RegisterFunction(BuiltinFunctions &set) {
	ScalarFunction curr_val("currval", {LogicalType::VARCHAR}, LogicalType::BIGINT,
	                        NextValFunction<CurrentSequenceValueOperator>, NextValBind, NextValDependency);
	curr_val.side_effects = FunctionSideEffects::HAS_SIDE_EFFECTS;
	set.AddFunction(curr_val);
}

} // namespace duckdb
