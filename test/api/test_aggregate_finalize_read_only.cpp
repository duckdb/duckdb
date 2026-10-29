#include "catch.hpp"
#include "test_helpers.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/aggregate_function_catalog_entry.hpp"
#include "duckdb/function/aggregate_function.hpp"
#include "duckdb/function/function_binder.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"

using namespace duckdb;

// The read-only finalize declaration is derived from the finalizer an operation implements: the factories pick
// FinalizeReadOnly(const STATE &) when it exists, replacing the finalize callback drops the declaration, and the
// checked helper is the only way to declare it on a function assembled from explicit callbacks. A wrong value is
// otherwise only observable as a race in the recursive CTE runtime, so the derivations are asserted directly here.

namespace {

struct SumState {
	int64_t value;
};

struct SumOperationBase {
	template <class STATE>
	static void Initialize(STATE &state) {
		state.value = 0;
	}
	template <class INPUT_TYPE, class STATE, class OP>
	static void Operation(STATE &state, const INPUT_TYPE &input, AggregateUnaryInput &) {
		state.value += input;
	}
	template <class INPUT_TYPE, class STATE, class OP>
	static void ConstantOperation(STATE &state, const INPUT_TYPE &input, AggregateUnaryInput &, idx_t count) {
		state.value += input * static_cast<INPUT_TYPE>(count);
	}
	template <class A_TYPE, class B_TYPE, class STATE, class OP>
	static void Operation(STATE &state, const A_TYPE &a, const B_TYPE &b, AggregateBinaryInput &) {
		state.value += a + b;
	}
	template <class STATE, class OP>
	static void Operation(STATE &state, AggregateInputData &, idx_t) {
		state.value++;
	}
	template <class STATE, class OP>
	static void ConstantOperation(STATE &state, AggregateInputData &, idx_t count) {
		state.value += static_cast<int64_t>(count);
	}
	template <class STATE, class OP>
	static void Combine(const STATE &source, STATE &target, AggregateInputData &) {
		target.value += source.value;
	}
	static bool IgnoreNull() {
		return true;
	}
};

//! Only reads the state
struct ReadOnlySumOperation : SumOperationBase {
	template <class T, class STATE>
	static void FinalizeReadOnly(const STATE &state, T &target, AggregateFinalizeData &) {
		target = state.value;
	}
};

//! Resets the state while finalizing, which the read-only path would not allow
struct ResettingSumOperation : SumOperationBase {
	template <class T, class STATE>
	static void Finalize(STATE &state, T &target, AggregateFinalizeData &) {
		target = state.value;
		state.value = 0;
	}
};

unique_ptr<Expression> Constant(const LogicalType &type) {
	return make_uniq<BoundConstantExpression>(Value(type));
}

unique_ptr<Expression> Constant(Value value) {
	return make_uniq<BoundConstantExpression>(std::move(value));
}

vector<unique_ptr<Expression>> Children() {
	return {};
}

template <class... REST>
vector<unique_ptr<Expression>> Children(unique_ptr<Expression> first, REST... rest) {
	auto result = Children(std::move(rest)...);
	result.insert(result.begin(), std::move(first));
	return result;
}

bool BoundFinalizeIsReadOnly(ClientContext &context, const char *name, vector<unique_ptr<Expression>> children) {
	auto &catalog = Catalog::GetSystemCatalog(context);
	auto &entry = catalog.GetEntry<AggregateFunctionCatalogEntry>(
	    context, QualifiedName(catalog.GetName(), Identifier::DefaultSchema(), name));
	vector<LogicalType> types;
	for (auto &child : children) {
		types.push_back(child->GetReturnType());
	}
	auto function = entry.functions.GetFunctionByArguments(context, types);
	FunctionBinder function_binder(context);
	auto bound = function_binder.BindAggregateFunction(function, std::move(children));
	return bound->Function().FinalizeIsReadOnly();
}

} // namespace

TEST_CASE("Aggregate factories derive the read-only finalize declaration from the operation",
          "[api][aggregate_function]") {
	auto read_only = AggregateFunction::UnaryAggregate<SumState, int64_t, int64_t, ReadOnlySumOperation>(
	    LogicalType::BIGINT, LogicalType::BIGINT);
	REQUIRE(read_only.FinalizeIsReadOnly());
	REQUIRE(read_only.GetStateFinalizeCallback() ==
	        AggregateFunction::StateFinalizeReadOnly<SumState, int64_t, ReadOnlySumOperation>);

	auto resetting = AggregateFunction::UnaryAggregate<SumState, int64_t, int64_t, ResettingSumOperation>(
	    LogicalType::BIGINT, LogicalType::BIGINT);
	REQUIRE_FALSE(resetting.FinalizeIsReadOnly());
	REQUIRE(resetting.GetStateFinalizeCallback() ==
	        AggregateFunction::StateFinalize<SumState, int64_t, ResettingSumOperation>);

	REQUIRE(AggregateFunction::NullaryAggregate<SumState, int64_t, ReadOnlySumOperation>(LogicalType::BIGINT)
	            .FinalizeIsReadOnly());
	REQUIRE_FALSE(AggregateFunction::NullaryAggregate<SumState, int64_t, ResettingSumOperation>(LogicalType::BIGINT)
	                  .FinalizeIsReadOnly());
	REQUIRE(AggregateFunction::BinaryAggregate<SumState, int64_t, int64_t, int64_t, ReadOnlySumOperation>(
	            LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::BIGINT)
	            .FinalizeIsReadOnly());
	REQUIRE_FALSE(AggregateFunction::BinaryAggregate<SumState, int64_t, int64_t, int64_t, ResettingSumOperation>(
	                  LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::BIGINT)
	                  .FinalizeIsReadOnly());

	// Generic properties cannot restore the declaration after a callback replacement.
	const auto saved_properties = read_only.GetProperties();
	read_only.SetStateFinalizeCallback(AggregateFunction::StateFinalize<SumState, int64_t, ResettingSumOperation>);
	REQUIRE_FALSE(read_only.FinalizeIsReadOnly());
	read_only.SetProperties(saved_properties);
	REQUIRE_FALSE(read_only.FinalizeIsReadOnly());
	AggregateFunction::UseReadOnlyFinalize<SumState, int64_t, ReadOnlySumOperation>(read_only);
	REQUIRE(read_only.FinalizeIsReadOnly());
	read_only.SetCallbacks(resetting.GetCallbacks());
	REQUIRE_FALSE(read_only.FinalizeIsReadOnly());

	// Normal copies retain the implementation's declaration and unrelated properties.
	AggregateFunction::UseReadOnlyFinalize<SumState, int64_t, ReadOnlySumOperation>(read_only);
	read_only.SetIsHolistic(true);
	auto copied = read_only;
	REQUIRE(copied.FinalizeIsReadOnly());
	REQUIRE(copied.IsHolistic());
	REQUIRE(copied == read_only);

	BoundAggregateFunction read_only_bound(copied);
	auto unmarked_bound = read_only_bound;
	REQUIRE(unmarked_bound == read_only_bound);
	unmarked_bound.SetStateFinalizeCallback(unmarked_bound.GetStateFinalizeCallback());
	REQUIRE_FALSE(unmarked_bound.FinalizeIsReadOnly());
	REQUIRE(unmarked_bound.GetCallbacks() == read_only_bound.GetCallbacks());
	REQUIRE(unmarked_bound != read_only_bound);
	unmarked_bound.ReplaceImplementation(read_only_bound);
	REQUIRE(unmarked_bound == read_only_bound);
	REQUIRE(unmarked_bound.FinalizeIsReadOnly());
	REQUIRE(unmarked_bound.IsHolistic());

	// A bound function copies the declaration of the implementation it was bound to
	BoundAggregateFunction bound(resetting);
	REQUIRE_FALSE(bound.FinalizeIsReadOnly());
	AggregateFunction::UseReadOnlyFinalize<SumState, int64_t, ReadOnlySumOperation>(read_only);
	bound.ReplaceImplementation(read_only);
	REQUIRE(bound.FinalizeIsReadOnly());
	bound.ReplaceImplementation(resetting);
	REQUIRE_FALSE(bound.FinalizeIsReadOnly());
}

TEST_CASE("Built-in aggregates declare read-only finalizes where their finalizer only reads",
          "[api][aggregate_function]") {
	DuckDB db(nullptr);
	Connection con(db);
	auto &context = *con.context;
	context.RunFunctionInTransaction([&]() {
		auto integer = LogicalType::INTEGER;
		auto decimal = LogicalType::DECIMAL(18, 3);
		auto integers = LogicalType::LIST(LogicalType::INTEGER);

		// The implicit payload aggregates of every keyed recursive CTE
		REQUIRE(BoundFinalizeIsReadOnly(context, "last", Children(Constant(integer))));
		REQUIRE(BoundFinalizeIsReadOnly(context, "last", Children(Constant(LogicalType::VARCHAR))));
		REQUIRE(BoundFinalizeIsReadOnly(context, "first", Children(Constant(decimal))));
		REQUIRE(BoundFinalizeIsReadOnly(context, "any_value", Children(Constant(integer))));
		// Hand-built functions declared through the checked helper
		REQUIRE(BoundFinalizeIsReadOnly(context, "count", Children(Constant(integer))));
		REQUIRE(BoundFinalizeIsReadOnly(context, "count_star", Children()));
		REQUIRE(BoundFinalizeIsReadOnly(context, "string_agg", Children(Constant(LogicalType::VARCHAR))));
		// Factory-built and bind-replaced implementations
		REQUIRE(BoundFinalizeIsReadOnly(context, "sum", Children(Constant(integer))));
		REQUIRE(BoundFinalizeIsReadOnly(context, "sum", Children(Constant(decimal))));
		REQUIRE(BoundFinalizeIsReadOnly(context, "sum", Children(Constant(LogicalType::DOUBLE))));
		REQUIRE(BoundFinalizeIsReadOnly(context, "avg", Children(Constant(decimal))));
		REQUIRE(BoundFinalizeIsReadOnly(context, "avg", Children(Constant(LogicalType::INTERVAL))));
		REQUIRE(BoundFinalizeIsReadOnly(context, "min", Children(Constant(integer))));
		REQUIRE(BoundFinalizeIsReadOnly(context, "min", Children(Constant(LogicalType::VARCHAR))));
		REQUIRE(BoundFinalizeIsReadOnly(context, "max", Children(Constant(LogicalType::DOUBLE))));
		REQUIRE(BoundFinalizeIsReadOnly(context, "arg_min", Children(Constant(integer), Constant(integer))));
		REQUIRE(
		    BoundFinalizeIsReadOnly(context, "arg_max", Children(Constant(LogicalType::VARCHAR), Constant(integer))));
		REQUIRE(BoundFinalizeIsReadOnly(context, "arg_min", Children(Constant(decimal), Constant(integer))));
		// No typed overload takes a DECIMAL "by" argument, so the binder falls back to the sort-key variant
		REQUIRE_FALSE(
		    BoundFinalizeIsReadOnly(context, "arg_max", Children(Constant(LogicalType::VARCHAR), Constant(decimal))));

		// Not declared read-only: the sort-key variants decode their keys on finalize, and the rest have not
		// been audited for deep-mutation freedom, so they conservatively keep the finalize lock
		REQUIRE_FALSE(BoundFinalizeIsReadOnly(context, "last", Children(Constant(integers))));
		REQUIRE_FALSE(BoundFinalizeIsReadOnly(context, "min", Children(Constant(integers))));
		REQUIRE_FALSE(
		    BoundFinalizeIsReadOnly(context, "min", Children(Constant(integer), Constant(Value::INTEGER(3)))));
		REQUIRE_FALSE(BoundFinalizeIsReadOnly(
		    context, "arg_min", Children(Constant(integer), Constant(integer), Constant(Value::INTEGER(2)))));
		REQUIRE_FALSE(BoundFinalizeIsReadOnly(context, "arg_min", Children(Constant(integers), Constant(integer))));
		REQUIRE_FALSE(BoundFinalizeIsReadOnly(context, "sum", Children(Constant(LogicalType::BIGNUM))));
		REQUIRE_FALSE(BoundFinalizeIsReadOnly(context, "fsum", Children(Constant(LogicalType::DOUBLE))));
		REQUIRE_FALSE(BoundFinalizeIsReadOnly(context, "bit_or", Children(Constant(integer))));
		REQUIRE_FALSE(
		    BoundFinalizeIsReadOnly(context, "approx_top_k", Children(Constant(integer), Constant(Value::INTEGER(2)))));
	});
}
