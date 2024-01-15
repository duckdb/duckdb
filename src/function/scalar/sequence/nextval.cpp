#include "duckdb/function/scalar/sequence_functions.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/dependency_list.hpp"
#include "duckdb/catalog/catalog_entry/sequence_catalog_entry.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/transaction/duck_transaction.hpp"
#include "duckdb/common/serializer/deserializer.hpp"
#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/common/vector_operations/unary_executor.hpp"
#include "duckdb/planner/binder.hpp"

namespace duckdb {

struct CurrentSequenceValueOperator {
	static int64_t Operation(DuckTransaction &transaction, SequenceCatalogEntry &seq) {
		return seq.CurrentValue();
	}
};

struct NextSequenceValueOperator {
	static int64_t Operation(DuckTransaction &transaction, SequenceCatalogEntry &seq) {
		return seq.NextValue(transaction);
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

void Serialize(Serializer &serializer, const optional_ptr<FunctionData> bind_data, const ScalarFunction &) {
	auto &next_val_bind_data = bind_data->Cast<NextvalBindData>();
	serializer.WritePropertyWithDefault(100, "sequence_create_info", next_val_bind_data.create_info);
}

unique_ptr<FunctionData> Deserialize(Deserializer &deserializer, ScalarFunction &) {
	auto create_info = deserializer.ReadPropertyWithDefault<unique_ptr<CreateInfo>>(100, "sequence_create_info",
	                                                                                unique_ptr<CreateInfo>());
	optional_ptr<SequenceCatalogEntry> catalog_entry_ptr;
	if (create_info) {
		auto &seq_info = create_info->Cast<CreateSequenceInfo>();
		auto &context = deserializer.Get<ClientContext &>();
		catalog_entry_ptr =
		    &Catalog::GetEntry<SequenceCatalogEntry>(context, seq_info.catalog, seq_info.schema, seq_info.name);
	}
	return make_uniq<NextvalBindData>(catalog_entry_ptr);
}

void NextvalFun::RegisterFunction(BuiltinFunctions &set) {
	ScalarFunction next_val("nextval", {LogicalType::VARCHAR}, LogicalType::BIGINT,
	                        NextValFunction<NextSequenceValueOperator>, NextValBind, NextValDependency);
	next_val.side_effects = FunctionSideEffects::HAS_SIDE_EFFECTS;
	next_val.serialize = Serialize;
	next_val.deserialize = Deserialize;
	set.AddFunction(next_val);
}

void CurrvalFun::RegisterFunction(BuiltinFunctions &set) {
	ScalarFunction curr_val("currval", {LogicalType::VARCHAR}, LogicalType::BIGINT,
	                        NextValFunction<CurrentSequenceValueOperator>, NextValBind, NextValDependency);
	curr_val.side_effects = FunctionSideEffects::HAS_SIDE_EFFECTS;
	curr_val.serialize = Serialize;
	curr_val.deserialize = Deserialize;
	set.AddFunction(curr_val);
}

} // namespace duckdb
