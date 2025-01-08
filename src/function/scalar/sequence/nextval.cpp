#include "duckdb/function/scalar/sequence_functions.hpp"
#include "duckdb/function/scalar/sequence_utils.hpp"

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
#include "duckdb/transaction/meta_transaction.hpp"
#include "duckdb/planner/binder.hpp"

namespace duckdb {

struct CurrentSequenceValueOperator {
	static int64_t Operation(DuckTransaction &, SequenceCatalogEntry &seq) {
		return seq.CurrentValue();
	}
};

struct NextSequenceValueOperator {
	static int64_t Operation(DuckTransaction &transaction, SequenceCatalogEntry &seq) {
		return seq.NextValue(transaction);
	}
};

SequenceCatalogEntry &BindSequence(Binder &binder, string &catalog, string &schema, const string &name) {
	// fetch the sequence from the catalog
	Binder::BindSchemaOrCatalog(binder.context, catalog, schema);
	return binder.EntryRetriever()
	    .GetEntry(CatalogType::SEQUENCE_ENTRY, catalog, schema, name)
	    ->Cast<SequenceCatalogEntry>();
}

SequenceCatalogEntry &BindSequenceFromContext(ClientContext &context, string &catalog, string &schema,
                                              const string &name) {
	Binder::BindSchemaOrCatalog(context, catalog, schema);
	return Catalog::GetEntry<SequenceCatalogEntry>(context, catalog, schema, name);
}

SequenceCatalogEntry &BindSequence(Binder &binder, const string &name) {
	auto qname = QualifiedName::Parse(name);
	return BindSequence(binder, qname.catalog, qname.schema, qname.name);
}

struct NextValLocalState : public FunctionLocalState {
	explicit NextValLocalState(DuckTransaction &transaction, SequenceCatalogEntry &sequence)
	    : transaction(transaction), sequence(sequence) {
	}

	DuckTransaction &transaction;
	SequenceCatalogEntry &sequence;
};

unique_ptr<FunctionLocalState> NextValLocalFunction(ExpressionState &state, const BoundFunctionExpression &expr,
                                                    FunctionData *bind_data) {
	if (!bind_data) {
		return nullptr;
	}
	auto &context = state.GetContext();
	auto &info = bind_data->Cast<NextvalBindData>();
	auto &sequence = info.sequence;
	auto &transaction = DuckTransaction::Get(context, sequence.catalog);
	return make_uniq<NextValLocalState>(transaction, sequence);
}

template <class OP>
static void NextValFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &func_expr = state.expr.Cast<BoundFunctionExpression>();
	if (!func_expr.bind_info) {
		// no bind info - return null
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
		ConstantVector::SetNull(result, true);
		return;
	}
	auto &lstate = ExecuteFunctionState::GetFunctionState(state)->Cast<NextValLocalState>();
	// sequence to use is hard coded
	// increment the sequence
	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto result_data = FlatVector::GetData<int64_t>(result);
	for (idx_t i = 0; i < args.size(); i++) {
		// get the next value from the sequence
		result_data[i] = OP::Operation(lstate.transaction, lstate.sequence);
	}
}

static unique_ptr<FunctionData> NextValBind(ScalarFunctionBindInput &bind_input, ScalarFunction &,
                                            vector<unique_ptr<Expression>> &arguments) {
	if (arguments[0]->HasParameter() || arguments[0]->return_type.id() == LogicalTypeId::UNKNOWN) {
		throw ParameterNotResolvedException();
	}
	if (!arguments[0]->IsFoldable()) {
		throw NotImplementedException(
		    "currval/nextval requires a constant sequence - non-constant sequences are no longer supported");
	}
	auto &binder = bind_input.binder;
	// parameter to nextval function is a foldable constant
	// evaluate the constant and perform the catalog lookup already
	auto seqname = ExpressionExecutor::EvaluateScalar(binder.context, *arguments[0]);
	if (seqname.IsNull()) {
		return nullptr;
	}
	auto &seq = BindSequence(binder, seqname.ToString());
	return make_uniq<NextvalBindData>(seq);
}

void Serialize(Serializer &serializer, const optional_ptr<FunctionData> bind_data, const ScalarFunction &) {
	auto &next_val_bind_data = bind_data->Cast<NextvalBindData>();
	serializer.WritePropertyWithDefault(100, "sequence_create_info", next_val_bind_data.create_info);
}

unique_ptr<FunctionData> Deserialize(Deserializer &deserializer, ScalarFunction &) {
	auto create_info = deserializer.ReadPropertyWithExplicitDefault<unique_ptr<CreateInfo>>(100, "sequence_create_info",
	                                                                                        unique_ptr<CreateInfo>());
	if (!create_info) {
		return nullptr;
	}
	auto &seq_info = create_info->Cast<CreateSequenceInfo>();
	auto &context = deserializer.Get<ClientContext &>();
	auto &sequence = BindSequenceFromContext(context, seq_info.catalog, seq_info.schema, seq_info.name);
	return make_uniq<NextvalBindData>(sequence);
}

void NextValModifiedDatabases(ClientContext &context, FunctionModifiedDatabasesInput &input) {
	if (!input.bind_data) {
		return;
	}
	auto &seq = input.bind_data->Cast<NextvalBindData>();
	input.properties.RegisterDBModify(seq.sequence.ParentCatalog(), context);
}

ScalarFunction NextvalFun::GetFunction() {
	ScalarFunction next_val("nextval", {LogicalType::VARCHAR}, LogicalType::BIGINT,
	                        NextValFunction<NextSequenceValueOperator>, nullptr, nullptr);
	next_val.bind_extended = NextValBind;
	next_val.stability = FunctionStability::VOLATILE;
	next_val.serialize = Serialize;
	next_val.deserialize = Deserialize;
	next_val.get_modified_databases = NextValModifiedDatabases;
	next_val.init_local_state = NextValLocalFunction;
	BaseScalarFunction::SetReturnsError(next_val);
	return next_val;
}

ScalarFunction CurrvalFun::GetFunction() {
	ScalarFunction curr_val("currval", {LogicalType::VARCHAR}, LogicalType::BIGINT,
	                        NextValFunction<CurrentSequenceValueOperator>, nullptr, nullptr);
	curr_val.bind_extended = NextValBind;
	curr_val.stability = FunctionStability::VOLATILE;
	curr_val.serialize = Serialize;
	curr_val.deserialize = Deserialize;
	curr_val.init_local_state = NextValLocalFunction;
	BaseScalarFunction::SetReturnsError(curr_val);
	return curr_val;
}

} // namespace duckdb
