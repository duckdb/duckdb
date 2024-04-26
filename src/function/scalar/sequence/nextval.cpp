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
#include "duckdb/transaction/meta_transaction.hpp"
#include "duckdb/planner/binder.hpp"

namespace duckdb {

struct CurrentSequenceValueOperator {
	static void Bind(ClientContext &, SequenceCatalogEntry &) {
	}

	static int64_t Operation(DuckTransaction &, SequenceCatalogEntry &seq) {
		return seq.CurrentValue();
	}
};

struct NextSequenceValueOperator {
	static void Bind(ClientContext &context, SequenceCatalogEntry &seq) {
		auto &meta_transaction = MetaTransaction::Get(context);
		meta_transaction.ModifyDatabase(seq.ParentCatalog().GetAttached());
	}

	static int64_t Operation(DuckTransaction &transaction, SequenceCatalogEntry &seq) {
		return seq.NextValue(transaction);
	}
};

template <class OP>
SequenceCatalogEntry &BindSequence(ClientContext &context, string &catalog, string &schema, const string &name) {
	// fetch the sequence from the catalog
	Binder::BindSchemaOrCatalog(context, catalog, schema);
	auto &sequence = Catalog::GetEntry<SequenceCatalogEntry>(context, catalog, schema, name);
	OP::Bind(context, sequence);
	return sequence;
}

template <class OP>
SequenceCatalogEntry &BindSequence(ClientContext &context, const string &name) {
	auto qname = QualifiedName::Parse(name);
	return BindSequence<OP>(context, qname.catalog, qname.schema, qname.name);
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
	auto &info = func_expr.bind_info->Cast<NextvalBindData>();
	auto &context = state.GetContext();
	auto &sequence = info.sequence;
	auto &transaction = DuckTransaction::Get(context, sequence.catalog);
	// sequence to use is hard coded
	// increment the sequence
	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto result_data = FlatVector::GetData<int64_t>(result);
	for (idx_t i = 0; i < args.size(); i++) {
		// get the next value from the sequence
		result_data[i] = OP::Operation(transaction, sequence);
	}
}

template <class OP>
static unique_ptr<FunctionData> NextValBind(ClientContext &context, ScalarFunction &,
                                            vector<unique_ptr<Expression>> &arguments) {
	if (!arguments[0]->IsFoldable()) {
		throw NotImplementedException(
		    "currval/nextval requires a constant sequence - non-constant sequences are no longer supported");
	}
	// parameter to nextval function is a foldable constant
	// evaluate the constant and perform the catalog lookup already
	auto seqname = ExpressionExecutor::EvaluateScalar(context, *arguments[0]);
	if (seqname.IsNull()) {
		return nullptr;
	}
	auto &seq = BindSequence<OP>(context, seqname.ToString());
	return make_uniq<NextvalBindData>(seq);
}

static void NextValDependency(BoundFunctionExpression &expr, LogicalDependencyList &dependencies) {
	if (!expr.bind_info) {
		return;
	}
	auto &info = expr.bind_info->Cast<NextvalBindData>();
	dependencies.AddDependency(info.sequence);
}

void Serialize(Serializer &serializer, const optional_ptr<FunctionData> bind_data, const ScalarFunction &) {
	auto &next_val_bind_data = bind_data->Cast<NextvalBindData>();
	serializer.WritePropertyWithDefault(100, "sequence_create_info", next_val_bind_data.create_info);
}

template <class OP>
unique_ptr<FunctionData> Deserialize(Deserializer &deserializer, ScalarFunction &) {
	auto create_info = deserializer.ReadPropertyWithDefault<unique_ptr<CreateInfo>>(100, "sequence_create_info",
	                                                                                unique_ptr<CreateInfo>());
	if (!create_info) {
		return nullptr;
	}
	auto &seq_info = create_info->Cast<CreateSequenceInfo>();
	auto &context = deserializer.Get<ClientContext &>();
	auto &sequence = BindSequence<OP>(context, seq_info.catalog, seq_info.schema, seq_info.name);
	return make_uniq<NextvalBindData>(sequence);
}

void NextvalFun::RegisterFunction(BuiltinFunctions &set) {
	ScalarFunction next_val("nextval", {LogicalType::VARCHAR}, LogicalType::BIGINT,
	                        NextValFunction<NextSequenceValueOperator>, NextValBind<NextSequenceValueOperator>,
	                        NextValDependency);
	next_val.stability = FunctionStability::VOLATILE;
	next_val.serialize = Serialize;
	next_val.deserialize = Deserialize<NextSequenceValueOperator>;
	set.AddFunction(next_val);
}

void CurrvalFun::RegisterFunction(BuiltinFunctions &set) {
	ScalarFunction curr_val("currval", {LogicalType::VARCHAR}, LogicalType::BIGINT,
	                        NextValFunction<CurrentSequenceValueOperator>, NextValBind<CurrentSequenceValueOperator>,
	                        NextValDependency);
	curr_val.stability = FunctionStability::VOLATILE;
	curr_val.serialize = Serialize;
	curr_val.deserialize = Deserialize<CurrentSequenceValueOperator>;
	set.AddFunction(curr_val);
}

} // namespace duckdb
