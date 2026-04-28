#include "duckdb/function/scalar/system_functions.hpp"

#include "duckdb/common/exception/transaction_exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/client_data.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/transaction/meta_transaction.hpp"

namespace duckdb {

namespace {

struct ShareTransactionData : FunctionData {
	explicit ShareTransactionData(string transaction_id_p) : transaction_id(std::move(transaction_id_p)) {
	}
	string transaction_id;

	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<ShareTransactionData>(transaction_id);
	}
	bool Equals(const FunctionData &other_p) const override {
		return transaction_id == other_p.Cast<ShareTransactionData>().transaction_id;
	}
};

unique_ptr<FunctionData> ShareTransactionBind(BindScalarFunctionInput &input) {
	auto &context = input.GetClientContext();
	if (!context.transaction.HasActiveTransaction() || context.transaction.IsAutoCommit()) {
		throw TransactionException(
		    "duckdb_share_transaction() must be called inside an explicit transaction (BEGIN first)");
	}
	auto &meta = context.transaction.ActiveTransaction();
	// Pin on first call: if we already have an id for this MetaTransaction, reuse it. The
	// rationale is stability — within one transaction, repeated calls return the same id even
	// if a write to a different database occurs between calls.
	auto cached = meta.GetSharedTransactionId();
	if (!cached.empty()) {
		return make_uniq<ShareTransactionData>(cached);
	}
	optional_ptr<AttachedDatabase> match = meta.ModifiedDatabase();
	if (!match) {
		// No database modified yet; fall back to the default (search-path) database.
		auto &db_manager = DatabaseManager::Get(context);
		auto default_db = db_manager.GetDefaultDatabase(context);
		auto db = db_manager.GetDatabase(context, default_db);
		if (!db) {
			throw TransactionException("duckdb_share_transaction(): could not resolve default database '%s'",
			                           default_db);
		}
		match = db.get();
	}
	// Ensure the transaction is materialized for this database. GetTransaction lazily starts one
	// if it doesn't exist - this gives the joiner a DuckTransaction to attach to.
	auto &transaction = meta.GetTransaction(*match);
	if (!transaction.IsDuckTransaction()) {
		throw TransactionException("duckdb_share_transaction(): database '%s' does not support shared transactions",
		                           match->GetName());
	}
	auto transaction_id =
	    StringUtil::Format("%llu/%s", static_cast<uint64_t>(context.GetConnectionId()), match->GetName());
	meta.SetSharedTransactionId(transaction_id);
	return make_uniq<ShareTransactionData>(std::move(transaction_id));
}

void ShareTransactionFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &func_expr = state.expr.Cast<BoundFunctionExpression>();
	const auto &info = func_expr.bind_info->Cast<ShareTransactionData>();
	result.Reference(Value(info.transaction_id), count_t(args.size()));
}

} // namespace

ScalarFunction ShareTransactionFun::GetFunction() {
	return ScalarFunction({}, LogicalType::VARCHAR, ShareTransactionFunction, ShareTransactionBind, nullptr, nullptr,
	                      LogicalType(LogicalTypeId::INVALID), FunctionStability::VOLATILE);
}

} // namespace duckdb
