#include "duckdb/main/capi/capi_internal.hpp"

#include "duckdb/execution/expression_executor.hpp"

using duckdb::CClientContextWrapper;
using duckdb::ExpressionWrapper;

void duckdb_destroy_expression(duckdb_expression *expr) {
	if (!expr || !*expr) {
		return;
	}
	auto wrapper = reinterpret_cast<ExpressionWrapper *>(*expr);
	delete wrapper;
	*expr = nullptr;
}

duckdb_logical_type duckdb_expression_return_type(duckdb_expression expr) {
	if (!expr) {
		return nullptr;
	}
	auto wrapper = reinterpret_cast<ExpressionWrapper *>(expr);
	auto logical_type = new duckdb::LogicalType(wrapper->expr->return_type);
	return reinterpret_cast<duckdb_logical_type>(logical_type);
}

bool duckdb_expression_is_foldable(duckdb_expression expr) {
	if (!expr) {
		return false;
	}
	auto wrapper = reinterpret_cast<ExpressionWrapper *>(expr);
	return wrapper->expr->IsFoldable();
}

duckdb_error_data duckdb_expression_fold(duckdb_client_context context, duckdb_expression expr,
                                         duckdb_value *out_value) {
	if (!expr || !duckdb_expression_is_foldable(expr)) {
		return nullptr;
	}

	auto value = new duckdb::Value;
	try {
		auto context_wrapper = reinterpret_cast<CClientContextWrapper *>(context);
		auto expr_wrapper = reinterpret_cast<ExpressionWrapper *>(expr);
		*value = duckdb::ExpressionExecutor::EvaluateScalar(context_wrapper->context, *expr_wrapper->expr);
		*out_value = reinterpret_cast<duckdb_value>(value);
	} catch (const duckdb::Exception &ex) {
		delete value;
		return duckdb_create_error_data(DUCKDB_ERROR_INVALID_INPUT, ex.what());
	} catch (const std::exception &ex) {
		delete value;
		return duckdb_create_error_data(DUCKDB_ERROR_INVALID_INPUT, ex.what());
	} catch (...) {
		delete value;
		return duckdb_create_error_data(DUCKDB_ERROR_INVALID_INPUT, "unknown error occurred during folding");
	}
	return nullptr;
}
