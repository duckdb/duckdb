#include "cpp11/protect.hpp"

#include "rapi.hpp"
#include "typesr.hpp"

#include "duckdb/common/arrow_wrapper.hpp"
#include "duckdb/planner/table_filter.hpp"
#include "duckdb/planner/filter/constant_filter.hpp"
#include "duckdb/planner/filter/conjunction_filter.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"

using namespace duckdb;

SEXP RApi::RegisterDataFrame(SEXP connsexp, SEXP namesexp, SEXP valuesexp) {
	if (TYPEOF(connsexp) != EXTPTRSXP) {
		cpp11::stop("duckdb_register_R: Need external pointer parameter for connection");
	}
	auto conn_wrapper = (ConnWrapper *)R_ExternalPtrAddr(connsexp);
	if (!conn_wrapper || !conn_wrapper->conn) {
		cpp11::stop("duckdb_register_R: Invalid connection");
	}
	if (TYPEOF(namesexp) != STRSXP || Rf_length(namesexp) != 1) {
		cpp11::stop("duckdb_register_R: Need single string parameter for name");
	}
	auto name = string(CHAR(STRING_ELT(namesexp, 0)));
	if (name.empty()) {
		cpp11::stop("duckdb_register_R: name parameter cannot be empty");
	}
	if (TYPEOF(valuesexp) != VECSXP || Rf_length(valuesexp) < 1 ||
	    strcmp("data.frame", CHAR(STRING_ELT(GET_CLASS(valuesexp), 0))) != 0) {
		cpp11::stop("duckdb_register_R: Need at least one-column data frame parameter for value");
	}
	try {
		conn_wrapper->conn->TableFunction("r_dataframe_scan", {Value::POINTER((uintptr_t)valuesexp)})
		    ->CreateView(name, true, true);
		auto key = Rf_install(("_registered_df_" + name).c_str());
		Rf_setAttrib(connsexp, key, valuesexp);
	} catch (std::exception &e) {
		cpp11::stop("duckdb_register_R: Failed to register data frame: %s", e.what());
	}
	return R_NilValue;
}

SEXP RApi::UnregisterDataFrame(SEXP connsexp, SEXP namesexp) {
	if (TYPEOF(connsexp) != EXTPTRSXP) {
		cpp11::stop("duckdb_unregister_R: Need external pointer parameter for connection");
	}
	auto conn_wrapper = (ConnWrapper *)R_ExternalPtrAddr(connsexp);
	if (!conn_wrapper || !conn_wrapper->conn) {
		cpp11::stop("duckdb_unregister_R: Invalid connection");
	}
	if (TYPEOF(namesexp) != STRSXP || Rf_length(namesexp) != 1) {
		cpp11::stop("duckdb_unregister_R: Need single string parameter for name");
	}
	auto name = string(CHAR(STRING_ELT(namesexp, 0)));
	auto key = Rf_install(("_registered_df_" + name).c_str());
	Rf_setAttrib(connsexp, key, R_NilValue);
	auto res = conn_wrapper->conn->Query("DROP VIEW IF EXISTS \"" + name + "\"");
	if (!res->success) {
		cpp11::stop(res->error.c_str());
	}
	return R_NilValue;
}

class RArrowTabularStreamFactory {
public:
	RArrowTabularStreamFactory(SEXP export_fun_p, SEXP arrow_scannable_p)
	    : arrow_scannable(arrow_scannable_p), export_fun(export_fun_p) {};

	static unique_ptr<ArrowArrayStreamWrapper>
	Produce(uintptr_t factory_p, std::pair<std::unordered_map<idx_t, string>, std::vector<string>> &project_columns,
	        TableFilterCollection *filters) {

		RProtector r;
		auto res = make_unique<ArrowArrayStreamWrapper>();
		auto factory = (RArrowTabularStreamFactory *)factory_p;
		auto stream_ptr_sexp =
		    r.Protect(Rf_ScalarReal(static_cast<double>(reinterpret_cast<uintptr_t>(&res->arrow_array_stream))));
		SEXP export_call;

		auto export_fun = r.Protect(VECTOR_ELT(factory->export_fun, 0));
		if (project_columns.second.empty()) {
			export_call = r.Protect(Rf_lang3(export_fun, factory->arrow_scannable, stream_ptr_sexp));
		} else {
			auto projection_sexp = r.Protect(RApi::StringsToSexp(project_columns.second));
			SEXP filters_sexp = r.Protect(Rf_ScalarLogical(true));
			if (filters && filters->table_filters && !filters->table_filters->filters.empty()) {

				filters_sexp = r.Protect(TransformFilter(*filters, project_columns.first, factory->export_fun));
			}
			export_call = r.Protect(
			    Rf_lang5(export_fun, factory->arrow_scannable, stream_ptr_sexp, projection_sexp, filters_sexp));
		}
		RApi::REvalThrows(export_call);
		return res;
	}

	SEXP arrow_scannable;
	SEXP export_fun;

private:
	static SEXP TransformFilterExpression(TableFilter &filter, const string &column_name, SEXP functions) {
		RProtector r;
		auto column_name_sexp = r.Protect(Rf_mkString(column_name.c_str()));
		auto column_name_expr = r.Protect(CreateFieldRef(functions, column_name_sexp));

		switch (filter.filter_type) {
		case TableFilterType::CONSTANT_COMPARISON: {
			auto constant_filter = (ConstantFilter &)filter;
			auto constant_sexp = r.Protect(RApiTypes::ValueToSexp(constant_filter.constant));
			auto constant_expr = r.Protect(CreateScalar(functions, constant_sexp));
			switch (constant_filter.comparison_type) {
			case ExpressionType::COMPARE_EQUAL: {
				return CreateExpression(functions, "equal", column_name_expr, constant_expr);
			}
			case ExpressionType::COMPARE_GREATERTHAN: {
				return CreateExpression(functions, "greater", column_name_expr, constant_expr);
			}
			case ExpressionType::COMPARE_GREATERTHANOREQUALTO: {
				return CreateExpression(functions, "greater_equal", column_name_expr, constant_expr);
			}
			case ExpressionType::COMPARE_LESSTHAN: {
				return CreateExpression(functions, "less", column_name_expr, constant_expr);
			}
			case ExpressionType::COMPARE_LESSTHANOREQUALTO: {
				return CreateExpression(functions, "less_equal", column_name_expr, constant_expr);
			}
			case ExpressionType::COMPARE_NOTEQUAL: {
				return CreateExpression(functions, "not_equal", column_name_expr, constant_expr);
			}
			default:
				throw InternalException("%s can't be transformed to Arrow Scan Pushdown Filter",
				                        filter.ToString(column_name));
			}
		}
		case TableFilterType::IS_NULL: {
			return CreateExpression(functions, "is_null", column_name_expr);
		}
		case TableFilterType::IS_NOT_NULL: {
			auto is_null_expr = r.Protect(CreateExpression(functions, "is_null", column_name_expr));
			return CreateExpression(functions, "invert", is_null_expr);
		}
		case TableFilterType::CONJUNCTION_AND: {
			auto &and_filter = (ConjunctionAndFilter &)filter;
			return TransformChildFilters(functions, column_name, "and_kleene", and_filter.child_filters);
		}
		case TableFilterType::CONJUNCTION_OR: {
			auto &and_filter = (ConjunctionAndFilter &)filter;
			return TransformChildFilters(functions, column_name, "or_kleene", and_filter.child_filters);
		}

		default:
			throw NotImplementedException("Arrow table filter pushdown %s not supported yet",
			                              filter.ToString(column_name));
		}
	}

	static SEXP TransformChildFilters(SEXP functions, const string &column_name, const string op,
	                                  vector<unique_ptr<TableFilter>> &filters) {
		auto fit = filters.begin();
		RProtector r;
		auto conjunction_sexp = r.Protect(TransformFilterExpression(**fit, column_name, functions));
		fit++;
		for (; fit != filters.end(); ++fit) {
			SEXP rhs = r.Protect(TransformFilterExpression(**fit, column_name, functions));
			conjunction_sexp = r.Protect(CreateExpression(functions, op, conjunction_sexp, rhs));
		}
		return conjunction_sexp;
	}

	static SEXP TransformFilter(TableFilterCollection &filter_collection, std::unordered_map<idx_t, string> &columns,
	                            SEXP functions) {
		RProtector r;

		auto fit = filter_collection.table_filters->filters.begin();
		SEXP res = r.Protect(TransformFilterExpression(*fit->second, columns[fit->first], functions));
		fit++;
		for (; fit != filter_collection.table_filters->filters.end(); ++fit) {
			SEXP rhs = r.Protect(TransformFilterExpression(*fit->second, columns[fit->first], functions));
			res = r.Protect(CreateExpression(functions, "and_kleene", res, rhs));
		}
		return res;
	}

	static SEXP CallArrowFactory(SEXP functions, idx_t idx, SEXP op1, SEXP op2 = R_NilValue, SEXP op3 = R_NilValue) {
		RProtector r;
		auto create_fun = r.Protect(VECTOR_ELT(functions, idx));
		SEXP create_call;
		if (Rf_isNull(op2)) {
			create_call = r.Protect(Rf_lang2(create_fun, op1));
		} else if (Rf_isNull(op3)) {
			create_call = r.Protect(Rf_lang3(create_fun, op1, op2));
		} else {
			create_call = r.Protect(Rf_lang4(create_fun, op1, op2, op3));
		}

		return RApi::REvalThrows(create_call);
	}

	static SEXP CreateExpression(SEXP functions, const string name, SEXP op1, SEXP op2 = R_NilValue) {
		RProtector r;
		auto name_sexp = r.Protect(Rf_mkString(name.c_str()));
		return CallArrowFactory(functions, 1, name_sexp, op1, op2);
	}

	static SEXP CreateFieldRef(SEXP functions, SEXP op) {
		return CallArrowFactory(functions, 2, op);
	}

	static SEXP CreateScalar(SEXP functions, SEXP op) {
		return CallArrowFactory(functions, 3, op);
	}
};

static SEXP duckdb_finalize_arrow_factory_R(SEXP factorysexp) {
	if (TYPEOF(factorysexp) != EXTPTRSXP) {
		cpp11::stop("duckdb_finalize_arrow_factory_R: Need external pointer parameter");
	}
	auto *factoryaddr = (RArrowTabularStreamFactory *)R_ExternalPtrAddr(factorysexp);
	if (factoryaddr) {
		R_ClearExternalPtr(factorysexp);
		delete factoryaddr;
	}
	return R_NilValue;
}

unique_ptr<TableFunctionRef> RApi::ArrowScanReplacement(const string &table_name, void *data) {
	auto db_wrapper = (DBWrapper *)data;
	lock_guard<mutex> arrow_scans_lock(db_wrapper->lock);
	for (auto &e : db_wrapper->arrow_scans) {
		if (e.first == table_name) {
			auto table_function = make_unique<TableFunctionRef>();
			vector<unique_ptr<ParsedExpression>> children;
			children.push_back(make_unique<ConstantExpression>(Value::POINTER((uintptr_t)R_ExternalPtrAddr(e.second))));
			children.push_back(
			    make_unique<ConstantExpression>(Value::POINTER((uintptr_t)RArrowTabularStreamFactory::Produce)));
			children.push_back(make_unique<ConstantExpression>(Value::UBIGINT(100000)));
			table_function->function = make_unique<FunctionExpression>("arrow_scan", move(children));
			return table_function;
		}
	}
	return nullptr;
}

SEXP RApi::RegisterArrow(SEXP connsexp, SEXP namesexp, SEXP export_funsexp, SEXP valuesexp) {
	if (TYPEOF(connsexp) != EXTPTRSXP) {
		cpp11::stop("duckdb_register_R: Need external pointer parameter for connection");
	}
	auto conn_wrapper = (ConnWrapper *)R_ExternalPtrAddr(connsexp);
	if (!conn_wrapper || !conn_wrapper->conn) {
		cpp11::stop("duckdb_register_R: Invalid connection");
	}
	if (TYPEOF(namesexp) != STRSXP || Rf_length(namesexp) != 1) {
		cpp11::stop("duckdb_register_R: Need single string parameter for name");
	}
	auto name = string(CHAR(STRING_ELT(namesexp, 0)));
	if (name.empty()) {
		cpp11::stop("duckdb_register_R: name parameter cannot be empty");
	}

	if (!IS_LIST(export_funsexp)) {
		cpp11::stop("duckdb_register_R: Need function list for export function");
	}

	RProtector r;
	auto stream_factory = new RArrowTabularStreamFactory(export_funsexp, valuesexp);
	// make r external ptr object to keep factory around until arrow table is unregistered
	SEXP factorysexp = r.Protect(R_MakeExternalPtr(stream_factory, R_NilValue, R_NilValue));
	R_RegisterCFinalizer(factorysexp, (void (*)(SEXP))duckdb_finalize_arrow_factory_R);

	{
		auto *db_wrapper = (DBWrapper *)R_ExternalPtrAddr(conn_wrapper->db_sexp);
		// TODO check if this name already exists?
		lock_guard<mutex> arrow_scans_lock(db_wrapper->lock);
		auto &arrow_scans = db_wrapper->arrow_scans;
		arrow_scans[name] = factorysexp;
	}
	SEXP state_list = r.Protect(NEW_LIST(3));
	SET_VECTOR_ELT(state_list, 0, export_funsexp);
	SET_VECTOR_ELT(state_list, 1, valuesexp);
	SET_VECTOR_ELT(state_list, 2, factorysexp);

	auto key = Rf_install(("_registered_arrow_" + name).c_str());
	Rf_setAttrib(conn_wrapper->db_sexp, key, state_list);

	return R_NilValue;
}

SEXP RApi::UnregisterArrow(SEXP connsexp, SEXP namesexp) {
	if (TYPEOF(connsexp) != EXTPTRSXP) {
		cpp11::stop("duckdb_unregister_R: Need external pointer parameter for connection");
	}
	auto conn_wrapper = (ConnWrapper *)R_ExternalPtrAddr(connsexp);
	if (!conn_wrapper || !conn_wrapper->conn) {
		cpp11::stop("duckdb_unregister_R: Invalid connection");
	}
	if (TYPEOF(namesexp) != STRSXP || Rf_length(namesexp) != 1) {
		cpp11::stop("duckdb_unregister_R: Need single string parameter for name");
	}

	auto name = string(CHAR(STRING_ELT(namesexp, 0)));
	{
		auto *db_wrapper = (DBWrapper *)R_ExternalPtrAddr(conn_wrapper->db_sexp);
		lock_guard<mutex> arrow_scans_lock(db_wrapper->lock);
		auto &arrow_scans = db_wrapper->arrow_scans;
		arrow_scans.erase(name);
	}
	auto key = Rf_install(("_registered_arrow_" + name).c_str());
	Rf_setAttrib(conn_wrapper->db_sexp, key, R_NilValue);

	return R_NilValue;
}
