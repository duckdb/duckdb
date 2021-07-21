#include "rapi.hpp"
#include "typesr.hpp"

#include "duckdb/common/arrow_wrapper.hpp"
#include "duckdb/planner/table_filter.hpp"
#include "duckdb/planner/filter/constant_filter.hpp"
#include "duckdb/planner/filter/conjunction_filter.hpp"

using namespace duckdb;

SEXP RApi::RegisterDataFrame(SEXP connsexp, SEXP namesexp, SEXP valuesexp) {
	if (TYPEOF(connsexp) != EXTPTRSXP) {
		Rf_error("duckdb_register_R: Need external pointer parameter for connection");
	}
	Connection *conn = (Connection *)R_ExternalPtrAddr(connsexp);
	if (!conn) {
		Rf_error("duckdb_register_R: Invalid connection");
	}
	if (TYPEOF(namesexp) != STRSXP || Rf_length(namesexp) != 1) {
		Rf_error("duckdb_register_R: Need single string parameter for name");
	}
	auto name = string(CHAR(STRING_ELT(namesexp, 0)));
	if (name.empty()) {
		Rf_error("duckdb_register_R: name parameter cannot be empty");
	}
	if (TYPEOF(valuesexp) != VECSXP || Rf_length(valuesexp) < 1 ||
	    strcmp("data.frame", CHAR(STRING_ELT(GET_CLASS(valuesexp), 0))) != 0) {
		Rf_error("duckdb_register_R: Need at least one-column data frame parameter for value");
	}
	try {
		conn->TableFunction("r_dataframe_scan", {Value::POINTER((uintptr_t)valuesexp)})->CreateView(name, true, true);
		auto key = Rf_install(("_registered_df_" + name).c_str());
		Rf_setAttrib(connsexp, key, valuesexp);
	} catch (std::exception &e) {
		Rf_error("duckdb_register_R: Failed to register data frame: %s", e.what());
	}
	return R_NilValue;
}

SEXP RApi::UnregisterDataFrame(SEXP connsexp, SEXP namesexp) {
	if (TYPEOF(connsexp) != EXTPTRSXP) {
		Rf_error("duckdb_unregister_R: Need external pointer parameter for connection");
	}
	Connection *conn = (Connection *)R_ExternalPtrAddr(connsexp);
	if (!conn) {
		Rf_error("duckdb_unregister_R: Invalid connection");
	}
	if (TYPEOF(namesexp) != STRSXP || Rf_length(namesexp) != 1) {
		Rf_error("duckdb_unregister_R: Need single string parameter for name");
	}
	auto name = string(CHAR(STRING_ELT(namesexp, 0)));
	auto key = Rf_install(("_registered_df_" + name).c_str());
	Rf_setAttrib(connsexp, key, R_NilValue);
	auto res = conn->Query("DROP VIEW IF EXISTS \"" + name + "\"");
	if (!res->success) {
		Rf_error(res->error.c_str());
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

		if (project_columns.second.empty()) {

			export_call = r.Protect(Rf_lang3(factory->export_fun, factory->arrow_scannable, stream_ptr_sexp));
		} else {
			auto projection_sexp = r.Protect(RApi::StringsToSexp(project_columns.second));
			SEXP filters_sexp = r.Protect(Rf_ScalarLogical(true));
			if (filters && filters->table_filters && !filters->table_filters->filters.empty()) {

				filters_sexp = r.Protect(TransformFilter(*filters, project_columns.first));
			}
			export_call = r.Protect(Rf_lang5(factory->export_fun, factory->arrow_scannable, stream_ptr_sexp,
			                                 projection_sexp, filters_sexp));
		}

		int err;
		R_tryEval(export_call, R_GlobalEnv, &err);
		if (err) {
			Rf_error("Failed to produce Arrow stream");
		}
		return res;
	}

	SEXP arrow_scannable;
	SEXP export_fun;

private:
	static SEXP TransformFilterExpression(TableFilter &filter, string &column_name) {
		RProtector r;
		auto filter_res = r.Protect(NEW_LIST(3));
		auto column_name_sexp = r.Protect(Rf_mkString(column_name.c_str()));

		switch (filter.filter_type) {
		case TableFilterType::CONSTANT_COMPARISON: {
			SET_ELEMENT(filter_res, 1, column_name_sexp);

			auto constant_filter = (ConstantFilter &)filter;
			SET_ELEMENT(filter_res, 2, RApiTypes::ValueToSexp(constant_filter.constant));

			switch (constant_filter.comparison_type) {
			case ExpressionType::COMPARE_EQUAL: {
				SET_ELEMENT(filter_res, 0, Rf_mkString("CONSTANT_COMPARISON_EQUAL"));
				break;
			}
			case ExpressionType::COMPARE_GREATERTHAN: {
				SET_ELEMENT(filter_res, 0, Rf_mkString("CONSTANT_COMPARISON_GREATERTHAN"));
				break;
			}
			default:
				throw std::runtime_error("Comparison Type can't be an Arrow Scan Pushdown Filter");
			}
		} break;

		case TableFilterType::IS_NOT_NULL: {
			auto constant_field = column_name;
			SET_ELEMENT(filter_res, 0, Rf_mkString("IS_NOT_NULL"));
			SET_ELEMENT(filter_res, 1, column_name_sexp);

		} break;

		case TableFilterType::CONJUNCTION_AND: {
			auto &and_filter = (ConjunctionAndFilter &)filter;
			auto fit = and_filter.child_filters.begin();
			filter_res = r.Protect(TransformFilterExpression(**fit, column_name));
			fit++;
			for (; fit != and_filter.child_filters.end(); ++fit) {
				SEXP rhs = r.Protect(TransformFilterExpression(**fit, column_name));
				filter_res = r.Protect(Conjunction(filter_res, rhs));
			}
		} break;

		default:
			throw NotImplementedException("Arrow table filter pushdown %s not supported yet",
			                              filter.ToString(column_name));
		}
		return filter_res;
	}

	static SEXP Conjunction(SEXP lhs, SEXP rhs) {
		RProtector r;
		auto newexpr = r.Protect(NEW_LIST(3));
		SET_ELEMENT(newexpr, 0, Rf_mkString("CONJUNCTION_AND"));
		SET_ELEMENT(newexpr, 1, lhs);
		SET_ELEMENT(newexpr, 2, rhs);
		return newexpr;
	}

	static SEXP TransformFilter(TableFilterCollection &filter_collection, std::unordered_map<idx_t, string> &columns) {
		RProtector r;

		auto fit = filter_collection.table_filters->filters.begin();
		SEXP res = r.Protect(TransformFilterExpression(*fit->second, columns[fit->first]));
		fit++;
		for (; fit != filter_collection.table_filters->filters.end(); ++fit) {
			SEXP rhs = r.Protect(TransformFilterExpression(*fit->second, columns[fit->first]));
			res = r.Protect(Conjunction(res, rhs));
		}
		return res;
	}
};

static SEXP duckdb_finalize_arrow_factory_R(SEXP factorysexp) {
	if (TYPEOF(factorysexp) != EXTPTRSXP) {
		Rf_error("duckdb_finalize_arrow_factory_R: Need external pointer parameter");
	}
	auto *factoryaddr = (RArrowTabularStreamFactory *)R_ExternalPtrAddr(factorysexp);
	if (factoryaddr) {
		R_ClearExternalPtr(factorysexp);
		delete factoryaddr;
	}
	return R_NilValue;
}

SEXP RApi::RegisterArrow(SEXP connsexp, SEXP namesexp, SEXP export_funsexp, SEXP valuesexp) {
	if (TYPEOF(connsexp) != EXTPTRSXP) {
		Rf_error("duckdb_register_R: Need external pointer parameter for connection");
	}
	Connection *conn = (Connection *)R_ExternalPtrAddr(connsexp);
	if (!conn) {
		Rf_error("duckdb_register_R: Invalid connection");
	}
	if (TYPEOF(namesexp) != STRSXP || Rf_length(namesexp) != 1) {
		Rf_error("duckdb_register_R: Need single string parameter for name");
	}
	auto name = string(CHAR(STRING_ELT(namesexp, 0)));
	if (name.empty()) {
		Rf_error("duckdb_register_R: name parameter cannot be empty");
	}

	if (!Rf_isFunction(export_funsexp)) {
		Rf_error("duckdb_register_R: Need function parameter for export function");
	}

	RProtector r;
	auto stream_factory = new RArrowTabularStreamFactory(export_funsexp, valuesexp);
	auto stream_factory_produce = RArrowTabularStreamFactory::Produce;
	conn->TableFunction("arrow_scan", {Value::POINTER((uintptr_t)stream_factory),
	                                   Value::POINTER((uintptr_t)stream_factory_produce), Value::UBIGINT(100000)})
	    ->CreateView(name, true, true);

	// make r external ptr object to keep factory around until arrow table is unregistered
	SEXP factorysexp = r.Protect(R_MakeExternalPtr(stream_factory, R_NilValue, R_NilValue));
	R_RegisterCFinalizer(factorysexp, (void (*)(SEXP))duckdb_finalize_arrow_factory_R);

	SEXP state_list = r.Protect(NEW_LIST(3));
	SET_VECTOR_ELT(state_list, 0, export_funsexp);
	SET_VECTOR_ELT(state_list, 1, valuesexp);
	SET_VECTOR_ELT(state_list, 2, factorysexp);

	auto key = Rf_install(("_registered_arrow_" + name).c_str());
	Rf_setAttrib(connsexp, key, state_list);

	return R_NilValue;
}

SEXP RApi::UnregisterArrow(SEXP connsexp, SEXP namesexp) {
	if (TYPEOF(connsexp) != EXTPTRSXP) {
		Rf_error("duckdb_unregister_R: Need external pointer parameter for connection");
	}
	Connection *conn = (Connection *)R_ExternalPtrAddr(connsexp);
	if (!conn) {
		Rf_error("duckdb_unregister_R: Invalid connection");
	}
	if (TYPEOF(namesexp) != STRSXP || Rf_length(namesexp) != 1) {
		Rf_error("duckdb_unregister_R: Need single string parameter for name");
	}
	auto name = string(CHAR(STRING_ELT(namesexp, 0)));
	auto key = Rf_install(("_registered_arrow_" + name).c_str());
	Rf_setAttrib(connsexp, key, R_NilValue);
	auto res = conn->Query("DROP VIEW IF EXISTS \"" + name + "\"");
	if (!res->success) {
		Rf_error(res->error.c_str());
	}
	return R_NilValue;
}
