#include "rapi.hpp"
#include "duckdb/common/arrow_wrapper.hpp"

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

	static unique_ptr<ArrowArrayStreamWrapper> Produce(uintptr_t factory_p) {
		RProtector r;
		int err;
		auto factory = (RArrowTabularStreamFactory *)factory_p;

		auto res = make_unique<ArrowArrayStreamWrapper>();
		auto stream_ptr_sexp =
		    r.Protect(Rf_ScalarReal(static_cast<double>(reinterpret_cast<uintptr_t>(&res->arrow_array_stream))));
		// export the arrow scannable to a stream pointer
		auto export_call = r.Protect(Rf_lang3(factory->export_fun, factory->arrow_scannable, stream_ptr_sexp));
		R_tryEval(export_call, R_GlobalEnv, &err);
		if (err) {
			Rf_error("Failed to produce arrow stream");
		}
		return res;
	}

	SEXP arrow_scannable;
	SEXP export_fun;
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
