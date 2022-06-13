#include "rapi.hpp"
#include "typesr.hpp"

#include "duckdb/common/arrow_wrapper.hpp"
#include "duckdb/planner/table_filter.hpp"
#include "duckdb/planner/filter/constant_filter.hpp"
#include "duckdb/planner/filter/conjunction_filter.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"

using namespace duckdb;

[[cpp11::register]] void rapi_register_df(duckdb::conn_eptr_t conn, std::string name, cpp11::data_frame value) {
	if (!conn || !conn->conn) {
		cpp11::stop("rapi_register_df: Invalid connection");
	}
	if (name.empty()) {
		cpp11::stop("rapi_register_df: Name cannot be empty");
	}
	if (value.ncol() < 1) {
		cpp11::stop("rapi_register_df: Data frame with at least one column required");
	}
	try {
		conn->conn->TableFunction("r_dataframe_scan", {Value::POINTER((uintptr_t)value.data())})
		    ->CreateView(name, true, true);
		static_cast<cpp11::sexp>(conn).attr("_registered_df_" + name) = value;
	} catch (std::exception &e) {
		cpp11::stop("rapi_register_df: Failed to register data frame: %s", e.what());
	}
}

[[cpp11::register]] void rapi_unregister_df(duckdb::conn_eptr_t conn, std::string name) {
	if (!conn || !conn->conn) {
		cpp11::stop("rapi_unregister_df: Invalid connection");
	}
	static_cast<cpp11::sexp>(conn).attr("_registered_df_" + name) = R_NilValue;
	auto res = conn->conn->Query("DROP VIEW IF EXISTS \"" + name + "\"");
	if (!res->success) {
		cpp11::stop(res->error.c_str());
	}
}

class RArrowTabularStreamFactory {
public:
	RArrowTabularStreamFactory(SEXP export_fun_p, SEXP arrow_scannable_p, ClientConfig &config)
	    : arrow_scannable(arrow_scannable_p), export_fun(export_fun_p), config(config) {};

	static unique_ptr<ArrowArrayStreamWrapper>
	Produce(uintptr_t factory_p, std::pair<std::unordered_map<idx_t, string>, std::vector<string>> &project_columns,
	        TableFilterSet *filters) {

		RProtector r;
		auto res = make_unique<ArrowArrayStreamWrapper>();
		auto factory = (RArrowTabularStreamFactory *)factory_p;
		auto stream_ptr_sexp =
		    r.Protect(Rf_ScalarReal(static_cast<double>(reinterpret_cast<uintptr_t>(&res->arrow_array_stream))));

		cpp11::function export_fun = VECTOR_ELT(factory->export_fun, 0);

		if (project_columns.second.empty()) {
			export_fun(factory->arrow_scannable, stream_ptr_sexp);
		} else {
			auto projection_sexp = r.Protect(StringsToSexp(project_columns.second));
			SEXP filters_sexp = r.Protect(Rf_ScalarLogical(true));
			if (filters && !filters->filters.empty()) {
				auto timezone_config = ClientConfig::ExtractTimezoneFromConfig(factory->config);
				filters_sexp =
				    r.Protect(TransformFilter(*filters, project_columns.first, factory->export_fun, timezone_config));
			}
			export_fun(factory->arrow_scannable, stream_ptr_sexp, projection_sexp, filters_sexp);
		}
		return res;
	}

	static void GetSchema(uintptr_t factory_p, ArrowSchemaWrapper &schema) {

		RProtector r;
		auto res = make_unique<ArrowArrayStreamWrapper>();
		auto factory = (RArrowTabularStreamFactory *)factory_p;
		auto schema_ptr_sexp =
		    r.Protect(Rf_ScalarReal(static_cast<double>(reinterpret_cast<uintptr_t>(&schema.arrow_schema))));

		cpp11::function export_fun = VECTOR_ELT(factory->export_fun, 4);

		export_fun(factory->arrow_scannable, schema_ptr_sexp);
	}

	SEXP arrow_scannable;
	SEXP export_fun;
	ClientConfig &config;

private:
	static SEXP TransformFilterExpression(TableFilter &filter, const string &column_name, SEXP functions,
	                                      string &timezone_config) {
		RProtector r;
		auto column_name_sexp = r.Protect(Rf_mkString(column_name.c_str()));
		auto column_name_expr = r.Protect(CreateFieldRef(functions, column_name_sexp));

		switch (filter.filter_type) {
		case TableFilterType::CONSTANT_COMPARISON: {
			auto constant_filter = (ConstantFilter &)filter;
			auto constant_sexp = r.Protect(RApiTypes::ValueToSexp(constant_filter.constant, timezone_config));
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
			return TransformChildFilters(functions, column_name, "and_kleene", and_filter.child_filters,
			                             timezone_config);
		}
		case TableFilterType::CONJUNCTION_OR: {
			auto &and_filter = (ConjunctionAndFilter &)filter;
			return TransformChildFilters(functions, column_name, "or_kleene", and_filter.child_filters,
			                             timezone_config);
		}

		default:
			throw NotImplementedException("Arrow table filter pushdown %s not supported yet",
			                              filter.ToString(column_name));
		}
	}

	static SEXP TransformChildFilters(SEXP functions, const string &column_name, const string op,
	                                  vector<unique_ptr<TableFilter>> &filters, string &timezone_config) {
		auto fit = filters.begin();
		RProtector r;
		auto conjunction_sexp = r.Protect(TransformFilterExpression(**fit, column_name, functions, timezone_config));
		fit++;
		for (; fit != filters.end(); ++fit) {
			SEXP rhs = r.Protect(TransformFilterExpression(**fit, column_name, functions, timezone_config));
			conjunction_sexp = r.Protect(CreateExpression(functions, op, conjunction_sexp, rhs));
		}
		return conjunction_sexp;
	}

	static SEXP TransformFilter(TableFilterSet &filter_collection, std::unordered_map<idx_t, string> &columns,
	                            SEXP functions, string &timezone_config) {
		RProtector r;

		auto fit = filter_collection.filters.begin();
		SEXP res = r.Protect(TransformFilterExpression(*fit->second, columns[fit->first], functions, timezone_config));
		fit++;
		for (; fit != filter_collection.filters.end(); ++fit) {
			SEXP rhs =
			    r.Protect(TransformFilterExpression(*fit->second, columns[fit->first], functions, timezone_config));
			res = r.Protect(CreateExpression(functions, "and_kleene", res, rhs));
		}
		return res;
	}

	static SEXP CallArrowFactory(SEXP functions, idx_t idx, SEXP op1, SEXP op2 = R_NilValue, SEXP op3 = R_NilValue) {
		cpp11::function create_fun = VECTOR_ELT(functions, idx);
		if (Rf_isNull(op2)) {
			return create_fun(op1);
		} else if (Rf_isNull(op3)) {
			return create_fun(op1, op2);
		} else {
			return create_fun(op1, op2, op3);
		}
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

unique_ptr<TableFunctionRef> duckdb::ArrowScanReplacement(ClientContext &context, const string &table_name,
                                                          ReplacementScanData *data_p) {
	auto &data = (ArrowScanReplacementData &)*data_p;
	auto db_wrapper = data.wrapper;
	lock_guard<mutex> arrow_scans_lock(db_wrapper->lock);
	for (auto &e : db_wrapper->arrow_scans) {
		if (e.first == table_name) {
			auto table_function = make_unique<TableFunctionRef>();
			vector<unique_ptr<ParsedExpression>> children;
			children.push_back(make_unique<ConstantExpression>(Value::POINTER((uintptr_t)R_ExternalPtrAddr(e.second))));
			children.push_back(
			    make_unique<ConstantExpression>(Value::POINTER((uintptr_t)RArrowTabularStreamFactory::Produce)));
			children.push_back(
			    make_unique<ConstantExpression>(Value::POINTER((uintptr_t)RArrowTabularStreamFactory::GetSchema)));
			children.push_back(make_unique<ConstantExpression>(Value::UBIGINT(100000)));
			table_function->function = make_unique<FunctionExpression>("arrow_scan", move(children));
			return table_function;
		}
	}
	return nullptr;
}

[[cpp11::register]] void rapi_register_arrow(duckdb::conn_eptr_t conn, std::string name, cpp11::list export_funs,
                                             cpp11::sexp valuesexp) {
	if (!conn || !conn->conn) {
		cpp11::stop("rapi_register_arrow: Invalid connection");
	}
	if (name.empty()) {
		cpp11::stop("rapi_register_arrow: Name cannot be empty");
	}

	auto stream_factory = new RArrowTabularStreamFactory(export_funs, valuesexp, conn->conn->context->config);
	// make r external ptr object to keep factory around until arrow table is unregistered
	cpp11::external_pointer<RArrowTabularStreamFactory> factorysexp(stream_factory);

	{
		// TODO check if this name already exists?
		lock_guard<mutex> arrow_scans_lock(conn->db_eptr->lock);
		auto &arrow_scans = conn->db_eptr->arrow_scans;
		arrow_scans[name] = factorysexp;
	}
	cpp11::writable::list state_list = {export_funs, valuesexp, factorysexp};
	static_cast<cpp11::sexp>(conn->db_eptr).attr("_registered_arrow_" + name) = state_list;
}

[[cpp11::register]] void rapi_unregister_arrow(duckdb::conn_eptr_t conn, std::string name) {
	if (!conn || !conn->conn) {
		cpp11::stop("rapi_unregister_arrow: Invalid connection");
	}

	{
		lock_guard<mutex> arrow_scans_lock(conn->db_eptr->lock);
		auto &arrow_scans = conn->db_eptr->arrow_scans;
		arrow_scans.erase(name);
	}
	static_cast<cpp11::sexp>(conn->db_eptr).attr("_registered_arrow_" + name) = R_NilValue;
}
