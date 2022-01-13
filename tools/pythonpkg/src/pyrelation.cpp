#include "duckdb_python/pyrelation.hpp"
#include "duckdb_python/pyconnection.hpp"
#include "duckdb_python/pyresult.hpp"
#include "duckdb/parser/qualified_name.hpp"
#include "duckdb/main/client_context.hpp"

namespace duckdb {

void DuckDBPyRelation::Initialize(py::handle &m) {
	py::class_<DuckDBPyRelation>(m, "DuckDBPyRelation", py::module_local())
	    .def_property_readonly("type", &DuckDBPyRelation::Type, "Get the type of the relation.")
	    .def_property_readonly("columns", &DuckDBPyRelation::Columns, "Get the names of the columns of this relation.")
	    .def_property_readonly("types", &DuckDBPyRelation::ColumnTypes, "Get the columns types of the result.")
	    .def_property_readonly("dtypes", &DuckDBPyRelation::ColumnTypes, "Get the columns types of the result.")
	    .def("filter", &DuckDBPyRelation::Filter, "Filter the relation object by the filter in filter_expr",
	         py::arg("filter_expr"))
	    .def("project", &DuckDBPyRelation::Project, "Project the relation object by the projection in project_expr",
	         py::arg("project_expr"))
	    .def("set_alias", &DuckDBPyRelation::SetAlias, "Rename the relation object to new alias", py::arg("alias"))
	    .def_property_readonly("alias", &DuckDBPyRelation::GetAlias, "Get the name of the current alias")
	    .def("order", &DuckDBPyRelation::Order, "Reorder the relation object by order_expr", py::arg("order_expr"))
	    .def("aggregate", &DuckDBPyRelation::Aggregate,
	         "Compute the aggregate aggr_expr by the optional groups group_expr on the relation", py::arg("aggr_expr"),
	         py::arg("group_expr") = "")
	    .def("union", &DuckDBPyRelation::Union,
	         "Create the set union of this relation object with another relation object in other_rel")
	    .def("except_", &DuckDBPyRelation::Except,
	         "Create the set except of this relation object with another relation object in other_rel",
	         py::arg("other_rel"))
	    .def("intersect", &DuckDBPyRelation::Intersect,
	         "Create the set intersection of this relation object with another relation object in other_rel",
	         py::arg("other_rel"))
	    .def("join", &DuckDBPyRelation::Join,
	         "Join the relation object with another relation object in other_rel using the join condition expression "
	         "in join_condition. Types supported are 'inner' and 'left'",
	         py::arg("other_rel"), py::arg("condition"), py::arg("type") = "inner")
	    .def("distinct", &DuckDBPyRelation::Distinct, "Retrieve distinct rows from this relation object")
	    .def("limit", &DuckDBPyRelation::Limit, "Only retrieve the first n rows from this relation object",
	         py::arg("n"))
	    .def("query", &DuckDBPyRelation::Query,
	         "Run the given SQL query in sql_query on the view named virtual_table_name that refers to the relation "
	         "object",
	         py::arg("virtual_table_name"), py::arg("sql_query"))
	    .def("execute", &DuckDBPyRelation::Execute, "Transform the relation into a result set")
	    .def("write_csv", &DuckDBPyRelation::WriteCsv, "Write the relation object to a CSV file in file_name",
	         py::arg("file_name"))
	    .def("insert_into", &DuckDBPyRelation::InsertInto,
	         "Inserts the relation object into an existing table named table_name", py::arg("table_name"))
	    .def("insert", &DuckDBPyRelation::Insert, "Inserts the given values into the relation", py::arg("values"))
	    .def("create", &DuckDBPyRelation::Create,
	         "Creates a new table named table_name with the contents of the relation object", py::arg("table_name"))
	    .def("create_view", &DuckDBPyRelation::CreateView,
	         "Creates a view named view_name that refers to the relation object", py::arg("view_name"),
	         py::arg("replace") = true)
	    .def("to_arrow_table", &DuckDBPyRelation::ToArrowTable, "Transforms the relation object into a Arrow table")
	    .def("arrow", &DuckDBPyRelation::ToArrowTable, "Transforms the relation object into a Arrow table")
	    .def("to_df", &DuckDBPyRelation::ToDF, "Transforms the relation object into a Data.Frame")
	    .def("df", &DuckDBPyRelation::ToDF, "Transforms the relation object into a Data.Frame")
	    .def("fetchone", &DuckDBPyRelation::Fetchone, "Execute and fetch a single row")
	    .def("fetchall", &DuckDBPyRelation::Fetchall, "Execute and fetch all rows")
	    .def("map", &DuckDBPyRelation::Map, py::arg("map_function"), "Calls the passed function on the relation")
	    .def("__str__", &DuckDBPyRelation::Print)
	    .def("__repr__", &DuckDBPyRelation::Print);
}

DuckDBPyRelation::DuckDBPyRelation(shared_ptr<Relation> rel) : rel(move(rel)) {
}

unique_ptr<DuckDBPyRelation> DuckDBPyRelation::FromDf(py::object df, DuckDBPyConnection *conn) {
	return conn->FromDF(std::move(df));
}

unique_ptr<DuckDBPyRelation> DuckDBPyRelation::Values(py::object values, DuckDBPyConnection *conn) {
	return conn->Values(std::move(values));
}

unique_ptr<DuckDBPyRelation> DuckDBPyRelation::FromQuery(const string &query, const string &alias,
                                                         DuckDBPyConnection *conn) {
	return conn->FromQuery(query, alias);
}

unique_ptr<DuckDBPyRelation> DuckDBPyRelation::FromCsvAuto(const string &filename, DuckDBPyConnection *conn) {
	return conn->FromCsvAuto(filename);
}

unique_ptr<DuckDBPyRelation> DuckDBPyRelation::FromParquet(const string &filename, bool binary_as_string,
                                                           DuckDBPyConnection *conn) {
	return conn->FromParquet(filename, binary_as_string);
}

unique_ptr<DuckDBPyRelation> DuckDBPyRelation::FromParquetDefault(const string &filename, DuckDBPyConnection *conn) {
	bool binary_as_string = false;
	Value result;
	if (conn->connection->context->TryGetCurrentSetting("binary_as_string", result)) {
		binary_as_string = result.GetValue<bool>();
	}
	return conn->FromParquet(filename, binary_as_string);
}

unique_ptr<DuckDBPyRelation> DuckDBPyRelation::FromArrowTable(py::object &table, DuckDBPyConnection *conn) {
	return conn->FromArrowTable(table);
}

unique_ptr<DuckDBPyRelation> DuckDBPyRelation::Project(const string &expr) {
	return make_unique<DuckDBPyRelation>(rel->Project(expr));
}

unique_ptr<DuckDBPyRelation> DuckDBPyRelation::ProjectDf(py::object df, const string &expr, DuckDBPyConnection *conn) {
	return conn->FromDF(std::move(df))->Project(expr);
}

unique_ptr<DuckDBPyRelation> DuckDBPyRelation::SetAlias(const string &expr) {
	return make_unique<DuckDBPyRelation>(rel->Alias(expr));
}

py::str DuckDBPyRelation::GetAlias() {
	return py::str(string(rel->GetAlias()));
}

unique_ptr<DuckDBPyRelation> DuckDBPyRelation::AliasDF(py::object df, const string &expr, DuckDBPyConnection *conn) {
	return conn->FromDF(std::move(df))->SetAlias(expr);
}

unique_ptr<DuckDBPyRelation> DuckDBPyRelation::Filter(const string &expr) {
	return make_unique<DuckDBPyRelation>(rel->Filter(expr));
}

unique_ptr<DuckDBPyRelation> DuckDBPyRelation::FilterDf(py::object df, const string &expr, DuckDBPyConnection *conn) {
	return conn->FromDF(std::move(df))->Filter(expr);
}

unique_ptr<DuckDBPyRelation> DuckDBPyRelation::Limit(int64_t n) {
	return make_unique<DuckDBPyRelation>(rel->Limit(n));
}

unique_ptr<DuckDBPyRelation> DuckDBPyRelation::LimitDF(py::object df, int64_t n, DuckDBPyConnection *conn) {
	return conn->FromDF(std::move(df))->Limit(n);
}

unique_ptr<DuckDBPyRelation> DuckDBPyRelation::Order(const string &expr) {
	return make_unique<DuckDBPyRelation>(rel->Order(expr));
}

unique_ptr<DuckDBPyRelation> DuckDBPyRelation::OrderDf(py::object df, const string &expr, DuckDBPyConnection *conn) {
	return conn->FromDF(std::move(df))->Order(expr);
}

unique_ptr<DuckDBPyRelation> DuckDBPyRelation::Aggregate(const string &expr, const string &groups) {
	if (!groups.empty()) {
		return make_unique<DuckDBPyRelation>(rel->Aggregate(expr, groups));
	}
	return make_unique<DuckDBPyRelation>(rel->Aggregate(expr));
}

unique_ptr<DuckDBPyRelation> DuckDBPyRelation::AggregateDF(py::object df, const string &expr, const string &groups,
                                                           DuckDBPyConnection *conn) {
	return conn->FromDF(std::move(df))->Aggregate(expr, groups);
}

unique_ptr<DuckDBPyRelation> DuckDBPyRelation::Distinct() {
	return make_unique<DuckDBPyRelation>(rel->Distinct());
}

unique_ptr<DuckDBPyRelation> DuckDBPyRelation::DistinctDF(py::object df, DuckDBPyConnection *conn) {
	return conn->FromDF(std::move(df))->Distinct();
}

py::object DuckDBPyRelation::ToDF() {
	auto res = make_unique<DuckDBPyResult>();
	{
		py::gil_scoped_release release;
		res->result = rel->Execute();
	}
	if (!res->result->success) {
		throw std::runtime_error(res->result->error);
	}
	return res->FetchDF();
}

py::object DuckDBPyRelation::Fetchone() {
	auto res = make_unique<DuckDBPyResult>();
	{
		py::gil_scoped_release release;
		res->result = rel->Execute();
	}
	if (!res->result->success) {
		throw std::runtime_error(res->result->error);
	}
	return res->Fetchone();
}

py::object DuckDBPyRelation::Fetchall() {
	auto res = make_unique<DuckDBPyResult>();
	{
		py::gil_scoped_release release;
		res->result = rel->Execute();
	}
	if (!res->result->success) {
		throw std::runtime_error(res->result->error);
	}
	return res->Fetchall();
}

py::object DuckDBPyRelation::ToArrowTable() {
	auto res = make_unique<DuckDBPyResult>();
	{
		py::gil_scoped_release release;
		res->result = rel->Execute();
	}
	if (!res->result->success) {
		throw std::runtime_error(res->result->error);
	}
	return res->FetchArrowTable();
}

unique_ptr<DuckDBPyRelation> DuckDBPyRelation::Union(DuckDBPyRelation *other) {
	return make_unique<DuckDBPyRelation>(rel->Union(other->rel));
}

unique_ptr<DuckDBPyRelation> DuckDBPyRelation::Except(DuckDBPyRelation *other) {
	return make_unique<DuckDBPyRelation>(rel->Except(other->rel));
}

unique_ptr<DuckDBPyRelation> DuckDBPyRelation::Intersect(DuckDBPyRelation *other) {
	return make_unique<DuckDBPyRelation>(rel->Intersect(other->rel));
}

unique_ptr<DuckDBPyRelation> DuckDBPyRelation::Join(DuckDBPyRelation *other, const string &condition,
                                                    const string &type) {
	JoinType dtype;
	string type_string = StringUtil::Lower(type);
	StringUtil::Trim(type_string);
	if (type_string == "inner") {
		dtype = JoinType::INNER;
	} else if (type_string == "left") {
		dtype = JoinType::LEFT;
	} else {
		throw std::runtime_error("Unsupported join type " + type_string + ", try 'inner' or 'left'");
	}
	return make_unique<DuckDBPyRelation>(rel->Join(other->rel, condition, dtype));
}

void DuckDBPyRelation::WriteCsv(const string &file) {
	rel->WriteCSV(file);
}

void DuckDBPyRelation::WriteCsvDF(py::object df, const string &file, DuckDBPyConnection *conn) {
	return conn->FromDF(std::move(df))->WriteCsv(file);
}

// should this return a rel with the new view?
unique_ptr<DuckDBPyRelation> DuckDBPyRelation::CreateView(const string &view_name, bool replace) {
	rel->CreateView(view_name, replace);
	return make_unique<DuckDBPyRelation>(rel);
}

unique_ptr<DuckDBPyResult> DuckDBPyRelation::Query(const string &view_name, const string &sql_query) {
	auto res = make_unique<DuckDBPyResult>();
	res->result = rel->Query(view_name, sql_query);
	if (!res->result->success) {
		throw std::runtime_error(res->result->error);
	}
	return res;
}

unique_ptr<DuckDBPyResult> DuckDBPyRelation::Execute() {
	auto res = make_unique<DuckDBPyResult>();
	{
		py::gil_scoped_release release;
		res->result = rel->Execute();
	}
	if (!res->result->success) {
		throw std::runtime_error(res->result->error);
	}
	return res;
}

unique_ptr<DuckDBPyResult> DuckDBPyRelation::QueryDF(py::object df, const string &view_name, const string &sql_query,
                                                     DuckDBPyConnection *conn) {
	return conn->FromDF(std::move(df))->Query(view_name, sql_query);
}

void DuckDBPyRelation::InsertInto(const string &table) {
	auto parsed_info = QualifiedName::Parse(table);
	if (parsed_info.schema.empty()) {
		//! No Schema Defined, we use default schema.
		rel->Insert(table);
	} else {
		//! Schema defined, we try to insert into it.
		rel->Insert(parsed_info.schema, parsed_info.name);
	};
}

void DuckDBPyRelation::Insert(py::object params) {
	vector<vector<Value>> values {DuckDBPyConnection::TransformPythonParamList(move(params))};
	py::gil_scoped_release release;
	rel->Insert(values);
}

void DuckDBPyRelation::Create(const string &table) {
	py::gil_scoped_release release;
	rel->Create(table);
}

unique_ptr<DuckDBPyRelation> DuckDBPyRelation::Map(py::function fun) {
	vector<Value> params;
	params.emplace_back(Value::POINTER((uintptr_t)fun.ptr()));
	auto res = make_unique<DuckDBPyRelation>(rel->TableFunction("python_map_function", params));
	res->map_function = fun;
	return res;
}

string DuckDBPyRelation::Print() {
	std::string rel_res_string;
	{
		py::gil_scoped_release release;
		rel_res_string = rel->Limit(10)->Execute()->ToString();
	}

	return rel->ToString() + "\n---------------------\n-- Result Preview  --\n---------------------\n" +
	       rel_res_string + "\n";
}

// TODO: RelationType to a python enum
py::str DuckDBPyRelation::Type() {
	return py::str(RelationTypeToString(rel->type));
}

py::list DuckDBPyRelation::Columns() {
	py::list res;
	for (auto &col : rel->Columns()) {
		res.append(col.name);
	}
	return res;
}

py::list DuckDBPyRelation::ColumnTypes() {
	py::list res;
	for (auto &col : rel->Columns()) {
		res.append(col.type.ToString());
	}
	return res;
}

} // namespace duckdb
