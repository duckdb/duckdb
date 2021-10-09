#include "duckdb_python/pyrelation.hpp"
#include "duckdb_python/pyconnection.hpp"
#include "duckdb_python/pyresult.hpp"
#include "duckdb/parser/qualified_name.hpp"
namespace duckdb {

void DuckDBPyRelation::Initialize(py::handle &m) {
	py::class_<DuckDBPyRelation>(m, "DuckDBPyRelation", py::module_local())
	    .def("filter", &DuckDBPyRelation::Filter, "Filter the relation object by the filter in filter_expr",
	         py::arg("filter_expr"))
	    .def("project", &DuckDBPyRelation::Project, "Project the relation object by the projection in project_expr",
	         py::arg("project_expr"))
	    .def("set_alias", &DuckDBPyRelation::Alias, "Rename the relation object to new alias", py::arg("alias"))
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
	         "in join_condition",
	         py::arg("other_rel"), py::arg("join_condition"))
	    .def("distinct", &DuckDBPyRelation::Distinct, "Retrieve distinct rows from this relation object")
	    .def("limit", &DuckDBPyRelation::Limit, "Only retrieve the first n rows from this relation object",
	         py::arg("n"))
	    .def("query_once", &DuckDBPyRelation::QueryOnce,
	         "Create a relation from the given SQL query in sql_query on the view named virtual_table_name that refers to the "
			 "relation object",
	         py::arg("virtual_table_name"), py::arg("sql_query"))
	    .def("query_relation", &DuckDBPyRelation::QueryRelation,
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
	    .def("__repr__", &DuckDBPyRelation::Print)
	    .def("__getattr__", &DuckDBPyRelation::Getattr);
}

DuckDBPyRelation::DuckDBPyRelation(shared_ptr<Relation> rel) : rel(move(rel)) {
}

unique_ptr<DuckDBPyRelation> DuckDBPyRelation::FromDf(py::object df) {
	return DuckDBPyConnection::DefaultConnection()->FromDF(std::move(df));
}

unique_ptr<DuckDBPyRelation> DuckDBPyRelation::Values(py::object values) {
	return DuckDBPyConnection::DefaultConnection()->Values(std::move(values));
}

unique_ptr<DuckDBPyRelation> DuckDBPyRelation::FromQuery(const string &query, const string &alias) {
	return DuckDBPyConnection::DefaultConnection()->FromQuery(query, alias);
}

unique_ptr<DuckDBPyRelation> DuckDBPyRelation::FromCsvAuto(const string &filename) {
	return DuckDBPyConnection::DefaultConnection()->FromCsvAuto(filename);
}

unique_ptr<DuckDBPyRelation> DuckDBPyRelation::FromParquet(const string &filename) {
	return DuckDBPyConnection::DefaultConnection()->FromParquet(filename);
}

unique_ptr<DuckDBPyRelation> DuckDBPyRelation::FromArrowTable(py::object &table) {
	return DuckDBPyConnection::DefaultConnection()->FromArrowTable(table);
}

unique_ptr<DuckDBPyRelation> DuckDBPyRelation::Project(const string &expr) {
	return make_unique<DuckDBPyRelation>(rel->Project(expr));
}

unique_ptr<DuckDBPyRelation> DuckDBPyRelation::ProjectDf(py::object df, const string &expr) {
	return DuckDBPyConnection::DefaultConnection()->FromDF(std::move(df))->Project(expr);
}

unique_ptr<DuckDBPyRelation> DuckDBPyRelation::Alias(const string &expr) {
	return make_unique<DuckDBPyRelation>(rel->Alias(expr));
}

unique_ptr<DuckDBPyRelation> DuckDBPyRelation::AliasDF(py::object df, const string &expr) {
	return DuckDBPyConnection::DefaultConnection()->FromDF(std::move(df))->Alias(expr);
}

unique_ptr<DuckDBPyRelation> DuckDBPyRelation::Filter(const string &expr) {
	return make_unique<DuckDBPyRelation>(rel->Filter(expr));
}

unique_ptr<DuckDBPyRelation> DuckDBPyRelation::FilterDf(py::object df, const string &expr) {
	return DuckDBPyConnection::DefaultConnection()->FromDF(std::move(df))->Filter(expr);
}

unique_ptr<DuckDBPyRelation> DuckDBPyRelation::Limit(int64_t n) {
	return make_unique<DuckDBPyRelation>(rel->Limit(n));
}

unique_ptr<DuckDBPyRelation> DuckDBPyRelation::LimitDF(py::object df, int64_t n) {
	return DuckDBPyConnection::DefaultConnection()->FromDF(std::move(df))->Limit(n);
}

unique_ptr<DuckDBPyRelation> DuckDBPyRelation::Order(const string &expr) {
	return make_unique<DuckDBPyRelation>(rel->Order(expr));
}

unique_ptr<DuckDBPyRelation> DuckDBPyRelation::OrderDf(py::object df, const string &expr) {
	return DuckDBPyConnection::DefaultConnection()->FromDF(std::move(df))->Order(expr);
}

unique_ptr<DuckDBPyRelation> DuckDBPyRelation::Aggregate(const string &expr, const string &groups) {
	if (!groups.empty()) {
		return make_unique<DuckDBPyRelation>(rel->Aggregate(expr, groups));
	}
	return make_unique<DuckDBPyRelation>(rel->Aggregate(expr));
}

unique_ptr<DuckDBPyRelation> DuckDBPyRelation::AggregateDF(py::object df, const string &expr, const string &groups) {
	return DuckDBPyConnection::DefaultConnection()->FromDF(std::move(df))->Aggregate(expr, groups);
}

unique_ptr<DuckDBPyRelation> DuckDBPyRelation::Distinct() {
	return make_unique<DuckDBPyRelation>(rel->Distinct());
}

unique_ptr<DuckDBPyRelation> DuckDBPyRelation::DistinctDF(py::object df) {
	return DuckDBPyConnection::DefaultConnection()->FromDF(std::move(df))->Distinct();
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

unique_ptr<DuckDBPyRelation> DuckDBPyRelation::Join(DuckDBPyRelation *other, const string &condition) {
	return make_unique<DuckDBPyRelation>(rel->Join(other->rel, condition));
}

void DuckDBPyRelation::WriteCsv(const string &file) {
	rel->WriteCSV(file);
}

void DuckDBPyRelation::WriteCsvDF(py::object df, const string &file) {
	return DuckDBPyConnection::DefaultConnection()->FromDF(std::move(df))->WriteCsv(file);
}

// should this return a rel with the new view?
unique_ptr<DuckDBPyRelation> DuckDBPyRelation::CreateView(const string &view_name, bool replace) {
	rel->CreateView(view_name, replace);
	return make_unique<DuckDBPyRelation>(rel);
}

unique_ptr<DuckDBPyRelation> DuckDBPyRelation::QueryRelation(const string &view_name, const string &sql_query) {
	return make_unique<DuckDBPyRelation>(rel->QueryRelation(view_name, sql_query));
}

unique_ptr<DuckDBPyResult> DuckDBPyRelation::QueryOnce(const string &view_name, const string &sql_query) {
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

unique_ptr<DuckDBPyResult> DuckDBPyRelation::QueryDF(py::object df, const string &view_name, const string &sql_query) {
	return DuckDBPyConnection::DefaultConnection()->FromDF(std::move(df))->QueryOnce(view_name, sql_query);
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
	rel->Insert(values);
}

void DuckDBPyRelation::Create(const string &table) {
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

py::object DuckDBPyRelation::Getattr(const py::str &key) {
	auto key_s = key.cast<string>();
	if (key_s == "alias") {
		return py::str(string(rel->GetAlias()));
	} else if (key_s == "type") {
		return py::str(RelationTypeToString(rel->type));
	} else if (key_s == "columns") {
		py::list res;
		for (auto &col : rel->Columns()) {
			res.append(col.name);
		}
		return move(res);
	} else if (key_s == "types" || key_s == "dtypes") {
		py::list res;
		for (auto &col : rel->Columns()) {
			res.append(col.type.ToString());
		}
		return move(res);
	}
	return py::none();
}

} // namespace duckdb
