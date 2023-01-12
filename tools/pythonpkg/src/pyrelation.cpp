#include "duckdb_python/pyrelation.hpp"
#include "duckdb_python/pyconnection.hpp"
#include "duckdb_python/pyresult.hpp"
#include "duckdb/parser/qualified_name.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb_python/vector_conversion.hpp"
#include "duckdb_python/pandas_type.hpp"
#include "duckdb/main/relation/query_relation.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/main/relation/view_relation.hpp"
#include "duckdb/function/pragma/pragma_functions.hpp"
#include "duckdb/parser/statement/pragma_statement.hpp"

namespace duckdb {

DuckDBPyRelation::DuckDBPyRelation(shared_ptr<Relation> rel) : rel(std::move(rel)) {
}

DuckDBPyRelation::DuckDBPyRelation(unique_ptr<DuckDBPyResult> result) : rel(nullptr), result(std::move(result)) {
}

unique_ptr<DuckDBPyRelation> DuckDBPyRelation::FromDf(const DataFrame &df, shared_ptr<DuckDBPyConnection> conn) {
	if (!conn) {
		conn = DuckDBPyConnection::DefaultConnection();
	}
	return conn->FromDF(df);
}

unique_ptr<DuckDBPyRelation> DuckDBPyRelation::Values(py::object values, shared_ptr<DuckDBPyConnection> conn) {
	if (!conn) {
		conn = DuckDBPyConnection::DefaultConnection();
	}
	return conn->Values(std::move(values));
}

unique_ptr<DuckDBPyRelation> DuckDBPyRelation::FromQuery(const string &query, const string &alias,
                                                         shared_ptr<DuckDBPyConnection> conn) {
	if (!conn) {
		conn = DuckDBPyConnection::DefaultConnection();
	}
	return conn->FromQuery(query, alias);
}

unique_ptr<DuckDBPyRelation> DuckDBPyRelation::RunQuery(const string &query, const string &alias,
                                                        shared_ptr<DuckDBPyConnection> conn) {
	if (!conn) {
		conn = DuckDBPyConnection::DefaultConnection();
	}
	return conn->RunQuery(query, alias);
}

unique_ptr<DuckDBPyRelation> DuckDBPyRelation::FromCsvAuto(const string &filename,
                                                           shared_ptr<DuckDBPyConnection> conn) {
	if (!conn) {
		conn = DuckDBPyConnection::DefaultConnection();
	}
	return conn->FromCsvAuto(filename);
}

unique_ptr<DuckDBPyRelation> DuckDBPyRelation::FromParquet(const string &file_glob, bool binary_as_string,
                                                           bool file_row_number, bool filename, bool hive_partitioning,
                                                           shared_ptr<DuckDBPyConnection> conn) {
	if (!conn) {
		conn = DuckDBPyConnection::DefaultConnection();
	}
	return conn->FromParquet(file_glob, binary_as_string, file_row_number, filename, hive_partitioning);
}

unique_ptr<DuckDBPyRelation> DuckDBPyRelation::FromParquets(const vector<string> &file_globs, bool binary_as_string,
                                                            bool file_row_number, bool filename, bool hive_partitioning,
                                                            shared_ptr<DuckDBPyConnection> conn) {
	if (!conn) {
		conn = DuckDBPyConnection::DefaultConnection();
	}
	return conn->FromParquets(file_globs, binary_as_string, file_row_number, filename, hive_partitioning);
}

unique_ptr<DuckDBPyRelation> DuckDBPyRelation::GetSubstrait(const string &query, shared_ptr<DuckDBPyConnection> conn) {
	if (!conn) {
		conn = DuckDBPyConnection::DefaultConnection();
	}
	return conn->GetSubstrait(query);
}

unique_ptr<DuckDBPyRelation> DuckDBPyRelation::GetSubstraitJSON(const string &query,
                                                                shared_ptr<DuckDBPyConnection> conn) {
	if (!conn) {
		conn = DuckDBPyConnection::DefaultConnection();
	}
	return conn->GetSubstraitJSON(query);
}

unique_ptr<DuckDBPyRelation> DuckDBPyRelation::FromSubstraitJSON(const string &json,
                                                                 shared_ptr<DuckDBPyConnection> conn) {
	if (!conn) {
		conn = DuckDBPyConnection::DefaultConnection();
	}
	return conn->FromSubstraitJSON(json);
}

unique_ptr<DuckDBPyRelation> DuckDBPyRelation::FromSubstrait(py::bytes &proto, shared_ptr<DuckDBPyConnection> conn) {
	if (!conn) {
		conn = DuckDBPyConnection::DefaultConnection();
	}
	return conn->FromSubstrait(proto);
}

unique_ptr<DuckDBPyRelation> DuckDBPyRelation::FromArrow(py::object &arrow_object,
                                                         shared_ptr<DuckDBPyConnection> conn) {
	if (!conn) {
		conn = DuckDBPyConnection::DefaultConnection();
	}
	return conn->FromArrow(arrow_object);
}

unique_ptr<DuckDBPyRelation> DuckDBPyRelation::Project(const string &expr) {
	auto projected_relation = make_unique<DuckDBPyRelation>(rel->Project(expr));
	projected_relation->rel->extra_dependencies = this->rel->extra_dependencies;
	return projected_relation;
}

unique_ptr<DuckDBPyRelation> DuckDBPyRelation::ProjectDf(const DataFrame &df, const string &expr,
                                                         shared_ptr<DuckDBPyConnection> conn) {
	if (!conn) {
		conn = DuckDBPyConnection::DefaultConnection();
	}
	return conn->FromDF(df)->Project(expr);
}

unique_ptr<DuckDBPyRelation> DuckDBPyRelation::SetAlias(const string &expr) {
	return make_unique<DuckDBPyRelation>(rel->Alias(expr));
}

py::str DuckDBPyRelation::GetAlias() {
	return py::str(string(rel->GetAlias()));
}

unique_ptr<DuckDBPyRelation> DuckDBPyRelation::AliasDF(const DataFrame &df, const string &expr,
                                                       shared_ptr<DuckDBPyConnection> conn) {
	if (!conn) {
		conn = DuckDBPyConnection::DefaultConnection();
	}
	return conn->FromDF(df)->SetAlias(expr);
}

unique_ptr<DuckDBPyRelation> DuckDBPyRelation::Filter(const string &expr) {
	return make_unique<DuckDBPyRelation>(rel->Filter(expr));
}

unique_ptr<DuckDBPyRelation> DuckDBPyRelation::FilterDf(const DataFrame &df, const string &expr,
                                                        shared_ptr<DuckDBPyConnection> conn) {
	if (!conn) {
		conn = DuckDBPyConnection::DefaultConnection();
	}
	return conn->FromDF(df)->Filter(expr);
}

unique_ptr<DuckDBPyRelation> DuckDBPyRelation::Limit(int64_t n, int64_t offset) {
	return make_unique<DuckDBPyRelation>(rel->Limit(n, offset));
}

unique_ptr<DuckDBPyRelation> DuckDBPyRelation::LimitDF(const DataFrame &df, int64_t n,
                                                       shared_ptr<DuckDBPyConnection> conn) {
	if (!conn) {
		conn = DuckDBPyConnection::DefaultConnection();
	}
	return conn->FromDF(df)->Limit(n);
}

unique_ptr<DuckDBPyRelation> DuckDBPyRelation::Order(const string &expr) {
	return make_unique<DuckDBPyRelation>(rel->Order(expr));
}

unique_ptr<DuckDBPyRelation> DuckDBPyRelation::OrderDf(const DataFrame &df, const string &expr,
                                                       shared_ptr<DuckDBPyConnection> conn) {
	if (!conn) {
		conn = DuckDBPyConnection::DefaultConnection();
	}
	return conn->FromDF(df)->Order(expr);
}

unique_ptr<DuckDBPyRelation> DuckDBPyRelation::Aggregate(const string &expr, const string &groups) {
	if (!groups.empty()) {
		return make_unique<DuckDBPyRelation>(rel->Aggregate(expr, groups));
	}
	return make_unique<DuckDBPyRelation>(rel->Aggregate(expr));
}

void DuckDBPyRelation::AssertResult() const {
	if (!result) {
		throw InvalidInputException("No open result set");
	}
}

void DuckDBPyRelation::AssertResultOpen() const {
	if (result && result->IsClosed()) {
		throw InvalidInputException("No open result set");
	}
}

py::list DuckDBPyRelation::Description() {
	if (!result) {
		ExecuteOrThrow();
	}
	AssertResultOpen();
	return result->Description();
}

unique_ptr<DuckDBPyRelation> DuckDBPyRelation::Describe() {
	auto &columns = rel->Columns();
	vector<string> column_list;
	column_list.reserve(columns.size());
	for (auto &column_rel : columns) {
		column_list.push_back(column_rel.Name());
	}
	auto expr = GenerateExpressionList("stats", column_list);
	return make_unique<DuckDBPyRelation>(rel->Project(expr)->Limit(1));
}

string DuckDBPyRelation::GenerateExpressionList(const string &function_name, const string &aggregated_columns,
                                                const string &groups, const string &function_parameter,
                                                const string &projected_columns, const string &window_function) {
	auto input = StringUtil::Split(aggregated_columns, ',');
	return GenerateExpressionList(function_name, input, groups, function_parameter, projected_columns, window_function);
}

string DuckDBPyRelation::GenerateExpressionList(const string &function_name, const vector<string> &input,
                                                const string &groups, const string &function_parameter,
                                                const string &projected_columns, const string &window_function) {
	string expr;
	if (!projected_columns.empty()) {
		expr = projected_columns + ", ";
	}
	for (idx_t i = 0; i < input.size(); i++) {
		if (function_parameter.empty()) {
			expr += function_name + "(" + input[i] + ") " + window_function;
		} else {
			expr += function_name + "(" + input[i] + "," + function_parameter + ")" + window_function;
		}

		if (i < input.size() - 1) {
			expr += ",";
		}
	}
	return expr;
}

unique_ptr<DuckDBPyRelation> DuckDBPyRelation::GenericAggregator(const string &function_name,
                                                                 const string &aggregated_columns, const string &groups,
                                                                 const string &function_parameter,
                                                                 const string &projected_columns) {

	//! Construct Aggregation Expression
	auto expr =
	    GenerateExpressionList(function_name, aggregated_columns, groups, function_parameter, projected_columns);
	return Aggregate(expr, groups);
}

unique_ptr<DuckDBPyRelation> DuckDBPyRelation::Sum(const string &sum_columns, const string &groups) {
	return GenericAggregator("sum", sum_columns, groups);
}

unique_ptr<DuckDBPyRelation> DuckDBPyRelation::Count(const string &count_columns, const string &groups) {
	return GenericAggregator("count", count_columns, groups);
}

unique_ptr<DuckDBPyRelation> DuckDBPyRelation::Median(const string &median_columns, const string &groups) {
	return GenericAggregator("median", median_columns, groups);
}

unique_ptr<DuckDBPyRelation> DuckDBPyRelation::Quantile(const string &q, const string &quantile_columns,
                                                        const string &groups) {
	return GenericAggregator("quantile", quantile_columns, groups, q);
}

unique_ptr<DuckDBPyRelation> DuckDBPyRelation::Min(const string &min_columns, const string &groups) {
	return GenericAggregator("min", min_columns, groups);
}

unique_ptr<DuckDBPyRelation> DuckDBPyRelation::Max(const string &max_columns, const string &groups) {
	return GenericAggregator("max", max_columns, groups);
}

unique_ptr<DuckDBPyRelation> DuckDBPyRelation::Mean(const string &mean_columns, const string &groups) {
	return GenericAggregator("avg", mean_columns, groups);
}

unique_ptr<DuckDBPyRelation> DuckDBPyRelation::Var(const string &var_columns, const string &groups) {
	return GenericAggregator("var_pop", var_columns, groups);
}

unique_ptr<DuckDBPyRelation> DuckDBPyRelation::STD(const string &std_columns, const string &groups) {
	return GenericAggregator("stddev_pop", std_columns, groups);
}

unique_ptr<DuckDBPyRelation> DuckDBPyRelation::ValueCounts(const string &count_column, const string &groups) {
	if (count_column.find(',') != string::npos) {
		throw InvalidInputException("Only one column is accepted in Value_Counts method");
	}
	return GenericAggregator("count", count_column, groups, "", count_column);
}

unique_ptr<DuckDBPyRelation> DuckDBPyRelation::MAD(const string &aggr_columns, const string &groups) {
	return GenericAggregator("mad", aggr_columns, groups);
}

unique_ptr<DuckDBPyRelation> DuckDBPyRelation::Mode(const string &aggr_columns, const string &groups) {
	return GenericAggregator("mode", aggr_columns, groups);
}

unique_ptr<DuckDBPyRelation> DuckDBPyRelation::Abs(const string &columns) {
	auto expr = GenerateExpressionList("abs", columns);
	return Project(expr);
}
unique_ptr<DuckDBPyRelation> DuckDBPyRelation::Prod(const string &aggr_columns, const string &groups) {
	return GenericAggregator("product", aggr_columns, groups);
}

unique_ptr<DuckDBPyRelation> DuckDBPyRelation::Skew(const string &aggr_columns, const string &groups) {
	return GenericAggregator("skewness", aggr_columns, groups);
}

unique_ptr<DuckDBPyRelation> DuckDBPyRelation::Kurt(const string &aggr_columns, const string &groups) {
	return GenericAggregator("kurtosis", aggr_columns, groups);
}

unique_ptr<DuckDBPyRelation> DuckDBPyRelation::SEM(const string &aggr_columns, const string &groups) {
	return GenericAggregator("sem", aggr_columns, groups);
}

idx_t DuckDBPyRelation::Length() {
	auto aggregate_rel = GenericAggregator("count", "*");
	aggregate_rel->Execute();
	D_ASSERT(aggregate_rel->result && aggregate_rel->result->result);
	auto tmp_res = std::move(aggregate_rel->result);
	return tmp_res->result->Fetch()->GetValue(0, 0).GetValue<idx_t>();
}

py::tuple DuckDBPyRelation::Shape() {
	auto length = Length();
	return py::make_tuple(length, rel->Columns().size());
}

unique_ptr<DuckDBPyRelation> DuckDBPyRelation::Unique(const string &std_columns) {
	return make_unique<DuckDBPyRelation>(rel->Project(std_columns)->Distinct());
}

unique_ptr<DuckDBPyRelation> DuckDBPyRelation::GenericWindowFunction(const string &function_name,
                                                                     const string &aggr_columns) {
	auto expr = GenerateExpressionList(function_name, aggr_columns, "", "", "",
	                                   "over (rows between unbounded preceding and current row) ");
	return make_unique<DuckDBPyRelation>(rel->Project(expr));
}

unique_ptr<DuckDBPyRelation> DuckDBPyRelation::CumSum(const string &aggr_columns) {
	return GenericWindowFunction("sum", aggr_columns);
}

unique_ptr<DuckDBPyRelation> DuckDBPyRelation::CumProd(const string &aggr_columns) {
	return GenericWindowFunction("product", aggr_columns);
}

unique_ptr<DuckDBPyRelation> DuckDBPyRelation::CumMax(const string &aggr_columns) {
	return GenericWindowFunction("max", aggr_columns);
}

unique_ptr<DuckDBPyRelation> DuckDBPyRelation::CumMin(const string &aggr_columns) {
	return GenericWindowFunction("min", aggr_columns);
}

unique_ptr<DuckDBPyRelation> DuckDBPyRelation::AggregateDF(const DataFrame &df, const string &expr,
                                                           const string &groups, shared_ptr<DuckDBPyConnection> conn) {
	if (!conn) {
		conn = DuckDBPyConnection::DefaultConnection();
	}
	return conn->FromDF(df)->Aggregate(expr, groups);
}

unique_ptr<DuckDBPyRelation> DuckDBPyRelation::Distinct() {
	return make_unique<DuckDBPyRelation>(rel->Distinct());
}

unique_ptr<DuckDBPyRelation> DuckDBPyRelation::DistinctDF(const DataFrame &df, shared_ptr<DuckDBPyConnection> conn) {
	if (!conn) {
		conn = DuckDBPyConnection::DefaultConnection();
	}
	return conn->FromDF(df)->Distinct();
}
duckdb::pyarrow::RecordBatchReader DuckDBPyRelation::FetchRecordBatchReader(idx_t chunk_size) {
	AssertResult();
	return result->FetchRecordBatchReader(chunk_size);
}

void DuckDBPyRelation::ExecuteOrThrow() {
	auto tmp_result = make_unique<DuckDBPyResult>();
	{
		py::gil_scoped_release release;
		tmp_result->result = rel->Execute();
	}
	if (tmp_result->result->HasError()) {
		tmp_result->result->ThrowError();
	}
	result = std::move(tmp_result);
}

DataFrame DuckDBPyRelation::FetchDF(bool date_as_object) {
	if (!result) {
		ExecuteOrThrow();
	}
	AssertResultOpen();
	auto df = result->FetchDF(date_as_object);
	result = nullptr;
	return df;
}

py::object DuckDBPyRelation::FetchOne() {
	if (!result) {
		ExecuteOrThrow();
	}
	AssertResultOpen();
	return result->Fetchone();
}

py::object DuckDBPyRelation::FetchMany(idx_t size) {
	if (!result) {
		ExecuteOrThrow();
	}
	AssertResultOpen();
	return result->Fetchmany(size);
}

py::object DuckDBPyRelation::FetchAll() {
	if (!result) {
		ExecuteOrThrow();
	}
	AssertResultOpen();
	auto res = result->Fetchall();
	result = nullptr;
	return res;
}

py::dict DuckDBPyRelation::FetchNumpy() {
	if (!result) {
		ExecuteOrThrow();
	}
	AssertResultOpen();
	auto res = result->FetchNumpy();
	result = nullptr;
	return res;
}

py::dict DuckDBPyRelation::FetchNumpyInternal(bool stream, idx_t vectors_per_chunk) {
	if (!result) {
		ExecuteOrThrow();
	}
	AssertResultOpen();
	auto res = result->FetchNumpyInternal(stream, vectors_per_chunk);
	result = nullptr;
	return res;
}

//! Should this also keep track of when the result is empty and set result->result_closed accordingly?
DataFrame DuckDBPyRelation::FetchDFChunk(idx_t vectors_per_chunk, bool date_as_object) {
	if (!result) {
		ExecuteOrThrow();
	}
	AssertResultOpen();
	return result->FetchDFChunk(vectors_per_chunk, date_as_object);
}

duckdb::pyarrow::Table DuckDBPyRelation::ToArrowTable(idx_t batch_size) {
	if (!result) {
		ExecuteOrThrow();
	}
	AssertResultOpen();
	auto res = result->FetchArrowTable(batch_size);
	result = nullptr;
	return res;
}

duckdb::pyarrow::RecordBatchReader DuckDBPyRelation::ToRecordBatch(idx_t batch_size) {
	if (!result) {
		ExecuteOrThrow();
	}
	AssertResultOpen();
	return result->FetchRecordBatchReader(batch_size);
}

void DuckDBPyRelation::Close() {
	if (!result) {
		ExecuteOrThrow();
	}
	AssertResultOpen();
	result->Close();
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
		throw InvalidInputException("Unsupported join type %s try 'inner' or 'left'", type_string);
	}
	return make_unique<DuckDBPyRelation>(rel->Join(other->rel, condition, dtype));
}

void DuckDBPyRelation::WriteCsv(const string &file) {
	rel->WriteCSV(file);
}

void DuckDBPyRelation::WriteCsvDF(const DataFrame &df, const string &file, shared_ptr<DuckDBPyConnection> conn) {
	if (!conn) {
		conn = DuckDBPyConnection::DefaultConnection();
	}
	return conn->FromDF(df)->WriteCsv(file);
}

// should this return a rel with the new view?
unique_ptr<DuckDBPyRelation> DuckDBPyRelation::CreateView(const string &view_name, bool replace) {
	rel->CreateView(view_name, replace);
	// We need to pass ownership of any Python Object Dependencies to the connection
	auto all_dependencies = rel->GetAllDependencies();
	rel->context.GetContext()->external_dependencies[view_name] = std::move(all_dependencies);
	return make_unique<DuckDBPyRelation>(rel);
}

static bool IsDescribeStatement(SQLStatement &statement) {
	if (statement.type != StatementType::PRAGMA_STATEMENT) {
		return false;
	}
	auto &pragma_statement = (PragmaStatement &)statement;
	if (pragma_statement.info->name != "show") {
		return false;
	}
	return true;
}

unique_ptr<DuckDBPyRelation> DuckDBPyRelation::Query(const string &view_name, const string &sql_query) {
	auto view_relation = CreateView(view_name);
	auto all_dependencies = rel->GetAllDependencies();
	rel->context.GetContext()->external_dependencies[view_name] = std::move(all_dependencies);

	Parser parser(rel->context.GetContext()->GetParserOptions());
	parser.ParseQuery(sql_query);
	if (parser.statements.size() != 1) {
		throw InvalidInputException("'DuckDBPyRelation.query' only accepts a single statement");
	}
	auto &statement = *parser.statements[0];
	if (statement.type == StatementType::SELECT_STATEMENT) {
		auto select_statement = unique_ptr_cast<SQLStatement, SelectStatement>(std::move(parser.statements[0]));
		auto query_relation =
		    make_shared<QueryRelation>(rel->context.GetContext(), std::move(select_statement), "query_relation");
		return make_unique<DuckDBPyRelation>(std::move(query_relation));
	} else if (IsDescribeStatement(statement)) {
		FunctionParameters parameters;
		parameters.values.emplace_back(view_name);
		auto query = PragmaShow(*rel->context.GetContext(), parameters);
		return Query(view_name, query);
	}
	{
		py::gil_scoped_release release;
		auto query_result = rel->context.GetContext()->Query(std::move(parser.statements[0]), false);
		// Execute it anyways, for creation/altering statements
		// We only care that it succeeds, we can't store the result
		D_ASSERT(query_result);
		if (query_result->HasError()) {
			query_result->ThrowError();
		}
	}
	return nullptr;
}

DuckDBPyRelation &DuckDBPyRelation::Execute() {
	if (!rel) {
		throw InvalidInputException("This relation was created from a result");
	}
	ExecuteOrThrow();
	return *this;
}

unique_ptr<DuckDBPyRelation> DuckDBPyRelation::QueryDF(const DataFrame &df, const string &view_name,
                                                       const string &sql_query, shared_ptr<DuckDBPyConnection> conn) {
	if (!conn) {
		conn = DuckDBPyConnection::DefaultConnection();
	}
	return conn->FromDF(df)->Query(view_name, sql_query);
}

void DuckDBPyRelation::InsertInto(const string &table) {
	auto parsed_info = QualifiedName::Parse(table);
	if (parsed_info.schema.empty()) {
		//! No Schema Defined, we use default schema.
		rel->Insert(table);
	} else {
		//! Schema defined, we try to insert into it.
		rel->Insert(parsed_info.schema, parsed_info.name);
	}
}

static bool IsAcceptedInsertRelationType(const Relation &relation) {
	return relation.type == RelationType::TABLE_RELATION;
}

void DuckDBPyRelation::Insert(const py::object &params) {
	if (!IsAcceptedInsertRelationType(*this->rel)) {
		throw InvalidInputException("'DuckDBPyRelation.insert' can only be used on a table relation");
	}
	vector<vector<Value>> values {DuckDBPyConnection::TransformPythonParamList(params)};
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
	auto relation = make_unique<DuckDBPyRelation>(rel->TableFunction("python_map_function", params));
	relation->rel->extra_dependencies = make_unique<PythonDependencies>(fun);
	return relation;
}

string DuckDBPyRelation::Print() {
	std::string rel_res_string;
	{
		py::gil_scoped_release release;
		rel_res_string = rel->Limit(10)->Execute()->ToString();
	}

	return rel->ToString() + "\n---------------------\n-- Result Preview --\n---------------------\n" + rel_res_string +
	       "\n";
}

string DuckDBPyRelation::Explain() {
	return rel->ToString(0);
}

// TODO: RelationType to a python enum
py::str DuckDBPyRelation::Type() {
	return py::str(RelationTypeToString(rel->type));
}

py::list DuckDBPyRelation::Columns() {
	py::list res;
	for (auto &col : rel->Columns()) {
		res.append(col.Name());
	}
	return res;
}

py::list DuckDBPyRelation::ColumnTypes() {
	py::list res;
	for (auto &col : rel->Columns()) {
		res.append(col.Type().ToString());
	}
	return res;
}

} // namespace duckdb
