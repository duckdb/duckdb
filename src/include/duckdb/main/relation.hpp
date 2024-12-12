//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/relation.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/enums/join_type.hpp"
#include "duckdb/common/enums/relation_type.hpp"
#include "duckdb/common/winapi.hpp"
#include "duckdb/common/enums/joinref_type.hpp"
#include "duckdb/main/query_result.hpp"
#include "duckdb/parser/column_definition.hpp"
#include "duckdb/common/named_parameter_map.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/client_context_wrapper.hpp"
#include "duckdb/main/external_dependencies.hpp"
#include "duckdb/parser/statement/explain_statement.hpp"
#include "duckdb/parser/parsed_expression.hpp"
#include "duckdb/parser/result_modifier.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/helper.hpp"

namespace duckdb {
struct BoundStatement;

class Binder;
class LogicalOperator;
class QueryNode;
class TableRef;

static string CreateRelationAlias(RelationType type, const string &alias) {
	if (!alias.empty()) {
		return alias;
	}
	return StringUtil::Format("%s_%s", EnumUtil::ToString(type), StringUtil::GenerateRandomName());
}

class RelationContextWrapper : public ClientContextWrapper {
public:
	~RelationContextWrapper() override = default;
	explicit RelationContextWrapper(const shared_ptr<ClientContext> &context) : ClientContextWrapper(context) {};

	explicit RelationContextWrapper(const ClientContextWrapper &context) : ClientContextWrapper(context) {};

	void TryBindRelation(Relation &relation, vector<ColumnDefinition> &columns) override {
		GetContext()->InternalTryBindRelation(relation, columns);
	}

private:
	weak_ptr<ClientContext> client_context;
};

class Relation : public enable_shared_from_this<Relation> {
public:
	Relation(const shared_ptr<ClientContext> &context_p, const RelationType type) : type(type) {
		context = make_shared_ptr<ClientContextWrapper>(context_p);
	}
	Relation(const shared_ptr<ClientContextWrapper> &context, RelationType type, const string &alias_p = "")
	    : context(context), type(type), alias(CreateRelationAlias(type, alias_p)) {
	}

	Relation(const shared_ptr<RelationContextWrapper> &context, RelationType type, const string &alias_p = "")
	    : context(context), type(type), alias(CreateRelationAlias(type, alias_p)) {
	}

	virtual ~Relation() = default;

	shared_ptr<ClientContextWrapper> context;
	RelationType type;
	const string alias;
	vector<shared_ptr<ExternalDependency>> external_dependencies;

public:
	DUCKDB_API virtual const vector<ColumnDefinition> &Columns() = 0;
	DUCKDB_API virtual unique_ptr<QueryNode> GetQueryNode();
	DUCKDB_API virtual BoundStatement Bind(Binder &binder);
	DUCKDB_API virtual string GetAlias();

	DUCKDB_API unique_ptr<QueryResult> ExecuteOrThrow();
	DUCKDB_API unique_ptr<QueryResult> Execute();
	DUCKDB_API string ToString();
	DUCKDB_API virtual string ToString(idx_t depth) = 0;

	DUCKDB_API void Print();
	DUCKDB_API void Head(idx_t limit = 10);

	DUCKDB_API shared_ptr<Relation> CreateView(const string &name, bool replace = true, bool temporary = false);
	DUCKDB_API shared_ptr<Relation> CreateView(const string &schema_name, const string &name, bool replace = true,
	                                           bool temporary = false);
	DUCKDB_API unique_ptr<QueryResult> Query(const string &sql) const;
	DUCKDB_API unique_ptr<QueryResult> Query(const string &name, const string &sql);

	//! Explain the query plan of this relation
	DUCKDB_API unique_ptr<QueryResult> Explain(ExplainType type = ExplainType::EXPLAIN_STANDARD,
	                                           ExplainFormat explain_format = ExplainFormat::DEFAULT);

	DUCKDB_API virtual unique_ptr<TableRef> GetTableRef();
	virtual bool IsReadOnly() {
		return true;
	}
	DUCKDB_API void TryBindRelation(vector<ColumnDefinition> &columns);

public:
	// PROJECT
	DUCKDB_API shared_ptr<Relation> Project(const string &select_list);
	DUCKDB_API shared_ptr<Relation> Project(const string &expression, const string &alias);
	DUCKDB_API shared_ptr<Relation> Project(const string &select_list, const vector<string> &aliases);
	DUCKDB_API shared_ptr<Relation> Project(const vector<string> &expressions);
	DUCKDB_API shared_ptr<Relation> Project(const vector<string> &expressions, const vector<string> &aliases);
	DUCKDB_API shared_ptr<Relation> Project(vector<unique_ptr<ParsedExpression>> expressions,
	                                        const vector<string> &aliases);

	// FILTER
	DUCKDB_API shared_ptr<Relation> Filter(const string &expression);
	DUCKDB_API shared_ptr<Relation> Filter(unique_ptr<ParsedExpression> expression);
	DUCKDB_API shared_ptr<Relation> Filter(const vector<string> &expressions);

	// LIMIT
	DUCKDB_API shared_ptr<Relation> Limit(int64_t n, int64_t offset = 0);

	// ORDER
	DUCKDB_API shared_ptr<Relation> Order(const string &expression);
	DUCKDB_API shared_ptr<Relation> Order(const vector<string> &expressions);
	DUCKDB_API shared_ptr<Relation> Order(vector<OrderByNode> expressions);

	// JOIN operation
	DUCKDB_API shared_ptr<Relation> Join(const shared_ptr<Relation> &other, const string &condition,
	                                     JoinType type = JoinType::INNER, JoinRefType ref_type = JoinRefType::REGULAR);
	shared_ptr<Relation> Join(const shared_ptr<Relation> &other, vector<unique_ptr<ParsedExpression>> condition,
	                          JoinType type = JoinType::INNER, JoinRefType ref_type = JoinRefType::REGULAR);

	// CROSS PRODUCT operation
	DUCKDB_API shared_ptr<Relation> CrossProduct(const shared_ptr<Relation> &other,
	                                             JoinRefType join_ref_type = JoinRefType::CROSS);

	// SET operations
	DUCKDB_API shared_ptr<Relation> Union(const shared_ptr<Relation> &other);
	DUCKDB_API shared_ptr<Relation> Except(const shared_ptr<Relation> &other);
	DUCKDB_API shared_ptr<Relation> Intersect(const shared_ptr<Relation> &other);

	// DISTINCT operation
	DUCKDB_API shared_ptr<Relation> Distinct();

	// AGGREGATES
	DUCKDB_API shared_ptr<Relation> Aggregate(const string &aggregate_list);
	DUCKDB_API shared_ptr<Relation> Aggregate(const vector<string> &aggregates);
	DUCKDB_API shared_ptr<Relation> Aggregate(vector<unique_ptr<ParsedExpression>> expressions);
	DUCKDB_API shared_ptr<Relation> Aggregate(const string &aggregate_list, const string &group_list);
	DUCKDB_API shared_ptr<Relation> Aggregate(const vector<string> &aggregates, const vector<string> &groups);
	DUCKDB_API shared_ptr<Relation> Aggregate(vector<unique_ptr<ParsedExpression>> expressions,
	                                          const string &group_list);

	// ALIAS
	DUCKDB_API shared_ptr<Relation> Alias(const string &alias);

	//! Insert the data from this relation into a table
	DUCKDB_API shared_ptr<Relation> InsertRel(const string &schema_name, const string &table_name);
	DUCKDB_API void Insert(const string &table_name);
	DUCKDB_API void Insert(const string &schema_name, const string &table_name);
	//! Insert a row (i.e.,list of values) into a table
	DUCKDB_API void Insert(const vector<vector<Value>> &values);
	DUCKDB_API void Insert(vector<vector<unique_ptr<ParsedExpression>>> &&expressions);
	//! Create a table and insert the data from this relation into that table
	DUCKDB_API shared_ptr<Relation> CreateRel(const string &schema_name, const string &table_name,
	                                          bool temporary = false);
	DUCKDB_API void Create(const string &table_name, bool temporary = false);
	DUCKDB_API void Create(const string &schema_name, const string &table_name, bool temporary = false);

	//! Write a relation to a CSV file
	DUCKDB_API shared_ptr<Relation>
	WriteCSVRel(const string &csv_file,
	            case_insensitive_map_t<vector<Value>> options = case_insensitive_map_t<vector<Value>>());
	DUCKDB_API void WriteCSV(const string &csv_file,
	                         case_insensitive_map_t<vector<Value>> options = case_insensitive_map_t<vector<Value>>());
	//! Write a relation to a Parquet file
	DUCKDB_API shared_ptr<Relation>
	WriteParquetRel(const string &parquet_file,
	                case_insensitive_map_t<vector<Value>> options = case_insensitive_map_t<vector<Value>>());
	DUCKDB_API void
	WriteParquet(const string &parquet_file,
	             case_insensitive_map_t<vector<Value>> options = case_insensitive_map_t<vector<Value>>());

	//! Update a table, can only be used on a TableRelation
	DUCKDB_API virtual void Update(const string &update, const string &condition = string());
	DUCKDB_API virtual void Update(vector<string> column_names, vector<unique_ptr<ParsedExpression>> &&update,
	                               unique_ptr<ParsedExpression> condition = nullptr);
	//! Delete from a table, can only be used on a TableRelation
	DUCKDB_API virtual void Delete(const string &condition = string());
	//! Create a relation from calling a table in/out function on the input relation
	//! Create a relation from calling a table in/out function on the input relation
	DUCKDB_API shared_ptr<Relation> TableFunction(const std::string &fname, const vector<Value> &values);
	DUCKDB_API shared_ptr<Relation> TableFunction(const std::string &fname, const vector<Value> &values,
	                                              const named_parameter_map_t &named_parameters);

public:
	//! Whether or not the relation inherits column bindings from its child or not, only relevant for binding
	virtual bool InheritsColumnBindings() {
		return false;
	}
	virtual Relation *ChildRelation() {
		return nullptr;
	}
	void AddExternalDependency(shared_ptr<ExternalDependency> dependency);
	DUCKDB_API vector<shared_ptr<ExternalDependency>> GetAllDependencies();

protected:
	DUCKDB_API static string RenderWhitespace(idx_t depth);

public:
	template <class TARGET>
	TARGET &Cast() {
		DynamicCastCheck<TARGET>(this);
		return reinterpret_cast<TARGET &>(*this);
	}
	template <class TARGET>
	const TARGET &Cast() const {
		DynamicCastCheck<TARGET>(this);
		return reinterpret_cast<const TARGET &>(*this);
	}
};

} // namespace duckdb
