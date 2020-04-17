// this file is used to instantiate symbols for LLDB so e.g.
// std::vector and std::unique_ptr can be accessed from the debugger

#ifdef DEBUG

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/list.hpp"
#include "duckdb/common/types/chunk_collection.hpp"
#include "duckdb/execution/aggregate_hashtable.hpp"
#include "duckdb/execution/column_binding_resolver.hpp"
#include "duckdb/execution/join_hashtable.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/main/materialized_query_result.hpp"
#include "duckdb/main/query_profiler.hpp"
#include "duckdb/main/query_result.hpp"
#include "duckdb/main/stream_query_result.hpp"
#include "duckdb/main/relation.hpp"
#include "duckdb/optimizer/join_order_optimizer.hpp"
#include "duckdb/optimizer/rule.hpp"
#include "duckdb/parser/constraint.hpp"
#include "duckdb/parser/constraints/list.hpp"
#include "duckdb/parser/expression/list.hpp"
#include "duckdb/parser/query_node.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/query_node/set_operation_node.hpp"
#include "duckdb/parser/statement/list.hpp"
#include "duckdb/planner/expression/list.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/planner/operator/list.hpp"
#include "duckdb/planner/operator/logical_join.hpp"
#include "duckdb/planner/query_node/bound_select_node.hpp"
#include "duckdb/planner/query_node/bound_set_operation_node.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/storage/write_ahead_log.hpp"
#include "duckdb/transaction/transaction.hpp"
#include "duckdb/parser/tableref/list.hpp"

using namespace duckdb;
using namespace std;
template class std::unique_ptr<SQLStatement>;
template class std::unique_ptr<AlterTableStatement>;
template class std::unique_ptr<CopyStatement>;
template class std::unique_ptr<CreateStatement>;
template class std::unique_ptr<DeleteStatement>;
template class std::unique_ptr<DropStatement>;
template class std::unique_ptr<InsertStatement>;
template class std::unique_ptr<SelectStatement>;
template class std::unique_ptr<TransactionStatement>;
template class std::unique_ptr<UpdateStatement>;
template class std::unique_ptr<PrepareStatement>;
template class std::unique_ptr<ExecuteStatement>;
template class std::unique_ptr<QueryNode>;
template class std::unique_ptr<SelectNode>;
template class std::unique_ptr<SetOperationNode>;
template class std::unique_ptr<ParsedExpression>;
template class std::unique_ptr<CaseExpression>;
template class std::unique_ptr<CastExpression>;
template class std::unique_ptr<ColumnRefExpression>;
template class std::unique_ptr<ComparisonExpression>;
template class std::unique_ptr<ConjunctionExpression>;
template class std::unique_ptr<ConstantExpression>;
template class std::unique_ptr<DefaultExpression>;
template class std::unique_ptr<FunctionExpression>;
template class std::unique_ptr<OperatorExpression>;
template class std::unique_ptr<ParameterExpression>;
template class std::unique_ptr<PreparedStatementData>;
template class std::unique_ptr<StarExpression>;
template class std::unique_ptr<SubqueryExpression>;
template class std::unique_ptr<WindowExpression>;
template class std::unique_ptr<Constraint>;
template class std::unique_ptr<NotNullConstraint>;
template class std::unique_ptr<CheckConstraint>;
template class std::unique_ptr<UniqueConstraint>;
// template class std::unique_ptr<TableRef>;
template class std::unique_ptr<BaseTableRef>;
template class std::unique_ptr<CrossProductRef>;
template class std::unique_ptr<JoinRef>;
template class std::unique_ptr<SubqueryRef>;
template class std::unique_ptr<TableFunctionRef>;

template class std::unique_ptr<Expression>;
template class std::unique_ptr<BoundQueryNode>;
template class std::unique_ptr<BoundSelectNode>;
template class std::unique_ptr<BoundSetOperationNode>;
template class std::unique_ptr<BoundAggregateExpression>;
template class std::unique_ptr<BoundCaseExpression>;
template class std::unique_ptr<BoundCastExpression>;
template class std::unique_ptr<BoundColumnRefExpression>;
template class std::unique_ptr<BoundComparisonExpression>;
template class std::unique_ptr<BoundConjunctionExpression>;
template class std::unique_ptr<BoundConstantExpression>;
template class std::unique_ptr<BoundDefaultExpression>;
template class std::unique_ptr<BoundFunctionExpression>;
template class std::unique_ptr<BoundOperatorExpression>;
template class std::unique_ptr<BoundParameterExpression>;
template class std::unique_ptr<BoundReferenceExpression>;
template class std::unique_ptr<BoundSubqueryExpression>;
template class std::unique_ptr<BoundWindowExpression>;
template class std::unique_ptr<CommonSubExpression>;

template class std::unique_ptr<CatalogEntry>;
template class std::unique_ptr<BindContext>;
template class std::unique_ptr<char[]>;
template class std::unique_ptr<QueryResult>;
template class std::unique_ptr<MaterializedQueryResult>;
template class std::unique_ptr<StreamQueryResult>;
template class std::unique_ptr<LogicalOperator>;
template class std::unique_ptr<PhysicalOperator>;
template class std::unique_ptr<PhysicalOperatorState>;
template class std::unique_ptr<sel_t[]>;
template class std::unique_ptr<StringHeap>;
template class std::unique_ptr<SuperLargeHashTable>;
template class std::unique_ptr<TableRef>;
template class std::unique_ptr<Transaction>;
template class std::unique_ptr<uint64_t[]>;
template class std::unique_ptr<data_t[]>;
template class std::unique_ptr<Vector[]>;
template class std::unique_ptr<DataChunk>;
template class std::unique_ptr<JoinHashTable>;
template class std::unique_ptr<JoinHashTable::ScanStructure>;
template class std::unique_ptr<data_ptr_t[]>;
template class std::unique_ptr<Rule>;
template class std::unique_ptr<LogicalFilter>;
template class std::unique_ptr<LogicalJoin>;
template class std::unique_ptr<LogicalComparisonJoin>;
template class std::unique_ptr<FilterInfo>;
template class std::unique_ptr<JoinOrderOptimizer::JoinNode>;
template class std::unique_ptr<SingleJoinRelation>;
template class std::shared_ptr<Relation>;
template class std::unique_ptr<CatalogSet>;
template class std::unique_ptr<PreparedStatementCatalogEntry>;
template class std::unique_ptr<Binder>;

#define INSTANTIATE_VECTOR(VECTOR_DEFINITION)                                                                          \
	template VECTOR_DEFINITION::size_type VECTOR_DEFINITION::size() const;                                             \
	template VECTOR_DEFINITION::const_reference VECTOR_DEFINITION::operator[](VECTOR_DEFINITION::size_type n) const;   \
	template VECTOR_DEFINITION::reference VECTOR_DEFINITION::operator[](VECTOR_DEFINITION::size_type n);               \
	template VECTOR_DEFINITION::const_reference VECTOR_DEFINITION::back() const;                                       \
	template VECTOR_DEFINITION::reference VECTOR_DEFINITION::back();                                                   \
	template VECTOR_DEFINITION::const_reference VECTOR_DEFINITION::front() const;                                      \
	template VECTOR_DEFINITION::reference VECTOR_DEFINITION::front();

INSTANTIATE_VECTOR(std::vector<ColumnDefinition>);
template class std::vector<ExpressionType>;
INSTANTIATE_VECTOR(std::vector<JoinCondition>);
INSTANTIATE_VECTOR(std::vector<OrderByNode>);
template class std::vector<uint64_t>;
template class std::vector<string>;
INSTANTIATE_VECTOR(std::vector<Expression *>)
INSTANTIATE_VECTOR(std::vector<std::unique_ptr<Expression>>)
INSTANTIATE_VECTOR(std::vector<std::unique_ptr<DataChunk>>);
INSTANTIATE_VECTOR(std::vector<std::unique_ptr<SQLStatement>>);
INSTANTIATE_VECTOR(std::vector<std::unique_ptr<PhysicalOperator>>);
INSTANTIATE_VECTOR(std::vector<std::unique_ptr<LogicalOperator>>);
INSTANTIATE_VECTOR(std::vector<std::unique_ptr<Transaction>>);
INSTANTIATE_VECTOR(std::vector<std::unique_ptr<JoinOrderOptimizer::JoinNode>>);
template class std::vector<TypeId>;
template class std::vector<Value>;
template class std::vector<int>;
INSTANTIATE_VECTOR(std::vector<std::unique_ptr<Rule>>);
template class std::vector<std::vector<Expression *>>;
template class std::vector<SQLType>;

template struct std::atomic<uint64_t>;
template class std::bitset<STANDARD_VECTOR_SIZE>;
template class std::unordered_map<PhysicalOperator *, QueryProfiler::TreeNode *>;
template class std::stack<PhysicalOperator *>;

#define INSTANTIATE_UNORDERED_MAP(MAP_DEFINITION)                                                                      \
	template MAP_DEFINITION::mapped_type &MAP_DEFINITION::operator[](MAP_DEFINITION::key_type &&k);                    \
	template MAP_DEFINITION::mapped_type &MAP_DEFINITION::operator[](const MAP_DEFINITION::key_type &k);

using catalog_map = std::unordered_map<string, unique_ptr<CatalogEntry>>;
INSTANTIATE_UNORDERED_MAP(catalog_map);

template class std::unordered_map<string, uint64_t>;
template class std::unordered_map<string, std::vector<string>>;
template class std::unordered_map<string, std::pair<uint64_t, Expression *>>;
// template class std::unordered_map<string, TableBinding>;
template class std::unordered_map<string, SelectStatement *>;
template class std::unordered_map<uint64_t, uint64_t>;

#endif
