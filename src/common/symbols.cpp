// this file is used to instantiate symbols for LLDB so e.g.
// vector and duckdb::unique_ptr can be accessed from the debugger

#ifdef DEBUG

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/list.hpp"
#include "duckdb/common/types/chunk_collection.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/execution/aggregate_hashtable.hpp"
#include "duckdb/execution/column_binding_resolver.hpp"
#include "duckdb/execution/join_hashtable.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/main/materialized_query_result.hpp"
#include "duckdb/main/query_profiler.hpp"
#include "duckdb/main/query_result.hpp"
#include "duckdb/main/relation.hpp"
#include "duckdb/main/stream_query_result.hpp"
#include "duckdb/optimizer/join_order/join_order_optimizer.hpp"
#include "duckdb/optimizer/rule.hpp"
#include "duckdb/parallel/pipeline.hpp"
#include "duckdb/parallel/meta_pipeline.hpp"
#include "duckdb/parser/constraint.hpp"
#include "duckdb/parser/constraints/list.hpp"
#include "duckdb/parser/expression/list.hpp"
#include "duckdb/parser/query_node.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/query_node/set_operation_node.hpp"
#include "duckdb/parser/statement/list.hpp"
#include "duckdb/parser/tableref/list.hpp"
#include "duckdb/planner/expression/list.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/planner/operator/list.hpp"
#include "duckdb/planner/operator/logical_join.hpp"
#include "duckdb/planner/query_node/bound_select_node.hpp"
#include "duckdb/planner/query_node/bound_set_operation_node.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/storage/write_ahead_log.hpp"
#include "duckdb/transaction/transaction.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/common/types/column/column_data_allocator.hpp"
#include "duckdb/common/vector.hpp"

using namespace duckdb;

namespace duckdb {

template class duckdb::unique_ptr<SQLStatement>;
template class duckdb::unique_ptr<AlterStatement>;
template class duckdb::unique_ptr<CopyStatement>;
template class duckdb::unique_ptr<CreateStatement>;
template class duckdb::unique_ptr<DeleteStatement>;
template class duckdb::unique_ptr<DropStatement>;
template class duckdb::unique_ptr<InsertStatement>;
template class duckdb::unique_ptr<SelectStatement>;
template class duckdb::unique_ptr<TransactionStatement>;
template class duckdb::unique_ptr<UpdateStatement>;
template class duckdb::unique_ptr<PrepareStatement>;
template class duckdb::unique_ptr<ExecuteStatement>;
template class duckdb::unique_ptr<VacuumStatement>;
template class duckdb::unique_ptr<QueryNode>;
template class duckdb::unique_ptr<SelectNode>;
template class duckdb::unique_ptr<SetOperationNode>;
template class duckdb::unique_ptr<ParsedExpression>;
template class duckdb::unique_ptr<CaseExpression>;
template class duckdb::unique_ptr<CastExpression>;
template class duckdb::unique_ptr<ColumnRefExpression>;
template class duckdb::unique_ptr<ComparisonExpression>;
template class duckdb::unique_ptr<ConjunctionExpression>;
template class duckdb::unique_ptr<ConstantExpression>;
template class duckdb::unique_ptr<DefaultExpression>;
template class duckdb::unique_ptr<FunctionExpression>;
template class duckdb::unique_ptr<OperatorExpression>;
template class duckdb::unique_ptr<ParameterExpression>;
template class duckdb::unique_ptr<StarExpression>;
template class duckdb::unique_ptr<SubqueryExpression>;
template class duckdb::unique_ptr<WindowExpression>;
template class duckdb::unique_ptr<Constraint>;
template class duckdb::unique_ptr<NotNullConstraint>;
template class duckdb::unique_ptr<CheckConstraint>;
template class duckdb::unique_ptr<UniqueConstraint>;
template class duckdb::unique_ptr<ForeignKeyConstraint>;
// template class duckdb::unique_ptr<TableRef>;
template class duckdb::unique_ptr<BaseTableRef>;
template class duckdb::unique_ptr<JoinRef>;
template class duckdb::unique_ptr<SubqueryRef>;
template class duckdb::unique_ptr<TableFunctionRef>;
template class duckdb::unique_ptr<Pipeline>;
template class duckdb::unique_ptr<RowGroup>;
template class duckdb::unique_ptr<RowDataBlock>;
template class duckdb::unique_ptr<RowDataCollection>;
template class duckdb::unique_ptr<ColumnDataCollection>;
template class duckdb::unique_ptr<PartitionedColumnData>;
template class duckdb::unique_ptr<VacuumInfo>;

template class duckdb::unique_ptr<Expression>;
template class duckdb::unique_ptr<BoundQueryNode>;
template class duckdb::unique_ptr<BoundSelectNode>;
template class duckdb::unique_ptr<BoundSetOperationNode>;
template class duckdb::unique_ptr<BoundAggregateExpression>;
template class duckdb::unique_ptr<BoundCaseExpression>;
template class duckdb::unique_ptr<BoundCastExpression>;
template class duckdb::unique_ptr<BoundColumnRefExpression>;
template class duckdb::unique_ptr<BoundComparisonExpression>;
template class duckdb::unique_ptr<BoundConjunctionExpression>;
template class duckdb::unique_ptr<BoundConstantExpression>;
template class duckdb::unique_ptr<BoundDefaultExpression>;
template class duckdb::unique_ptr<BoundFunctionExpression>;
template class duckdb::unique_ptr<BoundOperatorExpression>;
template class duckdb::unique_ptr<BoundParameterExpression>;
template class duckdb::unique_ptr<BoundReferenceExpression>;
template class duckdb::unique_ptr<BoundSubqueryExpression>;
template class duckdb::unique_ptr<BoundWindowExpression>;
template class duckdb::unique_ptr<BoundBaseTableRef>;

template class duckdb::unique_ptr<CatalogEntry>;
template class duckdb::unique_ptr<BindContext>;
template class duckdb::unique_ptr<char[]>;
template class duckdb::unique_ptr<QueryResult>;
template class duckdb::unique_ptr<MaterializedQueryResult>;
template class duckdb::unique_ptr<StreamQueryResult>;
template class duckdb::unique_ptr<LogicalOperator>;
template class duckdb::unique_ptr<PhysicalOperator>;
template class duckdb::unique_ptr<OperatorState>;
template class duckdb::unique_ptr<sel_t[]>;
template class duckdb::unique_ptr<StringHeap>;
template class duckdb::unique_ptr<GroupedAggregateHashTable>;
template class duckdb::unique_ptr<TableRef>;
template class duckdb::unique_ptr<Transaction>;
template class duckdb::unique_ptr<uint64_t[]>;
template class duckdb::unique_ptr<data_t[]>;
template class duckdb::unique_ptr<Vector[]>;
template class duckdb::unique_ptr<DataChunk>;
template class duckdb::unique_ptr<JoinHashTable>;
template class duckdb::unique_ptr<JoinHashTable::ScanStructure>;
template class duckdb::unique_ptr<JoinHashTable::ProbeSpill>;
template class duckdb::unique_ptr<data_ptr_t[]>;
template class duckdb::unique_ptr<Rule>;
template class duckdb::unique_ptr<LogicalFilter>;
template class duckdb::unique_ptr<LogicalJoin>;
template class duckdb::unique_ptr<LogicalComparisonJoin>;
template class duckdb::unique_ptr<FilterInfo>;
template class duckdb::unique_ptr<JoinNode>;
template class duckdb::unique_ptr<SingleJoinRelation>;
template class duckdb::unique_ptr<CatalogSet>;
template class duckdb::unique_ptr<Binder>;
template class duckdb::unique_ptr<PrivateAllocatorData>;

} // namespace duckdb

#define INSTANTIATE_VECTOR(VECTOR_DEFINITION)                                                                          \
	template std::VECTOR_DEFINITION::size_type std::VECTOR_DEFINITION::size() const;                                   \
	template std::VECTOR_DEFINITION::const_reference std::VECTOR_DEFINITION::operator[](                               \
	    std::VECTOR_DEFINITION::size_type n) const;                                                                    \
	template std::VECTOR_DEFINITION::reference std::VECTOR_DEFINITION::operator[](                                     \
	    std::VECTOR_DEFINITION::size_type n);                                                                          \
	template std::VECTOR_DEFINITION::const_reference std::VECTOR_DEFINITION::back() const;                             \
	template std::VECTOR_DEFINITION::reference std::VECTOR_DEFINITION::back();                                         \
	template std::VECTOR_DEFINITION::const_reference std::VECTOR_DEFINITION::front() const;                            \
	template std::VECTOR_DEFINITION::reference std::VECTOR_DEFINITION::front();

template class duckdb::vector<ExpressionType>;
template class duckdb::vector<uint64_t>;
template class duckdb::vector<string>;
template class duckdb::vector<PhysicalType>;
template class duckdb::vector<Value>;
template class duckdb::vector<int>;
template class duckdb::vector<duckdb::vector<Expression *>>;
template class duckdb::vector<LogicalType>;

INSTANTIATE_VECTOR(vector<ColumnDefinition>)
INSTANTIATE_VECTOR(vector<JoinCondition>)
INSTANTIATE_VECTOR(vector<OrderByNode>)
INSTANTIATE_VECTOR(vector<Expression *>)
INSTANTIATE_VECTOR(vector<BoundParameterExpression *>)
INSTANTIATE_VECTOR(vector<duckdb::unique_ptr<Expression>>)
INSTANTIATE_VECTOR(vector<duckdb::unique_ptr<DataChunk>>)
INSTANTIATE_VECTOR(vector<duckdb::unique_ptr<SQLStatement>>)
INSTANTIATE_VECTOR(vector<duckdb::unique_ptr<PhysicalOperator>>)
INSTANTIATE_VECTOR(vector<duckdb::unique_ptr<LogicalOperator>>)
INSTANTIATE_VECTOR(vector<duckdb::unique_ptr<Transaction>>)
INSTANTIATE_VECTOR(vector<duckdb::unique_ptr<JoinNode>>)
INSTANTIATE_VECTOR(vector<duckdb::unique_ptr<Rule>>)
INSTANTIATE_VECTOR(vector<std::shared_ptr<Event>>)
INSTANTIATE_VECTOR(vector<duckdb::unique_ptr<Pipeline>>)
INSTANTIATE_VECTOR(vector<std::shared_ptr<Pipeline>>)
INSTANTIATE_VECTOR(vector<std::weak_ptr<Pipeline>>)
INSTANTIATE_VECTOR(vector<std::shared_ptr<MetaPipeline>>)
INSTANTIATE_VECTOR(vector<duckdb::unique_ptr<JoinHashTable>>)
INSTANTIATE_VECTOR(vector<duckdb::unique_ptr<ColumnDataCollection>>)
INSTANTIATE_VECTOR(vector<std::shared_ptr<ColumnDataAllocator>>)
INSTANTIATE_VECTOR(vector<duckdb::unique_ptr<RowDataBlock>>)

template class std::shared_ptr<Relation>;
template class std::shared_ptr<Event>;
template class std::shared_ptr<Pipeline>;
template class std::shared_ptr<MetaPipeline>;
template class std::shared_ptr<RowGroupCollection>;
template class std::shared_ptr<ColumnDataAllocator>;
template class std::shared_ptr<PreparedStatementData>;
template class std::weak_ptr<Pipeline>;

#if !defined(__clang__)
template struct std::atomic<uint64_t>;
#endif

template class std::bitset<STANDARD_VECTOR_SIZE>;
template class std::unordered_map<PhysicalOperator *, QueryProfiler::TreeNode *>;
template class std::stack<PhysicalOperator *>;

/* -pedantic does not like this
#define INSTANTIATE_UNORDERED_MAP(MAP_DEFINITION)                                                                      \
    template MAP_DEFINITION::mapped_type &MAP_DEFINITION::operator[](MAP_DEFINITION::key_type &&k);                    \
    template MAP_DEFINITION::mapped_type &MAP_DEFINITION::operator[](const MAP_DEFINITION::key_type &k);

using catalog_map = std::unordered_map<string, unique_ptr<CatalogEntry>>;
INSTANTIATE_UNORDERED_MAP(catalog_map)
*/

template class std::unordered_map<string, uint64_t>;
template class std::unordered_map<string, duckdb::vector<string>>;
template class std::unordered_map<string, std::pair<uint64_t, Expression *>>;
// template class std::unordered_map<string, TableBinding>;
template class std::unordered_map<string, SelectStatement *>;
template class std::unordered_map<uint64_t, uint64_t>;

#endif
