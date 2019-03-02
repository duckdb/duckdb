// this file is used to instantiate symbols for LLDB so e.g.
// std::vector and std::unique_ptr can be accessed from the debugger

#ifdef DEBUG

#include "catalog/catalog.hpp"
#include "common/types/chunk_collection.hpp"
#include "execution/aggregate_hashtable.hpp"
#include "execution/column_binding_resolver.hpp"
#include "execution/join_hashtable.hpp"
#include "execution/physical_operator.hpp"
#include "main/query_profiler.hpp"
#include "main/result.hpp"
#include "optimizer/join_order_optimizer.hpp"
#include "optimizer/rule.hpp"
#include "parser/constraint.hpp"
#include "parser/query_node.hpp"
#include "parser/query_node/select_node.hpp"
#include "parser/tableref/list.hpp"
#include "planner/logical_operator.hpp"
#include "planner/operator/list.hpp"
#include "planner/operator/logical_join.hpp"
#include "storage/data_table.hpp"
#include "storage/write_ahead_log.hpp"

using namespace duckdb;
using namespace std;

template class std::unique_ptr<CatalogEntry>;
template class std::unique_ptr<Expression>;
template class std::unique_ptr<BindContext>;
template class std::unique_ptr<char[]>;
template class std::unique_ptr<DuckDBResult>;
template class std::unique_ptr<LogicalOperator>;
template class std::unique_ptr<PhysicalOperator>;
template class std::unique_ptr<PhysicalOperatorState>;
template class std::unique_ptr<sel_t[]>;
template class std::unique_ptr<SQLStatement>;
template class std::unique_ptr<StorageChunk>;
template class std::unique_ptr<StringHeap>;
template class std::unique_ptr<QueryNode>;
template class std::unique_ptr<SuperLargeHashTable>;
template class std::unique_ptr<TableRef>;
template class std::unique_ptr<Transaction>;
template class std::unique_ptr<uint64_t[]>;
template class std::unique_ptr<uint8_t[]>;
template class std::unique_ptr<Vector[]>;
template class std::unique_ptr<DataChunk>;
template class std::unique_ptr<ExpressionStatistics[]>;
template class std::unique_ptr<Constraint>;
template class std::unique_ptr<SelectStatement>;
template class std::unique_ptr<JoinHashTable>;
template class std::unique_ptr<JoinHashTable::ScanStructure>;
template class std::unique_ptr<JoinHashTable::Node>;
template class std::unique_ptr<uint8_t *[]>;
template class std::unique_ptr<Rule>;
template class std::unique_ptr<LogicalFilter>;
template class std::unique_ptr<LogicalJoin>;
template class std::unique_ptr<LogicalComparisonJoin>;
template class std::unique_ptr<SubqueryRef>;
template class std::unique_ptr<WindowExpression>;
template class std::unique_ptr<CreateViewInformation>;
template class std::unique_ptr<FilterInfo>;
template class std::unique_ptr<JoinOrderOptimizer::JoinNode>;
template class std::unique_ptr<Relation>;
template class std::unique_ptr<AggregateExpression>;
template class std::unique_ptr<CaseExpression>;
template class std::unique_ptr<CatalogSet>;
template class std::unique_ptr<PreparedStatementCatalogEntry>;
template class std::unique_ptr<CastExpression>;
template class std::unique_ptr<ColumnRefExpression>;
template class std::unique_ptr<Binder>;

#define INSTANTIATE_VECTOR(VECTOR_DEFINITION)                                                                          \
	template VECTOR_DEFINITION::size_type VECTOR_DEFINITION::size() const;                                             \
	template VECTOR_DEFINITION::const_reference VECTOR_DEFINITION::operator[](VECTOR_DEFINITION::size_type n) const;   \
	template VECTOR_DEFINITION::reference VECTOR_DEFINITION::operator[](VECTOR_DEFINITION::size_type n);               \
	template VECTOR_DEFINITION::const_reference VECTOR_DEFINITION::back() const;                                       \
	template VECTOR_DEFINITION::reference VECTOR_DEFINITION::back();                                                   \
	template VECTOR_DEFINITION::const_reference VECTOR_DEFINITION::front() const;                                      \
	template VECTOR_DEFINITION::reference VECTOR_DEFINITION::front();

template class std::vector<AggregateExpression *>;
template class std::vector<BoundTable>;
INSTANTIATE_VECTOR(std::vector<ColumnDefinition>);
template class std::vector<ExpressionType>;
INSTANTIATE_VECTOR(std::vector<JoinCondition>);
INSTANTIATE_VECTOR(std::vector<OrderByNode>);
template class std::vector<size_t>;
INSTANTIATE_VECTOR(std::vector<ExpressionStatistics>);
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
INSTANTIATE_VECTOR(std::vector<WALEntryData>);
INSTANTIATE_VECTOR(std::vector<std::unique_ptr<Rule>>);
template class std::vector<std::vector<Expression *>>;

template struct std::atomic<size_t>;
template class std::bitset<STANDARD_VECTOR_SIZE>;
template class std::bitset<STORAGE_CHUNK_SIZE>;
template class std::unordered_map<PhysicalOperator *, QueryProfiler::TreeNode *>;
template class std::stack<PhysicalOperator *>;

// template class std::unordered_map<string,
// std::unique_ptr<CatalogEntry>>;
template class std::unordered_map<string, size_t>;
template class std::unordered_map<string, std::vector<string>>;
template class std::unordered_map<string, std::pair<size_t, Expression *>>;
// template class std::unordered_map<string, TableBinding>;
template class std::unordered_map<string, SelectStatement *>;
template class std::unordered_map<size_t, size_t>;

#endif
