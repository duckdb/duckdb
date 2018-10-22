

// this file is used to instantiate symbols for LLDB so e.g.
// std::vector and std::unique_ptr can be accessed from the debugger

#ifdef DEBUG

#include "catalog/catalog.hpp"
#include "common/types/chunk_collection.hpp"
#include "execution/aggregate_hashtable.hpp"
#include "execution/column_binding_resolver.hpp"
#include "execution/physical_operator.hpp"
#include "main/query_profiler.hpp"
#include "main/result.hpp"
#include "optimizer/rule.hpp"
#include "parser/constraint.hpp"
#include "planner/logical_operator.hpp"
#include "planner/operator/logical_join.hpp"
#include "storage/data_table.hpp"

using namespace duckdb;
using namespace std;

template class std::unique_ptr<CatalogEntry>;
template class std::unique_ptr<Expression>;
template class std::unique_ptr<AbstractRuleNode>;
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
template class std::unique_ptr<SuperLargeHashTable>;
template class std::unique_ptr<TableRef>;
template class std::unique_ptr<Transaction>;
template class std::unique_ptr<uint64_t[]>;
template class std::unique_ptr<uint8_t[]>;
template class std::unique_ptr<Vector[]>;
template class std::unique_ptr<DataChunk>;
template class std::unique_ptr<Statistics[]>;
template class std::unique_ptr<Constraint>;
template class std::unique_ptr<SelectStatement>;

#define INSTANTIATE_VECTOR(VECTOR_DEFINITION)                                  \
	template VECTOR_DEFINITION::size_type VECTOR_DEFINITION::size() const;     \
	template VECTOR_DEFINITION::const_reference VECTOR_DEFINITION::operator[]( \
	    VECTOR_DEFINITION::size_type n) const;                                 \
	template VECTOR_DEFINITION::reference VECTOR_DEFINITION::operator[](       \
	    VECTOR_DEFINITION::size_type n);                                       \
	template VECTOR_DEFINITION::const_reference VECTOR_DEFINITION::back()      \
	    const;                                                                 \
	template VECTOR_DEFINITION::reference VECTOR_DEFINITION::back();           \
	template VECTOR_DEFINITION::const_reference VECTOR_DEFINITION::front()     \
	    const;                                                                 \
	template VECTOR_DEFINITION::reference VECTOR_DEFINITION::front();

template class std::vector<AggregateExpression *>;
template class std::vector<BoundTable>;
INSTANTIATE_VECTOR(std::vector<ColumnDefinition>);
template class std::vector<ExpressionType>;
INSTANTIATE_VECTOR(std::vector<JoinCondition>);
INSTANTIATE_VECTOR(std::vector<OrderByNode>);
template class std::vector<size_t>;
template class std::vector<Statistics>;
template class std::vector<std::string>;
INSTANTIATE_VECTOR(std::vector<std::unique_ptr<Expression>>)
INSTANTIATE_VECTOR(std::vector<std::unique_ptr<AbstractRuleNode>>);
INSTANTIATE_VECTOR(std::vector<std::unique_ptr<DataChunk>>);
INSTANTIATE_VECTOR(std::vector<std::unique_ptr<SQLStatement>>);
INSTANTIATE_VECTOR(std::vector<std::unique_ptr<PhysicalOperator>>);
INSTANTIATE_VECTOR(std::vector<std::unique_ptr<LogicalOperator>>);
INSTANTIATE_VECTOR(std::vector<std::unique_ptr<Transaction>>);
template class std::vector<TypeId>;
template class std::vector<Value>;
template class std::vector<int>;
INSTANTIATE_VECTOR(std::vector<WALEntryData>);

template struct std::atomic<size_t>;
template class std::bitset<STANDARD_VECTOR_SIZE>;
template class std::bitset<STORAGE_CHUNK_SIZE>;
template class std::unordered_map<PhysicalOperator *,
                                  QueryProfiler::TreeNode *>;
template class std::stack<PhysicalOperator *>;

// template class std::unordered_map<std::string,
// std::unique_ptr<CatalogEntry>>;
template class std::unordered_map<std::string, size_t>;
template class std::unordered_map<std::string, std::vector<std::string>>;
template class std::unordered_map<std::string, std::pair<size_t, Expression *>>;
// template class std::unordered_map<std::string, TableBinding>;
template class std::unordered_map<std::string, SelectStatement *>;

#endif
