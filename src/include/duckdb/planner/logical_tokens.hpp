//===----------------------------------------------------------------------===//
//                         DuckDB
//
// planner/logical_tokens.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

namespace duckdb {

class LogicalOperator;

class LogicalAggregate;
class LogicalAnyJoin;
class LogicalChunkGet;
class LogicalComparisonJoin;
class LogicalCreateTable;
class LogicalCreateIndex;
class LogicalCrossProduct;
class LogicalDelete;
class LogicalDelimGet;
class LogicalDelimJoin;
class LogicalDistinct;
class LogicalEmptyResult;
class LogicalExpressionGet;
class LogicalFilter;
class LogicalGet;
class LogicalIndexScan;
class LogicalJoin;
class LogicalLimit;
class LogicalOrder;
class LogicalProjection;
class LogicalInsert;
class LogicalCopyFromFile;
class LogicalCopyToFile;
class LogicalExplain;
class LogicalSetOperation;
class LogicalSubquery;
class LogicalUpdate;
class LogicalTableFunction;
class LogicalPrepare;
class LogicalPruneColumns;
class LogicalWindow;
class LogicalExecute;

} // namespace duckdb
