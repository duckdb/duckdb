//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/logical_tokens.hpp
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
class LogicalCopyToFile;
class LogicalCreate;
class LogicalCreateTable;
class LogicalCreateIndex;
class LogicalCreateMatView;
class LogicalCrossProduct;
class LogicalCTERef;
class LogicalDelete;
class LogicalDelimGet;
class LogicalDelimJoin;
class LogicalDistinct;
class LogicalDummyScan;
class LogicalEmptyResult;
class LogicalExecute;
class LogicalExplain;
class LogicalExport;
class LogicalExpressionGet;
class LogicalFilter;
class LogicalGet;
class LogicalInsert;
class LogicalJoin;
class LogicalLimit;
class LogicalOrder;
class LogicalPragma;
class LogicalPrepare;
class LogicalProjection;
class LogicalRecursiveCTE;
class LogicalSetOperation;
class LogicalSample;
class LogicalShow;
class LogicalSimple;
class LogicalSet;
class LogicalTopN;
class LogicalUnnest;
class LogicalUpdate;
class LogicalWindow;

} // namespace duckdb
