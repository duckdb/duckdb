#pragma once

#include <string>
#include <unordered_map>
#include <memory>
#include "substrait/plan.pb.h"
#include "duckdb/main/connection.hpp"

namespace duckdb {
class SubstraitToDuckDB {
public:
	SubstraitToDuckDB(Connection &con_p, string &serialized);
	//! Transforms Substrait Plan to DuckDB Relation
	shared_ptr<Relation> TransformPlan();

private:
	//! Transforms Substrait Plan Root To a DuckDB Relation
	shared_ptr<Relation> TransformRootOp(const substrait::RelRoot &sop);
	//! Transform Substrait Operations to DuckDB Relations
	shared_ptr<Relation> TransformOp(const substrait::Rel &sop);
	shared_ptr<Relation> TransformJoinOp(const substrait::Rel &sop);
	shared_ptr<Relation> TransformCrossProductOp(const substrait::Rel &sop);
	shared_ptr<Relation> TransformFetchOp(const substrait::Rel &sop);
	shared_ptr<Relation> TransformFilterOp(const substrait::Rel &sop);
	shared_ptr<Relation> TransformProjectOp(const substrait::Rel &sop);
	shared_ptr<Relation> TransformAggregateOp(const substrait::Rel &sop);
	shared_ptr<Relation> TransformReadOp(const substrait::Rel &sop);
	shared_ptr<Relation> TransformSortOp(const substrait::Rel &sop);

	//! Transform Substrait Expressions to DuckDB Expressions
	unique_ptr<ParsedExpression> TransformExpr(const substrait::Expression &sexpr);
	unique_ptr<ParsedExpression> TransformLiteralExpr(const substrait::Expression &sexpr);
	unique_ptr<ParsedExpression> TransformSelectionExpr(const substrait::Expression &sexpr);
	unique_ptr<ParsedExpression> TransformScalarFunctionExpr(const substrait::Expression &sexpr);
	unique_ptr<ParsedExpression> TransformIfThenExpr(const substrait::Expression &sexpr);
	unique_ptr<ParsedExpression> TransformCastExpr(const substrait::Expression &sexpr);
	unique_ptr<ParsedExpression> TransformInExpr(const substrait::Expression &sexpr);

	LogicalType SubstraitToDuckType(const ::substrait::Type &s_type);
	//! Looks up for aggregation function in functions_map
	string FindFunction(uint64_t id);

	//! Transform Substrait Sort Order to DuckDB Order
	OrderByNode TransformOrder(const substrait::SortField &sordf);
	//! DuckDB Connection
	Connection &con;
	//! Substrait Plan
	substrait::Plan plan;
	//! Variable used to register functions
	unordered_map<uint64_t, string> functions_map;
};
} // namespace duckdb
