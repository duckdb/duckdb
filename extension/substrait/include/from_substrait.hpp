#pragma once

#include <string>
#include <unordered_map>
#include <memory>
#include "substrait/plan.pb.h"

namespace io {
namespace substrait {
// class Plan;
class Expression;
class SortField;
class Rel;
} // namespace substrait
} // namespace io

namespace duckdb {
class ParsedExpression;
class Connection;
struct OrderByNode;
class Relation;
} // namespace duckdb

class SubstraitToDuckDB {
public:
	SubstraitToDuckDB(duckdb::Connection &con_p, substrait::Plan &plan_p);
	std::shared_ptr<duckdb::Relation> TransformPlan(const substrait::Plan &splan);

private:
	std::string FindFunction(uint64_t id);

	std::unique_ptr<duckdb::ParsedExpression> TransformExpr(const substrait::Expression &sexpr);
	duckdb::OrderByNode TransformOrder(const substrait::SortField &sordf);
	std::shared_ptr<duckdb::Relation> TransformOp(const substrait::Rel &sop);

	duckdb::Connection &con;
	substrait::Plan &plan;

	std::unordered_map<uint64_t, std::string> functions_map;
};