//===----------------------------------------------------------------------===//
//                         DuckDB
//
// execution/operator/projection/physical_projection.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "execution/physical_operator.hpp"

namespace duckdb {

class PhysicalProjection : public PhysicalOperator {
public:
	PhysicalProjection(LogicalOperator &op, vector<unique_ptr<Expression>> select_list)
	    : PhysicalOperator(PhysicalOperatorType::PROJECTION, op.types), select_list(move(select_list)) {
	}

	void _GetChunk(ClientContext &context, DataChunk &chunk, PhysicalOperatorState *state) override;

	void AcceptExpressions(SQLNodeVisitor *v) override {
		for (auto &e : select_list) {
			v->VisitExpression(&e);
		}
	}

	vector<unique_ptr<Expression>> select_list;
};

} // namespace duckdb
