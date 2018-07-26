
#pragma once

#include <vector>

#include "catalog/catalog.hpp"

#include "common/internal_types.hpp"
#include "common/printable.hpp"

#include "common/types/data_chunk.hpp"

#include "parser/expression/abstract_expression.hpp"
#include "parser/statement/select_statement.hpp"

namespace duckdb {
class PhysicalOperator;

class PhysicalOperatorState {
  public:
	PhysicalOperatorState(PhysicalOperator *child);
	virtual ~PhysicalOperatorState() {}

	DataChunk child_chunk;
	std::unique_ptr<PhysicalOperatorState> child_state;
};

class PhysicalOperator : public Printable {
  public:
	PhysicalOperator(PhysicalOperatorType type) : type(type) {}

	PhysicalOperatorType GetOperatorType() { return type; }

	virtual std::string ToString() const override;

	virtual void InitializeChunk(DataChunk &chunk) = 0;
	virtual void GetChunk(DataChunk &chunk, PhysicalOperatorState *state) = 0;

	virtual std::unique_ptr<PhysicalOperatorState> GetOperatorState() = 0;

	PhysicalOperatorType type;
	std::vector<std::unique_ptr<PhysicalOperator>> children;
};

class PhysicalAggregate : public PhysicalOperator {
  public:
	PhysicalAggregate(
	    PhysicalOperatorType type,
	    std::vector<std::unique_ptr<AbstractExpression>> select_list,
	    std::vector<std::unique_ptr<AbstractExpression>> groups)
	    : PhysicalOperator(type), select_list(std::move(select_list)),
	      groups(std::move(groups)) {}

	std::vector<std::unique_ptr<AbstractExpression>> select_list;
	std::vector<std::unique_ptr<AbstractExpression>> groups;
};

class PhysicalOrderBy : public PhysicalOperator {
  public:
	PhysicalOrderBy() : PhysicalOperator(PhysicalOperatorType::ORDER_BY) {}

	virtual void GetChunk(DataChunk &chunk,
	                      PhysicalOperatorState *state) override;
};
}
