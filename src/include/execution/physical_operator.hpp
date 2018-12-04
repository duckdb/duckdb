//===----------------------------------------------------------------------===// 
// 
//                         DuckDB 
// 
// execution/physical_operator.hpp
// 
// 
// 
//===----------------------------------------------------------------------===//

#pragma once

#include "catalog/catalog.hpp"

#include "common/common.hpp"
#include "common/printable.hpp"
#include "common/types/data_chunk.hpp"

#include "planner/logical_operator.hpp"

#include "parser/expression.hpp"
#include "parser/statement/select_statement.hpp"

namespace duckdb {
class ClientContext;
class ExpressionExecutor;
class PhysicalOperator;

//! The current state/context of the operator. The PhysicalOperatorState is
//! updated using the GetChunk function, and allows the caller to repeatedly
//! call the GetChunk function and get new batches of data everytime until the
//! data source is exhausted.
class PhysicalOperatorState {
  public:
	PhysicalOperatorState(PhysicalOperator *child, ExpressionExecutor *parent);
	virtual ~PhysicalOperatorState() {
	}

	//! Flag indicating whether or not the operator is finished [note: not all
	//! operators use this flag]
	bool finished;
	//! DataChunk that stores data from the child of this operator
	DataChunk child_chunk;
	//! State of the child of this operator
	std::unique_ptr<PhysicalOperatorState> child_state;

	ExpressionExecutor *parent;
};

//! PhysicalOperator is the base class of the physical operators present in the
//! execution plan
/*!
    The execution model is a pull-based execution model. GetChunk is called on
   the root node, which causes the root node to be executed, and presumably call
   GetChunk again on its child nodes. Every node in the operator chain has a
   state that is updated as GetChunk is called: PhysicalOperatorState (different
   operators subclass this state and add different properties).
*/
class PhysicalOperator : public Printable {
  public:
	PhysicalOperator(PhysicalOperatorType type, std::vector<TypeId> types) : type(type), types(types) {
	}

	PhysicalOperatorType GetOperatorType() {
		return type;
	}

	std::string ToString() const override;

	//! Return a vector of the types that will be returned by this operator
	std::vector<TypeId>& GetTypes() {
		return types;
	}
	//! Initialize a given chunk to the types that will be returned by this
	//! operator, this will prepare chunk for a call to GetChunk. This method
	//! only has to be called once for any amount of calls to GetChunk.
	virtual void InitializeChunk(DataChunk &chunk) {
		auto &types = GetTypes();
		chunk.Initialize(types);
	}
	//! Retrieves a chunk from this operator and stores it in the chunk
	//! variable.
	virtual void _GetChunk(ClientContext &context, DataChunk &chunk,
	                       PhysicalOperatorState *state) = 0;

	void GetChunk(ClientContext &context, DataChunk &chunk,
	              PhysicalOperatorState *state);

	//! Create a new empty instance of the operator state
	virtual std::unique_ptr<PhysicalOperatorState>
	GetOperatorState(ExpressionExecutor *executor) = 0;

	virtual std::string ExtraRenderInformation() {
		return "";
	}

	//! The physical operator type
	PhysicalOperatorType type;
	//! The set of children of the operator
	std::vector<std::unique_ptr<PhysicalOperator>> children;
	//! The types returned by this physical operator
	std::vector<TypeId> types;
};
} // namespace duckdb
