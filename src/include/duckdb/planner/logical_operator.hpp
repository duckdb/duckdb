//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/logical_operator.hpp
//
//
//===----------------------------------------------------------------------===//
#pragma once

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/common.hpp"
#include "duckdb/common/enums/logical_operator_type.hpp"
#include "duckdb/optimizer/cascade/base/CPropConstraint.h"
#include "duckdb/optimizer/cascade/operators/Operator.h"
#include "duckdb/optimizer/cascade/xforms/CXform.h"
#include "duckdb/optimizer/join_order/estimated_properties.hpp"
#include "duckdb/planner/column_binding.hpp"
#include "duckdb/planner/expression.hpp"
#include "duckdb/planner/logical_operator_visitor.hpp"
#include "duckdb/planner/plan_serialization.hpp"

#include <algorithm>
#include <functional>

namespace duckdb {

using namespace gpopt;

class FieldWriter;
class FieldReader;

//! The current version of the plan serialization format. Exposed via by @Serializer & @Deserializer
//! to be used by various Operator to know what format to read and write.
extern const uint64_t PLAN_SERIALIZATION_VERSION;

//! LogicalOperator is the base class of the logical operators present in the
//! logical query tree
class LogicalOperator : public Operator {
public:
	explicit LogicalOperator(LogicalOperatorType type);
	LogicalOperator(LogicalOperatorType type, vector<unique_ptr<Expression>> expressions);
	// copy ctor
	LogicalOperator(const LogicalOperator &other) = delete;
	~LogicalOperator() override;

public:
	// ---------------------------- ORCA -------------------------------------
	CReqdProp *PrpCreate() const override;

	ULONG DeriveJoinDepth(CExpressionHandle &exprhdl) override;

	ULONG HashValue() const override;

	CDrvdProp *PdpCreate() override;

	static CKeyCollection *PkcDeriveKeysPassThru(CExpressionHandle &expression_handle, ULONG ulChild);

	CPropConstraint *PpcDeriveConstraintFromPredicates(CExpressionHandle &exprhdl);

	static CPropConstraint *PpcDeriveConstraintPassThru(CExpressionHandle &exprhdl, ULONG ulChild);

	CKeyCollection *DeriveKeyCollection(CExpressionHandle &exprhdl) override;

	// Transformations: it outputs the candidate set of xforms
	virtual CXform_set *PxfsCandidates() const {
		return nullptr;
	}

	// ---------------------------- DuckDB -------------------------------------

	vector<ColumnBinding> GetColumnBindings() override;
	static vector<ColumnBinding> GenerateColumnBindings(idx_t table_idx, idx_t column_count);
	static vector<LogicalType> MapTypes(const vector<LogicalType> &types, const vector<idx_t> &projection_map);
	static vector<ColumnBinding> MapBindings(const vector<ColumnBinding> &types, const vector<idx_t> &projection_map);
	DUCKDB_API void Print();
	//! Debug method: verify that the integrity of expressions & child nodes are maintained
	virtual void Verify(ClientContext &context);

	virtual string GetName() const;
	virtual string ParamsToString() const;
	string ToString() const override;

	vector<const_reference<LogicalOperator>> GetChildren() const {
		vector<const_reference<LogicalOperator>> result;
		for (auto &child : children) {
			result.push_back(*static_cast<LogicalOperator *>(child.get()));
		}
		return result;
	}

	idx_t EstimateCardinality(ClientContext &context) override;

	//! Serializes a LogicalOperator to a stand-alone binary blob
	void Serialize(Serializer &serializer) const override;
	//! Serializes an LogicalOperator to a stand-alone binary blob
	void Serialize(FieldWriter &writer) const override = 0;

	static unique_ptr<LogicalOperator> Deserialize(Deserializer &deserializer, PlanDeserializationState &state);

	virtual unique_ptr<LogicalOperator> Copy(ClientContext &context) const;

	//! Returns the set of table indexes of this operator
	virtual vector<idx_t> GetTableIndex() const;

public:
	template <class TARGET>
	TARGET &Cast() {
		if (TARGET::TYPE != LogicalOperatorType::LOGICAL_INVALID && this->logical_type != TARGET::TYPE) {
			throw InternalException("Failed to cast logical operator to type - logical operator type mismatch");
		}
		return (TARGET &)*this;
	}

	template <class TARGET>
	const TARGET &Cast() const {
		if (TARGET::TYPE != LogicalOperatorType::LOGICAL_INVALID && this->logical_type != TARGET::TYPE) {
			throw InternalException("Failed to cast logical operator to type - logical operator type mismatch");
		}
		return (const TARGET &)*this;
	}
};
} // namespace duckdb