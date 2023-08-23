//---------------------------------------------------------------------------
//	@filename:
//		Operator.h
//
//	@doc:
//		Base class for all operators: logical, physical, patterns
//---------------------------------------------------------------------------
#pragma once

#include "duckdb/common/enums/physical_operator_type.hpp"
#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/base/CDrvdProp.h"
#include "duckdb/optimizer/cascade/base/CFunctionalDependency.h"
#include "duckdb/optimizer/cascade/base/CKeyCollection.h"
#include "duckdb/optimizer/cascade/base/CReqdProp.h"
#include "duckdb/optimizer/join_order/estimated_properties.hpp"
#include "duckdb/planner/expression.hpp"

namespace gpopt {
using namespace gpos;

class Operator;
class CCostContext;
class CGroupExpression;
class CDrvdPropPlan;
class CDrvdPropRelational;
class CDrvdPropCtxtPlan;
class CPropConstraint;

//---------------------------------------------------------------------------
//	@class:
//		Operator
//
//	@doc:
//		base class for all operators
//
//---------------------------------------------------------------------------
class Operator {
public:
	//! aggregate type
	enum EGbAggType { EGB_AGG_TYPE_GLOBAL, EGB_AGG_TYPE_LOCAL, EGB_AGG_TYPE_INTERMEDIATE, EGB_AGG_TYPE_SENTINEL };

	//! coercion form
	enum ECoercionForm { ECF_EXPLICIT_CALL, ECF_EXPLICIT_CAST, ECF_IMPLICIT_CAST, ECF_DONT_CARE };

public:
	//! The set of children of the operator
	duckdb::vector<duckdb::unique_ptr<Operator>> children;
	LogicalOperatorType logical_type = LogicalOperatorType::LOGICAL_INVALID;
	PhysicalOperatorType physical_type = PhysicalOperatorType::INVALID;

	// --------------------------- ORCA ------------------------
	CGroupExpression *m_group_expression;
	//! derived relational properties
	CDrvdPropRelational *m_derived_property_relation;
	//! derived properties of the carried plan
	CDrvdPropPlan *m_derived_property_plan;
	//! required plan properties
	CReqdPropPlan *m_required_plan_property;
	double m_cost;

	// --------------------------- DuckDB ----------------------
	duckdb::unique_ptr<EstimatedProperties> estimated_props;
	//! The types returned by this operator. Set by calling Operator::ResolveTypes.
	duckdb::vector<LogicalType> types;
	//! Estimated Cardinality
	idx_t estimated_cardinality;
	//! The set of expressions contained within the operator, if any
	duckdb::vector<duckdb::unique_ptr<Expression>> expressions;
	bool has_estimated_cardinality = false;

public:
	Operator() : m_cost(GPOPT_INVALID_COST) {
	}

	Operator(const Operator &other) = delete;

	virtual ~Operator() = default;

public:
	void ResolveOperatorTypes();

	void AddChild(duckdb::unique_ptr<Operator> child) {
		D_ASSERT(child);
		children.emplace_back(std::move(child));
	}
	
	//! Return a vector of the types that will be returned by this operator
	const duckdb::vector<LogicalType> &GetTypes() const {
		return types;
	}

	virtual idx_t EstimateCardinality(ClientContext &context);

	// ------------------------------------- ORCA ------------------------------------
	// Rehydrate expression from a given cost context and child expressions
	static Operator *PexprRehydrate(CCostContext *cost_context, duckdb::vector<Operator *> pdrgpexpr,
	                                CDrvdPropCtxtPlan *pdpctxtplan);
	// get the suitable derived property type based on operator
	CDrvdProp::EPropType Ept() const;

	ULONG Arity(int x = 0) const {
		if (x == 0) {
			return children.size();
		} else if (x == 1) {
			return expressions.size();
		}
		return children.size();
	}
	// get expression's derived property given its type
	CDrvdProp *Pdp(const CDrvdProp::EPropType ept) const;

	duckdb::vector<CFunctionalDependency *> DeriveFunctionalDependencies(CExpressionHandle &expression_handle);

	CReqdPropPlan *PrppCompute(CReqdPropPlan *required_properties_input);

	CReqdPropPlan *PrppDecorate(CReqdPropPlan *required_properties_input);

	CDrvdProp *PdpDerive(CDrvdPropCtxtPlan *pdpctxtL = nullptr);

	bool FMatchPattern(CGroupExpression *group_expression);
	// hash function
	static ULONG HashValue(const Operator *op);

	virtual ULONG HashValue() const;

public:
	//! Resolve types for this specific operator
	virtual void ResolveTypes() {};

	// ------------------------------------- ORCA ------------------------------------
	virtual CKeyCollection *DeriveKeyCollection(CExpressionHandle &expression_handle) {
		return nullptr;
	}

	virtual CPropConstraint *DerivePropertyConstraint(CExpressionHandle &expression_handle) {
		return nullptr;
	}

	virtual ULONG DeriveJoinDepth(CExpressionHandle &expression_handle) {
		return 0;
	}

	virtual Operator *SelfRehydrate(CCostContext *pcc, duckdb::vector<Operator *> pdrgpexpr,
	                                CDrvdPropCtxtPlan *pdpctxtplan) {
		return nullptr;
	}
	//! create container for derived properties
	virtual CDrvdProp *PdpCreate() {
		return nullptr;
	}
	//! is operator logical?
	virtual bool FLogical() const {
		return ((logical_type != LogicalOperatorType::LOGICAL_INVALID) &&
		        (physical_type == PhysicalOperatorType::INVALID));
	}
	//! is operator physical?
	virtual bool FPhysical() const {
		return ((logical_type == LogicalOperatorType::LOGICAL_INVALID) &&
		        (physical_type != PhysicalOperatorType::INVALID));
	}
	//! is operator physical aggregate?
	virtual bool FPhysicalAgg() const {
		return physical_type == PhysicalOperatorType::UNGROUPED_AGGREGATE;
	}
	//! is operator pattern?
	virtual bool FPattern() const {
		return ((logical_type == LogicalOperatorType::LOGICAL_EXTENSION_OPERATOR) &&
		        (physical_type != PhysicalOperatorType::EXTENSION));
	}
	//! sensitivity to order of inputs
	virtual bool FInputOrderSensitive() {
		return false;
	};
	//! create container for required properties
	virtual CReqdProp *PrpCreate() const {
		return nullptr;
	};
	//! match function, abstract to enforce an implementation for each new operator
	virtual bool Matches(Operator *pop) {
		return this == pop;
	};

	virtual duckdb::unique_ptr<Operator> Copy();

	virtual duckdb::unique_ptr<Operator> CopyWithNewGroupExpression(CGroupExpression *group_expression);

	virtual duckdb::unique_ptr<Operator> CopyWithNewChildren(CGroupExpression *group_expression,
	                                                         duckdb::vector<duckdb::unique_ptr<Operator>> pdrgpexpr,
	                                                         double cost);

	virtual void CE();

	// ------------------------------------- DuckDB -------------------------------------
	virtual string ToString() const {
		return "Something wrong happens";
	}

	virtual bool RequireOptimizer() const {
		return true;
	}

	virtual duckdb::vector<ColumnBinding> GetColumnBindings() {
		duckdb::vector<ColumnBinding> v;
		return v;
	}

	//! Serializes an LogicalOperator to a stand-alone binary blob
	virtual void Serialize(FieldWriter &writer) const {
	}
	//! Serializes a LogicalOperator to a stand-alone binary blob
	virtual void Serialize(Serializer &serializer) const {
	}

public:
	template <class TARGET>
	TARGET &Cast() {
		return (TARGET &)*this;
	}

	template <class TARGET>
	const TARGET &Cast() const {
		return (const TARGET &)*this;
	}
}; // class Operator
} // namespace gpopt
