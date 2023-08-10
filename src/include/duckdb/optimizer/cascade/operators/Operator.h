//---------------------------------------------------------------------------
//	@filename:
//		Operator.h
//
//	@doc:
//		Base class for all operators: logical, physical, patterns
//---------------------------------------------------------------------------
#ifndef GPOPT_Operator_H
#define GPOPT_Operator_H

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
using namespace duckdb;

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
	// aggregate type
	enum EGbAggType { EgbaggtypeGlobal, EgbaggtypeLocal, EgbaggtypeIntermediate, EgbaggtypeSentinel };

	// coercion form
	enum ECoercionForm { EcfExplicitCall, EcfExplicitCast, EcfImplicitCast, EcfDontCare };

public:
	CGroupExpression *m_pgexpr;

	// derived relational properties
	CDrvdPropRelational *m_pdprel;

	// derived physical properties
	CDrvdPropPlan *m_pdpplan;

	// required plan properties
	CReqdPropPlan *m_prpp;

	duckdb::unique_ptr<EstimatedProperties> estimated_props;

	//! The types returned by this operator. Set by calling Operator::ResolveTypes.
	duckdb::vector<LogicalType> types;

	//! Estimated Cardinality
	idx_t estimated_cardinality;

	//! The set of children of the operator
	duckdb::vector<duckdb::unique_ptr<Operator>> children;

	double m_cost;

	//! The set of expressions contained within the operator, if any
	duckdb::vector<duckdb::unique_ptr<Expression>> expressions;

	bool has_estimated_cardinality = false;

	LogicalOperatorType logical_type = LogicalOperatorType::LOGICAL_INVALID;

	PhysicalOperatorType physical_type = PhysicalOperatorType::INVALID;

	/* ctors and dtor */
public:
	// ctor
	explicit Operator() : m_cost(GPOPT_INVALID_COST) {
	}

	// copy ctor
	Operator(const Operator &other) = delete;

	// dtor
	virtual ~Operator() {
	}

public:
	// Rehydrate expression from a given cost context and child expressions
	static Operator *PexprRehydrate(CCostContext *pcc, duckdb::vector<Operator *> pdrgpexpr,
	                                CDrvdPropCtxtPlan *pdpctxtplan);

	//! Resolve types for this specific operator
	virtual void ResolveTypes() {
	}

	void ResolveOperatorTypes() {
		types.clear();
		// first resolve child types
		for (duckdb::unique_ptr<Operator> &child : children) {
			child->ResolveOperatorTypes();
		}
		// now resolve the types for this operator
		ResolveTypes();
		D_ASSERT(types.size() == GetColumnBindings().size());
	}

public:
	void AddChild(duckdb::unique_ptr<Operator> child) {
		D_ASSERT(child);
		children.emplace_back(std::move(child));
	}

	// get the suitable derived property type based on operator
	CDrvdProp::EPropType Ept() const {
		if (FLogical()) {
			return CDrvdProp::EptRelational;
		}
		if (FPhysical()) {
			return CDrvdProp::EptPlan;
		}
		return CDrvdProp::EptInvalid;
	};

	/* H-defined virtual functions */
public:
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

	virtual CKeyCollection *DeriveKeyCollection(CExpressionHandle &exprhdl) {
		return NULL;
	}

	virtual CPropConstraint *DerivePropertyConstraint(CExpressionHandle &exprhdl) {
		return NULL;
	}

	virtual ULONG DeriveJoinDepth(CExpressionHandle &exprhdl) {
		return 0;
	}

	virtual Operator *SelfRehydrate(CCostContext *pcc, duckdb::vector<Operator *> pdrgpexpr,
	                                CDrvdPropCtxtPlan *pdpctxtplan) {
		return nullptr;
	}

	// create container for derived properties
	virtual CDrvdProp *PdpCreate() {
		return nullptr;
	}

	bool FLogical() const {
		return ((logical_type != LogicalOperatorType::LOGICAL_INVALID) &&
		        (physical_type == PhysicalOperatorType::INVALID));
	}

	// is operator physical?
	bool FPhysical() const {
		return ((logical_type == LogicalOperatorType::LOGICAL_INVALID) &&
		        (physical_type != PhysicalOperatorType::INVALID));
	}

	virtual bool FPhysicalAgg() const {
		return physical_type == PhysicalOperatorType::UNGROUPED_AGGREGATE;
	}

	// is operator pattern?
	virtual bool FPattern() const {
		return ((logical_type == LogicalOperatorType::LOGICAL_EXTENSION_OPERATOR) &&
		        (physical_type != PhysicalOperatorType::EXTENSION));
	}

	// sensitivity to order of inputs
	virtual bool FInputOrderSensitive() {
		return false;
	};

	// match function;
	// abstract to enforce an implementation for each new operator
	virtual bool Matches(Operator *pop) {
		return this == pop;
	};

	// create container for required properties
	virtual CReqdProp *PrpCreate() const {
		return nullptr;
	}

	//! Serializes an LogicalOperator to a stand-alone binary blob
	virtual void Serialize(FieldWriter &writer) const {
	}

	//! Serializes a LogicalOperator to a stand-alone binary blob
	void Serialize(Serializer &serializer) const {
	}

	/* CPP-defined virtual functions */
public:
	virtual idx_t EstimateCardinality(ClientContext &context);

	virtual duckdb::unique_ptr<Operator> Copy();

	virtual duckdb::unique_ptr<Operator> CopywithNewGroupExpression(CGroupExpression *pgexpr);

	virtual duckdb::unique_ptr<Operator>
	CopywithNewChilds(CGroupExpression *pgexpr, duckdb::vector<duckdb::unique_ptr<Operator>> pdrgpexpr, double cost);

	/* CPP-defined functions */
public:
	duckdb::vector<CFunctionalDependency *> DeriveFunctionalDependencies(CExpressionHandle &exprhdl);

	CReqdPropPlan *PrppCompute(CReqdPropPlan *prppInput);

	CReqdPropPlan *PrppDecorate(CReqdPropPlan *prppInput);

	CDrvdProp *PdpDerive(CDrvdPropCtxtPlan *pdpctxtL = nullptr);

	bool FMatchPattern(CGroupExpression *pgexpr);

	// hash function
	static ULONG HashValue(const Operator *op);

	ULONG HashValue() const;

	/* H-defined functions */
public:
	ULONG Arity(int x = 0) const {
		if (x == 0) {
			return children.size();
		} else if (x == 1) {
			return expressions.size();
		}
		return children.size();
	}

	// get expression's derived property given its type
	CDrvdProp *Pdp(const CDrvdProp::EPropType ept) const {
		switch (ept) {
		case CDrvdProp::EptRelational:
			return (CDrvdProp *)m_pdprel;
		case CDrvdProp::EptPlan:
			return (CDrvdProp *)m_pdpplan;
		default:
			break;
		}
		return nullptr;
	};

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
#endif