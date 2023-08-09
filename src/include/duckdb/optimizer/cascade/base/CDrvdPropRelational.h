//---------------------------------------------------------------------------
//	@filename:
//		CDrvdPropRelational.h
//
//	@doc:
//		Derived logical properties
//---------------------------------------------------------------------------
#ifndef GPOPT_CDrvdPropRelational_H
#define GPOPT_CDrvdPropRelational_H

#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/planner/expression.hpp"
#include "duckdb/optimizer/cascade/base/CDrvdProp.h"
#include "duckdb/optimizer/cascade/base/CFunctionalDependency.h"
#include "duckdb/optimizer/cascade/base/CPropConstraint.h"
#include "duckdb/planner/column_binding.hpp"
#include <bitset>

using namespace gpos;

namespace gpopt
{
// fwd declaration
class CExpressionHandle;
class CReqdPropPlan;
class CKeyCollection;
class CPropConstraint;
class CPartInfo;

//---------------------------------------------------------------------------
//	@class:
//		CDrvdPropRelational
//
//	@doc:
//		Derived logical properties container.
//
//		These are properties than can be inferred from logical expressions or
//		Memo groups. This includes output columns, outer references, primary
//		keys. These properties hold regardless of the physical implementation
//		of an expression.
//
//---------------------------------------------------------------------------
class CDrvdPropRelational : public CDrvdProp
{
public:
	// See member variables (below) with the same name for description on what
	// the property types respresent
	enum EDrvdPropType
	{
		EdptPcrsOutput = 0, EdptPcrsOuter, EdptPcrsNotNull, EdptPcrsCorrelatedApply, EdptPkc, EdptPdrgpfd, EdptMaxCard, EdptPpc, EdptPfp, EdptJoinDepth, EdptTableDescriptor, EdptSentinel
	};

public:
	// bitset representing whether property has been derived
	bitset<EdptSentinel> m_is_prop_derived;

	// output columns
	duckdb::vector<ColumnBinding> m_pcrsOutput;

	// columns not defined in the underlying operator tree
	duckdb::vector<ColumnBinding> m_pcrsOuter;

	// output columns that do not allow null values
	duckdb::vector<ColumnBinding> m_pcrsNotNull;

	// columns from the inner child of a correlated-apply expression that can be used above the apply expression
	duckdb::vector<ColumnBinding> m_pcrsCorrelatedApply;

	// key collection
	CKeyCollection* m_pkc;

	// functional dependencies
	duckdb::vector<CFunctionalDependency*> m_pdrgpfd;

	// join depth (number of relations in underlying tree)
	ULONG m_ulJoinDepth;

	// constraint property
	CPropConstraint* m_ppc;

	// true if all logical operators in the group are of type CLogicalDynamicGet,
	// and the dynamic get has partial indexes
	bool m_fHasPartialIndexes;

	// Have all the properties been derived?
	//
	// NOTE1: This is set ONLY when Derive() is called. If all the properties
	// are independently derived, m_is_complete will remain false. In that
	// case, even though Derive() would attempt to derive all the properties
	// once again, it should be quick, since each individual member has been
	// cached.
	// NOTE2: Once these properties are detached from the
	// corresponding expression used to derive it, this MUST be set to true,
	// since after the detachment, there will be no way to derive the
	// properties once again.
	bool m_is_complete;

public:
	// helper for getting applicable FDs from child
	static duckdb::vector<CFunctionalDependency*> DeriveChildFunctionalDependencies(ULONG child_index, CExpressionHandle &exprhdl);

	// helper for creating local FDs
	static duckdb::vector<CFunctionalDependency*> DeriveLocalFunctionalDependencies(CExpressionHandle &exprhdl);

public:
	// output columns
	duckdb::vector<ColumnBinding> DeriveOutputColumns(CExpressionHandle &);

	// outer references
	duckdb::vector<ColumnBinding> DeriveOuterReferences(CExpressionHandle &);

	// nullable columns
	duckdb::vector<ColumnBinding> DeriveNotNullColumns(CExpressionHandle &);

	// columns from the inner child of a correlated-apply expression that can be used above the apply expression
	duckdb::vector<ColumnBinding> DeriveCorrelatedApplyColumns(CExpressionHandle &);

	// key collection
	CKeyCollection* DeriveKeyCollection(CExpressionHandle &);

	// functional dependencies
	duckdb::vector<CFunctionalDependency*> DeriveFunctionalDependencies(CExpressionHandle &);

	// join depth
	ULONG DeriveJoinDepth(CExpressionHandle &);

	// constraint property
	CPropConstraint* DerivePropertyConstraint(CExpressionHandle &);

	// has partial indexes
	bool DeriveHasPartialIndexes(CExpressionHandle &);

public:
	// ctor
	CDrvdPropRelational();

	// copy ctor
	CDrvdPropRelational(const CDrvdPropRelational &) = delete;

	// dtor
	virtual ~CDrvdPropRelational();

	// type of properties
	EPropType Ept() override
	{
		return EptRelational;
	}

	bool IsComplete() const override
	{
		return m_is_complete;
	}

	// derivation function
	void Derive(CExpressionHandle& exprhdl, CDrvdPropCtxt* pdpctxt) override;

	// output columns
	duckdb::vector<ColumnBinding> GetOutputColumns() const;

	// outer references
	duckdb::vector<ColumnBinding> GetOuterReferences() const;

	// nullable columns
	duckdb::vector<ColumnBinding> GetNotNullColumns() const;

	// columns from the inner child of a correlated-apply expression that can be used above the apply expression
	duckdb::vector<ColumnBinding> GetCorrelatedApplyColumns() const;

	// key collection
	CKeyCollection* GetKeyCollection() const;

	// functional dependencies
	duckdb::vector<CFunctionalDependency*> GetFunctionalDependencies() const;

	// join depth
	ULONG GetJoinDepth() const;

	// constraint property
	CPropConstraint* GetPropertyConstraint() const;

	// shorthand for conversion
	static CDrvdPropRelational* GetRelationalProperties(CDrvdProp* pdp);

	// check for satisfying required plan properties
	bool FSatisfies(const CReqdPropPlan *prpp) const override;
};	// class CDrvdPropRelational
}  // namespace gpopt
#endif