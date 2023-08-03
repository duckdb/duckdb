//---------------------------------------------------------------------------
//	@filename:
//		CDrvdPropRelational.cpp
//
//	@doc:
//		Relational derived properties;
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/base/CDrvdPropRelational.h"
#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/task/CWorker.h"
#include "duckdb/optimizer/cascade/base/CKeyCollection.h"
#include "duckdb/optimizer/cascade/base/CReqdPropPlan.h"
#include "duckdb/optimizer/cascade/base/CUtils.h"
#include "duckdb/optimizer/cascade/operators/CExpressionHandle.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CDrvdPropRelational::CDrvdPropRelational
//
//	@doc:
//		ctor
//
//---------------------------------------------------------------------------
CDrvdPropRelational::CDrvdPropRelational()
	: m_pkc(NULL), m_ulJoinDepth(0), m_ppc(NULL), m_is_complete(false)
{
}


//---------------------------------------------------------------------------
//	@function:
//		CDrvdPropRelational::~CDrvdPropRelational
//
//	@doc:
//		ctor
//
//---------------------------------------------------------------------------
CDrvdPropRelational::~CDrvdPropRelational()
{
}

//---------------------------------------------------------------------------
//	@function:
//		CDrvdPropRelational::Derive
//
//	@doc:
//		Derive relational props. This derives ALL properties
//
//---------------------------------------------------------------------------
void CDrvdPropRelational::Derive(CExpressionHandle& exprhdl, CDrvdPropCtxt* pdpctxt)
{
	// call output derivation function on the operator
	DeriveOutputColumns(exprhdl);
	// derive outer-references
	// DeriveOuterReferences(exprhdl);
	// derive not null columns
	DeriveNotNullColumns(exprhdl);
	// derive correlated apply columns
	// DeriveCorrelatedApplyColumns(exprhdl);
	// derive constraint
	DerivePropertyConstraint(exprhdl);
	// derive keys
	DeriveKeyCollection(exprhdl);
	// derive join depth
	DeriveJoinDepth(exprhdl);
	// derive functional dependencies
	DeriveFunctionalDependencies(exprhdl);
	m_is_complete = true;
}

//---------------------------------------------------------------------------
//	@function:
//		CDrvdPropRelational::FSatisfies
//
//	@doc:
//		Check for satisfying required properties
//
//---------------------------------------------------------------------------
bool CDrvdPropRelational::FSatisfies(const CReqdPropPlan* prpp) const
{
	auto v1 = GetOutputColumns();
	duckdb::vector<ColumnBinding> v(v1.size() + prpp->m_pcrs.size());
	auto itr = set_difference(v1.begin(), v1.end(), prpp->m_pcrs.begin(), prpp->m_pcrs.end(), v.begin());
	v.resize(itr - v.begin());
	return (v1.size() == prpp->m_pcrs.size() + v.size());
}

//---------------------------------------------------------------------------
//	@function:
//		CDrvdPropRelational::GetRelationalProperties
//
//	@doc:
//		Short hand for conversion
//
//---------------------------------------------------------------------------
CDrvdPropRelational* CDrvdPropRelational::GetRelationalProperties(CDrvdProp* pdp)
{
	return (CDrvdPropRelational*)pdp;
}

//---------------------------------------------------------------------------
//	@function:
//		CDrvdPropRelational::PdrgpfdChild
//
//	@doc:
//		Helper for getting applicable FDs from child
//
//---------------------------------------------------------------------------
duckdb::vector<CFunctionalDependency*> CDrvdPropRelational::DeriveChildFunctionalDependencies(ULONG child_index, CExpressionHandle &exprhdl)
{
	// get FD's of the child
	duckdb::vector<CFunctionalDependency*> pdrgpfdChild = exprhdl.Pdrgpfd(child_index);
	// get output columns of the parent
	duckdb::vector<ColumnBinding> pcrsOutput = exprhdl.DeriveOutputColumns();
	// collect child FD's that are applicable to the parent
	duckdb::vector<CFunctionalDependency*> pdrgpfd;
	const ULONG size = pdrgpfdChild.size();
	for (ULONG ul = 0; ul < size; ul++)
	{
		CFunctionalDependency* pfd = pdrgpfdChild[ul];
		// check applicability of FD's LHS
		if (CUtils::ContainsAll(pcrsOutput, pfd->PcrsKey()))
		{
			// decompose FD's RHS to extract the applicable part
			duckdb::vector<ColumnBinding> pcrsDetermined;
			duckdb::vector<ColumnBinding> v = pfd->PcrsDetermined();
			pcrsDetermined.insert(pcrsDetermined.end(),v.begin(), v.end());
			duckdb::vector<ColumnBinding> target;
			std::set_intersection(pcrsDetermined.begin(), pcrsDetermined.end(), pcrsOutput.begin(), pcrsOutput.end(), target.begin());
			if (0 < target.size())
			{
				// create a new FD and add it to the output array
				CFunctionalDependency* pfdNew = new CFunctionalDependency(pfd->PcrsKey(), pcrsDetermined);
				pdrgpfd.push_back(pfdNew);
			}
		}
	}
	return pdrgpfd;
}

//---------------------------------------------------------------------------
//	@function:
//		CDrvdPropRelational::PdrgpfdLocal
//
//	@doc:
//		Helper for deriving local FDs
//
//---------------------------------------------------------------------------
duckdb::vector<CFunctionalDependency*> CDrvdPropRelational::DeriveLocalFunctionalDependencies(CExpressionHandle &exprhdl)
{
	duckdb::vector<CFunctionalDependency*> pdrgpfd;
	// get local key
	CKeyCollection* pkc = exprhdl.DeriveKeyCollection();
	if (NULL == pkc)
	{
		return pdrgpfd;
	}
	ULONG ulKeys = pkc->Keys();
	for (ULONG ul = 0; ul < ulKeys; ul++)
	{
		duckdb::vector<ColumnBinding> pdrgpcrKey = pkc->PdrgpcrKey(ul);
		duckdb::vector<ColumnBinding> pcrsKey;
		pcrsKey.insert(pcrsKey.begin(), pdrgpcrKey.begin(), pdrgpcrKey.end());
		// get output columns
		duckdb::vector<ColumnBinding> pcrsOutput = exprhdl.DeriveOutputColumns();
		duckdb::vector<ColumnBinding> pcrsDetermined;
		pcrsDetermined.insert(pcrsDetermined.begin(), pcrsOutput.begin(), pcrsOutput.end());
		duckdb::vector<ColumnBinding> target;
		std::set_difference(pcrsDetermined.begin(), pcrsDetermined.end(), pcrsKey.begin(), pcrsKey.end(), target.begin());
		if (0 < target.size())
		{
			// add FD between key and the rest of output columns
			CFunctionalDependency* pfdLocal = new CFunctionalDependency(pcrsKey, pcrsDetermined);
			pdrgpfd.push_back(pfdLocal);
		}
	}
	return pdrgpfd;
}

// output columns
duckdb::vector<ColumnBinding> CDrvdPropRelational::GetOutputColumns() const
{
	return m_pcrsOutput;
}

// output columns
duckdb::vector<ColumnBinding> CDrvdPropRelational::DeriveOutputColumns(CExpressionHandle &exprhdl)
{
	if (!m_is_prop_derived[EdptPcrsOutput])
	{
		m_pcrsOutput = exprhdl.Pop()->GetColumnBindings();
	}
	m_is_prop_derived.set(EdptPcrsOutput, true);
	return m_pcrsOutput;
}

// outer references
duckdb::vector<ColumnBinding> CDrvdPropRelational::GetOuterReferences() const
{
	return m_pcrsOuter;
}

// outer references
duckdb::vector<ColumnBinding> CDrvdPropRelational::DeriveOuterReferences(CExpressionHandle &exprhdl)
{
	if (!m_is_prop_derived[EdptPcrsOuter])
	{
		m_pcrsOuter = exprhdl.Pop()->GetColumnBindings();
	}
	m_is_prop_derived.set(EdptPcrsOuter, true);
	return m_pcrsOuter;
}

// nullable columns
duckdb::vector<ColumnBinding> CDrvdPropRelational::GetNotNullColumns() const
{
	return m_pcrsNotNull;
}

duckdb::vector<ColumnBinding> CDrvdPropRelational::DeriveNotNullColumns(CExpressionHandle &exprhdl)
{
	if (!m_is_prop_derived[EdptPcrsNotNull])
	{
		m_pcrsNotNull = exprhdl.Pop()->GetColumnBindings();
	}
	m_is_prop_derived.set(EdptPcrsNotNull, true);
	return m_pcrsNotNull;
}

// columns from the inner child of a correlated-apply expression that can be used above the apply expression
duckdb::vector<ColumnBinding> CDrvdPropRelational::GetCorrelatedApplyColumns() const
{
	return m_pcrsCorrelatedApply;
}

duckdb::vector<ColumnBinding> CDrvdPropRelational::DeriveCorrelatedApplyColumns(CExpressionHandle &exprhdl)
{
	if (!m_is_prop_derived[EdptPcrsCorrelatedApply])
	{
		m_pcrsCorrelatedApply = exprhdl.Pop()->GetColumnBindings();
	}
	m_is_prop_derived.set(EdptPcrsCorrelatedApply, true);
	return m_pcrsCorrelatedApply;
}

// key collection
CKeyCollection* CDrvdPropRelational::GetKeyCollection() const
{
	return m_pkc;
}

CKeyCollection* CDrvdPropRelational::DeriveKeyCollection(CExpressionHandle &exprhdl)
{
	if (!m_is_prop_derived[EdptPkc])
	{
		m_pkc = ((LogicalOperator*)exprhdl.Pop())->DeriveKeyCollection(exprhdl);
	}
	m_is_prop_derived.set(EdptPkc, true);
	return m_pkc;
}

// functional dependencies
duckdb::vector<CFunctionalDependency*> CDrvdPropRelational::GetFunctionalDependencies() const
{
	return m_pdrgpfd;
}

duckdb::vector<CFunctionalDependency*> CDrvdPropRelational::DeriveFunctionalDependencies(CExpressionHandle &exprhdl)
{
	if (!m_is_prop_derived[EdptPdrgpfd])
	{
		duckdb::vector<CFunctionalDependency*> pdrgpfd;
		const ULONG arity = exprhdl.Arity();
		// collect applicable FD's from logical children
		for (ULONG ul = 0; ul < arity; ul++)
		{
			duckdb::vector<CFunctionalDependency*> pdrgpfdChild = DeriveChildFunctionalDependencies(ul, exprhdl);
			pdrgpfd.insert(pdrgpfdChild.begin(), pdrgpfdChild.end(), pdrgpfd.end());
		}
		// add local FD's
		duckdb::vector<CFunctionalDependency*> pdrgpfdLocal = DeriveLocalFunctionalDependencies(exprhdl);
		pdrgpfd.insert(pdrgpfdLocal.begin(), pdrgpfdLocal.end(), pdrgpfd.end());
		m_pdrgpfd = pdrgpfd;
	}
	m_is_prop_derived.set(EdptPdrgpfd, true);
	return m_pdrgpfd;
}

// join depth
ULONG CDrvdPropRelational::GetJoinDepth() const
{
	return m_ulJoinDepth;
}

ULONG CDrvdPropRelational::DeriveJoinDepth(CExpressionHandle &exprhdl)
{
	if (!m_is_prop_derived[EdptJoinDepth])
	{
		m_ulJoinDepth = ((LogicalOperator*)exprhdl.Pop())->DeriveJoinDepth(exprhdl);
	}
	m_is_prop_derived.set(EdptJoinDepth, true);
	return m_ulJoinDepth;
}

// constraint property
CPropConstraint* CDrvdPropRelational::GetPropertyConstraint() const
{
	return m_ppc;
}

CPropConstraint* CDrvdPropRelational::DerivePropertyConstraint(CExpressionHandle &exprhdl)
{
	if (!m_is_prop_derived[EdptPpc])
	{
		m_ppc = ((LogicalOperator*)exprhdl.Pop())->DerivePropertyConstraint(exprhdl);
	}
	m_is_prop_derived.set(EdptPpc, true);
	return m_ppc;
}