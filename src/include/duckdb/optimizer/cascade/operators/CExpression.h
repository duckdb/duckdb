//---------------------------------------------------------------------------
//	@filename:
//		CExpression.h
//
//	@doc:
//		Basic tree/DAG-based representation for an expression
//---------------------------------------------------------------------------
#ifndef GPOPT_CExpression_H
#define GPOPT_CExpression_H

#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/common/CDynamicPtrArray.h"
#include "duckdb/optimizer/cascade/common/CRefCount.h"

#include "duckdb/optimizer/cascade/base/CColRef.h"
#include "duckdb/optimizer/cascade/base/CCostContext.h"
#include "duckdb/optimizer/cascade/base/CDrvdPropRelational.h"
#include "duckdb/optimizer/cascade/base/CDrvdPropScalar.h"
#include "duckdb/optimizer/cascade/base/CKeyCollection.h"
#include "duckdb/optimizer/cascade/base/CPrintPrefix.h"
#include "duckdb/optimizer/cascade/base/CReqdProp.h"
#include "duckdb/optimizer/cascade/base/CReqdPropRelational.h"
#include "duckdb/optimizer/cascade/cost/CCost.h"
#include "duckdb/optimizer/cascade/metadata/CTableDescriptor.h"
#include "duckdb/optimizer/cascade/operators/COperator.h"
#include "duckdb/optimizer/cascade/statistics/CStatistics.h"


namespace gpopt
{
// cleanup function for arrays
class CExpression;
typedef CDynamicPtrArray<CExpression, CleanupRelease> CExpressionArray;

// array of arrays of expression pointers
typedef CDynamicPtrArray<CExpressionArray, CleanupRelease> CExpressionArrays;

class CGroupExpression;
class CDrvdPropPlan;
class CDrvdPropCtxt;
class CDrvdPropCtxtPlan;
class CPropConstraint;

using namespace gpos;
using namespace gpnaucrates;

//---------------------------------------------------------------------------
//	@class:
//		CExpression
//
//	@doc:
//		Simply dynamic array for pointer types
//
//---------------------------------------------------------------------------
class CExpression : public CRefCount
{
	friend class CExpressionHandle;

private:
	// memory pool
	CMemoryPool *m_mp;

	// operator class
	COperator *m_pop;

	// array of children
	CExpressionArray *m_pdrgpexpr;

	// derived relational properties
	CDrvdPropRelational *m_pdprel;

	// derived stats
	IStatistics *m_pstats;

	// required plan properties
	CReqdPropPlan *m_prpp;

	// derived physical properties
	CDrvdPropPlan *m_pdpplan;

	// derived scalar properties
	CDrvdPropScalar *m_pdpscalar;

	// group reference to Memo
	CGroupExpression *m_pgexpr;

	// cost of physical expression node when copied out of the memo
	CCost m_cost;

	// id of origin group, used for debugging expressions extracted from memo
	ULONG m_ulOriginGrpId;

	// id of origin group expression, used for debugging expressions extracted from memo
	ULONG m_ulOriginGrpExprId;

	// get expression's derived property given its type
	CDrvdProp *Pdp(const CDrvdProp::EPropType ept) const;

#ifdef GPOS_DEBUG

	// assert valid property derivation
	void AssertValidPropDerivation(const CDrvdProp::EPropType ept);

	// print expression properties
	void PrintProperties(IOstream &os, CPrintPrefix &pfx) const;

#endif	// GPOS_DEBUG

	// check if the expression satisfies partition enforcer condition
	BOOL FValidPartEnforcers(CDrvdPropCtxtPlan *pdpctxtplan);

	// check if the distributions of all children are compatible
	BOOL FValidChildrenDistribution(CDrvdPropCtxtPlan *pdpctxtplan);

	// copy group properties and stats to expression
	void CopyGroupPropsAndStats(IStatistics *input_stats);

	// decorate expression tree with required plan properties
	CReqdPropPlan *PrppDecorate(CMemoryPool *mp, CReqdPropPlan *prppInput);

	// private copy ctor
	CExpression(const CExpression &);

public:
	// ctor's with different arity

	// ctor for leaf nodes
	CExpression(CMemoryPool *mp, COperator *pop,
				CGroupExpression *pgexpr = NULL);

	// ctor for unary expressions
	CExpression(CMemoryPool *mp, COperator *pop, CExpression *pexpr);

	// ctor for binary expressions
	CExpression(CMemoryPool *mp, COperator *pop, CExpression *pexprChildFirst,
				CExpression *pexprChildSecond);

	// ctor for ternary expressions
	CExpression(CMemoryPool *mp, COperator *pop, CExpression *pexprChildFirst,
				CExpression *pexprChildSecond, CExpression *pexprChildThird);

	// ctor n-ary expressions
	CExpression(CMemoryPool *mp, COperator *pop, CExpressionArray *pdrgpexpr);

	// ctor for n-ary expression with origin group expression
	CExpression(CMemoryPool *mp, COperator *pop, CGroupExpression *pgexpr,
				CExpressionArray *pdrgpexpr, IStatistics *input_stats,
				CCost cost = GPOPT_INVALID_COST);

	// dtor
	~CExpression();

	// shorthand to access children
	CExpression *
	operator[](ULONG ulPos) const
	{
		GPOS_ASSERT(NULL != m_pdrgpexpr);
		return (*m_pdrgpexpr)[ulPos];
	};

	// arity function
	ULONG
	Arity() const
	{
		return m_pdrgpexpr == NULL ? 0 : m_pdrgpexpr->Size();
	}

	// accessor for operator
	COperator *
	Pop() const
	{
		GPOS_ASSERT(NULL != m_pop);
		return m_pop;
	}

	// accessor of children array
	CExpressionArray *
	PdrgPexpr() const
	{
		return m_pdrgpexpr;
	}

	// accessor for origin group expression
	CGroupExpression *
	Pgexpr() const
	{
		return m_pgexpr;
	}

	// accessor for computed required plan props
	CReqdPropPlan *
	Prpp() const
	{
		return m_prpp;
	}

	CDrvdPropRelational *GetDrvdPropRelational() const;

	CDrvdPropPlan *GetDrvdPropPlan() const;

	CDrvdPropScalar *GetDrvdPropScalar() const;

	// get derived statistics object
	const IStatistics *
	Pstats() const
	{
		return m_pstats;
	}

	// cost accessor
	CCost
	Cost() const
	{
		return m_cost;
	}

	// get the suitable derived property type based on operator
	CDrvdProp::EPropType Ept() const;

	// Derive all properties immediately. The suitable derived property is
	// determined internally. To derive properties on an on-demand bases, use
	// DeriveXXX() methods.
	CDrvdProp *PdpDerive(CDrvdPropCtxt *pdpctxt = NULL);

	// derive statistics
	IStatistics *PstatsDerive(CReqdPropRelational *prprel,
							  IStatisticsArray *stats_ctxt);

	// reset a derived property
	void ResetDerivedProperty(CDrvdProp::EPropType ept);

	// reset all derived properties
	void ResetDerivedProperties();

	// reset expression stats
	void ResetStats();

	// compute required plan properties of all expression nodes
	CReqdPropPlan *PrppCompute(CMemoryPool *mp, CReqdPropPlan *prppInput);

	// check for outer references
	BOOL HasOuterRefs();

	// print driver
	virtual IOstream &OsPrint(IOstream &os) const;

	// print driver, customized for expressions
	IOstream &OsPrintExpression(IOstream &os, const CPrintPrefix * = NULL,
								BOOL fLast = true) const;

	// match with group expression
	BOOL FMatchPattern(CGroupExpression *pgexpr) const;

	// return a copy of the expression with remapped columns
	CExpression *PexprCopyWithRemappedColumns(CMemoryPool *mp,
											  UlongToColRefMap *colref_mapping,
											  BOOL must_exist) const;

	// compare entire expression rooted here
	BOOL Matches(CExpression *pexpr) const;

#ifdef GPOS_DEBUG
	// match against given pattern
	BOOL FMatchPattern(CExpression *pexpr) const;

	// match children against given pattern
	BOOL FMatchPatternChildren(CExpression *pexpr) const;

	// compare entire expression rooted here
	BOOL FMatchDebug(CExpression *pexpr) const;

	// debug print; for interactive debugging sessions only
	// prints expression properties as well
	void DbgPrintWithProperties() const;
#endif	// GPOS_DEBUG

	// check if the expression satisfies given required properties
	BOOL FValidPlan(const CReqdPropPlan *prpp, CDrvdPropCtxtPlan *pdpctxtplan);

	// static hash function
	static ULONG HashValue(const CExpression *pexpr);

	// static hash function
	static ULONG UlHashDedup(const CExpression *pexpr);

	// rehydrate expression from a given cost context and child expressions
	static CExpression *PexprRehydrate(CMemoryPool *mp, CCostContext *pcc,
									   CExpressionArray *pdrgpexpr,
									   CDrvdPropCtxtPlan *pdpctxtplan);

	// Relational property accessors - derived as needed
	CColRefSet *DeriveOuterReferences();
	CColRefSet *DeriveOutputColumns();
	CColRefSet *DeriveNotNullColumns();
	CColRefSet *DeriveCorrelatedApplyColumns();
	CKeyCollection *DeriveKeyCollection();
	CPropConstraint *DerivePropertyConstraint();
	CMaxCard DeriveMaxCard();
	ULONG DeriveJoinDepth();
	CFunctionProp *DeriveFunctionProperties();
	CFunctionalDependencyArray *DeriveFunctionalDependencies();
	CPartInfo *DerivePartitionInfo();
	BOOL DeriveHasPartialIndexes();
	CTableDescriptor *DeriveTableDescriptor();

	// Scalar property accessors - derived as needed
	CColRefSet *DeriveDefinedColumns();
	CColRefSet *DeriveUsedColumns();
	CColRefSet *DeriveSetReturningFunctionColumns();
	BOOL DeriveHasSubquery();
	CPartInfo *DeriveScalarPartitionInfo();
	CFunctionProp *DeriveScalarFunctionProperties();
	BOOL DeriveHasNonScalarFunction();
	ULONG DeriveTotalDistinctAggs();
	BOOL DeriveHasMultipleDistinctAggs();
	BOOL DeriveHasScalarArrayCmp();

};	// class CExpression


// shorthand for printing
inline IOstream &
operator<<(IOstream &os, CExpression &expr)
{
	return expr.OsPrint(os);
}

// hash map from ULONG to expression
typedef CHashMap<ULONG, CExpression, gpos::HashValue<ULONG>,
				 gpos::Equals<ULONG>, CleanupDelete<ULONG>,
				 CleanupRelease<CExpression> >
	UlongToExprMap;

// map iterator
typedef CHashMapIter<ULONG, CExpression, gpos::HashValue<ULONG>,
					 gpos::Equals<ULONG>, CleanupDelete<ULONG>,
					 CleanupRelease<CExpression> >
	UlongToExprMapIter;

}  // namespace gpopt

#endif
