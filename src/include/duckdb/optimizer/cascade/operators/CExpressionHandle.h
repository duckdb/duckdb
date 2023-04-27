//---------------------------------------------------------------------------
//	@filename:
//		CExpressionHandle.h
//
//	@doc:
//		Handle to convey context wherever an expression is used in a shallow
//		context, i.e. operator and the properties of its children but no
//		access to the children is needed.
//---------------------------------------------------------------------------
#ifndef GPOPT_CExpressionHandle_H
#define GPOPT_CExpressionHandle_H

#include "gpos/base.h"
#include "gpos/common/CDynamicPtrArray.h"
#include "gpos/common/CRefCount.h"

#include "gpopt/base/CDrvdProp.h"
#include "gpopt/base/CDrvdPropRelational.h"
#include "gpopt/base/CReqdProp.h"
#include "gpopt/operators/CExpression.h"
#include "gpopt/search/CGroupExpression.h"
#include "naucrates/statistics/CStatistics.h"


namespace gpopt
{
// fwd declaration
class COperator;
class CDrvdPropPlan;
class CDrvdPropScalar;
class CColRefSet;
class CPropConstraint;
class CCostContext;

using namespace gpos;



//---------------------------------------------------------------------------
//	@class:
//		CExpressionHandle
//
//	@doc:
//		Context for expression; abstraction for group expressions and
//		stand-alone expressions/DAGs;
//		a handle is attached to either an expression or a group expression
//
//---------------------------------------------------------------------------
class CExpressionHandle
{
	friend class CExpression;

private:
	// memory pool
	CMemoryPool *m_mp;

	// attached expression
	CExpression *m_pexpr;

	// attached group expression
	CGroupExpression *m_pgexpr;

	// attached cost context
	CCostContext *m_pcc;

	// derived plan properties of the gexpr attached by a CostContext under
	// the default CDrvdPropCtxtPlan. See DerivePlanPropsForCostContext()
	// NB: does NOT support on-demand property derivation
	CDrvdProp *m_pdpplan;

	// statistics of attached expr/gexpr;
	// set during derived stats computation
	IStatistics *m_pstats;

	// required properties of attached expr/gexpr;
	// set during required property computation
	CReqdProp *m_prp;

	// array of children's derived stats
	IStatisticsArray *m_pdrgpstat;

	// array of children's required properties
	CReqdPropArray *m_pdrgprp;

	// private copy ctor
	CExpressionHandle(const CExpressionHandle &);

	// return an array of stats objects starting from the first stats object referenced by child
	IStatisticsArray *PdrgpstatOuterRefs(IStatisticsArray *statistics_array,
										 ULONG child_index);

	// check if stats are derived for attached expression and its children
	BOOL FStatsDerived() const;

	// copy stats from attached expression/group expression to local stats members
	void CopyStats();

	// return True if handle is attached to a leaf pattern
	BOOL FAttachedToLeafPattern() const;

	// stat derivation at root operator where handle is attached
	void DeriveRootStats(IStatisticsArray *stats_ctxt);

public:
	// ctor
	explicit CExpressionHandle(CMemoryPool *mp);

	// dtor
	~CExpressionHandle();

	// attach handle to a given expression
	void Attach(CExpression *pexpr);

	// attach handle to a given group expression
	void Attach(CGroupExpression *pgexpr);

	// attach handle to a given cost context
	void Attach(CCostContext *pcc);

	// recursive property derivation,
	void DeriveProps(CDrvdPropCtxt *pdpctxt);

	// recursive stats derivation
	void DeriveStats(IStatisticsArray *stats_ctxt,
					 BOOL fComputeRootStats = true);

	// stats derivation for attached cost context
	void DeriveCostContextStats();

	// stats derivation using given properties and context
	void DeriveStats(CMemoryPool *pmpLocal, CMemoryPool *pmpGlobal,
					 CReqdPropRelational *prprel, IStatisticsArray *stats_ctxt);

	// derive the properties of the plan carried by attached cost context,
	// using default CDrvdPropCtxtPlan
	void DerivePlanPropsForCostContext();

	// initialize required properties container
	void InitReqdProps(CReqdProp *prpInput);

	// compute required properties of the n-th child
	void ComputeChildReqdProps(ULONG child_index, CDrvdPropArray *pdrgpdpCtxt,
							   ULONG ulOptReq);

	// copy required properties of the n-th child
	void CopyChildReqdProps(ULONG child_index, CReqdProp *prp);

	// compute required columns of the n-th child
	void ComputeChildReqdCols(ULONG child_index, CDrvdPropArray *pdrgpdpCtxt);

	// required properties computation of all children
	void ComputeReqdProps(CReqdProp *prpInput, ULONG ulOptReq);

	// derived relational props of n-th child
	CDrvdPropRelational *GetRelationalProperties(ULONG child_index) const;

	// derived stats of n-th child
	IStatistics *Pstats(ULONG child_index) const;

	// derived plan props of n-th child
	CDrvdPropPlan *Pdpplan(ULONG child_index) const;

	// derived scalar props of n-th child
	CDrvdPropScalar *GetDrvdScalarProps(ULONG child_index) const;

	// derived properties of attached expr/gexpr
	CDrvdProp *Pdp() const;

	// derived relational properties of attached expr/gexpr
	CDrvdPropRelational *GetRelationalProperties() const;

	// stats of attached expr/gexpr
	IStatistics *Pstats();

	// required properties of attached expr/gexpr
	CReqdProp *
	Prp() const
	{
		return m_prp;
	}

	// check if given child is a scalar
	BOOL FScalarChild(ULONG child_index) const;

	// required relational props of n-th child
	CReqdPropRelational *GetReqdRelationalProps(ULONG child_index) const;

	// required plan props of n-th child
	CReqdPropPlan *Prpp(ULONG child_index) const;

	// arity function
	ULONG Arity() const;

	// index of the last non-scalar child
	ULONG UlLastNonScalarChild() const;

	// index of the first non-scalar child
	ULONG UlFirstNonScalarChild() const;

	// number of non-scalar children
	ULONG UlNonScalarChildren() const;

	// accessor for operator
	COperator *Pop() const;

	// accessor for child operator
	COperator *Pop(ULONG child_index) const;

	// accessor for grandchild operator
	COperator *PopGrandchild(ULONG child_index, ULONG grandchild_index,
							 CCostContext **grandchildContext) const;

	// accessor for expression
	CExpression *
	Pexpr() const
	{
		return m_pexpr;
	}

	// accessor for group expression
	CGroupExpression *
	Pgexpr() const
	{
		return m_pgexpr;
	}

	// check for outer references
	BOOL
	HasOuterRefs()
	{
		return (0 < DeriveOuterReferences()->Size());
	}

	// check if attached expression must execute on a single host
	BOOL
	NeedsSingletonExecution()
	{
		return DeriveFunctionProperties()->NeedsSingletonExecution();
	}

	// check for outer references in the given child
	BOOL
	HasOuterRefs(ULONG child_index)
	{
		return (0 < DeriveOuterReferences(child_index)->Size());
	}

	// get next child index based on child optimization order, return true if such index could be found
	BOOL FNextChildIndex(ULONG *pulChildIndex  // output: index to be changed
	) const;

	// return the index of first child to be optimized
	ULONG UlFirstOptimizedChildIndex() const;

	// return the index of last child to be optimized
	ULONG UlLastOptimizedChildIndex() const;

	// return the index of child to be optimized next to the given child
	ULONG UlNextOptimizedChildIndex(ULONG child_index) const;

	// return the index of child optimized before the given child
	ULONG UlPreviousOptimizedChildIndex(ULONG child_index) const;

	// get the function properties of a child
	CFunctionProp *PfpChild(ULONG child_index);

	// check whether an expression's children have a volatile function
	BOOL FChildrenHaveVolatileFuncScan();

	// return a representative (inexact) scalar child at given index
	CExpression *PexprScalarRepChild(ULONG child_index) const;

	// return a representative (inexact) scalar expression attached to handle
	CExpression *PexprScalarRep() const;

	// return an exact scalar child at given index or return null if not possible
	CExpression *PexprScalarExactChild(ULONG child_index,
									   BOOL error_on_null_return = false) const;

	// return an exact scalar expression attached to handle or null if not possible
	CExpression *PexprScalarExact() const;

	void DeriveProducerStats(ULONG child_index, CColRefSet *pcrsStat);

	// return the columns used by a logical operator internally as well
	// as columns used by all its scalar children
	CColRefSet *PcrsUsedColumns(CMemoryPool *mp);

	CColRefSet *DeriveOuterReferences();
	CColRefSet *DeriveOuterReferences(ULONG child_index);

	CColRefSet *DeriveOutputColumns();
	CColRefSet *DeriveOutputColumns(ULONG child_index);

	CColRefSet *DeriveNotNullColumns();
	CColRefSet *DeriveNotNullColumns(ULONG child_index);

	CColRefSet *DeriveCorrelatedApplyColumns();
	CColRefSet *DeriveCorrelatedApplyColumns(ULONG child_index);

	CMaxCard DeriveMaxCard();
	CMaxCard DeriveMaxCard(ULONG child_index);

	CKeyCollection *DeriveKeyCollection();
	CKeyCollection *DeriveKeyCollection(ULONG child_index);

	CPropConstraint *DerivePropertyConstraint();
	CPropConstraint *DerivePropertyConstraint(ULONG child_index);

	ULONG DeriveJoinDepth();
	ULONG DeriveJoinDepth(ULONG child_index);

	CFunctionProp *DeriveFunctionProperties();
	CFunctionProp *DeriveFunctionProperties(ULONG child_index);

	CFunctionalDependencyArray *Pdrgpfd();
	CFunctionalDependencyArray *Pdrgpfd(ULONG child_index);

	CPartInfo *DerivePartitionInfo();
	CPartInfo *DerivePartitionInfo(ULONG child_index);

	BOOL DeriveHasPartialIndexes();
	BOOL DeriveHasPartialIndexes(ULONG child_index);

	CTableDescriptor *DeriveTableDescriptor();
	CTableDescriptor *DeriveTableDescriptor(ULONG child_index);

	// Scalar property accessors
	CColRefSet *DeriveDefinedColumns(ULONG child_index);
	CColRefSet *DeriveUsedColumns(ULONG child_index);
	CColRefSet *DeriveSetReturningFunctionColumns(ULONG child_index);
	BOOL DeriveHasSubquery(ULONG child_index);
	CPartInfo *DeriveScalarPartitionInfo(ULONG child_index);
	CFunctionProp *DeriveScalarFunctionProperties(ULONG child_index);
	BOOL DeriveHasNonScalarFunction(ULONG child_index);
	ULONG DeriveTotalDistinctAggs(ULONG child_index);
	BOOL DeriveHasMultipleDistinctAggs(ULONG child_index);
	BOOL DeriveHasScalarArrayCmp(ULONG child_index);

};	// class CExpressionHandle

}  // namespace gpopt

#endif
