//---------------------------------------------------------------------------
//	@filename:
//		CXformUtils.h
//
//	@doc:
//		Utility functions for xforms
//---------------------------------------------------------------------------
#ifndef GPOPT_CXformUtils_H
#define GPOPT_CXformUtils_H

#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/base/CCastUtils.h"
#include "duckdb/optimizer/cascade/base/CColRef.h"
#include "duckdb/optimizer/cascade/base/CUtils.h"
#include "duckdb/optimizer/cascade/operators/ops.h"
#include "duckdb/optimizer/cascade/xforms/CXform.h"

namespace gpopt
{
using namespace gpos;

// forward declarations
class CGroupExpression;
class CColRefSet;
class CExpression;
class CLogical;
class CLogicalDynamicGet;
class CPartConstraint;
class CTableDescriptor;

// structure describing a candidate for a partial dynamic index scan
struct SPartDynamicIndexGetInfo
{
	// md index
	const IMDIndex *m_pmdindex;

	// part constraint
	CPartConstraint *m_part_constraint;

	// index predicate expressions
	CExpressionArray *m_pdrgpexprIndex;

	// residual expressions
	CExpressionArray *m_pdrgpexprResidual;

	// ctor
	SPartDynamicIndexGetInfo(const IMDIndex *pmdindex,
							 CPartConstraint *ppartcnstr,
							 CExpressionArray *pdrgpexprIndex,
							 CExpressionArray *pdrgpexprResidual)
		: m_pmdindex(pmdindex),
		  m_part_constraint(ppartcnstr),
		  m_pdrgpexprIndex(pdrgpexprIndex),
		  m_pdrgpexprResidual(pdrgpexprResidual)
	{
		GPOS_ASSERT(NULL != ppartcnstr);
	}

	// dtor
	~SPartDynamicIndexGetInfo()
	{
		m_part_constraint->Release();
		CRefCount::SafeRelease(m_pdrgpexprIndex);
		CRefCount::SafeRelease(m_pdrgpexprResidual);
	}
};

// arrays over partial dynamic index get candidates
typedef CDynamicPtrArray<SPartDynamicIndexGetInfo, CleanupDelete>
	SPartDynamicIndexGetInfoArray;
typedef CDynamicPtrArray<SPartDynamicIndexGetInfoArray, CleanupRelease>
	SPartDynamicIndexGetInfoArrays;

// map of expression to array of expressions
typedef CHashMap<CExpression, CExpressionArray, CExpression::HashValue,
				 CUtils::Equals, CleanupRelease<CExpression>,
				 CleanupRelease<CExpressionArray> >
	ExprToExprArrayMap;

// iterator of map of expression to array of expressions
typedef CHashMapIter<CExpression, CExpressionArray, CExpression::HashValue,
					 CUtils::Equals, CleanupRelease<CExpression>,
					 CleanupRelease<CExpressionArray> >
	ExprToExprArrayMapIter;

// array of array of expressions
typedef CDynamicPtrArray<CExpressionArray, CleanupRelease> CExpressionArrays;

//---------------------------------------------------------------------------
//	@class:
//		CXformUtils
//
//	@doc:
//		Utility functions for xforms
//
//---------------------------------------------------------------------------
class CXformUtils
{
private:
	// enum marking the index column types
	enum EIndexCols
	{
		EicKey,
		EicIncluded
	};

	typedef CLogical *(*PDynamicIndexOpConstructor)(
		CMemoryPool *mp, const IMDIndex *pmdindex, CTableDescriptor *ptabdesc,
		ULONG ulOriginOpId, CName *pname, ULONG ulPartIndex,
		CColRefArray *pdrgpcrOutput, CColRef2dArray *pdrgpdrgpcrPart,
		ULONG ulSecondaryPartIndexId, CPartConstraint *ppartcnstr,
		CPartConstraint *ppartcnstrRel);

	typedef CLogical *(*PStaticIndexOpConstructor)(
		CMemoryPool *mp, const IMDIndex *pmdindex, CTableDescriptor *ptabdesc,
		ULONG ulOriginOpId, CName *pname, CColRefArray *pdrgpcrOutput);

	typedef CExpression *(PRewrittenIndexPath)(CMemoryPool *mp, CExpression *pexprIndexCond, CExpression *pexprResidualCond, const IMDIndex *pmdindex, CTableDescriptor *ptabdesc, COperator *popLogical);

    // Check if given xform id is in the given array of xforms
	static BOOL FXformInArray(CXform::EXformId exfid, CXform::EXformId rgXforms[], ULONG ulXforms);
    
	// Comparator used in sorting arrays of project elements based on the column id of the first entry
	static INT ICmpPrjElemsArr(const void *pvFst, const void *pvSnd);

public:
    // return true if stats derivation is needed for this xform
    static BOOL CXformUtils::FDeriveStatsBeforeXform(CXform *pxform);

    // return true if xform is a subquery decorrelation xform
	static BOOL FSubqueryDecorrelation(CXform *pxform);
    
    // return true if xform is a subquery unnesting xform
	static BOOL FSubqueryUnnesting(CXform *pxform);
    
    // return true if xform should be applied to the next binding
    static BOOL FApplyToNextBinding(CXform* pxform, CExpression* pexprLastBinding /* last extracted xform pattern */);

	// Helper for adding CTE producer to global CTE info structure
	static CExpression* PexprAddCTEProducer(CMemoryPool *mp, ULONG ulCTEId, CColRefArray *colref_array, CExpression *pexpr);

	// Does transformation generate an Apply expression
	static BOOL FGenerateApply(CXform::EXformId exfid)
	{
		return CXform::ExfSelect2Apply == exfid || CXform::ExfProject2Apply == exfid || CXform::ExfGbAgg2Apply == exfid || CXform::ExfSubqJoin2Apply == exfid || CXform::ExfSubqNAryJoin2Apply == exfid || CXform::ExfSequenceProject2Apply == exfid;
	}

	// convert an Agg window function into regular Agg
	static CExpression* PexprWinFuncAgg2ScalarAgg(CMemoryPool *mp, CExpression *pexprWinFunc);

	// Create a map from the argument of each Distinct Agg to the array of project elements that define Distinct Aggs on the same argument
	static void MapPrjElemsWithDistinctAggs(CMemoryPool *mp, CExpression *pexprPrjList, ExprToExprArrayMap **pphmexprdrgpexpr, ULONG *pulDifferentDQAs);
	
	static INT ICmpPrjElemsArr(const void *pvFst, const void *pvSnd);
	
	// Create a map from the argument of each Distinct Agg to the array of project elements that define Distinct Aggs on the same argument
	static CExpressionArrays* PdrgpdrgpexprSortedPrjElemsArray(CMemoryPool *mp, ExprToExprArrayMap *phmexprdrgpexpr);

	// Convert GbAgg with distinct aggregates to a join
	static CExpression *PexprGbAggOnCTEConsumer2Join(CMemoryPool *mp, CExpression *pexprGbAgg);
};	// class CXformUtils
}

#endif 