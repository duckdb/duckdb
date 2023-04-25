//---------------------------------------------------------------------------
//	@filename:
//		COperator.h
//
//	@doc:
//		Base class for all operators: logical, physical, scalar, patterns
//---------------------------------------------------------------------------
#ifndef GPOPT_COperator_H
#define GPOPT_COperator_H

#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/common/CHashMap.h"
#include "duckdb/optimizer/cascade/common/CRefCount.h"

#include "duckdb/optimizer/cascade/base/CColRefSet.h"
#include "duckdb/optimizer/cascade/base/CDrvdProp.h"
#include "duckdb/optimizer/cascade/base/CFunctionProp.h"
#include "duckdb/optimizer/cascade/base/CReqdPropPlan.h"
#include "duckdb/optimizer/cascade/base/CReqdPropRelational.h"
#include "duckdb/optimizer/cascade/statistics/CStatistics.h"

namespace gpopt
{
using namespace gpos;

// forward declarations
class CExpressionHandle;
class CReqdPropPlan;
class CReqdPropRelational;

// dynamic array for operators
typedef CDynamicPtrArray<COperator, CleanupRelease> COperatorArray;

// hash map mapping CColRef -> CColRef
typedef CHashMap<CColRef, CColRef, CColRef::HashValue, CColRef::Equals,
				 CleanupNULL<CColRef>, CleanupNULL<CColRef> >
	ColRefToColRefMap;

//---------------------------------------------------------------------------
//	@class:
//		COperator
//
//	@doc:
//		base class for all operators
//
//---------------------------------------------------------------------------
class COperator : public CRefCount
{
private:
	// private copy ctor
	COperator(COperator &);

protected:
	// operator id that is unique over all instances of all operator types
	// for the current query
	ULONG m_ulOpId;

	// memory pool for internal allocations
	CMemoryPool *m_mp;

	// is pattern of xform
	BOOL m_fPattern;

	// return an addref'ed copy of the operator
	virtual COperator *PopCopyDefault();

	// derive data access function property from children
	static IMDFunction::EFuncDataAcc EfdaDeriveFromChildren(
		CExpressionHandle &exprhdl, IMDFunction::EFuncDataAcc efdaDefault);

	// derive stability function property from children
	static IMDFunction::EFuncStbl EfsDeriveFromChildren(
		CExpressionHandle &exprhdl, IMDFunction::EFuncStbl efsDefault);

	// derive function properties from children
	static CFunctionProp *PfpDeriveFromChildren(
		CMemoryPool *mp, CExpressionHandle &exprhdl,
		IMDFunction::EFuncStbl efsDefault,
		IMDFunction::EFuncDataAcc efdaDefault, BOOL fHasVolatileFunctionScan,
		BOOL fScan);

	// generate unique operator ids
	static ULONG m_aulOpIdCounter;

public:
	// identification
	enum EOperatorId
	{
		EopLogicalGet,
		EopLogicalExternalGet,
		EopLogicalIndexGet,
		EopLogicalBitmapTableGet,
		EopLogicalSelect,
		EopLogicalUnion,
		EopLogicalUnionAll,
		EopLogicalIntersect,
		EopLogicalIntersectAll,
		EopLogicalDifference,
		EopLogicalDifferenceAll,
		EopLogicalInnerJoin,
		EopLogicalNAryJoin,
		EopLogicalLeftOuterJoin,
		EopLogicalLeftSemiJoin,
		EopLogicalLeftAntiSemiJoin,
		EopLogicalLeftAntiSemiJoinNotIn,
		EopLogicalFullOuterJoin,
		EopLogicalGbAgg,
		EopLogicalGbAggDeduplicate,
		EopLogicalLimit,
		EopLogicalProject,
		EopLogicalRename,
		EopLogicalInnerApply,
		EopLogicalInnerCorrelatedApply,
		EopLogicalIndexApply,
		EopLogicalLeftOuterApply,
		EopLogicalLeftOuterCorrelatedApply,
		EopLogicalLeftSemiApply,
		EopLogicalLeftSemiCorrelatedApply,
		EopLogicalLeftSemiApplyIn,
		EopLogicalLeftSemiCorrelatedApplyIn,
		EopLogicalLeftAntiSemiApply,
		EopLogicalLeftAntiSemiCorrelatedApply,
		EopLogicalLeftAntiSemiApplyNotIn,
		EopLogicalLeftAntiSemiCorrelatedApplyNotIn,
		EopLogicalConstTableGet,
		EopLogicalDynamicGet,
		EopLogicalDynamicIndexGet,
		EopLogicalSequence,
		EopLogicalTVF,
		EopLogicalCTEAnchor,
		EopLogicalCTEProducer,
		EopLogicalCTEConsumer,
		EopLogicalSequenceProject,
		EopLogicalInsert,
		EopLogicalDelete,
		EopLogicalUpdate,
		EopLogicalDML,
		EopLogicalSplit,
		EopLogicalRowTrigger,
		EopLogicalPartitionSelector,
		EopLogicalAssert,
		EopLogicalMaxOneRow,

		EopScalarCmp,
		EopScalarIsDistinctFrom,
		EopScalarIdent,
		EopScalarProjectElement,
		EopScalarProjectList,
		EopScalarNAryJoinPredList,
		EopScalarConst,
		EopScalarBoolOp,
		EopScalarFunc,
		EopScalarMinMax,
		EopScalarAggFunc,
		EopScalarWindowFunc,
		EopScalarOp,
		EopScalarNullIf,
		EopScalarNullTest,
		EopScalarBooleanTest,
		EopScalarIf,
		EopScalarSwitch,
		EopScalarSwitchCase,
		EopScalarCaseTest,
		EopScalarCast,
		EopScalarCoerceToDomain,
		EopScalarCoerceViaIO,
		EopScalarArrayCoerceExpr,
		EopScalarCoalesce,
		EopScalarArray,
		EopScalarArrayCmp,
		EopScalarArrayRef,
		EopScalarArrayRefIndexList,

		EopScalarAssertConstraintList,
		EopScalarAssertConstraint,

		EopScalarSubquery,
		EopScalarSubqueryAny,
		EopScalarSubqueryAll,
		EopScalarSubqueryExists,
		EopScalarSubqueryNotExists,

		EopScalarDMLAction,

		EopScalarBitmapIndexProbe,
		EopScalarBitmapBoolOp,

		EopPhysicalTableScan,
		EopPhysicalExternalScan,
		EopPhysicalIndexScan,
		EopPhysicalBitmapTableScan,
		EopPhysicalFilter,
		EopPhysicalInnerNLJoin,
		EopPhysicalInnerIndexNLJoin,
		EopPhysicalCorrelatedInnerNLJoin,
		EopPhysicalLeftOuterNLJoin,
		EopPhysicalLeftOuterIndexNLJoin,
		EopPhysicalCorrelatedLeftOuterNLJoin,
		EopPhysicalLeftSemiNLJoin,
		EopPhysicalCorrelatedLeftSemiNLJoin,
		EopPhysicalCorrelatedInLeftSemiNLJoin,
		EopPhysicalLeftAntiSemiNLJoin,
		EopPhysicalCorrelatedLeftAntiSemiNLJoin,
		EopPhysicalLeftAntiSemiNLJoinNotIn,
		EopPhysicalCorrelatedNotInLeftAntiSemiNLJoin,
		EopPhysicalFullMergeJoin,
		EopPhysicalDynamicTableScan,
		EopPhysicalSequence,
		EopPhysicalTVF,
		EopPhysicalCTEProducer,
		EopPhysicalCTEConsumer,
		EopPhysicalSequenceProject,
		EopPhysicalDynamicIndexScan,

		EopPhysicalInnerHashJoin,
		EopPhysicalLeftOuterHashJoin,
		EopPhysicalLeftSemiHashJoin,
		EopPhysicalLeftAntiSemiHashJoin,
		EopPhysicalLeftAntiSemiHashJoinNotIn,

		EopPhysicalMotionGather,
		EopPhysicalMotionBroadcast,
		EopPhysicalMotionHashDistribute,
		EopPhysicalMotionRoutedDistribute,
		EopPhysicalMotionRandom,

		EopPhysicalHashAgg,
		EopPhysicalHashAggDeduplicate,
		EopPhysicalStreamAgg,
		EopPhysicalStreamAggDeduplicate,
		EopPhysicalScalarAgg,

		EopPhysicalSerialUnionAll,
		EopPhysicalParallelUnionAll,

		EopPhysicalSort,
		EopPhysicalLimit,
		EopPhysicalComputeScalar,
		EopPhysicalSpool,
		EopPhysicalPartitionSelector,
		EopPhysicalPartitionSelectorDML,

		EopPhysicalConstTableGet,

		EopPhysicalDML,
		EopPhysicalSplit,
		EopPhysicalRowTrigger,

		EopPhysicalAssert,

		EopPatternTree,
		EopPatternLeaf,
		EopPatternMultiLeaf,
		EopPatternMultiTree,

		EopLogicalDynamicBitmapTableGet,
		EopPhysicalDynamicBitmapTableScan,

		EopSentinel
	};

	// aggregate type
	enum EGbAggType
	{
		EgbaggtypeGlobal,		 // global group by aggregate
		EgbaggtypeLocal,		 // local group by aggregate
		EgbaggtypeIntermediate,	 // intermediate group by aggregate

		EgbaggtypeSentinel
	};

	// coercion form
	enum ECoercionForm
	{
		EcfExplicitCall,  // display as a function call
		EcfExplicitCast,  // display as an explicit cast
		EcfImplicitCast,  // implicit cast, so hide it
		EcfDontCare		  // don't care about display
	};

	// ctor
	explicit COperator(CMemoryPool *mp);

	// dtor
	virtual ~COperator()
	{
	}

	// the id of the operator
	ULONG
	UlOpId() const
	{
		return m_ulOpId;
	}

	// ident accessors
	virtual EOperatorId Eopid() const = 0;

	// return a string for operator name
	virtual const CHAR *SzId() const = 0;

	// the following functions check operator's type

	// is operator logical?
	virtual BOOL
	FLogical() const
	{
		return false;
	}

	// is operator physical?
	virtual BOOL
	FPhysical() const
	{
		return false;
	}

	// is operator scalar?
	virtual BOOL
	FScalar() const
	{
		return false;
	}

	// is operator pattern?
	virtual BOOL
	FPattern() const
	{
		return false;
	}

	// hash function
	virtual ULONG HashValue() const;

	// sensitivity to order of inputs
	virtual BOOL FInputOrderSensitive() const = 0;

	// match function;
	// abstract to enforce an implementation for each new operator
	virtual BOOL Matches(COperator *pop) const = 0;

	// create container for derived properties
	virtual CDrvdProp *PdpCreate(CMemoryPool *mp) const = 0;

	// create container for required properties
	virtual CReqdProp *PrpCreate(CMemoryPool *mp) const = 0;

	// return a copy of the operator with remapped columns
	virtual COperator *PopCopyWithRemappedColumns(
		CMemoryPool *mp, UlongToColRefMap *colref_mapping, BOOL must_exist) = 0;

	// print
	virtual IOstream &OsPrint(IOstream &os) const;

};	// class COperator

}  // namespace gpopt

#endif
