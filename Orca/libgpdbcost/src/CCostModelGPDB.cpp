//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 Pivotal Inc.
//
//	@filename:
//		CCostModelGPDB.cpp
//
//	@doc:
//		Implementation of GPDB cost model
//---------------------------------------------------------------------------

#include "gpdbcost/CCostModelGPDB.h"

#include "gpopt/base/CColRefSetIter.h"
#include "gpopt/base/COrderSpec.h"
#include "gpopt/base/CWindowFrame.h"
#include "gpopt/engine/CHint.h"
#include "gpopt/metadata/CIndexDescriptor.h"
#include "gpopt/metadata/CTableDescriptor.h"
#include "gpopt/operators/CExpression.h"
#include "gpopt/operators/CExpressionHandle.h"
#include "gpopt/operators/CPhysicalDynamicIndexScan.h"
#include "gpopt/operators/CPhysicalHashAgg.h"
#include "gpopt/operators/CPhysicalIndexScan.h"
#include "gpopt/operators/CPhysicalMotion.h"
#include "gpopt/operators/CPhysicalPartitionSelector.h"
#include "gpopt/operators/CPhysicalSequenceProject.h"
#include "gpopt/operators/CPhysicalUnionAll.h"
#include "gpopt/operators/CPredicateUtils.h"
#include "gpopt/operators/CScalarBitmapIndexProbe.h"
#include "gpopt/optimizer/COptimizerConfig.h"
#include "naucrates/statistics/CStatisticsUtils.h"

using namespace gpos;
using namespace gpdbcost;


// initialization of cost functions
const CCostModelGPDB::SCostMapping CCostModelGPDB::m_rgcm[] = {
	{COperator::EopPhysicalTableScan, CostScan},
	{COperator::EopPhysicalDynamicTableScan, CostScan},
	{COperator::EopPhysicalExternalScan, CostScan},

	{COperator::EopPhysicalFilter, CostFilter},

	{COperator::EopPhysicalIndexScan, CostIndexScan},
	{COperator::EopPhysicalDynamicIndexScan, CostIndexScan},
	{COperator::EopPhysicalBitmapTableScan, CostBitmapTableScan},
	{COperator::EopPhysicalDynamicBitmapTableScan, CostBitmapTableScan},

	{COperator::EopPhysicalSequenceProject, CostSequenceProject},

	{COperator::EopPhysicalCTEProducer, CostCTEProducer},
	{COperator::EopPhysicalCTEConsumer, CostCTEConsumer},
	{COperator::EopPhysicalConstTableGet, CostConstTableGet},
	{COperator::EopPhysicalDML, CostDML},

	{COperator::EopPhysicalHashAgg, CostHashAgg},
	{COperator::EopPhysicalHashAggDeduplicate, CostHashAgg},
	{COperator::EopPhysicalScalarAgg, CostScalarAgg},
	{COperator::EopPhysicalStreamAgg, CostStreamAgg},
	{COperator::EopPhysicalStreamAggDeduplicate, CostStreamAgg},

	{COperator::EopPhysicalSequence, CostSequence},

	{COperator::EopPhysicalSort, CostSort},

	{COperator::EopPhysicalTVF, CostTVF},

	{COperator::EopPhysicalSerialUnionAll, CostUnionAll},
	{COperator::EopPhysicalParallelUnionAll, CostUnionAll},

	{COperator::EopPhysicalInnerHashJoin, CostHashJoin},
	{COperator::EopPhysicalLeftSemiHashJoin, CostHashJoin},
	{COperator::EopPhysicalLeftAntiSemiHashJoin, CostHashJoin},
	{COperator::EopPhysicalLeftAntiSemiHashJoinNotIn, CostHashJoin},
	{COperator::EopPhysicalLeftOuterHashJoin, CostHashJoin},

	{COperator::EopPhysicalInnerIndexNLJoin, CostIndexNLJoin},
	{COperator::EopPhysicalLeftOuterIndexNLJoin, CostIndexNLJoin},

	{COperator::EopPhysicalMotionGather, CostMotion},
	{COperator::EopPhysicalMotionBroadcast, CostMotion},
	{COperator::EopPhysicalMotionHashDistribute, CostMotion},
	{COperator::EopPhysicalMotionRandom, CostMotion},
	{COperator::EopPhysicalMotionRoutedDistribute, CostMotion},

	{COperator::EopPhysicalInnerNLJoin, CostNLJoin},
	{COperator::EopPhysicalLeftSemiNLJoin, CostNLJoin},
	{COperator::EopPhysicalLeftAntiSemiNLJoin, CostNLJoin},
	{COperator::EopPhysicalLeftAntiSemiNLJoinNotIn, CostNLJoin},
	{COperator::EopPhysicalLeftOuterNLJoin, CostNLJoin},
	{COperator::EopPhysicalCorrelatedInnerNLJoin, CostNLJoin},
	{COperator::EopPhysicalCorrelatedLeftOuterNLJoin, CostNLJoin},
	{COperator::EopPhysicalCorrelatedLeftSemiNLJoin, CostNLJoin},
	{COperator::EopPhysicalCorrelatedInLeftSemiNLJoin, CostNLJoin},
	{COperator::EopPhysicalCorrelatedLeftAntiSemiNLJoin, CostNLJoin},
	{COperator::EopPhysicalCorrelatedNotInLeftAntiSemiNLJoin, CostNLJoin},

	{COperator::EopPhysicalFullMergeJoin, CostMergeJoin},
};

//---------------------------------------------------------------------------
//	@function:
//		CCostModelGPDB::CCostModelGPDB
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CCostModelGPDB::CCostModelGPDB(CMemoryPool *mp, ULONG ulSegments,
							   CCostModelParamsGPDB *pcp)
	: m_mp(mp), m_num_of_segments(ulSegments)
{
	GPOS_ASSERT(0 < ulSegments);

	if (NULL == pcp)
	{
		m_cost_model_params = GPOS_NEW(mp) CCostModelParamsGPDB(mp);
	}
	else
	{
		GPOS_ASSERT(NULL != pcp);

		m_cost_model_params = pcp;
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CCostModelGPDB::DRowsPerHost
//
//	@doc:
//		Return number of rows per host
//
//---------------------------------------------------------------------------
CDouble
CCostModelGPDB::DRowsPerHost(CDouble dRowsTotal) const
{
	return dRowsTotal / m_num_of_segments;
}


//---------------------------------------------------------------------------
//	@function:
//		CCostModelGPDB::~CCostModelGPDB
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CCostModelGPDB::~CCostModelGPDB()
{
	m_cost_model_params->Release();
}


//---------------------------------------------------------------------------
//	@function:
//		CCostModelGPDB::CostTupleProcessing
//
//	@doc:
//		Cost of tuple processing
//
//---------------------------------------------------------------------------
CCost
CCostModelGPDB::CostTupleProcessing(DOUBLE rows, DOUBLE width,
									ICostModelParams *pcp)
{
	GPOS_ASSERT(NULL != pcp);

	const CDouble dTupDefaultProcCostUnit =
		pcp->PcpLookup(CCostModelParamsGPDB::EcpTupDefaultProcCostUnit)->Get();
	GPOS_ASSERT(0 < dTupDefaultProcCostUnit);

	return CCost(rows * width * dTupDefaultProcCostUnit);
}


//
//---------------------------------------------------------------------------
//	@function:
//		CCostModelGPDB::CostScanOutput
//
//	@doc:
//		Helper function to return cost of producing output tuples from
//		Scan operator
//
//---------------------------------------------------------------------------
CCost
CCostModelGPDB::CostScanOutput(CMemoryPool *,  // mp
							   DOUBLE rows, DOUBLE width, DOUBLE num_rebinds,
							   ICostModelParams *pcp)
{
	GPOS_ASSERT(NULL != pcp);

	const CDouble dOutputTupCostUnit =
		pcp->PcpLookup(CCostModelParamsGPDB::EcpOutputTupCostUnit)->Get();
	GPOS_ASSERT(0 < dOutputTupCostUnit);

	return CCost(num_rebinds * (rows * width * dOutputTupCostUnit));
}


//---------------------------------------------------------------------------
//	@function:
//		CCostModelGPDB::CostUnary
//
//	@doc:
//		Helper function to return cost of a plan rooted by unary operator
//
//---------------------------------------------------------------------------
CCost
CCostModelGPDB::CostUnary(CMemoryPool *mp, CExpressionHandle &exprhdl,
						  const SCostingInfo *pci, ICostModelParams *pcp)
{
	GPOS_ASSERT(NULL != pci);
	GPOS_ASSERT(NULL != pcp);

	DOUBLE rows = pci->Rows();
	DOUBLE width = pci->Width();
	DOUBLE num_rebinds = pci->NumRebinds();

	CCost costLocal =
		CCost(num_rebinds * CostTupleProcessing(rows, width, pcp).Get());
	CCost costChild = CostChildren(mp, exprhdl, pci, pcp);

	return costLocal + costChild;
}


//---------------------------------------------------------------------------
//	@function:
//		CCostModelGPDB::CostSpooling
//
//	@doc:
//		Helper function to compute spooling cost
//
//---------------------------------------------------------------------------
CCost
CCostModelGPDB::CostSpooling(CMemoryPool *mp, CExpressionHandle &exprhdl,
							 const SCostingInfo *pci, ICostModelParams *pcp)
{
	GPOS_ASSERT(NULL != pci);
	GPOS_ASSERT(NULL != pcp);

	const CDouble dMaterializeCostUnit =
		pcp->PcpLookup(CCostModelParamsGPDB::EcpMaterializeCostUnit)->Get();
	GPOS_ASSERT(0 < dMaterializeCostUnit);

	DOUBLE rows = pci->Rows();
	DOUBLE width = pci->Width();
	DOUBLE num_rebinds = pci->NumRebinds();

	// materialization cost is correlated with the number of rows and width of returning tuples.
	CCost costLocal =
		CCost(num_rebinds * (width * rows * dMaterializeCostUnit));
	CCost costChild = CostChildren(mp, exprhdl, pci, pcp);

	return costLocal + costChild;
}

//---------------------------------------------------------------------------
//	@function:
//		CCostModelGPDB::FUnary
//
//	@doc:
//		Check if given operator is unary
//
//---------------------------------------------------------------------------
BOOL
CCostModelGPDB::FUnary(COperator::EOperatorId op_id)
{
	return COperator::EopPhysicalAssert == op_id ||
		   COperator::EopPhysicalComputeScalar == op_id ||
		   COperator::EopPhysicalLimit == op_id ||
		   COperator::EopPhysicalPartitionSelector == op_id ||
		   COperator::EopPhysicalPartitionSelectorDML == op_id ||
		   COperator::EopPhysicalRowTrigger == op_id ||
		   COperator::EopPhysicalSplit == op_id ||
		   COperator::EopPhysicalSpool == op_id;
}


//---------------------------------------------------------------------------
//	@function:
//		CCostModelGPDB::CostChildren
//
//	@doc:
//		Add up children costs
//
//---------------------------------------------------------------------------
CCost
CCostModelGPDB::CostChildren(CMemoryPool *mp, CExpressionHandle &exprhdl,
							 const SCostingInfo *pci, ICostModelParams *pcp)
{
	GPOS_ASSERT(NULL != pci);
	GPOS_ASSERT(NULL != pcp);

	DOUBLE *pdCost = pci->PdCost();
	const ULONG size = pci->ChildCount();
	BOOL fFilterParent =
		(COperator::EopPhysicalFilter == exprhdl.Pop()->Eopid());

	DOUBLE res = 0.0;
	for (ULONG ul = 0; ul < size; ul++)
	{
		DOUBLE dCostChild = pdCost[ul];
		COperator *popChild = exprhdl.Pop(ul);
		if (NULL != popChild &&
			(CUtils::FPhysicalScan(popChild) ||
			 COperator::EopPhysicalPartitionSelector == popChild->Eopid()))
		{
			// by default, compute scan output cost based on full Scan
			DOUBLE dScanRows = pci->PdRows()[ul];
			COperator *scanOp = popChild;

			if (fFilterParent)
			{
				CPhysicalPartitionSelector *ps =
					dynamic_cast<CPhysicalPartitionSelector *>(popChild);

				if (ps)
				{
					CCostContext *grandchildContext = NULL;

					scanOp = exprhdl.PopGrandchild(ul, 0, &grandchildContext);
					CPhysicalDynamicScan *scan =
						dynamic_cast<CPhysicalDynamicScan *>(scanOp);

					if (scan && scan->ScanId() == ps->ScanId() &&
						grandchildContext)
					{
						// We have a filter on top of a partition selector on top of a scan.
						// Base the scan output cost on the combination (filter + part sel + scan)
						// on the rows that are produced by the scan, since the runtime execution
						// plan with be sequence(part_sel, scan+filter). Note that the cost of
						// the partition selector is ignored here. It may be higher than that of
						// the complete tree (filter + part sel + scan).
						// See method CTranslatorExprToDXL::PdxlnPartitionSelectorWithInlinedCondition()
						// for how we inline the predicate into the dynamic table scan.
						dCostChild = grandchildContext->Cost().Get();
						dScanRows = pci->Rows();
					}
				}
				else
				{
					// if parent is filter, compute scan output cost based on rows produced by Filter operator
					dScanRows = pci->Rows();
				}
			}

			if (CUtils::FPhysicalScan(scanOp))
			{
				// Note: We assume that width and rebinds are the same for scan, partition selector and filter
				dCostChild = dCostChild +
							 CostScanOutput(mp, dScanRows, pci->GetWidth()[ul],
											pci->PdRebinds()[ul], pcp)
								 .Get();
			}
		}

		res = res + dCostChild;
	}

	return CCost(res);
}

//---------------------------------------------------------------------------
//	@function:
//		CCostModelGPDB::CostMaxChild
//
//	@doc:
//		Returns cost of highest costed child
//
//---------------------------------------------------------------------------

CCost
CCostModelGPDB::CostMaxChild(CMemoryPool *, CExpressionHandle &,
							 const SCostingInfo *pci, ICostModelParams *)
{
	GPOS_ASSERT(NULL != pci);

	DOUBLE *pdCost = pci->PdCost();
	const ULONG size = pci->ChildCount();

	DOUBLE res = 0.0;
	for (ULONG ul = 0; ul < size; ul++)
	{
		if (pdCost[ul] > res)
		{
			res = pdCost[ul];
		}
	}

	return CCost(res);
}

//---------------------------------------------------------------------------
//	@function:
//		CCostModelGPDB::CostCTEProducer
//
//	@doc:
//		Cost of CTE producer
//
//---------------------------------------------------------------------------
CCost
CCostModelGPDB::CostCTEProducer(CMemoryPool *mp, CExpressionHandle &exprhdl,
								const CCostModelGPDB *pcmgpdb,
								const SCostingInfo *pci)
{
	GPOS_ASSERT(NULL != pcmgpdb);
	GPOS_ASSERT(NULL != pci);
	GPOS_ASSERT(COperator::EopPhysicalCTEProducer == exprhdl.Pop()->Eopid());

	CCost cost = CostUnary(mp, exprhdl, pci, pcmgpdb->GetCostModelParams());

	// In GPDB, the child of a ShareInputScan representing the producer can
	// only be a materialize or sort. Here, we check if a materialize node
	// needs to be added during DXL->PlStmt translation

	COperator *popChild = exprhdl.Pop(0 /*child_index*/);
	if (NULL == popChild)
	{
		// child operator is not known, this could happen when computing cost bound
		return cost;
	}

	COperator::EOperatorId op_id = popChild->Eopid();
	if (COperator::EopPhysicalSpool != op_id &&
		COperator::EopPhysicalSort != op_id)
	{
		// no materialize needed
		return cost;
	}

	// a materialize (spool) node is added during DXL->PlStmt translation,
	// we need to add the cost of writing the tuples to disk
	const CDouble dMaterializeCostUnit =
		pcmgpdb->GetCostModelParams()
			->PcpLookup(CCostModelParamsGPDB::EcpMaterializeCostUnit)
			->Get();
	GPOS_ASSERT(0 < dMaterializeCostUnit);

	CCost costSpooling = CCost(pci->NumRebinds() * (pci->Rows() * pci->Width() *
													dMaterializeCostUnit));

	return cost + costSpooling;
}


//---------------------------------------------------------------------------
//	@function:
//		CCostModelGPDB::CostCTEConsumer
//
//	@doc:
//		Cost of CTE consumer
//
//---------------------------------------------------------------------------
CCost
CCostModelGPDB::CostCTEConsumer(CMemoryPool *,	// mp
								CExpressionHandle &
#ifdef GPOS_DEBUG
									exprhdl
#endif	// GPOS_DEBUG
								,
								const CCostModelGPDB *pcmgpdb,
								const SCostingInfo *pci)
{
	GPOS_ASSERT(NULL != pcmgpdb);
	GPOS_ASSERT(NULL != pci);
	GPOS_ASSERT(COperator::EopPhysicalCTEConsumer == exprhdl.Pop()->Eopid());

	const CDouble dInitScan =
		pcmgpdb->GetCostModelParams()
			->PcpLookup(CCostModelParamsGPDB::EcpInitScanFactor)
			->Get();
	const CDouble dTableScanCostUnit =
		pcmgpdb->GetCostModelParams()
			->PcpLookup(CCostModelParamsGPDB::EcpTableScanCostUnit)
			->Get();
	const CDouble dOutputTupCostUnit =
		pcmgpdb->GetCostModelParams()
			->PcpLookup(CCostModelParamsGPDB::EcpOutputTupCostUnit)
			->Get();
	GPOS_ASSERT(0 < dOutputTupCostUnit);
	GPOS_ASSERT(0 < dTableScanCostUnit);

	return CCost(pci->NumRebinds() *
				 (dInitScan + pci->Rows() * pci->Width() *
								  (dTableScanCostUnit + dOutputTupCostUnit)));
}


//---------------------------------------------------------------------------
//	@function:
//		CCostModelGPDB::CostConstTableGet
//
//	@doc:
//		Cost of const table get
//
//---------------------------------------------------------------------------
CCost
CCostModelGPDB::CostConstTableGet(CMemoryPool *,  // mp
								  CExpressionHandle &
#ifdef GPOS_DEBUG
									  exprhdl
#endif	// GPOS_DEBUG
								  ,
								  const CCostModelGPDB *pcmgpdb,
								  const SCostingInfo *pci)
{
	GPOS_ASSERT(NULL != pcmgpdb);
	GPOS_ASSERT(NULL != pci);
	GPOS_ASSERT(COperator::EopPhysicalConstTableGet == exprhdl.Pop()->Eopid());

	return CCost(pci->NumRebinds() *
				 CostTupleProcessing(pci->Rows(), pci->Width(),
									 pcmgpdb->GetCostModelParams())
					 .Get());
}


//---------------------------------------------------------------------------
//	@function:
//		CCostModelGPDB::CostDML
//
//	@doc:
//		Cost of DML
//
//---------------------------------------------------------------------------
CCost
CCostModelGPDB::CostDML(CMemoryPool *mp, CExpressionHandle &exprhdl,
						const CCostModelGPDB *pcmgpdb, const SCostingInfo *pci)
{
	GPOS_ASSERT(NULL != pcmgpdb);
	GPOS_ASSERT(NULL != pci);
	GPOS_ASSERT(COperator::EopPhysicalDML == exprhdl.Pop()->Eopid());

	const CDouble dTupUpdateBandwidth =
		pcmgpdb->GetCostModelParams()
			->PcpLookup(CCostModelParamsGPDB::EcpTupUpdateBandwith)
			->Get();
	GPOS_ASSERT(0 < dTupUpdateBandwidth);

	const DOUBLE num_rows_outer = pci->PdRows()[0];
	const DOUBLE dWidthOuter = pci->GetWidth()[0];

	CCost costLocal = CCost(pci->NumRebinds() * (num_rows_outer * dWidthOuter) /
							dTupUpdateBandwidth);
	CCost costChild =
		CostChildren(mp, exprhdl, pci, pcmgpdb->GetCostModelParams());

	return costLocal + costChild;
}


//---------------------------------------------------------------------------
//	@function:
//		CCostModelGPDB::CostScalarAgg
//
//	@doc:
//		Cost of scalar agg
//
//---------------------------------------------------------------------------
CCost
CCostModelGPDB::CostScalarAgg(CMemoryPool *mp, CExpressionHandle &exprhdl,
							  const CCostModelGPDB *pcmgpdb,
							  const SCostingInfo *pci)
{
	GPOS_ASSERT(NULL != pcmgpdb);
	GPOS_ASSERT(NULL != pci);
	GPOS_ASSERT(COperator::EopPhysicalScalarAgg == exprhdl.Pop()->Eopid());

	const DOUBLE num_rows_outer = pci->PdRows()[0];
	const DOUBLE dWidthOuter = pci->GetWidth()[0];

	// get the number of aggregate columns
	const ULONG ulAggCols = exprhdl.DeriveUsedColumns(1)->Size();
	// get the number of aggregate functions
	const ULONG ulAggFunctions = exprhdl.PexprScalarRepChild(1)->Arity();

	const CDouble dHashAggInputTupWidthCostUnit =
		pcmgpdb->GetCostModelParams()
			->PcpLookup(CCostModelParamsGPDB::EcpHashAggInputTupWidthCostUnit)
			->Get();
	GPOS_ASSERT(0 < dHashAggInputTupWidthCostUnit);

	// scalarAgg cost is correlated with rows and width of the input tuples and the number of columns used in aggregate
	// It also depends on the complexity of the aggregate algorithm, which is hard to model yet shared by all the aggregate
	// operators, thus we ignore this part of cost for all.
	CCost costLocal = CCost(pci->NumRebinds() *
							(num_rows_outer * dWidthOuter * ulAggCols *
							 ulAggFunctions * dHashAggInputTupWidthCostUnit));

	CCost costChild =
		CostChildren(mp, exprhdl, pci, pcmgpdb->GetCostModelParams());
	return costLocal + costChild;
}


//---------------------------------------------------------------------------
//	@function:
//		CCostModelGPDB::CostStreamAgg
//
//	@doc:
//		Cost of stream agg
//
//---------------------------------------------------------------------------
CCost
CCostModelGPDB::CostStreamAgg(CMemoryPool *mp, CExpressionHandle &exprhdl,
							  const CCostModelGPDB *pcmgpdb,
							  const SCostingInfo *pci)
{
	GPOS_ASSERT(NULL != pcmgpdb);
	GPOS_ASSERT(NULL != pci);

#ifdef GPOS_DEBUG
	COperator::EOperatorId op_id = exprhdl.Pop()->Eopid();
	GPOS_ASSERT(COperator::EopPhysicalStreamAgg == op_id ||
				COperator::EopPhysicalStreamAggDeduplicate == op_id);
#endif	// GPOS_DEBUG

	const CDouble dHashAggOutputTupWidthCostUnit =
		pcmgpdb->GetCostModelParams()
			->PcpLookup(CCostModelParamsGPDB::EcpHashAggOutputTupWidthCostUnit)
			->Get();
	const CDouble dTupDefaultProcCostUnit =
		pcmgpdb->GetCostModelParams()
			->PcpLookup(CCostModelParamsGPDB::EcpTupDefaultProcCostUnit)
			->Get();
	GPOS_ASSERT(0 < dHashAggOutputTupWidthCostUnit);
	GPOS_ASSERT(0 < dTupDefaultProcCostUnit);

	DOUBLE num_rows_outer = pci->PdRows()[0];
	DOUBLE dWidthOuter = pci->GetWidth()[0];

	// streamAgg cost is correlated with rows and width of input tuples and rows and width of output tuples
	CCost costLocal =
		CCost(pci->NumRebinds() *
			  (num_rows_outer * dWidthOuter * dTupDefaultProcCostUnit +
			   pci->Rows() * pci->Width() * dHashAggOutputTupWidthCostUnit));
	CCost costChild =
		CostChildren(mp, exprhdl, pci, pcmgpdb->GetCostModelParams());
	return costLocal + costChild;
}


//---------------------------------------------------------------------------
//	@function:
//		CCostModelGPDB::CostSequence
//
//	@doc:
//		Cost of sequence
//
//---------------------------------------------------------------------------
CCost
CCostModelGPDB::CostSequence(CMemoryPool *mp, CExpressionHandle &exprhdl,
							 const CCostModelGPDB *pcmgpdb,
							 const SCostingInfo *pci)
{
	GPOS_ASSERT(NULL != pcmgpdb);
	GPOS_ASSERT(NULL != pci);
	GPOS_ASSERT(COperator::EopPhysicalSequence == exprhdl.Pop()->Eopid());

	CCost costLocal = CCost(pci->NumRebinds() *
							CostTupleProcessing(pci->Rows(), pci->Width(),
												pcmgpdb->GetCostModelParams())
								.Get());
	CCost costChild =
		CostChildren(mp, exprhdl, pci, pcmgpdb->GetCostModelParams());

	return costLocal + costChild;
}


//---------------------------------------------------------------------------
//	@function:
//		CCostModelGPDB::CostSort
//
//	@doc:
//		Cost of sort
//
//---------------------------------------------------------------------------
CCost
CCostModelGPDB::CostSort(CMemoryPool *mp, CExpressionHandle &exprhdl,
						 const CCostModelGPDB *pcmgpdb, const SCostingInfo *pci)
{
	GPOS_ASSERT(NULL != pcmgpdb);
	GPOS_ASSERT(NULL != pci);
	GPOS_ASSERT(COperator::EopPhysicalSort == exprhdl.Pop()->Eopid());

	// log operation below
	const CDouble rows = CDouble(std::max(1.0, pci->Rows()));
	const CDouble num_rebinds = CDouble(pci->NumRebinds());
	const CDouble width = CDouble(pci->Width());

	const CDouble dSortTupWidthCost =
		pcmgpdb->GetCostModelParams()
			->PcpLookup(CCostModelParamsGPDB::EcpSortTupWidthCostUnit)
			->Get();
	GPOS_ASSERT(0 < dSortTupWidthCost);

	// sort cost is correlated with the number of rows and width of input tuples. We use n*log(n) for sorting complexity.
	CCost costLocal =
		CCost(num_rebinds * (rows * rows.Log2() * width * dSortTupWidthCost));
	CCost costChild =
		CostChildren(mp, exprhdl, pci, pcmgpdb->GetCostModelParams());

	return costLocal + costChild;
}


//---------------------------------------------------------------------------
//	@function:
//		CCostModelGPDB::CostTVF
//
//	@doc:
//		Cost of table valued function
//
//---------------------------------------------------------------------------
CCost
CCostModelGPDB::CostTVF(CMemoryPool *,	// mp
						CExpressionHandle &
#ifdef GPOS_DEBUG
							exprhdl
#endif	// GPOS_DEBUG
						,
						const CCostModelGPDB *pcmgpdb, const SCostingInfo *pci)
{
	GPOS_ASSERT(NULL != pcmgpdb);
	GPOS_ASSERT(NULL != pci);
	GPOS_ASSERT(COperator::EopPhysicalTVF == exprhdl.Pop()->Eopid());

	return CCost(pci->NumRebinds() *
				 CostTupleProcessing(pci->Rows(), pci->Width(),
									 pcmgpdb->GetCostModelParams())
					 .Get());
}


//---------------------------------------------------------------------------
//	@function:
//		CCostModelGPDB::CostUnionAll
//
//	@doc:
//		Cost of UnionAll
//
//---------------------------------------------------------------------------
CCost
CCostModelGPDB::CostUnionAll(CMemoryPool *mp, CExpressionHandle &exprhdl,
							 const CCostModelGPDB *pcmgpdb,
							 const SCostingInfo *pci)
{
	GPOS_ASSERT(NULL != pcmgpdb);
	GPOS_ASSERT(NULL != pci);
	GPOS_ASSERT(NULL != CPhysicalUnionAll::PopConvert(exprhdl.Pop()));

	if (COperator::EopPhysicalParallelUnionAll == exprhdl.Pop()->Eopid())
	{
		return CostMaxChild(mp, exprhdl, pci, pcmgpdb->GetCostModelParams());
	}

	CCost costLocal = CCost(pci->NumRebinds() *
							CostTupleProcessing(pci->Rows(), pci->Width(),
												pcmgpdb->GetCostModelParams())
								.Get());
	CCost costChild =
		CostChildren(mp, exprhdl, pci, pcmgpdb->GetCostModelParams());

	return costLocal + costChild;
}


//---------------------------------------------------------------------------
//	@function:
//		CCostModelGPDB::CostHashAgg
//
//	@doc:
//		Cost of hash agg
//
//---------------------------------------------------------------------------
CCost
CCostModelGPDB::CostHashAgg(CMemoryPool *mp, CExpressionHandle &exprhdl,
							const CCostModelGPDB *pcmgpdb,
							const SCostingInfo *pci)
{
	GPOS_ASSERT(NULL != pcmgpdb);
	GPOS_ASSERT(NULL != pci);

#ifdef GPOS_DEBUG
	COperator::EOperatorId op_id = exprhdl.Pop()->Eopid();
	GPOS_ASSERT(COperator::EopPhysicalHashAgg == op_id ||
				COperator::EopPhysicalHashAggDeduplicate == op_id);
#endif	// GPOS_DEBUG

	DOUBLE num_rows_outer = pci->PdRows()[0];

	// A local hash agg may stream partial aggregates to global agg when it's hash table is full to avoid spilling.
	// This is dertermined by the order of tuples received by local agg. In the worst case, the local hash agg may
	// see a tuple from each different group until its hash table fills up all available memory, and hence it produces
	// tuples as many as its input size. On the other hand, in the best case, the local agg may receive tuples sorted
	// by grouping columns, which allows it to complete all local aggregation in memory and produce exactly tuples as
	// the number of groups.
	//
	// Since we do not know the order of tuples received by local hash agg, we assume the number of output rows from local
	// agg is the average between input and output rows

	// the cardinality out as (rows + num_rows_outer)/2 to increase the local hash agg cost
	DOUBLE rows = pci->Rows();
	CPhysicalHashAgg *popAgg = CPhysicalHashAgg::PopConvert(exprhdl.Pop());
	if ((COperator::EgbaggtypeLocal == popAgg->Egbaggtype()) &&
		popAgg->FGeneratesDuplicates())
	{
		rows = (rows + num_rows_outer) / 2.0;
	}

	// get the number of grouping columns
	const ULONG ulGrpCols = CPhysicalHashAgg::PopConvert(exprhdl.Pop())
								->PdrgpcrGroupingCols()
								->Size();

	const CDouble dHashAggInputTupColumnCostUnit =
		pcmgpdb->GetCostModelParams()
			->PcpLookup(CCostModelParamsGPDB::EcpHashAggInputTupColumnCostUnit)
			->Get();
	const CDouble dHashAggInputTupWidthCostUnit =
		pcmgpdb->GetCostModelParams()
			->PcpLookup(CCostModelParamsGPDB::EcpHashAggInputTupWidthCostUnit)
			->Get();
	const CDouble dHashAggOutputTupWidthCostUnit =
		pcmgpdb->GetCostModelParams()
			->PcpLookup(CCostModelParamsGPDB::EcpHashAggOutputTupWidthCostUnit)
			->Get();
	GPOS_ASSERT(0 < dHashAggInputTupColumnCostUnit);
	GPOS_ASSERT(0 < dHashAggInputTupWidthCostUnit);
	GPOS_ASSERT(0 < dHashAggOutputTupWidthCostUnit);

	// hashAgg cost contains three parts: build hash table, aggregate tuples, and output tuples.
	// 1. build hash table is correlated with the number of rows and width of input tuples and the number of columns used.
	// 2. cost of aggregate tuples depends on the complexity of aggregation algorithm and thus is ignored.
	// 3. cost of output tuples is correlated with rows and width of returning tuples.
	CCost costLocal =
		CCost(pci->NumRebinds() *
			  (num_rows_outer * ulGrpCols * dHashAggInputTupColumnCostUnit +
			   num_rows_outer * ulGrpCols * pci->Width() *
				   dHashAggInputTupWidthCostUnit +
			   rows * pci->Width() * dHashAggOutputTupWidthCostUnit));
	CCost costChild =
		CostChildren(mp, exprhdl, pci, pcmgpdb->GetCostModelParams());

	return costLocal + costChild;
}


//---------------------------------------------------------------------------
//	@function:
//		CCostModelGPDB::CostHashJoin
//
//	@doc:
//		Cost of hash join
//
//---------------------------------------------------------------------------
CCost
CCostModelGPDB::CostHashJoin(CMemoryPool *mp, CExpressionHandle &exprhdl,
							 const CCostModelGPDB *pcmgpdb,
							 const SCostingInfo *pci)
{
	GPOS_ASSERT(NULL != pcmgpdb);
	GPOS_ASSERT(NULL != pci);
#ifdef GPOS_DEBUG
	COperator::EOperatorId op_id = exprhdl.Pop()->Eopid();
	GPOS_ASSERT(COperator::EopPhysicalInnerHashJoin == op_id ||
				COperator::EopPhysicalLeftSemiHashJoin == op_id ||
				COperator::EopPhysicalLeftAntiSemiHashJoin == op_id ||
				COperator::EopPhysicalLeftAntiSemiHashJoinNotIn == op_id ||
				COperator::EopPhysicalLeftOuterHashJoin == op_id);
#endif	// GPOS_DEBUG

	const DOUBLE num_rows_outer = pci->PdRows()[0];
	const DOUBLE dWidthOuter = pci->GetWidth()[0];
	const DOUBLE dRowsInner = pci->PdRows()[1];
	const DOUBLE dWidthInner = pci->GetWidth()[1];

	const CDouble dHJHashTableInitCostFactor =
		pcmgpdb->GetCostModelParams()
			->PcpLookup(CCostModelParamsGPDB::EcpHJHashTableInitCostFactor)
			->Get();
	const CDouble dHJHashTableColumnCostUnit =
		pcmgpdb->GetCostModelParams()
			->PcpLookup(CCostModelParamsGPDB::EcpHJHashTableColumnCostUnit)
			->Get();
	const CDouble dHJHashTableWidthCostUnit =
		pcmgpdb->GetCostModelParams()
			->PcpLookup(CCostModelParamsGPDB::EcpHJHashTableWidthCostUnit)
			->Get();
	const CDouble dJoinFeedingTupColumnCostUnit =
		pcmgpdb->GetCostModelParams()
			->PcpLookup(CCostModelParamsGPDB::EcpJoinFeedingTupColumnCostUnit)
			->Get();
	const CDouble dJoinFeedingTupWidthCostUnit =
		pcmgpdb->GetCostModelParams()
			->PcpLookup(CCostModelParamsGPDB::EcpJoinFeedingTupWidthCostUnit)
			->Get();
	const CDouble dHJHashingTupWidthCostUnit =
		pcmgpdb->GetCostModelParams()
			->PcpLookup(CCostModelParamsGPDB::EcpHJHashingTupWidthCostUnit)
			->Get();
	const CDouble dJoinOutputTupCostUnit =
		pcmgpdb->GetCostModelParams()
			->PcpLookup(CCostModelParamsGPDB::EcpJoinOutputTupCostUnit)
			->Get();
	const CDouble dHJSpillingMemThreshold =
		pcmgpdb->GetCostModelParams()
			->PcpLookup(CCostModelParamsGPDB::EcpHJSpillingMemThreshold)
			->Get();
	const CDouble dHJFeedingTupColumnSpillingCostUnit =
		pcmgpdb->GetCostModelParams()
			->PcpLookup(
				CCostModelParamsGPDB::EcpHJFeedingTupColumnSpillingCostUnit)
			->Get();
	const CDouble dHJFeedingTupWidthSpillingCostUnit =
		pcmgpdb->GetCostModelParams()
			->PcpLookup(
				CCostModelParamsGPDB::EcpHJFeedingTupWidthSpillingCostUnit)
			->Get();
	const CDouble dHJHashingTupWidthSpillingCostUnit =
		pcmgpdb->GetCostModelParams()
			->PcpLookup(
				CCostModelParamsGPDB::EcpHJHashingTupWidthSpillingCostUnit)
			->Get();
	const CDouble dPenalizeHJSkewUpperLimit =
		pcmgpdb->GetCostModelParams()
			->PcpLookup(CCostModelParamsGPDB::EcpPenalizeHJSkewUpperLimit)
			->Get();
	GPOS_ASSERT(0 < dHJHashTableInitCostFactor);
	GPOS_ASSERT(0 < dHJHashTableColumnCostUnit);
	GPOS_ASSERT(0 < dHJHashTableWidthCostUnit);
	GPOS_ASSERT(0 < dJoinFeedingTupColumnCostUnit);
	GPOS_ASSERT(0 < dJoinFeedingTupWidthCostUnit);
	GPOS_ASSERT(0 < dHJHashingTupWidthCostUnit);
	GPOS_ASSERT(0 < dJoinOutputTupCostUnit);
	GPOS_ASSERT(0 < dHJSpillingMemThreshold);
	GPOS_ASSERT(0 < dHJFeedingTupColumnSpillingCostUnit);
	GPOS_ASSERT(0 < dHJFeedingTupWidthSpillingCostUnit);
	GPOS_ASSERT(0 < dHJHashingTupWidthSpillingCostUnit);
	GPOS_ASSERT(0 < dPenalizeHJSkewUpperLimit);

	// get the number of columns used in join condition
	CExpression *pexprJoinCond = exprhdl.PexprScalarRepChild(2);
	CColRefSet *pcrsUsed = pexprJoinCond->DeriveUsedColumns();
	const ULONG ulColsUsed = pcrsUsed->Size();

	// TODO 2014-03-14
	// currently, we hard coded a spilling memory threshold for judging whether hash join spills or not
	// In the future, we should calculate it based on the number of memory-intensive operators and statement memory available
	CCost costLocal(0);

	// inner tuples fit in memory
	if (dRowsInner * dWidthInner <= dHJSpillingMemThreshold)
	{
		// hash join cost contains four parts:
		// 1. build hash table with inner tuples. This part is correlated with rows and width of
		// inner tuples and the number of columns used in join condition.
		// 2. feeding outer tuples. This part is correlated with rows and width of outer tuples
		// and the number of columns used.
		// 3. matching inner tuples. This part is correlated with rows and width of inner tuples.
		// 4. output tuples. This part is correlated with outer rows and width of the join result.
		costLocal = CCost(
			pci->NumRebinds() *
			(
				// cost of building hash table
				dRowsInner * (ulColsUsed * dHJHashTableColumnCostUnit +
							  dWidthInner * dHJHashTableWidthCostUnit) +
				// cost of feeding outer tuples
				ulColsUsed * num_rows_outer * dJoinFeedingTupColumnCostUnit +
				dWidthOuter * num_rows_outer * dJoinFeedingTupWidthCostUnit +
				// cost of matching inner tuples
				dWidthInner * dRowsInner * dHJHashingTupWidthCostUnit +
				// cost of output tuples
				pci->Rows() * pci->Width() * dJoinOutputTupCostUnit));
	}
	else
	{
		// inner tuples spill

		// hash join cost if spilling is the same as the non-spilling case, except that
		// parameter values are different.
		costLocal = CCost(
			pci->NumRebinds() *
			(dHJHashTableInitCostFactor +
			 dRowsInner * (ulColsUsed * dHJHashTableColumnCostUnit +
						   dWidthInner * dHJHashTableWidthCostUnit) +
			 ulColsUsed * num_rows_outer * dHJFeedingTupColumnSpillingCostUnit +
			 dWidthOuter * num_rows_outer * dHJFeedingTupWidthSpillingCostUnit +
			 dWidthInner * dRowsInner * dHJHashingTupWidthSpillingCostUnit +
			 pci->Rows() * pci->Width() * dJoinOutputTupCostUnit));
	}
	CCost costChild =
		CostChildren(mp, exprhdl, pci, pcmgpdb->GetCostModelParams());

	CDouble skew_ratio = 1;
	ULONG arity = exprhdl.Arity();

	// Hashjoin with skewed HashRedistribute below them are expensive
	// find out if there is a skewed redistribute child of this HashJoin.
	if (!GPOS_FTRACE(EopttracePenalizeSkewedHashJoin))
	{
		for (ULONG ul = 0; ul < arity - 1; ++ul)
		{
			COperator *popChild = exprhdl.Pop(ul);
			if (NULL == popChild ||
				COperator::EopPhysicalMotionHashDistribute != popChild->Eopid())
			{
				continue;
			}

			CPhysicalMotion *motion = CPhysicalMotion::PopConvert(popChild);
			CColRefSet *columns = motion->Pds()->PcrsUsed(mp);

			// we decide if there is a skew by calculating the NDVs of the HashRedistribute
			CDouble ndv = 1.0;
			CColRefSetIter iter(*columns);
			while (iter.Advance())
			{
				CColRef *colref = iter.Pcr();
				ndv = ndv * pci->Pcstats(ul)->GetNDVs(colref);
			}

			// if the NDVs are less than number of segments then there is definitely
			// a skew. NDV < 1 implies no stats exist for the columns involved. So we don't
			// want to take any decision.
			// In case of a skew, penalize the local cost of HashJoin with a
			// skew ratio = (num of segments)/ndv
			if (ndv < pcmgpdb->UlHosts() && (ndv >= 1))
			{
				CDouble sk = pcmgpdb->UlHosts() / ndv;
				skew_ratio = CDouble(std::max(sk.Get(), skew_ratio.Get()));
			}

			columns->Release();
		}
	}

	// if we end up penalizing redistribute too much, we will start getting gather motions
	// which are not necessarily a good idea. So we maintain a upper limit of skew.
	skew_ratio =
		CDouble(std::min(dPenalizeHJSkewUpperLimit.Get(), skew_ratio.Get()));
	return costChild + CCost(costLocal.Get() * skew_ratio);
}

//---------------------------------------------------------------------------
//	@function:
//		CCostModelGPDB::CostMergeJoin
//
//	@doc:
//		Cost of merge join
//
//---------------------------------------------------------------------------
CCost
CCostModelGPDB::CostMergeJoin(CMemoryPool *mp, CExpressionHandle &exprhdl,
							  const CCostModelGPDB *pcmgpdb,
							  const SCostingInfo *pci)
{
	GPOS_ASSERT(NULL != pcmgpdb);
	GPOS_ASSERT(NULL != pci);
#ifdef GPOS_DEBUG
	COperator::EOperatorId op_id = exprhdl.Pop()->Eopid();
	GPOS_ASSERT(COperator::EopPhysicalFullMergeJoin == op_id);
#endif	// GPOS_DEBUG

	const DOUBLE num_rows_outer = pci->PdRows()[0];
	const DOUBLE dWidthOuter = pci->GetWidth()[0];
	const DOUBLE dRowsInner = pci->PdRows()[1];
	const DOUBLE dWidthInner = pci->GetWidth()[1];

	const CDouble dJoinFeedingTupColumnCostUnit =
		pcmgpdb->GetCostModelParams()
			->PcpLookup(CCostModelParamsGPDB::EcpJoinFeedingTupColumnCostUnit)
			->Get();
	const CDouble dJoinFeedingTupWidthCostUnit =
		pcmgpdb->GetCostModelParams()
			->PcpLookup(CCostModelParamsGPDB::EcpJoinFeedingTupWidthCostUnit)
			->Get();
	const CDouble dJoinOutputTupCostUnit =
		pcmgpdb->GetCostModelParams()
			->PcpLookup(CCostModelParamsGPDB::EcpJoinOutputTupCostUnit)
			->Get();
	const CDouble dFilterColCostUnit =
		pcmgpdb->GetCostModelParams()
			->PcpLookup(CCostModelParamsGPDB::EcpFilterColCostUnit)
			->Get();
	const CDouble dOutputTupCostUnit =
		pcmgpdb->GetCostModelParams()
			->PcpLookup(CCostModelParamsGPDB::EcpOutputTupCostUnit)
			->Get();
	GPOS_ASSERT(0 < dJoinFeedingTupColumnCostUnit);
	GPOS_ASSERT(0 < dJoinFeedingTupWidthCostUnit);
	GPOS_ASSERT(0 < dJoinOutputTupCostUnit);
	GPOS_ASSERT(0 < dFilterColCostUnit);
	GPOS_ASSERT(0 < dOutputTupCostUnit);

	// get the number of columns used in join condition
	CExpression *pexprJoinCond = exprhdl.PexprScalarRepChild(2);
	CColRefSet *pcrsUsed = pexprJoinCond->DeriveUsedColumns();
	const ULONG ulColsUsed = pcrsUsed->Size();

	// We assume for costing, that the outer tuples are unique. This means that
	// we will never have to rescan a portion of the inner side.
	CCost costLocal = CCost(
		pci->NumRebinds() *
		(
			// feeding cost of outer
			ulColsUsed * num_rows_outer * dJoinFeedingTupColumnCostUnit +
			dWidthOuter * num_rows_outer * dJoinFeedingTupWidthCostUnit +
			// cost of matching
			(dRowsInner + num_rows_outer) * ulColsUsed * dFilterColCostUnit +
			// cost of extracting matched inner side
			pci->Rows() * dWidthInner * dOutputTupCostUnit +
			// cost of output tuples
			pci->Rows() * pci->Width() * dJoinOutputTupCostUnit));

	CCost costChild =
		CostChildren(mp, exprhdl, pci, pcmgpdb->GetCostModelParams());

	return costChild + costLocal;
}


//---------------------------------------------------------------------------
//	@function:
//		CCostModelGPDB::CostIndexNLJoin
//
//	@doc:
//		Cost of inner or outer index-nljoin
//
//---------------------------------------------------------------------------
CCost
CCostModelGPDB::CostIndexNLJoin(CMemoryPool *mp, CExpressionHandle &exprhdl,
								const CCostModelGPDB *pcmgpdb,
								const SCostingInfo *pci)
{
	GPOS_ASSERT(NULL != pcmgpdb);
	GPOS_ASSERT(NULL != pci);
	GPOS_ASSERT(
		COperator::EopPhysicalInnerIndexNLJoin == exprhdl.Pop()->Eopid() ||
		COperator::EopPhysicalLeftOuterIndexNLJoin == exprhdl.Pop()->Eopid());

	const DOUBLE num_rows_outer = pci->PdRows()[0];
	const DOUBLE dWidthOuter = pci->GetWidth()[0];

	const CDouble dJoinFeedingTupColumnCostUnit =
		pcmgpdb->GetCostModelParams()
			->PcpLookup(CCostModelParamsGPDB::EcpJoinFeedingTupColumnCostUnit)
			->Get();
	const CDouble dJoinFeedingTupWidthCostUnit =
		pcmgpdb->GetCostModelParams()
			->PcpLookup(CCostModelParamsGPDB::EcpJoinFeedingTupWidthCostUnit)
			->Get();
	const CDouble dJoinOutputTupCostUnit =
		pcmgpdb->GetCostModelParams()
			->PcpLookup(CCostModelParamsGPDB::EcpJoinOutputTupCostUnit)
			->Get();
	GPOS_ASSERT(0 < dJoinFeedingTupColumnCostUnit);
	GPOS_ASSERT(0 < dJoinFeedingTupWidthCostUnit);
	GPOS_ASSERT(0 < dJoinOutputTupCostUnit);

	// get the number of columns used in join condition
	CExpression *pexprJoinCond = exprhdl.PexprScalarRepChild(2);
	CColRefSet *pcrsUsed = pexprJoinCond->DeriveUsedColumns();
	const ULONG ulColsUsed = pcrsUsed->Size();

	// cost of Index apply contains three parts:
	// 1. feeding outer tuples. This part is correlated with rows and width of outer tuples
	// and the number of columns used.
	// 2. repetitive index scan of inner side for each feeding tuple. This part of cost is
	// calculated and counted in its index scan child node.
	// 3. output tuples. This part is correlated with outer rows and width of the join result.
	CCost costLocal =
		CCost(pci->NumRebinds() *
			  (
				  // cost of feeding outer tuples
				  ulColsUsed * num_rows_outer * dJoinFeedingTupColumnCostUnit +
				  dWidthOuter * num_rows_outer * dJoinFeedingTupWidthCostUnit +
				  // cost of output tuples
				  pci->Rows() * pci->Width() * dJoinOutputTupCostUnit));

	CCost costChild =
		CostChildren(mp, exprhdl, pci, pcmgpdb->GetCostModelParams());

	ULONG risk = pci->Pcstats()->StatsEstimationRisk();
	ULONG ulPenalizationFactor = 1;
	const CDouble dIndexJoinAllowedRiskThreshold =
		pcmgpdb->GetCostModelParams()
			->PcpLookup(CCostModelParamsGPDB::EcpIndexJoinAllowedRiskThreshold)
			->Get();
	BOOL fInnerJoin =
		COperator::EopPhysicalInnerIndexNLJoin == exprhdl.Pop()->Eopid();

	// Only apply penalize factor for inner index nestloop join, because we are more confident
	// on the cardinality estimation of outer join than inner join. So don't penalize outer join
	// cost, otherwise Orca generate bad plan.
	if (fInnerJoin && dIndexJoinAllowedRiskThreshold < risk)
	{
		ulPenalizationFactor = risk;
	}

	return CCost(ulPenalizationFactor * (costLocal + costChild));
}


//---------------------------------------------------------------------------
//	@function:
//		CCostModelGPDB::CostNLJoin
//
//	@doc:
//		Cost of nljoin
//
//---------------------------------------------------------------------------
CCost
CCostModelGPDB::CostNLJoin(CMemoryPool *mp, CExpressionHandle &exprhdl,
						   const CCostModelGPDB *pcmgpdb,
						   const SCostingInfo *pci)
{
	GPOS_ASSERT(NULL != pcmgpdb);
	GPOS_ASSERT(NULL != pci);
	GPOS_ASSERT(CUtils::FNLJoin(exprhdl.Pop()));

	const DOUBLE num_rows_outer = pci->PdRows()[0];
	const DOUBLE dWidthOuter = pci->GetWidth()[0];
	const DOUBLE dRowsInner = pci->PdRows()[1];
	const DOUBLE dWidthInner = pci->GetWidth()[1];

	const CDouble dJoinFeedingTupColumnCostUnit =
		pcmgpdb->GetCostModelParams()
			->PcpLookup(CCostModelParamsGPDB::EcpJoinFeedingTupColumnCostUnit)
			->Get();
	const CDouble dJoinFeedingTupWidthCostUnit =
		pcmgpdb->GetCostModelParams()
			->PcpLookup(CCostModelParamsGPDB::EcpJoinFeedingTupWidthCostUnit)
			->Get();
	const CDouble dJoinOutputTupCostUnit =
		pcmgpdb->GetCostModelParams()
			->PcpLookup(CCostModelParamsGPDB::EcpJoinOutputTupCostUnit)
			->Get();
	const CDouble dInitScan =
		pcmgpdb->GetCostModelParams()
			->PcpLookup(CCostModelParamsGPDB::EcpInitScanFactor)
			->Get();
	const CDouble dTableScanCostUnit =
		pcmgpdb->GetCostModelParams()
			->PcpLookup(CCostModelParamsGPDB::EcpTableScanCostUnit)
			->Get();
	const CDouble dFilterColCostUnit =
		pcmgpdb->GetCostModelParams()
			->PcpLookup(CCostModelParamsGPDB::EcpFilterColCostUnit)
			->Get();
	const CDouble dOutputTupCostUnit =
		pcmgpdb->GetCostModelParams()
			->PcpLookup(CCostModelParamsGPDB::EcpOutputTupCostUnit)
			->Get();
	const CDouble dNLJFactor =
		pcmgpdb->GetCostModelParams()
			->PcpLookup(CCostModelParamsGPDB::EcpNLJFactor)
			->Get();

	GPOS_ASSERT(0 < dJoinFeedingTupColumnCostUnit);
	GPOS_ASSERT(0 < dJoinFeedingTupWidthCostUnit);
	GPOS_ASSERT(0 < dJoinOutputTupCostUnit);
	GPOS_ASSERT(0 < dInitScan);
	GPOS_ASSERT(0 < dTableScanCostUnit);
	GPOS_ASSERT(0 < dFilterColCostUnit);
	GPOS_ASSERT(0 < dOutputTupCostUnit);
	GPOS_ASSERT(0 < dNLJFactor);

	// get the number of columns used in join condition
	CExpression *pexprJoinCond = exprhdl.PexprScalarRepChild(2);
	CColRefSet *pcrsUsed = pexprJoinCond->DeriveUsedColumns();
	const ULONG ulColsUsed = pcrsUsed->Size();

	// cost of nested loop join contains three parts:
	// 1. feeding outer tuples. This part is correlated with rows and width of outer tuples
	// and the number of columns used.
	// 2. repetitive scan of inner side for each feeding tuple. This part of cost consists of
	// the following:
	// a. repetitive scan and filter of the materialized inner side
	// b. extract matched inner side tuples
	// with the cardinality of outer tuples, rows and width of the materialized inner side.
	// 3. output tuples. This part is correlated with outer rows and width of the join result.
	CCost costLocal = CCost(
		pci->NumRebinds() *
		(
			// cost of feeding outer tuples
			ulColsUsed * num_rows_outer * dJoinFeedingTupColumnCostUnit +
			dWidthOuter * num_rows_outer * dJoinFeedingTupWidthCostUnit +
			// cost of repetitive table scan of inner side
			dInitScan +
			num_rows_outer * (
								 // cost of scan of inner side
								 dRowsInner * dWidthInner * dTableScanCostUnit +
								 // cost of filter of inner side
								 dRowsInner * ulColsUsed * dFilterColCostUnit)
			// cost of extracting matched inner side
			+ pci->Rows() * dWidthInner * dOutputTupCostUnit +
			// cost of output tuples
			pci->Rows() * pci->Width() * dJoinOutputTupCostUnit));

	CCost costChild =
		CostChildren(mp, exprhdl, pci, pcmgpdb->GetCostModelParams());

	CCost costTotal = CCost(costLocal + costChild);

	// amplify NLJ cost based on NLJ factor and stats estimation risk
	// we don't want to penalize index join compared to nested loop join, so we make sure
	// that every time index join is penalized, we penalize nested loop join by at least the
	// same amount
	CDouble dPenalization = dNLJFactor;
	const CDouble dRisk(pci->Pcstats()->StatsEstimationRisk());
	if (dRisk > dPenalization)
	{
		const CDouble dIndexJoinAllowedRiskThreshold =
			pcmgpdb->GetCostModelParams()
				->PcpLookup(
					CCostModelParamsGPDB::EcpIndexJoinAllowedRiskThreshold)
				->Get();
		if (dIndexJoinAllowedRiskThreshold < dRisk)
		{
			dPenalization = dRisk;
		}
	}

	return CCost(costTotal * dPenalization);
}


//---------------------------------------------------------------------------
//	@function:
//		CCostModelGPDB::CostMotion
//
//	@doc:
//		Cost of motion
//
//---------------------------------------------------------------------------
CCost
CCostModelGPDB::CostMotion(CMemoryPool *mp, CExpressionHandle &exprhdl,
						   const CCostModelGPDB *pcmgpdb,
						   const SCostingInfo *pci)
{
	GPOS_ASSERT(NULL != pcmgpdb);
	GPOS_ASSERT(NULL != pci);

	COperator::EOperatorId op_id = exprhdl.Pop()->Eopid();
	GPOS_ASSERT(COperator::EopPhysicalMotionGather == op_id ||
				COperator::EopPhysicalMotionBroadcast == op_id ||
				COperator::EopPhysicalMotionHashDistribute == op_id ||
				COperator::EopPhysicalMotionRandom == op_id ||
				COperator::EopPhysicalMotionRoutedDistribute == op_id);

	const DOUBLE num_rows_outer = pci->PdRows()[0];
	const DOUBLE dWidthOuter = pci->GetWidth()[0];

	// motion cost contains three parts: sending cost, interconnect cost, and receiving cost.
	// TODO 2014-03-18
	// in current cost model, interconnect cost is tied with receiving cost. Because we
	// only have one set calibration results in the dimension of the number of segments.
	// Once we calibrate the cost model with different number of segments, I will update
	// the function.

	CDouble dSendCostUnit(0);
	CDouble dRecvCostUnit(0);
	CDouble recvCost(0);

	CCost costLocal(0);
	if (COperator::EopPhysicalMotionBroadcast == op_id)
	{
		// broadcast cost is amplified by the number of segments
		dSendCostUnit =
			pcmgpdb->GetCostModelParams()
				->PcpLookup(CCostModelParamsGPDB::EcpBroadcastSendCostUnit)
				->Get();
		dRecvCostUnit =
			pcmgpdb->GetCostModelParams()
				->PcpLookup(CCostModelParamsGPDB::EcpBroadcastRecvCostUnit)
				->Get();

		recvCost =
			num_rows_outer * dWidthOuter * pcmgpdb->UlHosts() * dRecvCostUnit;
	}
	else if (COperator::EopPhysicalMotionHashDistribute == op_id ||
			 COperator::EopPhysicalMotionRandom == op_id ||
			 COperator::EopPhysicalMotionRoutedDistribute == op_id)
	{
		dSendCostUnit =
			pcmgpdb->GetCostModelParams()
				->PcpLookup(CCostModelParamsGPDB::EcpRedistributeSendCostUnit)
				->Get();
		dRecvCostUnit =
			pcmgpdb->GetCostModelParams()
				->PcpLookup(CCostModelParamsGPDB::EcpRedistributeRecvCostUnit)
				->Get();

		// Adjust the cost of no-op hashed distribution to correctly reflect that no tuple movement is needed
		CPhysicalMotion *pMotion = CPhysicalMotion::PopConvert(exprhdl.Pop());
		CDistributionSpec *pds = pMotion->Pds();
		if (CDistributionSpec::EdtHashedNoOp == pds->Edt())
		{
			// promote the plan with redistribution on same distributed columns of base table for parallel append
			dSendCostUnit =
				pcmgpdb->GetCostModelParams()
					->PcpLookup(CCostModelParamsGPDB::EcpNoOpCostUnit)
					->Get();
			dRecvCostUnit =
				pcmgpdb->GetCostModelParams()
					->PcpLookup(CCostModelParamsGPDB::EcpNoOpCostUnit)
					->Get();
		}

		recvCost = pci->Rows() * pci->Width() * dRecvCostUnit;
	}
	else if (COperator::EopPhysicalMotionGather == op_id)
	{
		dSendCostUnit =
			pcmgpdb->GetCostModelParams()
				->PcpLookup(CCostModelParamsGPDB::EcpGatherSendCostUnit)
				->Get();
		dRecvCostUnit =
			pcmgpdb->GetCostModelParams()
				->PcpLookup(CCostModelParamsGPDB::EcpGatherRecvCostUnit)
				->Get();

		recvCost =
			num_rows_outer * dWidthOuter * pcmgpdb->UlHosts() * dRecvCostUnit;
	}

	GPOS_ASSERT(0 <= dSendCostUnit);
	GPOS_ASSERT(0 <= dRecvCostUnit);

	costLocal =
		CCost(pci->NumRebinds() *
			  (num_rows_outer * dWidthOuter * dSendCostUnit + recvCost));


	if (COperator::EopPhysicalMotionBroadcast == op_id)
	{
		COptimizerConfig *optimizer_config =
			COptCtxt::PoctxtFromTLS()->GetOptimizerConfig();
		ULONG broadcast_threshold =
			optimizer_config->GetHint()->UlBroadcastThreshold();

		if (num_rows_outer > broadcast_threshold)
		{
			DOUBLE ulPenalizationFactor = 100000000000000.0;
			costLocal = CCost(ulPenalizationFactor);
		}
	}


	CCost costChild =
		CostChildren(mp, exprhdl, pci, pcmgpdb->GetCostModelParams());

	return costLocal + costChild;
}


//---------------------------------------------------------------------------
//	@function:
//		CCostModelGPDB::CostSequenceProject
//
//	@doc:
//		Cost of sequence project
//
//---------------------------------------------------------------------------
CCost
CCostModelGPDB::CostSequenceProject(CMemoryPool *mp, CExpressionHandle &exprhdl,
									const CCostModelGPDB *pcmgpdb,
									const SCostingInfo *pci)
{
	GPOS_ASSERT(NULL != pcmgpdb);
	GPOS_ASSERT(NULL != pci);
	GPOS_ASSERT(COperator::EopPhysicalSequenceProject ==
				exprhdl.Pop()->Eopid());

	const DOUBLE num_rows_outer = pci->PdRows()[0];
	const DOUBLE dWidthOuter = pci->GetWidth()[0];

	ULONG ulSortCols = 0;
	COrderSpecArray *pdrgpos =
		CPhysicalSequenceProject::PopConvert(exprhdl.Pop())->Pdrgpos();
	const ULONG ulOrderSpecs = pdrgpos->Size();
	for (ULONG ul = 0; ul < ulOrderSpecs; ul++)
	{
		COrderSpec *pos = (*pdrgpos)[ul];
		ulSortCols += pos->UlSortColumns();
	}

	const CDouble dTupDefaultProcCostUnit =
		pcmgpdb->GetCostModelParams()
			->PcpLookup(CCostModelParamsGPDB::EcpTupDefaultProcCostUnit)
			->Get();
	GPOS_ASSERT(0 < dTupDefaultProcCostUnit);

	// we process (sorted window of) input tuples to compute window function values
	CCost costLocal =
		CCost(pci->NumRebinds() * (ulSortCols * num_rows_outer * dWidthOuter *
								   dTupDefaultProcCostUnit));
	CCost costChild =
		CostChildren(mp, exprhdl, pci, pcmgpdb->GetCostModelParams());

	return costLocal + costChild;
}


//---------------------------------------------------------------------------
//	@function:
//		CCostModelGPDB::CostIndexScan
//
//	@doc:
//		Cost of index scan
//
//---------------------------------------------------------------------------
CCost
CCostModelGPDB::CostIndexScan(CMemoryPool *,  // mp
							  CExpressionHandle &exprhdl,
							  const CCostModelGPDB *pcmgpdb,
							  const SCostingInfo *pci)
{
	GPOS_ASSERT(NULL != pcmgpdb);
	GPOS_ASSERT(NULL != pci);

	COperator *pop = exprhdl.Pop();
	COperator::EOperatorId op_id = pop->Eopid();
	GPOS_ASSERT(COperator::EopPhysicalIndexScan == op_id ||
				COperator::EopPhysicalDynamicIndexScan == op_id);

	const CDouble dTableWidth =
		CPhysicalScan::PopConvert(pop)->PstatsBaseTable()->Width();

	const CDouble dIndexFilterCostUnit =
		pcmgpdb->GetCostModelParams()
			->PcpLookup(CCostModelParamsGPDB::EcpIndexFilterCostUnit)
			->Get();
	const CDouble dIndexScanTupCostUnit =
		pcmgpdb->GetCostModelParams()
			->PcpLookup(CCostModelParamsGPDB::EcpIndexScanTupCostUnit)
			->Get();
	const CDouble dIndexScanTupRandomFactor =
		pcmgpdb->GetCostModelParams()
			->PcpLookup(CCostModelParamsGPDB::EcpIndexScanTupRandomFactor)
			->Get();
	GPOS_ASSERT(0 < dIndexFilterCostUnit);
	GPOS_ASSERT(0 < dIndexScanTupCostUnit);
	GPOS_ASSERT(0 < dIndexScanTupRandomFactor);

	CDouble dRowsIndex = pci->Rows();

	ULONG ulIndexKeys = 1;
	if (COperator::EopPhysicalIndexScan == op_id)
	{
		ulIndexKeys = CPhysicalIndexScan::PopConvert(pop)->Pindexdesc()->Keys();
	}
	else
	{
		ulIndexKeys =
			CPhysicalDynamicIndexScan::PopConvert(pop)->Pindexdesc()->Keys();
	}

	// TODO: 2014-02-01
	// Add logic to judge if the index column used in the filter is the first key of a multi-key index or not.
	// and separate the cost functions for the two cases.

	// index scan cost contains two parts: index-column lookup and output tuple cost.
	// 1. index-column lookup: correlated with index lookup rows, the number of index columns used in lookup,
	// table width and a randomIOFactor.
	// 2. output tuple cost: this is handled by the Filter on top of IndexScan, if no Filter exists, we add output cost
	// when we sum-up children cost

	CDouble dCostPerIndexRow = ulIndexKeys * dIndexFilterCostUnit +
							   dTableWidth * dIndexScanTupCostUnit;
	return CCost(pci->NumRebinds() *
				 (dRowsIndex * dCostPerIndexRow + dIndexScanTupRandomFactor));
}


CCost
CCostModelGPDB::CostBitmapTableScan(CMemoryPool *mp, CExpressionHandle &exprhdl,
									const CCostModelGPDB *pcmgpdb,
									const SCostingInfo *pci)
{
	GPOS_ASSERT(NULL != pcmgpdb);
	GPOS_ASSERT(NULL != pci);
	GPOS_ASSERT(
		COperator::EopPhysicalBitmapTableScan == exprhdl.Pop()->Eopid() ||
		COperator::EopPhysicalDynamicBitmapTableScan == exprhdl.Pop()->Eopid());

	CCost result(0.0);
	CExpression *pexprIndexCond =
		exprhdl.PexprScalarRepChild(1 /*child_index*/);
	CColRefSet *pcrsUsed = pexprIndexCond->DeriveUsedColumns();
	CColRefSet *outerRefs = exprhdl.DeriveOuterReferences();
	CColRefSet *pcrsLocalUsed = GPOS_NEW(mp) CColRefSet(mp, *pcrsUsed);
	IMDIndex::EmdindexType indexType = IMDIndex::EmdindSentinel;

	if (COperator::EopScalarBitmapIndexProbe == pexprIndexCond->Pop()->Eopid())
	{
		indexType = CScalarBitmapIndexProbe::PopConvert(pexprIndexCond->Pop())
						->Pindexdesc()
						->IndexType();
	}

	BOOL isInPredOnBtreeIndex =
		(IMDIndex::EmdindBtree == indexType &&
		 COperator::EopScalarArrayCmp == (*pexprIndexCond)[0]->Pop()->Eopid());

	// subtract outer references from the used colrefs, so we can see
	// how many colrefs are used for this table
	pcrsLocalUsed->Exclude(outerRefs);

	const DOUBLE rows = pci->Rows();
	const DOUBLE width = pci->Width();
	CDouble dInitRebind =
		pcmgpdb->GetCostModelParams()
			->PcpLookup(CCostModelParamsGPDB::EcpBitmapScanRebindCost)
			->Get();

	if (COperator::EopScalarBitmapIndexProbe !=
			pexprIndexCond->Pop()->Eopid() ||
		1 < pcrsLocalUsed->Size() ||
		(isInPredOnBtreeIndex && rows > 2.0 &&
		 !GPOS_FTRACE(EopttraceCalibratedBitmapIndexCostModel)))
	{
		// Child is Bitmap AND/OR, or we use Multi column index or this is an IN predicate
		// that's used with the "calibrated" cost model.
		// Handling the IN predicate in this code path is to avoid plan regressions from
		// earlier versions of the code that treated IN predicates like ORs and therefore
		// also handled them in this code path. This is especially noticeable for btree
		// indexes that often have a high NDV, because the small/large NDV cost model
		// produces very high cost for cases with a higher NDV.
		const CDouble dIndexFilterCostUnit =
			pcmgpdb->GetCostModelParams()
				->PcpLookup(CCostModelParamsGPDB::EcpIndexFilterCostUnit)
				->Get();

		GPOS_ASSERT(0 < dIndexFilterCostUnit);

		// check whether the user specified overriding values in gucs
		CDouble dInitScan =
			pcmgpdb->GetCostModelParams()
				->PcpLookup(CCostModelParamsGPDB::EcpInitScanFactor)
				->Get();

		GPOS_ASSERT(dInitScan >= 0 && dInitRebind >= 0);

		// For now we are trying to cost Bitmap Scan similar to Index Scan. dIndexFilterCostUnit is
		// the dominant factor in costing Index Scan so we are using it in our model. Also we are giving
		// Bitmap Scan a start up cost similar to Sequential Scan. Note that in this code path we add the
		// relatively high dInitScan cost, while in the other code paths below (CostBitmapLargeNDV and
		// CostBitmapSmallNDV) we don't. That's something we should look into.

		// Conceptually the cost of evaluating index qual is also linear in the
		// number of index columns, but we're only accounting for the dominant cost

		result =
			CCost(	// cost for each byte returned by the index scan plus cost for incremental rebinds
				pci->NumRebinds() *
					(rows * width * dIndexFilterCostUnit + dInitRebind) +
				// init cost
				dInitScan);
	}
	else
	{
		// if the expression is const table get, the pcrsUsed is empty
		// so we use minimum value MinDistinct for dNDV in that case.
		CDouble dNDV = CHistogram::MinDistinct;
		CDouble dNDVThreshold =
			pcmgpdb->GetCostModelParams()
				->PcpLookup(CCostModelParamsGPDB::EcpBitmapNDVThreshold)
				->Get();

		if (rows < 1.0)
		{
			// if we aren't accessing a row every rebind, then don't charge a cost for those cases where we don't have a row
			dNDV = rows;
		}
		else if (1 == pcrsLocalUsed->Size())  // if you only have one local pred
		{
			CColRef *pcrIndexCond = pcrsLocalUsed->PcrFirst();
			GPOS_ASSERT(NULL != pcrIndexCond);
			// get the num distinct for the rows returned by the predicate
			dNDV = pci->Pcstats()->GetNDVs(pcrIndexCond);
			// if there's an outerref
			if (1 < pcrsUsed->Size() && COperator::EopScalarBitmapIndexProbe ==
											pexprIndexCond->Pop()->Eopid())
			{
				CExpression *pexprScalarCmp = (*pexprIndexCond)[0];
				// The index condition contains an outer reference. Adjust
				// the NDV to 1, if we compare the colref with a single value
				if (CPredicateUtils::IsEqualityOp(pexprScalarCmp))
				{
					dNDV = 1.0;
				}
			}
		}

		if (!GPOS_FTRACE(EopttraceCalibratedBitmapIndexCostModel))
		{
			// optimizer_cost_model = 'calibrated'
			if (dNDVThreshold <= dNDV)
			{
				result = CostBitmapLargeNDV(pcmgpdb, pci, dNDV);
			}
			else
			{
				result = CostBitmapSmallNDV(pcmgpdb, pci, dNDV);
			}
		}
		else
		{
			// optimizer_cost_model = 'experimental'
			CDouble dBitmapIO =
				pcmgpdb->GetCostModelParams()
					->PcpLookup(CCostModelParamsGPDB::EcpBitmapIOCostSmallNDV)
					->Get();
			CDouble c5_dInitScan =
				pcmgpdb->GetCostModelParams()
					->PcpLookup(CCostModelParamsGPDB::EcpInitScanFactor)
					->Get();
			CDouble c3_dBitmapPageCost =
				pcmgpdb->GetCostModelParams()
					->PcpLookup(CCostModelParamsGPDB::EcpBitmapPageCost)
					->Get();
			BOOL isAOTable = CPhysicalScan::PopConvert(exprhdl.Pop())
								 ->Ptabdesc()
								 ->IsAORowOrColTable();

			// some cost constants determined with the cal_bitmap_test.py script
			CDouble c1_cost_per_row(0.03);
			CDouble c2_cost_per_byte(0.0001);
			CDouble bitmap_union_cost_per_distinct_value(0.000027);
			CDouble init_cost_advantage_for_bitmap_scan(0.9);

			if (IMDIndex::EmdindBtree == indexType)
			{
				// btree indexes are not sensitive to the NDV, since they don't have any bitmaps
				c3_dBitmapPageCost = 0.0;
			}

			// Give the index scan a small initial advantage over the table scan, so we use indexes
			// for small tables - this should avoid having table scan and index scan costs being
			// very close together for many small queries.
			c5_dInitScan = c5_dInitScan * init_cost_advantage_for_bitmap_scan;

			// The numbers below were experimentally determined using regression analysis in the cal_bitmap_test.py script
			// The following dSizeCost is in the form C1 * rows + C2 * rows * width. This is because the width should have
			// significantly less weight than rows as the execution time does not grow as fast in regards to width
			CDouble dSizeCost = dBitmapIO * (rows * c1_cost_per_row +
											 rows * width * c2_cost_per_byte);

			CDouble bitmapUnionCost = 0;

			if (!isAOTable && indexType == IMDIndex::EmdindBitmap && dNDV > 1.0)
			{
				CDouble baseTableRows = CPhysicalScan::PopConvert(exprhdl.Pop())
											->PstatsBaseTable()
											->Rows();

				// for bitmap index scans on heap tables, we found that there is an additional cost
				// associated with unioning them that is proportional to the number of bitmaps involved
				// (dNDV-1) times the width of the bitmap (proportional to the number of rows in the table)
				bitmapUnionCost = std::max(0.0, dNDV.Get() - 1.0) *
								  baseTableRows *
								  bitmap_union_cost_per_distinct_value;
			}

			result = CCost(pci->NumRebinds() *
							   (dSizeCost + dNDV * c3_dBitmapPageCost +
								dInitRebind + bitmapUnionCost) +
						   c5_dInitScan);
		}
	}

	pcrsLocalUsed->Release();

	return result;
}


CCost
CCostModelGPDB::CostBitmapSmallNDV(const CCostModelGPDB *pcmgpdb,
								   const SCostingInfo *pci, CDouble dNDV)
{
	const DOUBLE rows = pci->Rows();
	const DOUBLE width = pci->Width();

	CDouble dSize = (rows * width) * 0.001;

	CDouble dBitmapIO =
		pcmgpdb->GetCostModelParams()
			->PcpLookup(CCostModelParamsGPDB::EcpBitmapIOCostSmallNDV)
			->Get();
	CDouble dBitmapPageCost =
		pcmgpdb->GetCostModelParams()
			->PcpLookup(CCostModelParamsGPDB::EcpBitmapPageCostSmallNDV)
			->Get();
	CDouble effectiveNDV = dNDV;

	if (rows < 1.0)
	{
		// if we aren't accessing a row every rebind, then don't charge a cost for those cases where we don't have a row
		effectiveNDV = rows;
	}

	return CCost(pci->NumRebinds() *
				 (dBitmapIO * dSize + dBitmapPageCost * effectiveNDV));
}


CCost
CCostModelGPDB::CostBitmapLargeNDV(const CCostModelGPDB *pcmgpdb,
								   const SCostingInfo *pci, CDouble dNDV)
{
	const DOUBLE rows = pci->Rows();
	const DOUBLE width = pci->Width();

	CDouble dSize = (rows * width * dNDV) * 0.001;
	CDouble dBitmapIO =
		pcmgpdb->GetCostModelParams()
			->PcpLookup(CCostModelParamsGPDB::EcpBitmapIOCostLargeNDV)
			->Get();
	CDouble dBitmapPageCost =
		pcmgpdb->GetCostModelParams()
			->PcpLookup(CCostModelParamsGPDB::EcpBitmapPageCostLargeNDV)
			->Get();

	return CCost(pci->NumRebinds() *
				 (dBitmapIO * dSize + dBitmapPageCost * dNDV));
}

//---------------------------------------------------------------------------
//	@function:
//		CCostModelGPDB::CostScan
//
//	@doc:
//		Cost of scan
//
//---------------------------------------------------------------------------
CCost
CCostModelGPDB::CostScan(CMemoryPool *,	 // mp
						 CExpressionHandle &exprhdl,
						 const CCostModelGPDB *pcmgpdb, const SCostingInfo *pci)
{
	GPOS_ASSERT(NULL != pcmgpdb);
	GPOS_ASSERT(NULL != pci);

	COperator *pop = exprhdl.Pop();
	COperator::EOperatorId op_id = pop->Eopid();
	GPOS_ASSERT(COperator::EopPhysicalTableScan == op_id ||
				COperator::EopPhysicalDynamicTableScan == op_id ||
				COperator::EopPhysicalExternalScan == op_id);

	const CDouble dInitScan =
		pcmgpdb->GetCostModelParams()
			->PcpLookup(CCostModelParamsGPDB::EcpInitScanFactor)
			->Get();
	const CDouble dTableWidth =
		CPhysicalScan::PopConvert(pop)->PstatsBaseTable()->Width();

	// Get total rows for each host to scan
	const CDouble dTableScanCostUnit =
		pcmgpdb->GetCostModelParams()
			->PcpLookup(CCostModelParamsGPDB::EcpTableScanCostUnit)
			->Get();
	GPOS_ASSERT(0 < dTableScanCostUnit);

	switch (op_id)
	{
		case COperator::EopPhysicalTableScan:
		case COperator::EopPhysicalDynamicTableScan:
		case COperator::EopPhysicalExternalScan:
			// table scan cost considers only retrieving tuple cost,
			// since we scan the entire table here, the cost is correlated with table rows and table width,
			// since Scan's parent operator may be a filter that will be pushed into Scan node in GPDB plan,
			// we add Scan output tuple cost in the parent operator and not here
			return CCost(
				pci->NumRebinds() *
				(dInitScan + pci->Rows() * dTableWidth * dTableScanCostUnit));
		default:
			GPOS_ASSERT(!"invalid index scan");
			return CCost(0);
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CCostModelGPDB::CostFilter
//
//	@doc:
//		Cost of filter
//
//---------------------------------------------------------------------------
CCost
CCostModelGPDB::CostFilter(CMemoryPool *mp, CExpressionHandle &exprhdl,
						   const CCostModelGPDB *pcmgpdb,
						   const SCostingInfo *pci)
{
	GPOS_ASSERT(NULL != pcmgpdb);
	GPOS_ASSERT(NULL != pci);
	GPOS_ASSERT(COperator::EopPhysicalFilter == exprhdl.Pop()->Eopid());

	const DOUBLE dInput = pci->PdRows()[0];
	const ULONG ulFilterCols = exprhdl.DeriveUsedColumns(1)->Size();

	const CDouble dFilterColCostUnit =
		pcmgpdb->GetCostModelParams()
			->PcpLookup(CCostModelParamsGPDB::EcpFilterColCostUnit)
			->Get();
	GPOS_ASSERT(0 < dFilterColCostUnit);

	// filter cost is correlated with the input rows and the number of filter columns.
	CCost costLocal = CCost(dInput * ulFilterCols * dFilterColCostUnit);

	costLocal = CCost(costLocal.Get() * pci->NumRebinds());
	CCost costChild =
		CostChildren(mp, exprhdl, pci, pcmgpdb->GetCostModelParams());

	return costLocal + costChild;
}


//---------------------------------------------------------------------------
//	@function:
//		CCostModelGPDB::Cost
//
//	@doc:
//		Main driver
//
//---------------------------------------------------------------------------
CCost
CCostModelGPDB::Cost(
	CExpressionHandle &exprhdl,	 // handle gives access to expression properties
	const SCostingInfo *pci) const
{
	GPOS_ASSERT(NULL != pci);

	COperator::EOperatorId op_id = exprhdl.Pop()->Eopid();
	if (FUnary(op_id))
	{
		return CostUnary(m_mp, exprhdl, pci, m_cost_model_params);
	}

	FnCost *pfnc = NULL;
	const ULONG size = GPOS_ARRAY_SIZE(m_rgcm);

	// find the cost function corresponding to the given operator
	for (ULONG ul = 0; pfnc == NULL && ul < size; ul++)
	{
		if (op_id == m_rgcm[ul].m_eopid)
		{
			pfnc = m_rgcm[ul].m_pfnc;
		}
	}
	GPOS_ASSERT(NULL != pfnc);

	return pfnc(m_mp, exprhdl, this, pci);
}

// EOF
