//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 Pivotal Inc.
//
//	@filename:
//		CCostModelGPDBLegacy.cpp
//
//	@doc:
//		Implementation of GPDB's legacy cost model
//---------------------------------------------------------------------------

#include "gpdbcost/CCostModelGPDBLegacy.h"

#include "gpopt/base/COrderSpec.h"
#include "gpopt/base/CWindowFrame.h"
#include "gpopt/metadata/CIndexDescriptor.h"
#include "gpopt/metadata/CTableDescriptor.h"
#include "gpopt/operators/CExpressionHandle.h"
#include "gpopt/operators/CPhysicalSequenceProject.h"

using namespace gpos;
using namespace gpdbcost;


// initialization of cost functions
const CCostModelGPDBLegacy::SCostMapping CCostModelGPDBLegacy::m_rgcm[] = {
	{COperator::EopPhysicalTableScan, CostScan},
	{COperator::EopPhysicalDynamicTableScan, CostScan},
	{COperator::EopPhysicalExternalScan, CostScan},

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
};

//---------------------------------------------------------------------------
//	@function:
//		CCostModelGPDBLegacy::CCostModelGPDBLegacy
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CCostModelGPDBLegacy::CCostModelGPDBLegacy(CMemoryPool *mp, ULONG ulSegments,
										   ICostModelParamsArray *pdrgpcp)
	: m_mp(mp), m_num_of_segments(ulSegments)
{
	GPOS_ASSERT(0 < ulSegments);

	m_cost_model_params = GPOS_NEW(mp) CCostModelParamsGPDBLegacy(mp);
	SetParams(pdrgpcp);
}


//---------------------------------------------------------------------------
//	@function:
//		CCostModelGPDBLegacy::DRowsPerHost
//
//	@doc:
//		Return number of rows per host
//
//---------------------------------------------------------------------------
CDouble
CCostModelGPDBLegacy::DRowsPerHost(CDouble dRowsTotal) const
{
	return dRowsTotal / m_num_of_segments;
}


//---------------------------------------------------------------------------
//	@function:
//		CCostModelGPDBLegacy::~CCostModelGPDBLegacy
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CCostModelGPDBLegacy::~CCostModelGPDBLegacy()
{
	m_cost_model_params->Release();
}


//---------------------------------------------------------------------------
//	@function:
//		CCostModelGPDBLegacy::CostTupleProcessing
//
//	@doc:
//		Cost of tuple processing
//
//---------------------------------------------------------------------------
CCost
CCostModelGPDBLegacy::CostTupleProcessing(DOUBLE rows, DOUBLE width,
										  ICostModelParams *pcp)
{
	GPOS_ASSERT(NULL != pcp);

	CDouble dTupProcBandwidth =
		pcp->PcpLookup(CCostModelParamsGPDBLegacy::EcpTupProcBandwidth)->Get();
	GPOS_ASSERT(0 < dTupProcBandwidth);

	return CCost((rows * width) / dTupProcBandwidth);
}


//---------------------------------------------------------------------------
//	@function:
//		CCostModelGPDBLegacy::CostUnary
//
//	@doc:
//		Helper function to return cost of a plan rooted by unary operator
//
//---------------------------------------------------------------------------
CCost
CCostModelGPDBLegacy::CostUnary(DOUBLE rows, DOUBLE width, DOUBLE num_rebinds,
								DOUBLE *pdcostChildren, ICostModelParams *pcp)
{
	GPOS_ASSERT(NULL != pcp);

	CCost costLocal =
		CCost(num_rebinds * CostTupleProcessing(rows, width, pcp).Get());
	CCost costChild = CostSum(pdcostChildren, 1 /*size*/);

	return costLocal + costChild;
}


//---------------------------------------------------------------------------
//	@function:
//		CCostModelGPDBLegacy::CostSpooling
//
//	@doc:
//		Helper function to compute spooling cost
//
//---------------------------------------------------------------------------
CCost
CCostModelGPDBLegacy::CostSpooling(DOUBLE rows, DOUBLE width,
								   DOUBLE num_rebinds, DOUBLE *pdcostChildren,
								   ICostModelParams *pcp)
{
	GPOS_ASSERT(NULL != pcp);

	return CostUnary(rows, width, num_rebinds, pdcostChildren, pcp);
}


//---------------------------------------------------------------------------
//	@function:
//		CCostModelGPDBLegacy::CostRedistribute
//
//	@doc:
//		Cost of various redistribute motion operators
//
//---------------------------------------------------------------------------
CCost
CCostModelGPDBLegacy::CostRedistribute(DOUBLE rows, DOUBLE width,
									   ICostModelParams *pcp)
{
	GPOS_ASSERT(NULL != pcp);

	CDouble dNetBandwidth =
		pcp->PcpLookup(CCostModelParamsGPDBLegacy::EcpNetBandwidth)->Get();
	GPOS_ASSERT(0 < dNetBandwidth);

	return CCost(rows * width / dNetBandwidth);
}


//---------------------------------------------------------------------------
//	@function:
//		CCostModelGPDBLegacy::FUnary
//
//	@doc:
//		Check if given operator is unary
//
//---------------------------------------------------------------------------
BOOL
CCostModelGPDBLegacy::FUnary(COperator::EOperatorId op_id)
{
	return COperator::EopPhysicalAssert == op_id ||
		   COperator::EopPhysicalComputeScalar == op_id ||
		   COperator::EopPhysicalFilter == op_id ||
		   COperator::EopPhysicalLimit == op_id ||
		   COperator::EopPhysicalPartitionSelector == op_id ||
		   COperator::EopPhysicalPartitionSelectorDML == op_id ||
		   COperator::EopPhysicalRowTrigger == op_id ||
		   COperator::EopPhysicalSplit == op_id ||
		   COperator::EopPhysicalSpool == op_id;
}


//---------------------------------------------------------------------------
//	@function:
//		CCostModelGPDBLegacy::Cost
//
//	@doc:
//		Add up array of costs
//
//---------------------------------------------------------------------------
CCost
CCostModelGPDBLegacy::CostSum(DOUBLE *pdCost, ULONG size)
{
	GPOS_ASSERT(NULL != pdCost);

	DOUBLE res = 1.0;
	for (ULONG ul = 0; ul < size; ul++)
	{
		res = res + pdCost[ul];
	}

	return CCost(res);
}


//---------------------------------------------------------------------------
//	@function:
//		CCostModelGPDBLegacy::CostCTEProducer
//
//	@doc:
//		Cost of CTE producer
//
//---------------------------------------------------------------------------
CCost
CCostModelGPDBLegacy::CostCTEProducer(CMemoryPool *,  // mp
									  CExpressionHandle &exprhdl,
									  const CCostModelGPDBLegacy *pcmgpdb,
									  const SCostingInfo *pci)
{
	GPOS_ASSERT(NULL != pcmgpdb);
	GPOS_ASSERT(NULL != pci);
	GPOS_ASSERT(COperator::EopPhysicalCTEProducer == exprhdl.Pop()->Eopid());

	CCost cost = CostUnary(pci->Rows(), pci->Width(), pci->NumRebinds(),
						   pci->PdCost(), pcmgpdb->GetCostModelParams());

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
	CCost costSpooling =
		CostSpooling(pci->Rows(), pci->Width(), pci->NumRebinds(),
					 pci->PdCost(), pcmgpdb->GetCostModelParams());

	return cost + costSpooling;
}


//---------------------------------------------------------------------------
//	@function:
//		CCostModelGPDBLegacy::CostCTEConsumer
//
//	@doc:
//		Cost of CTE consumer
//
//---------------------------------------------------------------------------
CCost
CCostModelGPDBLegacy::CostCTEConsumer(CMemoryPool *,  // mp
									  CExpressionHandle &
#ifdef GPOS_DEBUG
										  exprhdl
#endif	// GPOS_DEBUG
									  ,
									  const CCostModelGPDBLegacy *pcmgpdb,
									  const SCostingInfo *pci)
{
	GPOS_ASSERT(NULL != pcmgpdb);
	GPOS_ASSERT(NULL != pci);
	GPOS_ASSERT(COperator::EopPhysicalCTEConsumer == exprhdl.Pop()->Eopid());

	CDouble dSeqIOBandwidth =
		pcmgpdb->GetCostModelParams()
			->PcpLookup(CCostModelParamsGPDBLegacy::EcpSeqIOBandwidth)
			->Get();
	GPOS_ASSERT(0 < dSeqIOBandwidth);

	return CCost(pci->NumRebinds() * pci->Rows() * pci->Width() /
				 dSeqIOBandwidth);
}


//---------------------------------------------------------------------------
//	@function:
//		CCostModelGPDBLegacy::CostConstTableGet
//
//	@doc:
//		Cost of const table get
//
//---------------------------------------------------------------------------
CCost
CCostModelGPDBLegacy::CostConstTableGet(CMemoryPool *,	// mp
										CExpressionHandle &
#ifdef GPOS_DEBUG
											exprhdl
#endif	// GPOS_DEBUG
										,
										const CCostModelGPDBLegacy *pcmgpdb,
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
//		CCostModelGPDBLegacy::CostDML
//
//	@doc:
//		Cost of DML
//
//---------------------------------------------------------------------------
CCost
CCostModelGPDBLegacy::CostDML(CMemoryPool *,  // mp
							  CExpressionHandle &
#ifdef GPOS_DEBUG
								  exprhdl
#endif	// GPOS_DEBUG
							  ,
							  const CCostModelGPDBLegacy *pcmgpdb,
							  const SCostingInfo *pci)
{
	GPOS_ASSERT(NULL != pcmgpdb);
	GPOS_ASSERT(NULL != pci);
	GPOS_ASSERT(COperator::EopPhysicalDML == exprhdl.Pop()->Eopid());

	CDouble dTupUpdateBandwidth =
		pcmgpdb->GetCostModelParams()
			->PcpLookup(CCostModelParamsGPDBLegacy::EcpTupUpdateBandwith)
			->Get();
	GPOS_ASSERT(0 < dTupUpdateBandwidth);

	DOUBLE num_rows_outer = pci->PdRows()[0];
	DOUBLE dWidthOuter = pci->GetWidth()[0];

	CCost costLocal = CCost(pci->NumRebinds() * (num_rows_outer * dWidthOuter) /
							dTupUpdateBandwidth);
	CCost costChild = CostSum(pci->PdCost(), pci->ChildCount());

	return costLocal + costChild;
}


//---------------------------------------------------------------------------
//	@function:
//		CCostModelGPDBLegacy::CostScalarAgg
//
//	@doc:
//		Cost of scalar agg
//
//---------------------------------------------------------------------------
CCost
CCostModelGPDBLegacy::CostScalarAgg(CMemoryPool *,	// mp
									CExpressionHandle &
#ifdef GPOS_DEBUG
										exprhdl
#endif	// GPOS_DEBUG
									,
									const CCostModelGPDBLegacy *pcmgpdb,
									const SCostingInfo *pci)
{
	GPOS_ASSERT(NULL != pcmgpdb);
	GPOS_ASSERT(NULL != pci);
	GPOS_ASSERT(COperator::EopPhysicalScalarAgg == exprhdl.Pop()->Eopid());

	DOUBLE num_rows_outer = pci->PdRows()[0];
	DOUBLE dWidthOuter = pci->GetWidth()[0];

	// local cost is the cost of processing 1 tuple (size of output) +
	// cost of processing N tuples (size of input)
	CDouble dTupProcBandwidth =
		pcmgpdb->GetCostModelParams()
			->PcpLookup(CCostModelParamsGPDBLegacy::EcpTupProcBandwidth)
			->Get();
	GPOS_ASSERT(0 < dTupProcBandwidth);

	CCost costLocal =
		CCost(pci->NumRebinds() *
			  (1.0 * pci->Width() + num_rows_outer * dWidthOuter) /
			  dTupProcBandwidth);
	CCost costChild = CostSum(pci->PdCost(), pci->ChildCount());

	return costLocal + costChild;
}


//---------------------------------------------------------------------------
//	@function:
//		CCostModelGPDBLegacy::CostStreamAgg
//
//	@doc:
//		Cost of stream agg
//
//---------------------------------------------------------------------------
CCost
CCostModelGPDBLegacy::CostStreamAgg(CMemoryPool *,	// mp
									CExpressionHandle &
#ifdef GPOS_DEBUG
										exprhdl
#endif	// GPOS_DEBUG
									,
									const CCostModelGPDBLegacy *pcmgpdb,
									const SCostingInfo *pci)
{
	GPOS_ASSERT(NULL != pcmgpdb);
	GPOS_ASSERT(NULL != pci);

#ifdef GPOS_DEBUG
	COperator::EOperatorId op_id = exprhdl.Pop()->Eopid();
	GPOS_ASSERT(COperator::EopPhysicalStreamAgg == op_id ||
				COperator::EopPhysicalStreamAggDeduplicate == op_id);
#endif	// GPOS_DEBUG

	DOUBLE num_rows_outer = pci->PdRows()[0];
	DOUBLE dWidthOuter = pci->GetWidth()[0];

	CDouble dTupProcBandwidth =
		pcmgpdb->GetCostModelParams()
			->PcpLookup(CCostModelParamsGPDBLegacy::EcpTupProcBandwidth)
			->Get();
	GPOS_ASSERT(0 < dTupProcBandwidth);

	CCost costLocal =
		CCost(pci->NumRebinds() *
			  (pci->Rows() * pci->Width() + num_rows_outer * dWidthOuter) /
			  dTupProcBandwidth);
	CCost costChild = CostSum(pci->PdCost(), pci->ChildCount());

	return costLocal + costChild;
}


//---------------------------------------------------------------------------
//	@function:
//		CCostModelGPDBLegacy::CostSequence
//
//	@doc:
//		Cost of sequence
//
//---------------------------------------------------------------------------
CCost
CCostModelGPDBLegacy::CostSequence(CMemoryPool *,  // mp
								   CExpressionHandle &
#ifdef GPOS_DEBUG
									   exprhdl
#endif	// GPOS_DEBUG
								   ,
								   const CCostModelGPDBLegacy *pcmgpdb,
								   const SCostingInfo *pci)
{
	GPOS_ASSERT(NULL != pcmgpdb);
	GPOS_ASSERT(NULL != pci);
	GPOS_ASSERT(COperator::EopPhysicalSequence == exprhdl.Pop()->Eopid());

	CCost costLocal = CCost(pci->NumRebinds() *
							CostTupleProcessing(pci->Rows(), pci->Width(),
												pcmgpdb->GetCostModelParams())
								.Get());
	CCost costChild = CostSum(pci->PdCost(), pci->ChildCount());

	return costLocal + costChild;
}


//---------------------------------------------------------------------------
//	@function:
//		CCostModelGPDBLegacy::CostSort
//
//	@doc:
//		Cost of sort
//
//---------------------------------------------------------------------------
CCost
CCostModelGPDBLegacy::CostSort(CMemoryPool *,  // mp
							   CExpressionHandle &
#ifdef GPOS_DEBUG
								   exprhdl
#endif	// GPOS_DEBUG
							   ,
							   const CCostModelGPDBLegacy *pcmgpdb,
							   const SCostingInfo *pci)
{
	GPOS_ASSERT(NULL != pcmgpdb);
	GPOS_ASSERT(NULL != pci);
	GPOS_ASSERT(COperator::EopPhysicalSort == exprhdl.Pop()->Eopid());

	// log operation below
	CDouble rows = CDouble(std::max(1.0, pci->Rows()));
	CDouble num_rebinds = CDouble(pci->NumRebinds());
	CDouble width = CDouble(pci->Width());
	CDouble dTupProcBandwidth =
		pcmgpdb->GetCostModelParams()
			->PcpLookup(CCostModelParamsGPDBLegacy::EcpTupProcBandwidth)
			->Get();
	GPOS_ASSERT(0 < dTupProcBandwidth);

	CCost costLocal = CCost(
		num_rebinds * (rows * rows.Log2() * width * width / dTupProcBandwidth));
	CCost costChild = CostSum(pci->PdCost(), pci->ChildCount());

	return costLocal + costChild;
}


//---------------------------------------------------------------------------
//	@function:
//		CCostModelGPDBLegacy::CostTVF
//
//	@doc:
//		Cost of table valued function
//
//---------------------------------------------------------------------------
CCost
CCostModelGPDBLegacy::CostTVF(CMemoryPool *,  // mp
							  CExpressionHandle &
#ifdef GPOS_DEBUG
								  exprhdl
#endif	// GPOS_DEBUG
							  ,
							  const CCostModelGPDBLegacy *pcmgpdb,
							  const SCostingInfo *pci)
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
//		CCostModelGPDBLegacy::CostUnionAll
//
//	@doc:
//		Cost of UnionAll
//
//---------------------------------------------------------------------------
CCost
CCostModelGPDBLegacy::CostUnionAll(CMemoryPool *,  // mp
								   CExpressionHandle &
#ifdef GPOS_DEBUG
									   exprhdl
#endif	// GPOS_DEBUG
								   ,
								   const CCostModelGPDBLegacy *pcmgpdb,
								   const SCostingInfo *pci)
{
	GPOS_ASSERT(NULL != pcmgpdb);
	GPOS_ASSERT(NULL != pci);
	GPOS_ASSERT(COperator::EopPhysicalSerialUnionAll == exprhdl.Pop()->Eopid());

	CCost costLocal = CCost(pci->NumRebinds() *
							CostTupleProcessing(pci->Rows(), pci->Width(),
												pcmgpdb->GetCostModelParams())
								.Get());
	CCost costChild = CostSum(pci->PdCost(), pci->ChildCount());

	return costLocal + costChild;
}


//---------------------------------------------------------------------------
//	@function:
//		CCostModelGPDBLegacy::CostHashAgg
//
//	@doc:
//		Cost of hash agg
//
//---------------------------------------------------------------------------
CCost
CCostModelGPDBLegacy::CostHashAgg(CMemoryPool *,  // mp
								  CExpressionHandle &
#ifdef GPOS_DEBUG
									  exprhdl
#endif	// GPOS_DEBUG
								  ,
								  const CCostModelGPDBLegacy *pcmgpdb,
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
	DOUBLE dWidthOuter = pci->GetWidth()[0];

	CDouble dTupProcBandwidth =
		pcmgpdb->GetCostModelParams()
			->PcpLookup(CCostModelParamsGPDBLegacy::EcpTupProcBandwidth)
			->Get();
	CDouble dHashFactor =
		pcmgpdb->GetCostModelParams()
			->PcpLookup(CCostModelParamsGPDBLegacy::EcpHashFactor)
			->Get();
	GPOS_ASSERT(0 < dTupProcBandwidth);

	CCost costLocal = CCost(pci->NumRebinds() *
							(pci->Rows() * pci->Width() +
							 dHashFactor * num_rows_outer * dWidthOuter) /
							dTupProcBandwidth);
	CCost costChild = CostSum(pci->PdCost(), pci->ChildCount());

	return costLocal + costChild;
}


//---------------------------------------------------------------------------
//	@function:
//		CCostModelGPDBLegacy::CostHashJoin
//
//	@doc:
//		Cost of hash join
//
//---------------------------------------------------------------------------
CCost
CCostModelGPDBLegacy::CostHashJoin(CMemoryPool *,  // mp
								   CExpressionHandle &
#ifdef GPOS_DEBUG
									   exprhdl
#endif	// GPOS_DEBUG
								   ,
								   const CCostModelGPDBLegacy *pcmgpdb,
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

	DOUBLE num_rows_outer = pci->PdRows()[0];
	DOUBLE dWidthOuter = pci->GetWidth()[0];
	DOUBLE dRowsInner = pci->PdRows()[1];
	DOUBLE dWidthInner = pci->GetWidth()[1];

	CDouble dHashFactor =
		pcmgpdb->GetCostModelParams()
			->PcpLookup(CCostModelParamsGPDBLegacy::EcpHashFactor)
			->Get();
	CDouble dHashJoinFactor =
		pcmgpdb->GetCostModelParams()
			->PcpLookup(CCostModelParamsGPDBLegacy::EcpHJFactor)
			->Get();
	CDouble dTupProcBandwidth =
		pcmgpdb->GetCostModelParams()
			->PcpLookup(CCostModelParamsGPDBLegacy::EcpTupProcBandwidth)
			->Get();
	GPOS_ASSERT(0 < dTupProcBandwidth);

	CCost costLocal =
		CCost(pci->NumRebinds() *
			  (num_rows_outer * dWidthOuter +
			   dRowsInner * dWidthInner * dHashFactor * dHashJoinFactor) /
			  dTupProcBandwidth);
	CCost costChild = CostSum(pci->PdCost(), pci->ChildCount());

	return costChild + costLocal;
}


//---------------------------------------------------------------------------
//	@function:
//		CCostModelGPDBLegacy::CostIndexNLJoin
//
//	@doc:
//		Cost of inner or outer index-nljoin
//
//---------------------------------------------------------------------------
CCost
CCostModelGPDBLegacy::CostIndexNLJoin(CMemoryPool *,  // mp
									  CExpressionHandle &
#ifdef GPOS_DEBUG
										  exprhdl
#endif	// GPOS_DEBUG
									  ,
									  const CCostModelGPDBLegacy *pcmgpdb,
									  const SCostingInfo *pci)
{
	GPOS_ASSERT(NULL != pcmgpdb);
	GPOS_ASSERT(NULL != pci);
	GPOS_ASSERT(
		COperator::EopPhysicalInnerIndexNLJoin == exprhdl.Pop()->Eopid() ||
		COperator::EopPhysicalLeftOuterIndexNLJoin == exprhdl.Pop()->Eopid());

	CCost costLocal = CCost(pci->NumRebinds() *
							CostTupleProcessing(pci->Rows(), pci->Width(),
												pcmgpdb->GetCostModelParams())
								.Get());
	CCost costChild = CostSum(pci->PdCost(), pci->ChildCount());

	return costLocal + costChild;
}


//---------------------------------------------------------------------------
//	@function:
//		CCostModelGPDBLegacy::CostNLJoin
//
//	@doc:
//		Cost of nljoin
//
//---------------------------------------------------------------------------
CCost
CCostModelGPDBLegacy::CostNLJoin(CMemoryPool *,	 // mp
								 CExpressionHandle &
#ifdef GPOS_DEBUG
									 exprhdl
#endif	// GPOS_DEBUG
								 ,
								 const CCostModelGPDBLegacy *pcmgpdb,
								 const SCostingInfo *pci)
{
	GPOS_ASSERT(NULL != pcmgpdb);
	GPOS_ASSERT(NULL != pci);
	GPOS_ASSERT(CUtils::FNLJoin(exprhdl.Pop()));

	DOUBLE num_rows_outer = pci->PdRows()[0];
	DOUBLE dWidthOuter = pci->GetWidth()[0];
	DOUBLE dRowsInner = pci->PdRows()[1];
	DOUBLE dWidthInner = pci->GetWidth()[1];

	CDouble dNLJFactor =
		pcmgpdb->GetCostModelParams()
			->PcpLookup(CCostModelParamsGPDBLegacy::EcpNLJFactor)
			->Get();
	CDouble dNLJOuterFactor =
		pcmgpdb->GetCostModelParams()
			->PcpLookup(CCostModelParamsGPDBLegacy::EcpNLJOuterFactor)
			->Get();
	CDouble dTupProcBandwidth =
		pcmgpdb->GetCostModelParams()
			->PcpLookup(CCostModelParamsGPDBLegacy::EcpTupProcBandwidth)
			->Get();
	GPOS_ASSERT(0 < dTupProcBandwidth);
	GPOS_ASSERT(0 < dNLJOuterFactor);
	GPOS_ASSERT(0 < dNLJFactor);

	CCost costLocal = CCost(pci->NumRebinds() *
							(num_rows_outer * dWidthOuter * dRowsInner *
							 dWidthInner * dNLJOuterFactor) /
							dTupProcBandwidth);
	CCost costChild = CostSum(pci->PdCost(), pci->ChildCount());

	CCost costTotal = CCost(costLocal + costChild);

	// amplify NLJ cost based on NLJ factor
	return CCost(costTotal * dNLJFactor);
}


//---------------------------------------------------------------------------
//	@function:
//		CCostModelGPDBLegacy::CostMotion
//
//	@doc:
//		Cost of motion
//
//---------------------------------------------------------------------------
CCost
CCostModelGPDBLegacy::CostMotion(CMemoryPool *,	 // mp
								 CExpressionHandle &exprhdl,
								 const CCostModelGPDBLegacy *pcmgpdb,
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

	DOUBLE num_rows_outer = pci->PdRows()[0];
	DOUBLE dWidthOuter = pci->GetWidth()[0];

	CCost costLocal(0);
	if (COperator::EopPhysicalMotionBroadcast == op_id)
	{
		// broadcast cost is amplified by the number of segments
		CDouble dNetBandwidth =
			pcmgpdb->GetCostModelParams()
				->PcpLookup(CCostModelParamsGPDBLegacy::EcpNetBandwidth)
				->Get();
		GPOS_ASSERT(0 < dNetBandwidth);

		costLocal = CCost(pci->NumRebinds() * num_rows_outer * dWidthOuter *
						  pcmgpdb->UlHosts() / dNetBandwidth);
	}
	else
	{
		costLocal = CCost(pci->NumRebinds() *
						  CostRedistribute(num_rows_outer, dWidthOuter,
										   pcmgpdb->GetCostModelParams())
							  .Get());
	}
	CCost costChild = CostSum(pci->PdCost(), pci->ChildCount());

	return costLocal + costChild;
}


//---------------------------------------------------------------------------
//	@function:
//		CCostModelGPDBLegacy::CostSequenceProject
//
//	@doc:
//		Cost of sequence project
//
//---------------------------------------------------------------------------
CCost
CCostModelGPDBLegacy::CostSequenceProject(CMemoryPool *,  // mp
										  CExpressionHandle &exprhdl,
										  const CCostModelGPDBLegacy *pcmgpdb,
										  const SCostingInfo *pci)
{
	GPOS_ASSERT(NULL != pcmgpdb);
	GPOS_ASSERT(NULL != pci);
	GPOS_ASSERT(COperator::EopPhysicalSequenceProject ==
				exprhdl.Pop()->Eopid());

	DOUBLE num_rows_outer = pci->PdRows()[0];
	DOUBLE dWidthOuter = pci->GetWidth()[0];

	ULONG ulSortCols = 0;
	COrderSpecArray *pdrgpos =
		CPhysicalSequenceProject::PopConvert(exprhdl.Pop())->Pdrgpos();
	const ULONG ulOrderSpecs = pdrgpos->Size();
	for (ULONG ul = 0; ul < ulOrderSpecs; ul++)
	{
		COrderSpec *pos = (*pdrgpos)[ul];
		ulSortCols += pos->UlSortColumns();
	}

	CDouble dTupProcBandwidth =
		pcmgpdb->GetCostModelParams()
			->PcpLookup(CCostModelParamsGPDBLegacy::EcpTupProcBandwidth)
			->Get();
	GPOS_ASSERT(0 < dTupProcBandwidth);

	// we process (sorted window of) input tuples to compute window function values
	CCost costLocal =
		CCost(pci->NumRebinds() * (ulSortCols * num_rows_outer * dWidthOuter) /
			  dTupProcBandwidth);
	CCost costChild = CostSum(pci->PdCost(), pci->ChildCount());

	return costLocal + costChild;
}


//---------------------------------------------------------------------------
//	@function:
//		CCostModelGPDBLegacy::CostIndexScan
//
//	@doc:
//		Cost of index scan
//
//---------------------------------------------------------------------------
CCost
CCostModelGPDBLegacy::CostIndexScan(CMemoryPool *,	// mp
									CExpressionHandle &exprhdl,
									const CCostModelGPDBLegacy *pcmgpdb,
									const SCostingInfo *pci)
{
	GPOS_ASSERT(NULL != pcmgpdb);
	GPOS_ASSERT(NULL != pci);

	COperator::EOperatorId op_id = exprhdl.Pop()->Eopid();
	GPOS_ASSERT(COperator::EopPhysicalIndexScan == op_id ||
				COperator::EopPhysicalDynamicIndexScan == op_id);

	CDouble dRandomIOBandwidth =
		pcmgpdb->GetCostModelParams()
			->PcpLookup(CCostModelParamsGPDBLegacy::EcpRandomIOBandwidth)
			->Get();
	GPOS_ASSERT(0 < dRandomIOBandwidth);

	switch (op_id)
	{
		case COperator::EopPhysicalDynamicIndexScan:
		case COperator::EopPhysicalIndexScan:
			return CCost(pci->NumRebinds() * (pci->Rows() * pci->Width()) /
						 dRandomIOBandwidth);

		default:
			GPOS_ASSERT(!"invalid index scan");
			return CCost(0);
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CCostModelGPDBLegacy::CostBitmapTableScan
//
//	@doc:
//		Cost of bitmap table scan
//
//---------------------------------------------------------------------------
CCost
CCostModelGPDBLegacy::CostBitmapTableScan(CMemoryPool *,  // mp,
										  CExpressionHandle &
#ifdef GPOS_DEBUG
											  exprhdl
#endif	// GPOS_DEBUG
										  ,
										  const CCostModelGPDBLegacy *pcmgpdb,
										  const SCostingInfo *pci)
{
	GPOS_ASSERT(NULL != pcmgpdb);
	GPOS_ASSERT(NULL != pci);
	GPOS_ASSERT(
		COperator::EopPhysicalBitmapTableScan == exprhdl.Pop()->Eopid() ||
		COperator::EopPhysicalDynamicBitmapTableScan == exprhdl.Pop()->Eopid());

	CDouble dSeqIOBandwidth =
		pcmgpdb->GetCostModelParams()
			->PcpLookup(CCostModelParamsGPDBLegacy::EcpSeqIOBandwidth)
			->Get();
	GPOS_ASSERT(0 < dSeqIOBandwidth);

	// TODO: ; 2014-04-11; compute the real cost here
	return CCost(pci->NumRebinds() * (pci->Rows() * pci->Width()) /
				 dSeqIOBandwidth * 0.5);
}


//---------------------------------------------------------------------------
//	@function:
//		CCostModelGPDBLegacy::CostScan
//
//	@doc:
//		Cost of scan
//
//---------------------------------------------------------------------------
CCost
CCostModelGPDBLegacy::CostScan(CMemoryPool *,  // mp
							   CExpressionHandle &exprhdl,
							   const CCostModelGPDBLegacy *pcmgpdb,
							   const SCostingInfo *pci)
{
	GPOS_ASSERT(NULL != pcmgpdb);
	GPOS_ASSERT(NULL != pci);

	COperator *pop = exprhdl.Pop();
	COperator::EOperatorId op_id = pop->Eopid();
	GPOS_ASSERT(COperator::EopPhysicalTableScan == op_id ||
				COperator::EopPhysicalDynamicTableScan == op_id ||
				COperator::EopPhysicalExternalScan == op_id);

	CDouble dSeqIOBandwidth =
		pcmgpdb->GetCostModelParams()
			->PcpLookup(CCostModelParamsGPDBLegacy::EcpSeqIOBandwidth)
			->Get();
	GPOS_ASSERT(0 < dSeqIOBandwidth);

	switch (op_id)
	{
		case COperator::EopPhysicalTableScan:
		case COperator::EopPhysicalDynamicTableScan:
		case COperator::EopPhysicalExternalScan:
			return CCost(pci->NumRebinds() * (pci->Rows() * pci->Width()) /
						 dSeqIOBandwidth);

		default:
			GPOS_ASSERT(!"invalid index scan");
			return CCost(0);
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CCostModelGPDBLegacy::Cost
//
//	@doc:
//		Main driver
//
//---------------------------------------------------------------------------
CCost
CCostModelGPDBLegacy::Cost(
	CExpressionHandle &exprhdl,	 // handle gives access to expression properties
	const SCostingInfo *pci) const
{
	GPOS_ASSERT(NULL != pci);

	COperator::EOperatorId op_id = exprhdl.Pop()->Eopid();
	if (FUnary(op_id))
	{
		return CostUnary(pci->Rows(), pci->Width(), pci->NumRebinds(),
						 pci->PdCost(), m_cost_model_params);
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
