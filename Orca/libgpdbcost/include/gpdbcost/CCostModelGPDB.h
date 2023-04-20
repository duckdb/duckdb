//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 Pivotal Inc.
//
//	@filename:
//		CCostModelGPDB.h
//
//	@doc:
//		GPDB cost model
//---------------------------------------------------------------------------
#ifndef GPDBCOST_CCostModelGPDB_H
#define GPDBCOST_CCostModelGPDB_H

#include "gpos/base.h"
#include "gpos/common/CDouble.h"

#include "gpdbcost/CCostModelParamsGPDB.h"
#include "gpopt/cost/CCost.h"
#include "gpopt/cost/ICostModel.h"
#include "gpopt/cost/ICostModelParams.h"


namespace gpdbcost
{
using namespace gpos;
using namespace gpopt;
using namespace gpmd;


//---------------------------------------------------------------------------
//	@class:
//		CCostModelGPDB
//
//	@doc:
//		GPDB cost model
//
//---------------------------------------------------------------------------
class CCostModelGPDB : public ICostModel
{
private:
	// definition of operator processor
	typedef CCost(FnCost)(CMemoryPool *, CExpressionHandle &,
						  const CCostModelGPDB *, const SCostingInfo *);

	//---------------------------------------------------------------------------
	//	@struct:
	//		SCostMapping
	//
	//	@doc:
	//		Mapping of operator to a cost function
	//
	//---------------------------------------------------------------------------
	struct SCostMapping
	{
		// physical operator id
		COperator::EOperatorId m_eopid;

		// pointer to cost function
		FnCost *m_pfnc;

	};	// struct SCostMapping

	// memory pool
	CMemoryPool *m_mp;

	// number of segments
	ULONG m_num_of_segments;

	// cost model parameters
	CCostModelParamsGPDB *m_cost_model_params;

	// array of mappings
	static const SCostMapping m_rgcm[];

	// return cost of processing the given number of rows
	static CCost CostTupleProcessing(DOUBLE rows, DOUBLE width,
									 ICostModelParams *pcp);

	// helper function to return cost of producing output tuples from Scan operator
	static CCost CostScanOutput(CMemoryPool *mp, DOUBLE rows, DOUBLE width,
								DOUBLE num_rebinds, ICostModelParams *pcp);

	// helper function to return cost of a plan rooted by unary operator
	static CCost CostUnary(CMemoryPool *mp, CExpressionHandle &exprhdl,
						   const SCostingInfo *pci, ICostModelParams *pcp);

	// cost of spooling
	static CCost CostSpooling(CMemoryPool *mp, CExpressionHandle &exprhdl,
							  const SCostingInfo *pci, ICostModelParams *pcp);

	// add up children cost
	static CCost CostChildren(CMemoryPool *mp, CExpressionHandle &exprhdl,
							  const SCostingInfo *pci, ICostModelParams *pcp);

	// returns cost of highest costed child
	static CCost CostMaxChild(CMemoryPool *mp, CExpressionHandle &exprhdl,
							  const SCostingInfo *pci, ICostModelParams *pcp);

	// check if given operator is unary
	static BOOL FUnary(COperator::EOperatorId op_id);

	// cost of scan
	static CCost CostScan(CMemoryPool *mp, CExpressionHandle &exprhdl,
						  const CCostModelGPDB *pcmgpdb,
						  const SCostingInfo *pci);

	// cost of filter
	static CCost CostFilter(CMemoryPool *mp, CExpressionHandle &exprhdl,
							const CCostModelGPDB *pcmgpdb,
							const SCostingInfo *pci);

	// cost of index scan
	static CCost CostIndexScan(CMemoryPool *mp, CExpressionHandle &exprhdl,
							   const CCostModelGPDB *pcmgpdb,
							   const SCostingInfo *pci);

	// cost of bitmap table scan
	static CCost CostBitmapTableScan(CMemoryPool *mp,
									 CExpressionHandle &exprhdl,
									 const CCostModelGPDB *pcmgpdb,
									 const SCostingInfo *pci);

	// cost of sequence project
	static CCost CostSequenceProject(CMemoryPool *mp,
									 CExpressionHandle &exprhdl,
									 const CCostModelGPDB *pcmgpdb,
									 const SCostingInfo *pci);

	// cost of CTE producer
	static CCost CostCTEProducer(CMemoryPool *mp, CExpressionHandle &exprhdl,
								 const CCostModelGPDB *pcmgpdb,
								 const SCostingInfo *pci);

	// cost of CTE consumer
	static CCost CostCTEConsumer(CMemoryPool *mp, CExpressionHandle &exprhdl,
								 const CCostModelGPDB *pcmgpdb,
								 const SCostingInfo *pci);

	// cost of const table get
	static CCost CostConstTableGet(CMemoryPool *mp, CExpressionHandle &exprhdl,
								   const CCostModelGPDB *pcmgpdb,
								   const SCostingInfo *pci);

	// cost of DML
	static CCost CostDML(CMemoryPool *mp, CExpressionHandle &exprhdl,
						 const CCostModelGPDB *pcmgpdb,
						 const SCostingInfo *pci);

	// cost of hash agg
	static CCost CostHashAgg(CMemoryPool *mp, CExpressionHandle &exprhdl,
							 const CCostModelGPDB *pcmgpdb,
							 const SCostingInfo *pci);

	// cost of scalar agg
	static CCost CostScalarAgg(CMemoryPool *mp, CExpressionHandle &exprhdl,
							   const CCostModelGPDB *pcmgpdb,
							   const SCostingInfo *pci);

	// cost of stream agg
	static CCost CostStreamAgg(CMemoryPool *mp, CExpressionHandle &exprhdl,
							   const CCostModelGPDB *pcmgpdb,
							   const SCostingInfo *pci);

	// cost of sequence
	static CCost CostSequence(CMemoryPool *mp, CExpressionHandle &exprhdl,
							  const CCostModelGPDB *pcmgpdb,
							  const SCostingInfo *pci);

	// cost of sort
	static CCost CostSort(CMemoryPool *mp, CExpressionHandle &exprhdl,
						  const CCostModelGPDB *pcmgpdb,
						  const SCostingInfo *pci);

	// cost of TVF
	static CCost CostTVF(CMemoryPool *mp, CExpressionHandle &exprhdl,
						 const CCostModelGPDB *pcmgpdb,
						 const SCostingInfo *pci);

	// cost of UnionAll
	static CCost CostUnionAll(CMemoryPool *mp, CExpressionHandle &exprhdl,
							  const CCostModelGPDB *pcmgpdb,
							  const SCostingInfo *pci);

	// cost of hash join
	static CCost CostHashJoin(CMemoryPool *mp, CExpressionHandle &exprhdl,
							  const CCostModelGPDB *pcmgpdb,
							  const SCostingInfo *pci);

	// cost of merge join
	static CCost CostMergeJoin(CMemoryPool *mp, CExpressionHandle &exprhdl,
							   const CCostModelGPDB *pcmgpdb,
							   const SCostingInfo *pci);

	// cost of nljoin
	static CCost CostNLJoin(CMemoryPool *mp, CExpressionHandle &exprhdl,
							const CCostModelGPDB *pcmgpdb,
							const SCostingInfo *pci);

	// cost of inner or outer index-nljoin
	static CCost CostIndexNLJoin(CMemoryPool *mp, CExpressionHandle &exprhdl,
								 const CCostModelGPDB *pcmgpdb,
								 const SCostingInfo *pci);

	// cost of motion
	static CCost CostMotion(CMemoryPool *mp, CExpressionHandle &exprhdl,
							const CCostModelGPDB *pcmgpdb,
							const SCostingInfo *pci);

	// cost of bitmap scan when the NDV is small
	static CCost CostBitmapSmallNDV(const CCostModelGPDB *pcmgpdb,
									const SCostingInfo *pci, CDouble dNDV);

	// cost of bitmap scan when the NDV is large
	static CCost CostBitmapLargeNDV(const CCostModelGPDB *pcmgpdb,
									const SCostingInfo *pci, CDouble dNDV);

public:
	// ctor
	CCostModelGPDB(CMemoryPool *mp, ULONG ulSegments,
				   CCostModelParamsGPDB *pcp = NULL);

	// dtor
	virtual ~CCostModelGPDB();

	// number of segments
	ULONG
	UlHosts() const
	{
		return m_num_of_segments;
	}

	// return number of rows per host
	virtual CDouble DRowsPerHost(CDouble dRowsTotal) const;

	// return cost model parameters
	virtual ICostModelParams *
	GetCostModelParams() const
	{
		return m_cost_model_params;
	}


	// main driver for cost computation
	virtual CCost Cost(CExpressionHandle &exprhdl,
					   const SCostingInfo *pci) const;

	// cost model type
	virtual ECostModelType
	Ecmt() const
	{
		return ICostModel::EcmtGPDBCalibrated;
	}

};	// class CCostModelGPDB

}  // namespace gpdbcost

#endif	// !GPDBCOST_CCostModelGPDB_H

// EOF
