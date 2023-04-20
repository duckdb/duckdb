//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 Pivotal Inc.
//
//	@filename:
//		CCostModelParamsGPDB.h
//
//	@doc:
//		Parameters in GPDB cost model
//---------------------------------------------------------------------------
#ifndef GPDBCOST_CCostModelParamsGPDB_H
#define GPDBCOST_CCostModelParamsGPDB_H

#include "gpos/base.h"
#include "gpos/common/CDouble.h"
#include "gpos/common/CRefCount.h"
#include "gpos/string/CWStringConst.h"

#include "gpopt/cost/ICostModelParams.h"


namespace gpopt
{
using namespace gpos;


//---------------------------------------------------------------------------
//	@class:
//		CCostModelParamsGPDB
//
//	@doc:
//		Parameters in GPDB cost model
//
//---------------------------------------------------------------------------
class CCostModelParamsGPDB : public ICostModelParams
{
public:
	// enumeration of cost model params
	enum ECostParam
	{
		EcpSeqIOBandwidth = 0,	// sequential i/o bandwidth
		EcpRandomIOBandwidth,	// random i/o bandwidth
		EcpTupProcBandwidth,	// tuple processing bandwidth
		EcpOutputBandwidth,		// output bandwidth
		EcpInitScanFactor,		// scan initialization cost factor
		EcpTableScanCostUnit,	// table scan cost per tuple
		// index scan params
		EcpInitIndexScanFactor,	  // index scan initialization cost factor
		EcpIndexBlockCostUnit,	  // coefficient for reading leaf index blocks
		EcpIndexFilterCostUnit,	  // coefficient for index column filtering
		EcpIndexScanTupCostUnit,  // index scan cost per tuple retrieving
		EcpIndexScanTupRandomFactor,  // random IO cost per tuple retrieving
		// filter operator params
		EcpFilterColCostUnit,  // coefficient for filtering tuple per column
		EcpOutputTupCostUnit,  // coefficient for output tuple per width
		// motion operator params
		EcpGatherSendCostUnit,	// sending cost per tuple in gather motion
		EcpGatherRecvCostUnit,	// receiving cost per tuple in gather motion
		EcpRedistributeSendCostUnit,  // sending cost per tuple in redistribute motion
		EcpRedistributeRecvCostUnit,  // receiving cost per tuple in redistribute motion
		EcpBroadcastSendCostUnit,  // sending cost per tuple in broadcast motion
		EcpBroadcastRecvCostUnit,  // receiving cost per tuple in broadcast motion
		EcpNoOpCostUnit,		   // cost per tuple in No-Op motion
		// general join params
		EcpJoinFeedingTupColumnCostUnit,  // feeding cost per tuple per column in join operator
		EcpJoinFeedingTupWidthCostUnit,	 // feeding cost per tuple per width in join operator
		EcpJoinOutputTupCostUnit,  // output cost per tuple in join operator
		// hash join params
		EcpHJSpillingMemThreshold,	// memory threshold for hash join spilling
		EcpHJHashTableInitCostFactor,  // initial cost for building hash table for hash join
		EcpHJHashTableColumnCostUnit,  // building hash table cost for per tuple per column
		EcpHJHashTableWidthCostUnit,  // building hash table cost for per tuple with unit width
		EcpHJHashingTupWidthCostUnit,  // hashing cost per tuple with unit width in hash join
		EcpHJFeedingTupColumnSpillingCostUnit,	// feeding cost per tuple per column in hash join if spilling
		EcpHJFeedingTupWidthSpillingCostUnit,  // feeding cost per tuple with unit width in hash join if spilling
		EcpHJHashingTupWidthSpillingCostUnit,  // hashing cost per tuple with unit width in hash join if spilling
		// hash agg params
		EcpHashAggInputTupColumnCostUnit,  // cost for building hash table for per tuple per grouping column in hash aggregate
		EcpHashAggInputTupWidthCostUnit,  // cost for building hash table for per tuple with unit width in hash aggregate
		EcpHashAggOutputTupWidthCostUnit,  // cost for outputting for per tuple with unit width in hash aggregate
		// sort params
		EcpSortTupWidthCostUnit,  // sorting cost per tuple with unit width
		// tuple processing params
		EcpTupDefaultProcCostUnit,	// cost for processing per tuple with unit width
		// materialization params
		EcpMaterializeCostUnit,	 // cost for materializing per tuple with unit width

		EcpTupUpdateBandwith,  // tuple update bandwidth
		EcpNetBandwidth,	   // network bandwidth
		EcpSegments,		   // number of segments
		EcpNLJFactor,		   // nested loop factor
		EcpHJFactor,		   // hash join factor - to represent spilling cost
		EcpHashFactor,		   // hash building factor
		EcpDefaultCost,		   // default cost
		EcpIndexJoinAllowedRiskThreshold,  // largest estimation risk for which we do not penalize index join

		EcpBitmapIOCostLargeNDV,	// bitmap IO co-efficient for large NDV
		EcpBitmapIOCostSmallNDV,	// bitmap IO co-efficient for smaller NDV
		EcpBitmapPageCostLargeNDV,	// bitmap page cost for large NDV
		EcpBitmapPageCostSmallNDV,	// bitmap page cost for smaller NDV
		EcpBitmapPageCost,			// bitmap page cost when not considering NDV
		EcpBitmapNDVThreshold,		// bitmap NDV threshold
		EcpBitmapScanRebindCost,	// cost of rebind operation in a bitmap scan
		EcpPenalizeHJSkewUpperLimit,  // upper limit for penalizing a skewed hashjoin operator

		EcpSentinel
	};

private:
	// memory pool
	CMemoryPool *m_mp;

	// array of parameters
	// cost param enum is used as index in this array
	SCostParam *m_rgpcp[EcpSentinel];

	// default value of sequential i/o bandwidth
	static const CDouble DSeqIOBandwidthVal;

	// default value of random i/o bandwidth
	static const CDouble DRandomIOBandwidthVal;

	// default value of tuple processing bandwidth
	static const CDouble DTupProcBandwidthVal;

	// default value of output bandwidth
	static const CDouble DOutputBandwidthVal;

	// default value of scan initialization cost
	static const CDouble DInitScanFacorVal;

	// default value of table scan cost unit
	static const CDouble DTableScanCostUnitVal;

	// default value of index scan initialization cost
	static const CDouble DInitIndexScanFactorVal;

	// default value of index block cost unit
	static const CDouble DIndexBlockCostUnitVal;

	// default value of index filtering cost unit
	static const CDouble DIndexFilterCostUnitVal;

	// default value of index scan cost unit per tuple per unit width
	static const CDouble DIndexScanTupCostUnitVal;

	// default value of index scan random IO cost unit per tuple
	static const CDouble DIndexScanTupRandomFactorVal;

	// default value of filter column cost unit
	static const CDouble DFilterColCostUnitVal;

	// default value of output tuple cost unit
	static const CDouble DOutputTupCostUnitVal;

	// default value of sending tuple cost unit for gather motion
	static const CDouble DGatherSendCostUnitVal;

	// default value of receiving tuple cost unit for gather motion
	static const CDouble DGatherRecvCostUnitVal;

	// default value of sending tuple cost unit for redistribute motion
	static const CDouble DRedistributeSendCostUnitVal;

	// default value of receiving tuple cost unit for redistribute motion
	static const CDouble DRedistributeRecvCostUnitVal;

	// default value of sending tuple cost unit for broadcast motion
	static const CDouble DBroadcastSendCostUnitVal;

	// default value of receiving tuple cost unit for broadcast motion
	static const CDouble DBroadcastRecvCostUnitVal;

	// default value of tuple cost unit for No-Op motion
	static const CDouble DNoOpCostUnitVal;

	// default value of feeding cost per tuple per column in join operator
	static const CDouble DJoinFeedingTupColumnCostUnitVal;

	// default value of feeding cost per tuple per width in join operator
	static const CDouble DJoinFeedingTupWidthCostUnitVal;

	// default value of output cost per tuple in join operator
	static const CDouble DJoinOutputTupCostUnitVal;

	// default value of memory threshold for hash join spilling
	static const CDouble DHJSpillingMemThresholdVal;

	// default value of initial cost for building hash table for hash join
	static const CDouble DHJHashTableInitCostFactorVal;

	// default value of building hash table cost for per tuple per column
	static const CDouble DHJHashTableColumnCostUnitVal;

	// default value of building hash table cost for per tuple with unit width
	static const CDouble DHJHashTableWidthCostUnitVal;

	// default value of hashing cost per tuple with unit width in hash join
	static const CDouble DHJHashingTupWidthCostUnitVal;

	// default value of feeding cost per tuple per column in hash join if spilling
	static const CDouble DHJFeedingTupColumnSpillingCostUnitVal;

	// default value of feeding cost per tuple with unit width in hash join if spilling
	static const CDouble DHJFeedingTupWidthSpillingCostUnitVal;

	// default value of hashing cost per tuple with unit width in hash join if spilling
	static const CDouble DHJHashingTupWidthSpillingCostUnitVal;

	// default value of cost for building hash table for per tuple per grouping column in hash aggregate
	static const CDouble DHashAggInputTupColumnCostUnitVal;

	// default value of cost for building hash table for per tuple with unit width in hash aggregate
	static const CDouble DHashAggInputTupWidthCostUnitVal;

	// default value of cost for outputting for per tuple with unit width in hash aggregate
	static const CDouble DHashAggOutputTupWidthCostUnitVal;

	// default value of sorting cost per tuple with unit width
	static const CDouble DSortTupWidthCostUnitVal;

	// default value of cost for processing per tuple with unit width
	static const CDouble DTupDefaultProcCostUnitVal;

	// default value of cost for materializing per tuple with unit width
	static const CDouble DMaterializeCostUnitVal;

	// default value of tuple update bandwidth
	static const CDouble DTupUpdateBandwidthVal;

	// default value of network bandwidth
	static const CDouble DNetBandwidthVal;

	// default value of number of segments
	static const CDouble DSegmentsVal;

	// default value of nested loop factor
	static const CDouble DNLJFactorVal;

	// default value of hash join factor
	static const CDouble DHJFactorVal;

	// default value of hash building factor
	static const CDouble DHashFactorVal;

	// default cost value when one is not computed
	static const CDouble DDefaultCostVal;

	// largest estimation risk for which we do not penalize index join
	static const CDouble DIndexJoinAllowedRiskThreshold;

	// default bitmap IO cost when NDV is smaller
	static const CDouble DBitmapIOCostLargeNDV;

	// default bitmap IO cost when NDV is smaller
	static const CDouble DBitmapIOCostSmallNDV;

	// default bitmap page cost when NDV is larger
	static const CDouble DBitmapPageCostLargeNDV;

	// default bitmap page cost when NDV is smaller
	static const CDouble DBitmapPageCostSmallNDV;

	// default bitmap page cost with no assumption about NDV
	static const CDouble DBitmapPageCost;

	// default threshold of NDV for bitmap costing
	static const CDouble DBitmapNDVThreshold;

	// cost of bitmap scan rebind
	static const CDouble DBitmapScanRebindCost;

	// upper limit for penalizing a skewed hash operator
	static const CDouble DPenalizeHJSkewUpperLimit;

	// private copy ctor
	CCostModelParamsGPDB(CCostModelParamsGPDB &);

public:
	// ctor
	explicit CCostModelParamsGPDB(CMemoryPool *mp);

	// dtor
	virtual ~CCostModelParamsGPDB();

	// lookup param by id
	virtual SCostParam *PcpLookup(ULONG id) const;

	// lookup param by name
	virtual SCostParam *PcpLookup(const CHAR *szName) const;

	// set param by id
	virtual void SetParam(ULONG id, CDouble dVal, CDouble dLowerBound,
						  CDouble dUpperBound);

	// set param by name
	virtual void SetParam(const CHAR *szName, CDouble dVal, CDouble dLowerBound,
						  CDouble dUpperBound);

	// print function
	virtual IOstream &OsPrint(IOstream &os) const;

	virtual BOOL Equals(ICostModelParams *pcm) const;

	virtual const CHAR *SzNameLookup(ULONG id) const;

};	// class CCostModelParamsGPDB

}  // namespace gpopt

#endif	// !GPDBCOST_CCostModelParamsGPDB_H

// EOF
