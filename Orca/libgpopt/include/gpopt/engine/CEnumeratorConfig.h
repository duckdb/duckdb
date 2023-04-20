//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 EMC Corp.
//
//	@filename:
//		CEnumeratorConfig.h
//
//	@doc:
//		Configurations of plan enumerator
//---------------------------------------------------------------------------
#ifndef GPOPT_CEnumeratorConfig_H
#define GPOPT_CEnumeratorConfig_H

#include "gpos/base.h"
#include "gpos/common/CDynamicPtrArray.h"
#include "gpos/common/CRefCount.h"

#include "gpopt/cost/CCost.h"
#include "naucrates/traceflags/traceflags.h"

#define GPOPT_UNBOUNDED_COST_THRESHOLD 0.0

namespace gpos
{
class CWStringDynamic;
}

namespace gpopt
{
using namespace gpos;

// fwd declarations
class CExpression;

// type definition of plan checker
typedef BOOL(FnPlanChecker)(CExpression *);

//---------------------------------------------------------------------------
//	@class:
//		CEnumeratorConfig
//
//	@doc:
//		Configurations of plan enumerator
//
//---------------------------------------------------------------------------
class CEnumeratorConfig : public CRefCount
{
private:
	//---------------------------------------------------------------------------
	//	@class:
	//		SSamplePlan
	//
	//	@doc:
	//		Internal structure to represent samples of plan space
	//
	//---------------------------------------------------------------------------
	struct SSamplePlan
	{
	private:
		// plan id
		ULLONG m_plan_id;

		// plan cost
		CCost m_cost;

	public:
		// ctor
		SSamplePlan(ULLONG plan_id, CCost cost)
			: m_plan_id(plan_id), m_cost(cost)
		{
		}

		// dtor
		virtual ~SSamplePlan(){};

		// return plan id
		ULLONG
		GetPlanId() const
		{
			return m_plan_id;
		}

		// return plan cost
		CCost
		Cost() const
		{
			return m_cost;
		}

	};	// struct SSamplePlan

	// array og unsigned long long int
	typedef CDynamicPtrArray<SSamplePlan, CleanupDelete> SSamplePlanArray;

	// memory pool
	CMemoryPool *m_mp;

	// identifier of chosen plan
	ULLONG m_plan_id;

	// size of plan space
	ULLONG m_ullSpaceSize;

	// number of required samples
	ULLONG m_ullInputSamples;

	// cost of best plan found
	CCost m_costBest;

	// max cost of a created plan sample
	CCost m_costMax;

	// max cost of accepted samples as a ratio to best plan cost
	CDouble m_dCostThreshold;

	// sampled plans
	SSamplePlanArray *m_pdrgpsp;

	// step value used in fitting cost distribution
	CDouble m_dStep;

	// x-values of fitted cost distribution
	DOUBLE *m_pdX;

	// y-values of fitted cost distribution
	DOUBLE *m_pdY;

	// size of fitted cost distribution
	ULONG m_ulDistrSize;

	// restrict plan sampling to plans satisfying required properties
	BOOL m_fSampleValidPlans;

	// plan checker function
	FnPlanChecker *m_pfpc;

	// initialize size of cost distribution
	void InitCostDistrSize();

	// inaccessible copy ctor
	CEnumeratorConfig(const CEnumeratorConfig &);

	// compute Gaussian probability value
	static DOUBLE DGaussian(DOUBLE d, DOUBLE dMean, DOUBLE dStd);

public:
	// ctor
	CEnumeratorConfig(CMemoryPool *mp, ULLONG plan_id, ULLONG ullSamples,
					  CDouble cost_threshold = GPOPT_UNBOUNDED_COST_THRESHOLD);

	// dtor
	virtual ~CEnumeratorConfig();

	// return plan id
	ULLONG
	GetPlanId() const
	{
		return m_plan_id;
	}

	// return enumerated space size
	ULLONG
	GetPlanSpaceSize() const
	{
		return m_ullSpaceSize;
	}

	// set plan space size
	void
	SetPlanSpaceSize(ULLONG ullSpaceSize)
	{
		m_ullSpaceSize = ullSpaceSize;
	}

	// return number of required samples
	ULLONG
	UllInputSamples() const
	{
		return m_ullInputSamples;
	}

	// return number of created samples
	ULONG
	UlCreatedSamples() const
	{
		return m_pdrgpsp->Size();
	}

	// set plan id
	void
	SetPlanId(ULLONG plan_id)
	{
		m_plan_id = plan_id;
	}

	// return cost threshold
	CDouble
	DCostThreshold() const
	{
		return m_dCostThreshold;
	}

	// return id of a plan sample
	ULLONG
	UllPlanSample(ULONG ulPos) const
	{
		return (*m_pdrgpsp)[ulPos]->GetPlanId();
	}

	// set cost of best plan found
	void
	SetBestCost(CCost cost)
	{
		m_costBest = cost;
	}

	// return cost of best plan found
	CCost
	CostBest() const
	{
		return m_costBest;
	}

	// return cost of a plan sample
	CCost
	CostPlanSample(ULONG ulPos) const
	{
		return (*m_pdrgpsp)[ulPos]->Cost();
	}

	// add a new plan to sample
	BOOL FAddSample(ULLONG plan_id, CCost cost);

	// clear samples
	void ClearSamples();

	// return x-value of cost distribution
	CDouble DCostDistrX(ULONG ulPos) const;

	// return y-value of cost distribution
	CDouble DCostDistrY(ULONG ulPos) const;

	// fit cost distribution on generated samples
	void FitCostDistribution();

	// return size of fitted cost distribution
	ULONG
	UlCostDistrSize() const
	{
		return m_ulDistrSize;
	}

	// is enumeration enabled?
	BOOL
	FEnumerate() const
	{
		return GPOS_FTRACE(EopttraceEnumeratePlans);
	}

	// is sampling enabled?
	BOOL
	FSample() const
	{
		return GPOS_FTRACE(EopttraceSamplePlans);
	}

	// return plan checker function
	FnPlanChecker *
	Pfpc() const
	{
		return m_pfpc;
	}

	// set plan checker function
	void
	SetPlanChecker(FnPlanChecker *pfpc)
	{
		GPOS_ASSERT(NULL != pfpc);

		m_pfpc = pfpc;
	}

	// restrict sampling to plans satisfying required properties
	// we need to change settings for testing
	void
	SetSampleValidPlans(BOOL fSampleValidPlans)
	{
		m_fSampleValidPlans = fSampleValidPlans;
	}

	// return true if sampling can only generate valid plans
	BOOL
	FSampleValidPlans() const
	{
		return m_fSampleValidPlans;
	}

	// check given plan using PlanChecker function
	BOOL
	FCheckPlan(CExpression *pexpr) const
	{
		GPOS_ASSERT(NULL != pexpr);

		if (NULL != m_pfpc)
		{
			return m_pfpc(pexpr);
		}

		return true;
	}

	// dump samples to an output file
	void DumpSamples(CWStringDynamic *str, ULONG ulSessionId,
					 ULONG ulCommandId);

	// dump fitted cost distribution to an output file
	void DumpCostDistr(CWStringDynamic *str, ULONG ulSessionId,
					   ULONG ulCommandId);

	// print ids of plans in the generated sample
	void PrintPlanSample() const;

	// compute Gaussian kernel density
	static void GussianKernelDensity(DOUBLE *pdObervationX,
									 DOUBLE *pdObervationY,
									 ULONG ulObservations, DOUBLE *pdX,
									 DOUBLE *pdY, ULONG size);

	// generate default enumerator configurations
	static CEnumeratorConfig *
	PecDefault(CMemoryPool *mp)
	{
		return GPOS_NEW(mp)
			CEnumeratorConfig(mp, 0 /*plan_id*/, 0 /*ullSamples*/);
	}

	// generate enumerator configuration for a given plan id
	static CEnumeratorConfig *
	GetEnumeratorCfg(CMemoryPool *mp, ULLONG plan_id)
	{
		return GPOS_NEW(mp) CEnumeratorConfig(mp, plan_id, 0 /*ullSamples*/);
	}


};	// class CEnumeratorConfig

}  // namespace gpopt

#endif	// !GPOPT_CEnumeratorConfig_H


// EOF
