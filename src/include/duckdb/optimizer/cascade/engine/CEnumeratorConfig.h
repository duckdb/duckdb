//---------------------------------------------------------------------------
//	@filename:
//		CEnumeratorConfig.h
//
//	@doc:
//		Configurations of plan enumerator
//---------------------------------------------------------------------------
#ifndef GPOPT_CEnumeratorConfig_H
#define GPOPT_CEnumeratorConfig_H

#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/planner/expression.hpp"

#define GPOPT_UNBOUNDED_COST_THRESHOLD 0.0

namespace gpos
{
class CWStringDynamic;
}

namespace gpopt
{
using namespace gpos;
using namespace duckdb;

// type definition of plan checker
typedef bool(FnPlanChecker)(shared_ptr<Expression>);

//---------------------------------------------------------------------------
//	@class:
//		CEnumeratorConfig
//
//	@doc:
//		Configurations of plan enumerator
//
//---------------------------------------------------------------------------
class CEnumeratorConfig
{
public:
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
	public:
		// plan id
		ULLONG m_plan_id;

		// plan cost
		double m_cost;

	public:
		// ctor
		SSamplePlan(ULLONG plan_id, double cost)
			: m_plan_id(plan_id), m_cost(cost)
		{
		}

		// dtor
		virtual ~SSamplePlan(){};
	};	// struct SSamplePlan

	// identifier of chosen plan
	ULLONG m_plan_id;

	// size of plan space
	ULLONG m_ullSpaceSize;

	// number of required samples
	ULLONG m_ullInputSamples;

	// cost of best plan found
	double m_costBest;

	// max cost of a created plan sample
	double m_costMax;

	// max cost of accepted samples as a ratio to best plan cost
	double m_dCostThreshold;

	// sampled plans
	duckdb::vector<shared_ptr<SSamplePlan>> m_pdrgpsp;

	// step value used in fitting cost distribution
	double m_dStep;

	// x-values of fitted cost distribution
	duckdb::vector<double> m_pdX;

	// y-values of fitted cost distribution
	duckdb::vector<double> m_pdY;

	// size of fitted cost distribution
	ULONG m_ulDistrSize;

	// restrict plan sampling to plans satisfying required properties
	bool m_fSampleValidPlans;

	// plan checker function
	shared_ptr<FnPlanChecker> m_pfpc;

	// initialize size of cost distribution
	void InitCostDistrSize();

	// inaccessible copy ctor
	CEnumeratorConfig(const CEnumeratorConfig &);

	// compute Gaussian probability value
	static double DGaussian(double d, double dMean, double dStd);

public:
	// ctor
	CEnumeratorConfig(ULLONG plan_id, ULLONG ullSamples, double cost_threshold = GPOPT_UNBOUNDED_COST_THRESHOLD);

	// dtor
	virtual ~CEnumeratorConfig();

	// return plan id
	ULLONG GetPlanId() const
	{
		return m_plan_id;
	}

	// return enumerated space size
	ULLONG GetPlanSpaceSize() const
	{
		return m_ullSpaceSize;
	}

	// set plan space size
	void SetPlanSpaceSize(ULLONG ullSpaceSize)
	{
		m_ullSpaceSize = ullSpaceSize;
	}

	// return number of required samples
	ULLONG UllInputSamples() const
	{
		return m_ullInputSamples;
	}

	// return number of created samples
	ULONG UlCreatedSamples() const
	{
		return m_pdrgpsp.size();
	}

	// set plan id
	void SetPlanId(ULLONG plan_id)
	{
		m_plan_id = plan_id;
	}

	// return cost threshold
	double DCostThreshold() const
	{
		return m_dCostThreshold;
	}

	// return id of a plan sample
	ULLONG UllPlanSample(ULONG ulPos) const
	{
		return m_pdrgpsp[ulPos]->m_plan_id;
	}

	// set cost of best plan found
	void SetBestCost(double cost)
	{
		m_costBest = cost;
	}

	// return cost of best plan found
	double CostBest() const
	{
		return m_costBest;
	}

	// return cost of a plan sample
	double CostPlanSample(ULONG ulPos) const
	{
		return m_pdrgpsp[ulPos]->m_cost;
	}

	// add a new plan to sample
	bool FAddSample(ULLONG plan_id, double cost);

	// clear samples
	void ClearSamples();

	// return x-value of cost distribution
	double DCostDistrX(ULONG ulPos) const;

	// return y-value of cost distribution
	double DCostDistrY(ULONG ulPos) const;

	// fit cost distribution on generated samples
	void FitCostDistribution();

	// return size of fitted cost distribution
	ULONG UlCostDistrSize() const
	{
		return m_ulDistrSize;
	}

	// is enumeration enabled?
	bool FEnumerate() const
	{
		return true;
	}

	// is sampling enabled?
	bool FSample() const
	{
		return false;
	}

	// return plan checker function
	shared_ptr<FnPlanChecker> Pfpc() const
	{
		return m_pfpc;
	}

	// set plan checker function
	void SetPlanChecker(shared_ptr<FnPlanChecker> pfpc)
	{
		m_pfpc = pfpc;
	}

	// restrict sampling to plans satisfying required properties
	// we need to change settings for testing
	void SetSampleValidPlans(bool fSampleValidPlans)
	{
		m_fSampleValidPlans = fSampleValidPlans;
	}

	// return true if sampling can only generate valid plans
	bool FSampleValidPlans() const
	{
		return m_fSampleValidPlans;
	}

	// check given plan using PlanChecker function
	bool FCheckPlan(shared_ptr<Expression> pexpr) const
	{
		if (nullptr != m_pfpc)
		{
			return (*m_pfpc)(pexpr);
		}
		return true;
	}

	// compute Gaussian kernel density
	static void GussianKernelDensity(duckdb::vector<double> pdObervationX, duckdb::vector<double> pdObervationY, ULONG ulObservations, duckdb::vector<double> pdX, duckdb::vector<double> pdY, ULONG size);

	// generate default enumerator configurations
	static shared_ptr<CEnumeratorConfig> PecDefault()
	{
		return make_shared<CEnumeratorConfig>(0, 0);
	}

	// generate enumerator configuration for a given plan id
	static shared_ptr<CEnumeratorConfig> GetEnumeratorCfg(ULLONG plan_id)
	{
		return make_shared<CEnumeratorConfig>(plan_id, 0);
	}
};	// class CEnumeratorConfig
}  // namespace gpopt
#endif