//---------------------------------------------------------------------------
//	@filename:
//		CEnumeratorConfig.cpp
//
//	@doc:
//		Implementation of plan enumerator config
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/engine/CEnumeratorConfig.h"
#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/io/COstreamFile.h"
#include "duckdb/optimizer/cascade/task/CTask.h"
#include "duckdb/optimizer/cascade/task/CWorker.h"
#include "duckdb/optimizer/cascade/base/CCostContext.h"
#include <math.h>

using namespace gpos;
using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CEnumeratorConfig::CEnumeratorConfig
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CEnumeratorConfig::CEnumeratorConfig(ULLONG plan_id, ULLONG ullSamples, double cost_threshold)
	: m_plan_id(plan_id), m_ullSpaceSize(0), m_ullInputSamples(ullSamples), m_costBest(GPOPT_INVALID_COST), m_costMax(GPOPT_INVALID_COST), m_dCostThreshold(cost_threshold), m_dStep(0.5), m_ulDistrSize(0), m_fSampleValidPlans(true)
{
}

//---------------------------------------------------------------------------
//	@function:
//		CEnumeratorConfig::~CEnumeratorConfig
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CEnumeratorConfig::~CEnumeratorConfig()
{
	m_pdrgpsp.clear();
}

//---------------------------------------------------------------------------
//	@function:
//		CEnumeratorConfig::DCostDistrX
//
//	@doc:
//		Return x-value of a fitted cost distribution
//
//---------------------------------------------------------------------------
double CEnumeratorConfig::DCostDistrX(ULONG ulPos) const
{
	return m_pdX[ulPos];
}

//---------------------------------------------------------------------------
//	@function:
//		CEnumeratorConfig::DCostDistrY
//
//	@doc:
//		Return y-value of a fitted cost distribution
//
//---------------------------------------------------------------------------
double CEnumeratorConfig::DCostDistrY(ULONG ulPos) const
{
	return m_pdY[ulPos];
}

//---------------------------------------------------------------------------
//	@function:
//		CEnumeratorConfig::ClearSamples
//
//	@doc:
//		Clear samples
//
//---------------------------------------------------------------------------
void CEnumeratorConfig::ClearSamples()
{
	m_pdX.clear();
	m_pdY.clear();
	m_ulDistrSize = 0;
	m_pdrgpsp.clear();
}

//---------------------------------------------------------------------------
//	@function:
//		CEnumeratorConfig::FAddSample
//
//	@doc:
//		Add a new plan to sample
//
//---------------------------------------------------------------------------
bool CEnumeratorConfig::FAddSample(ULLONG plan_id, double cost)
{
	bool fAccept = (GPOPT_UNBOUNDED_COST_THRESHOLD == m_dCostThreshold) || (cost <= m_costBest * m_dCostThreshold);
	if (fAccept)
	{
		m_pdrgpsp.emplace_back(make_shared<SSamplePlan>(plan_id, cost));
		if (GPOPT_INVALID_COST == m_costMax || cost > m_costMax)
		{
			m_costMax = cost;
		}
	}
	return fAccept;
}

//---------------------------------------------------------------------------
//	@function:
//		CEnumeratorConfig::DGaussian
//
//	@doc:
//		Compute Gaussian probability value
//
//---------------------------------------------------------------------------
double CEnumeratorConfig::DGaussian(double d, double dMean, double dStd)
{
	const double dE = 2.71828182846;		// e: natural logarithm base
	const double dSqrt2pi = 2.50662827463;	// sqrt(2*pi)
	double diff = pow((d - dMean) / dStd, 2.0);
	// compute Gaussian probability:
	// G(x) = \frac{1}{\sigma * \sqrt{2 \pi}} e^{-0.5 * (\frac{x-\mu}{\sigma})^2}
	return (1.0 / (dStd * dSqrt2pi)) * pow(dE, -0.5 * diff);
}

//---------------------------------------------------------------------------
//	@function:
//		CEnumeratorConfig::InitCostDistrSize
//
//	@doc:
//		Initialize size of cost distribution
//
//---------------------------------------------------------------------------
void CEnumeratorConfig::InitCostDistrSize()
{
	//  bound estimated distribution using relative cost of most expensive plan
	double dMax = log2(m_costMax / CostBest());
	// fix number of points in estimated distribution to 100
	m_dStep = double(dMax / 100.0);
	// compute target distribution size
	m_ulDistrSize = (ULONG)(floor(dMax / m_dStep) + 1.0);
}

//---------------------------------------------------------------------------
//	@function:
//		CEnumeratorConfig::GussianKernelDensity
//
//	@doc:
//		Compute Gaussian Kernel density
//
//---------------------------------------------------------------------------
void CEnumeratorConfig::GussianKernelDensity(duckdb::vector<double> pdObervationX, duckdb::vector<double> pdObervationY, ULONG ulObservations, duckdb::vector<double> pdX, duckdb::vector<double> pdY, ULONG size)
{
	// finding observations span to determine kernel bandwidth
	double dMin = pdObervationX[0];
	double dMax = pdObervationX[0];
	for (ULONG ul = 1; ul < ulObservations; ul++)
	{
		if (pdObervationX[ul] > dMax)
		{
			dMax = pdObervationX[ul];
		}

		if (pdObervationX[ul] < dMin)
		{
			dMin = pdObervationX[ul];
		}
	}
	// kernel bandwidth set to 1% of distribution span
	double dBandWidth = 0.01 * (dMax - dMin);
	for (ULONG ul = 0; ul < size; ul++)
	{
		double dx = pdX[ul];
		double dy = 0;
		for (ULONG ulObs = 0; ulObs < ulObservations; ulObs++)
		{
			double dObsX = pdObervationX[ulObs];
			double dObsY = pdObervationY[ulObs];
			double dDiff = (dx - dObsX) / dBandWidth;
			dy = dy + dObsY * DGaussian(dDiff, 0.0, 1.0);
		}
		dy = dy / (dBandWidth * ulObservations);
		pdY[ul] = dy;
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CEnumeratorConfig::FitCostDistribution
//
//	@doc:
//		Fit cost distribution on generated samples
//
//---------------------------------------------------------------------------
void CEnumeratorConfig::FitCostDistribution()
{
	m_pdX.clear();
	m_pdY.clear();
	InitCostDistrSize();
	ULONG ulCreatedSamples = UlCreatedSamples();
	for(ULONG i = 0; i < m_ulDistrSize; i++)
	{
		m_pdX.emplace_back(0.0);
		m_pdY.emplace_back(0.0);
	}
	duckdb::vector<double> pdObervationX;
	duckdb::vector<double> pdObervationY;
	for (ULONG ul = 0; ul < ulCreatedSamples; ul++)
	{
		pdObervationX.emplace_back(log2(CostPlanSample(ul) / CostBest()));
		pdObervationY.emplace_back(1.0);
	}
	double d = 0.0;
	for (ULONG ul = 0; ul < m_ulDistrSize; ul++)
	{
		m_pdX[ul] = d;
		m_pdY[ul] = 0.0;
		d = d + m_dStep;
	}
	GussianKernelDensity(pdObervationX, pdObervationY, ulCreatedSamples, m_pdX, m_pdY, m_ulDistrSize);
}