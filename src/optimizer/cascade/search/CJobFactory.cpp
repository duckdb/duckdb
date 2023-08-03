//---------------------------------------------------------------------------
//	@filename:
//		CJobFactory.cpp
//
//	@doc:
//		Implementation of optimizer job base class
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/search/CJobFactory.h"
#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/task/CWorker.h"
#include "duckdb/optimizer/cascade/search/CGroupExpression.h"

using namespace gpopt;
using namespace gpos;

//---------------------------------------------------------------------------
//	@function:
//		CJobFactory::CJobFactory
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CJobFactory::CJobFactory(ULONG ulJobs)
	: m_ulJobs(ulJobs), m_pspjGroupOptimization(nullptr), m_pspjGroupImplementation(nullptr), m_pspjGroupExploration(nullptr), m_pspjGroupExpressionOptimization(nullptr), m_pspjGroupExpressionImplementation(nullptr), m_pspjGroupExpressionExploration(nullptr), m_pspjTransformation(nullptr)
{
	// initialize factories to be used first
	Release(PjCreate(CJob::EjtGroupExploration));
	Release(PjCreate(CJob::EjtGroupExpressionExploration));
	Release(PjCreate(CJob::EjtTransformation));
}

//---------------------------------------------------------------------------
//	@function:
//		CJobFactory::~CJobFactory
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CJobFactory::~CJobFactory()
{
}

//---------------------------------------------------------------------------
//	@function:
//		CJobFactory::PjCreate
//
//	@doc:
//		Create job of specific type
//
//---------------------------------------------------------------------------
CJob* CJobFactory::PjCreate(CJob::EJobType ejt)
{
	CJob* pj;
	switch (ejt)
	{
		case CJob::EjtTest:
			break;
		case CJob::EjtGroupOptimization:
			pj = PtRetrieve<CJobGroupOptimization>(m_pspjGroupOptimization);
			break;
		case CJob::EjtGroupImplementation:
			pj = PtRetrieve<CJobGroupImplementation>(m_pspjGroupImplementation);
			break;
		case CJob::EjtGroupExploration:
			pj = PtRetrieve<CJobGroupExploration>(m_pspjGroupExploration);
			break;
		case CJob::EjtGroupExpressionOptimization:
			pj = PtRetrieve<CJobGroupExpressionOptimization>(m_pspjGroupExpressionOptimization);
			break;
		case CJob::EjtGroupExpressionImplementation:
			pj = PtRetrieve<CJobGroupExpressionImplementation>(m_pspjGroupExpressionImplementation);
			break;
		case CJob::EjtGroupExpressionExploration:
			pj = PtRetrieve<CJobGroupExpressionExploration>(m_pspjGroupExpressionExploration);
			break;
		case CJob::EjtTransformation:
			pj = PtRetrieve<CJobTransformation>(m_pspjTransformation);
			break;
		case CJob::EjtInvalid:
			GPOS_ASSERT(!"Invalid job type");
	}
	// prepare task
	pj->Reset();
	pj->SetJobType(ejt);
	return pj;
}

//---------------------------------------------------------------------------
//	@function:
//		CJobFactory::Release
//
//	@doc:
//		Release completed job
//
//---------------------------------------------------------------------------
void CJobFactory::Release(CJob* pj)
{
	switch (pj->Ejt())
	{
		case CJob::EjtTest:
			break;
		case CJob::EjtGroupOptimization:
			Release(CJobGroupOptimization::PjConvert(pj), m_pspjGroupOptimization);
			break;
		case CJob::EjtGroupImplementation:
			Release(CJobGroupImplementation::PjConvert(pj), m_pspjGroupImplementation);
			break;
		case CJob::EjtGroupExploration:
			Release(CJobGroupExploration::PjConvert(pj), m_pspjGroupExploration);
			break;
		case CJob::EjtGroupExpressionOptimization:
			Release(CJobGroupExpressionOptimization::PjConvert(pj), m_pspjGroupExpressionOptimization);
			break;
		case CJob::EjtGroupExpressionImplementation:
			Release(CJobGroupExpressionImplementation::PjConvert(pj), m_pspjGroupExpressionImplementation);
			break;
		case CJob::EjtGroupExpressionExploration:
			Release(CJobGroupExpressionExploration::PjConvert(pj), m_pspjGroupExpressionExploration);
			break;
		case CJob::EjtTransformation:
			Release(CJobTransformation::PjConvert(pj), m_pspjTransformation);
			break;
		default:
			GPOS_ASSERT(!"Invalid job type");
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CJobFactory::Truncate
//
//	@doc:
//		Truncate the container for the specific job type
//
//---------------------------------------------------------------------------
void CJobFactory::Truncate(CJob::EJobType ejt)
{
	// need to suspend cancellation while truncating job pool
	{
		switch (ejt)
		{
			case CJob::EjtTest:
				break;
			case CJob::EjtGroupOptimization:
				TruncatePool(m_pspjGroupOptimization);
				break;
			case CJob::EjtGroupImplementation:
				TruncatePool(m_pspjGroupImplementation);
				break;
			case CJob::EjtGroupExploration:
				TruncatePool(m_pspjGroupExploration);
				break;
			case CJob::EjtGroupExpressionOptimization:
				TruncatePool(m_pspjGroupExpressionOptimization);
				break;
			case CJob::EjtGroupExpressionImplementation:
				TruncatePool(m_pspjGroupExpressionImplementation);
				break;
			case CJob::EjtGroupExpressionExploration:
				TruncatePool(m_pspjGroupExpressionExploration);
				break;
			case CJob::EjtTransformation:
				TruncatePool(m_pspjTransformation);
				break;
			case CJob::EjtInvalid:
				GPOS_ASSERT(!"Invalid job type");
		}
	}
}