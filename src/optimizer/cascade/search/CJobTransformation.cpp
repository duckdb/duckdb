//---------------------------------------------------------------------------
//	@filename:
//		CJobTransformation.cpp
//
//	@doc:
//		Implementation of group expression transformation job
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/search/CJobTransformation.h"
#include "duckdb/optimizer/cascade/engine/CEngine.h"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/optimizer/cascade/search/CGroup.h"
#include "duckdb/optimizer/cascade/search/CGroupExpression.h"
#include "duckdb/optimizer/cascade/search/CJobFactory.h"
#include "duckdb/optimizer/cascade/search/CScheduler.h"
#include "duckdb/optimizer/cascade/search/CSchedulerContext.h"

using namespace gpopt;

// State transition diagram for transformation job state machine;
//
// +-----------------+
// | estInitialized: |
// | EevtTransform() |
// +-----------------+
//   |
//   | eevCompleted
//   v
// +-----------------+
// |  estCompleted   |
// +-----------------+
//
const CJobTransformation::EEvent rgeev5[CJobTransformation::estSentinel][CJobTransformation::estSentinel] =
{
	{ CJobTransformation::eevSentinel, CJobTransformation::eevCompleted},
	{ CJobTransformation::eevSentinel, CJobTransformation::eevSentinel},
};

//---------------------------------------------------------------------------
//	@function:
//		CJobTransformation::CJobTransformation
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CJobTransformation::CJobTransformation()
{
}

//---------------------------------------------------------------------------
//	@function:
//		CJobTransformation::~CJobTransformation
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CJobTransformation::~CJobTransformation()
{
}

//---------------------------------------------------------------------------
//	@function:
//		CJobTransformation::Init
//
//	@doc:
//		Initialize job
//
//---------------------------------------------------------------------------
void CJobTransformation::Init(CGroupExpression* pgexpr, CXform* pxform)
{
	m_pgexpr = pgexpr;
	m_xform = pxform;
	m_jsm.Init(rgeev5);
	// set job actions
	m_jsm.SetAction(estInitialized, EevtTransform);
	// mark as initialized
	CJob::SetInit();
}

//---------------------------------------------------------------------------
//	@function:
//		CJobTransformation::EevtTransform
//
//	@doc:
//		Apply transformation action
//
//---------------------------------------------------------------------------
CJobTransformation::EEvent CJobTransformation::EevtTransform(CSchedulerContext* psc, CJob* pj)
{
	// get a job pointer
	CJobTransformation* pjt = PjConvert(pj);
	CGroupExpression* pgexpr = pjt->m_pgexpr;
	CXform* pxform = pjt->m_xform;
	// insert transformation results to memo
	CXformResult* pxfres = new CXformResult();
	ULONG ulElapsedTime = 0;
	ULONG ulNumberOfBindings = 0;
	pgexpr->Transform(pxform, pxfres, &ulElapsedTime, &ulNumberOfBindings);
	psc->m_peng->InsertXformResult(pgexpr->m_pgroup, pxfres, pxform->ID(), pgexpr, ulElapsedTime, ulNumberOfBindings);
	return eevCompleted;
}

//---------------------------------------------------------------------------
//	@function:
//		CJobTransformation::FExecute
//
//	@doc:
//		Main job function
//
//---------------------------------------------------------------------------
bool CJobTransformation::FExecute(CSchedulerContext* psc)
{
	return m_jsm.FRun(psc, this);
}

//---------------------------------------------------------------------------
//	@function:
//		CJobTransformation::ScheduleJob
//
//	@doc:
//		Schedule a new transformation job
//
//---------------------------------------------------------------------------
void CJobTransformation::ScheduleJob(CSchedulerContext* psc, CGroupExpression* pgexpr, CXform* pxform, CJob* pjParent)
{
	CJob* pj = psc->m_pjf->PjCreate(CJob::EjtTransformation);
	// initialize job
	CJobTransformation* pjt = PjConvert(pj);
	pjt->Init(pgexpr, pxform);
	psc->m_psched->Add(pjt, pjParent);
}