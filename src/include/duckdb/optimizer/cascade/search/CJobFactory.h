//---------------------------------------------------------------------------
//	@filename:
//		CJobFactory.h
//
//	@doc:
//		Highly concurrent job factory;
//		Uses bulk memory allocation and atomic primitives to
//		create and recycle jobs with minimal sychronization;
//---------------------------------------------------------------------------
#ifndef GPOPT_CJobFactory_H
#define GPOPT_CJobFactory_H

#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/common/CSyncPool.h"
#include "duckdb/optimizer/cascade/search/CJob.h"
#include "duckdb/optimizer/cascade/search/CJobGroupExploration.h"
#include "duckdb/optimizer/cascade/search/CJobGroupExpressionExploration.h"
#include "duckdb/optimizer/cascade/search/CJobGroupExpressionImplementation.h"
#include "duckdb/optimizer/cascade/search/CJobGroupExpressionOptimization.h"
#include "duckdb/optimizer/cascade/search/CJobGroupImplementation.h"
#include "duckdb/optimizer/cascade/search/CJobGroupOptimization.h"
#include "duckdb/optimizer/cascade/search/CJobTransformation.h"

using namespace gpos;

namespace gpopt
{
//---------------------------------------------------------------------------
//	@class:
//		CJobFactory
//
//	@doc:
//		Highly concurrent job factory
//
//		The factory uses bulk memory allocation to create and recycle jobs with
//		minimal synchronization. The factory maintains a lock-free list defined
//		by the class CSyncPool for each job type. This allows concurrent
//		retrieval of jobs from the lists without the need for synchronization
//		through heavy locking operations.
//		A lock-free list is pre-allocated as an array of given size. The
//		allocation of lock-free lists happens lazily when the first job of a
//		given type is created.
//		Each job is given a unique id. When a job needs to be retrieved from
//		the list, atomic operations are used to reserve a free job object and
//		return it to the caller.
//
//---------------------------------------------------------------------------
class CJobFactory
{
public:
	// number of jobs in each pool
	const ULONG m_ulJobs;

	// container for group optimization jobs
	CSyncPool<CJobGroupOptimization>* m_pspjGroupOptimization;

	// container for group implementation jobs
	CSyncPool<CJobGroupImplementation>* m_pspjGroupImplementation;

	// container for group exploration jobs
	CSyncPool<CJobGroupExploration>* m_pspjGroupExploration;

	// container for group expression optimization jobs
	CSyncPool<CJobGroupExpressionOptimization>* m_pspjGroupExpressionOptimization;

	// container for group expression implementation jobs
	CSyncPool<CJobGroupExpressionImplementation>* m_pspjGroupExpressionImplementation;

	// container for group expression exploration jobs
	CSyncPool<CJobGroupExpressionExploration>* m_pspjGroupExpressionExploration;

	// container for transformation jobs
	CSyncPool<CJobTransformation>* m_pspjTransformation;

	// retrieve job of specific type
	template <class T> T* PtRetrieve(CSyncPool<T>* &pspt)
	{
		if (nullptr == pspt)
		{
			pspt = new CSyncPool<T>(m_ulJobs);
			T* tmp = new T();
			SIZE_T id_offset = (SIZE_T)(&(tmp->m_id)) - (SIZE_T)tmp;
			pspt->Init((gpos::ULONG)id_offset);
		}
		return pspt->PtRetrieve();
	}

	// release job
	template <class T> void Release(T* pt, CSyncPool<T>* pspt)
	{
		pspt->Recycle(pt);
	}

	// truncate job pool
	template <class T> void TruncatePool(CSyncPool<T>* &pspt)
	{
		delete pspt;
		pspt = nullptr;
	}

public:
	// ctor
	CJobFactory(ULONG ulJobs);

	// no copy ctor
	CJobFactory(const CJobFactory &) = delete;

	// dtor
	~CJobFactory();

public:
	// create job of specific type
	CJob* PjCreate(CJob::EJobType ejt);

	// release completed job
	void Release(CJob* pj);

	// truncate the container for the specific job type
	void Truncate(CJob::EJobType ejt);

};	// class CJobFactory
}  // namespace gpopt
#endif