/*-------------------------------------------------------------------------
 *
 * parallel.h
 *	  Infrastructure for launching parallel workers
 *
 * Portions Copyright (c) 1996-2014, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/access/parallel.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef PARALLEL_H
#define PARALLEL_H

#include "access/xlogdefs.h"
#include "lib/ilist.h"
#include "postmaster/bgworker.h"
#include "storage/shm_mq.h"
#include "storage/shm_toc.h"
#include "utils/elog.h"

typedef void (*parallel_worker_main_type) (dsm_segment *seg, shm_toc *toc);

typedef struct ParallelWorkerInfo
{
	BackgroundWorkerHandle *bgwhandle;
	shm_mq_handle *error_mqh;
	int32		pid;
} ParallelWorkerInfo;

typedef struct ParallelContext
{
	dlist_node	node;
	SubTransactionId subid;
	int			nworkers;
	parallel_worker_main_type entrypoint;
	char	   *library_name;
	char	   *function_name;
	ErrorContextCallback *error_context_stack;
	shm_toc_estimator estimator;
	dsm_segment *seg;
	void	   *private_memory;
	shm_toc    *toc;
	ParallelWorkerInfo *worker;
} ParallelContext;

extern bool ParallelMessagePending;
extern int	ParallelWorkerNumber;
extern bool InitializingParallelWorker;

#define		IsParallelWorker()		(ParallelWorkerNumber >= 0)

extern ParallelContext *CreateParallelContext(parallel_worker_main_type entrypoint, int nworkers);
extern ParallelContext *CreateParallelContextForExternalFunction(char *library_name, char *function_name, int nworkers);
extern void InitializeParallelDSM(ParallelContext *);
extern void LaunchParallelWorkers(ParallelContext *);
extern void WaitForParallelWorkersToFinish(ParallelContext *);
extern void DestroyParallelContext(ParallelContext *);
extern bool ParallelContextActive(void);

extern void HandleParallelMessageInterrupt(void);
extern void HandleParallelMessages(void);
extern void AtEOXact_Parallel(bool isCommit);
extern void AtEOSubXact_Parallel(bool isCommit, SubTransactionId mySubId);
extern void ParallelWorkerReportLastRecEnd(XLogRecPtr);

#endif   /* PARALLEL_H */
