//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		COptimizer.h
//
//	@doc:
//		Optimizer class, entry point for query optimization
//---------------------------------------------------------------------------
#ifndef GPOPT_COptimizer_H
#define GPOPT_COptimizer_H

#include "gpos/base.h"

#include "gpopt/eval/CConstExprEvaluatorDefault.h"
#include "gpopt/search/CSearchStage.h"
#include "naucrates/dxl/operators/CDXLNode.h"

namespace gpdxl
{
class CDXLNode;
}

namespace gpmd
{
class IMDProvider;
}


using namespace gpos;
using namespace gpdxl;

namespace gpopt
{
// forward declarations
class ICostModel;
class COptimizerConfig;
class CQueryContext;
class CEnumeratorConfig;

//---------------------------------------------------------------------------
//	@class:
//		COptimizer
//
//	@doc:
//		Optimizer class, entry point for query optimization
//
//---------------------------------------------------------------------------
class COptimizer
{
private:
	// handle exception after finalizing minidump
	static void HandleExceptionAfterFinalizingMinidump(CException &ex);

	// optimize query in the given query context
	static CExpression *PexprOptimize(CMemoryPool *mp, CQueryContext *pqc,
									  CSearchStageArray *search_stage_array);

	// translate an optimizer expression into a DXL tree
	static CDXLNode *CreateDXLNode(CMemoryPool *mp, CMDAccessor *md_accessor,
								   CExpression *pexpr,
								   CColRefArray *colref_array,
								   CMDNameArray *pdrgpmdname, ULONG ulHosts);

	// helper function to print query expression
	static void PrintQuery(CMemoryPool *mp, CExpression *pexprTranslated,
						   CQueryContext *pqc);

	// helper function to print query plan
	static void PrintPlan(CMemoryPool *mp, CExpression *pexprPlan);

	// helper function to dump plan samples
	static void DumpSamples(CMemoryPool *mp, CEnumeratorConfig *pec,
							ULONG ulSessionId, ULONG ulCmdId);

	// print query or plan tree
	static void PrintQueryOrPlan(CMemoryPool *mp, CExpression *pexpr,
								 CQueryContext *pqc = NULL);

	// Check for a plan with CTE, if both CTEProducer and CTEConsumer are executed on the same locality.
	static void CheckCTEConsistency(CMemoryPool *mp, CExpression *pexpr);

public:
	// main optimizer function
	static CDXLNode *PdxlnOptimize(
		CMemoryPool *mp,
		CMDAccessor *md_accessor,  // MD accessor
		const CDXLNode *query,
		const CDXLNodeArray
			*query_output_dxlnode_array,  // required output columns
		const CDXLNodeArray *cte_producers,
		IConstExprEvaluator *pceeval,  // constant expression evaluator
		ULONG ulHosts,		// number of hosts (data nodes) in the system
		ULONG ulSessionId,	// session id used for logging and minidumps
		ULONG ulCmdId,		// command id used for logging and minidumps
		CSearchStageArray *search_stage_array,	// search strategy
		COptimizerConfig *optimizer_config,		// optimizer configurations
		const CHAR *szMinidumpFileName =
			NULL  // name of minidump file to be created
	);
};	// class COptimizer
}  // namespace gpopt

#endif	// !GPOPT_COptimizer_H

// EOF
