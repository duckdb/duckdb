//---------------------------------------------------------------------------
//	@filename:
//		CErrorHandlerStandard.cpp
//
//	@doc:
//		Implements standard error handler
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/error/CErrorHandlerStandard.h"
#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/error/CErrorContext.h"
#include "duckdb/optimizer/cascade/error/CLogger.h"
#include "duckdb/optimizer/cascade/io/ioutils.h"
#include "duckdb/optimizer/cascade/string/CWStringStatic.h"
#include "duckdb/optimizer/cascade/task/CAutoSuspendAbort.h"
#include "duckdb/optimizer/cascade/task/CTask.h"

using namespace gpos;

//---------------------------------------------------------------------------
//	@function:
//		CErrorHandlerStandard::Process
//
//	@doc:
//		Process pending error context;
//
//---------------------------------------------------------------------------
void CErrorHandlerStandard::Process(CException exception)
{
	CTask *task = CTask::Self();
	GPOS_ASSERT(NULL != task && "No task in current context");
	IErrorContext *err_ctxt = task->GetErrCtxt();
	CLogger *log = dynamic_cast<CLogger *>(task->GetErrorLogger());
	GPOS_ASSERT(err_ctxt->IsPending() && "No error to process");
	GPOS_ASSERT(err_ctxt->GetException() == exception && "Exception processed different from pending");
	// print error stack trace
	if (CException::ExmaSystem == exception.Major() && !err_ctxt->IsRethrown())
	{
		if ((CException::ExmiIOError == exception.Minor() || CException::ExmiNetError == exception.Minor()) && 0 < errno)
		{
			err_ctxt->AppendErrnoMsg();
		}
		if (ILogger::EeilMsgHeaderStack <= log->InfoLevel())
		{
			err_ctxt->AppendStackTrace();
		}
	}
	// scope for suspending cancellation
	{
		// suspend cancellation
		CAutoSuspendAbort asa;
		// log error message
		log->Log(err_ctxt->GetErrorMsg(), err_ctxt->GetSeverity(), __FILE__, __LINE__);
	}
}