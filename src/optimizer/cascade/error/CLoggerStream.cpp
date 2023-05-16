//---------------------------------------------------------------------------
//	@filename:
//		CLoggerStream.cpp
//
//	@doc:
//		Implementation of stream logging
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/error/CLoggerStream.h"
#include "duckdb/optimizer/cascade/utils.h"

using namespace gpos;

CLoggerStream CLoggerStream::m_stdout_stream_logger(oswcout);
CLoggerStream CLoggerStream::m_stderr_stream_logger(oswcerr);

//---------------------------------------------------------------------------
//	@function:
//		CLoggerStream::CLoggerStream
//
//	@doc:
//
//---------------------------------------------------------------------------
CLoggerStream::CLoggerStream(IOstream &os) : CLogger(), m_os(os)
{
}

//---------------------------------------------------------------------------
//	@function:
//		CLoggerStream::~CLoggerStream
//
//	@doc:
//
//---------------------------------------------------------------------------
CLoggerStream::~CLoggerStream()
{
}