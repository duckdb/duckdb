//---------------------------------------------------------------------------
//	@filename:
//		CLoggerSyslog.cpp
//
//	@doc:
//		Implementation of Syslog logging
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/error/CLoggerSyslog.h"
#include <syslog.h>
#include "duckdb/optimizer/cascade/common/syslibwrapper.h"
#include "duckdb/optimizer/cascade/string/CStringStatic.h"

using namespace gpos;

// initialization of static members
CLoggerSyslog CLoggerSyslog::m_alert_logger(NULL /*szName*/,
#ifndef GPOS_SunOS
											LOG_PERROR |
#endif	// GPOS_SunOS
												LOG_CONS,
											LOG_ALERT);


//---------------------------------------------------------------------------
//	@function:
//		CLoggerSyslog::CLoggerSyslog
//
//	@doc:
//		Ctor - set executable name, initialization flags and message priority
//
//---------------------------------------------------------------------------
CLoggerSyslog::CLoggerSyslog(const CHAR *proc_name, ULONG init_mask, ULONG message_priority)
	: m_proc_name(proc_name), m_init_mask(init_mask), m_message_priority(message_priority)
{
}


//---------------------------------------------------------------------------
//	@function:
//		CLoggerSyslog::~CLoggerSyslog
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CLoggerSyslog::~CLoggerSyslog()
{
}

//---------------------------------------------------------------------------
//	@function:
//		CLoggerSyslog::Write
//
//	@doc:
//		Write string to syslog
//
//---------------------------------------------------------------------------
void CLoggerSyslog::Write(const WCHAR *log_entry, ULONG	// severity
)
{
	CHAR *buffer = CLogger::Msg();

	// create message
	CStringStatic str(buffer, GPOS_LOG_MESSAGE_BUFFER_SIZE);
	str.AppendConvert(log_entry);

	// send message to syslog
	syslib::OpenLog(m_proc_name, m_init_mask, LOG_USER);
	syslib::SysLog(m_message_priority, buffer);
	syslib::CloseLog();
}


//---------------------------------------------------------------------------
//	@function:
//		CLoggerSyslog::Write
//
//	@doc:
//		Write alert message to syslog - use ASCII characters only
//
//---------------------------------------------------------------------------
void CLoggerSyslog::Alert(const WCHAR *msg)
{
	m_alert_logger.Write(msg, CException::ExsevError);
}

// EOF
