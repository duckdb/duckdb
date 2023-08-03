//---------------------------------------------------------------------------
//	@filename:
//		CFileDescriptor.cpp
//
//	@doc:
//		File descriptor abstraction
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/io/CFileDescriptor.h"
#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/io/ioutils.h"
#include "duckdb/optimizer/cascade/string/CStringStatic.h"
#include <assert.h>

using namespace gpos;

//---------------------------------------------------------------------------
//	@function:
//		CFileDescriptor::CFileDescriptor
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CFileDescriptor::CFileDescriptor() : m_file_descriptor(GPOS_FILE_DESCR_INVALID)
{
}

//---------------------------------------------------------------------------
//	@function:
//		CFileDescriptor::OpenFile
//
//	@doc:
//		Open file descriptor
//
//---------------------------------------------------------------------------
void CFileDescriptor::OpenFile(const CHAR *file_path, ULONG mode, ULONG permission_bits)
{
	BOOL fOpened = false;
	while (!fOpened)
	{
		m_file_descriptor = GPOS_FILE_DESCR_INVALID;
		// create file with given mode and permissions and check to simulate I/O error
		GPOS_CHECK_SIM_IO_ERR(&m_file_descriptor, ioutils::OpenFile(file_path, mode, permission_bits));
		// check for error
		if (GPOS_FILE_DESCR_INVALID == m_file_descriptor)
		{
			// in case an interrupt was received we retry
			if (EINTR == errno)
			{
				continue;
			}
			assert(false);
		}
		fOpened = true;
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CFile::~CFile
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CFileDescriptor::~CFileDescriptor()
{
	if (IsFileOpen())
	{
		CloseFile();
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CFile::CloseFile
//
//	@doc:
//		Close file
//
//---------------------------------------------------------------------------
void CFileDescriptor::CloseFile()
{
	BOOL fClosed = false;
	while (!fClosed)
	{
		INT res = ioutils::CloseFile(m_file_descriptor);
		// check for error
		if (0 != res)
		{
			// in case an interrupt was received we retry
			if (EINTR == errno)
			{
				continue;
			}
		}
		fClosed = true;
	}
	m_file_descriptor = GPOS_FILE_DESCR_INVALID;
}