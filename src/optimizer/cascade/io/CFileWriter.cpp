//---------------------------------------------------------------------------
//	@filename:
//		CFileWriter.cpp
//
//	@doc:
//		Implementation of file handler for raw output
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/io/CFileWriter.h"
#include <fcntl.h>
#include <assert.h>
#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/io/ioutils.h"

using namespace gpos;

//---------------------------------------------------------------------------
//	@function:
//		CFileWriter::CFileWriter
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CFileWriter::CFileWriter() : CFileDescriptor(), m_file_size(0)
{
}

//---------------------------------------------------------------------------
//	@function:
//		CFileWriter::Open
//
//	@doc:
//		Open file for writing
//
//---------------------------------------------------------------------------
void CFileWriter::Open(const CHAR *file_path, ULONG permission_bits)
{
	OpenFile(file_path, O_CREAT | O_WRONLY | O_RDONLY | O_TRUNC, permission_bits);
	m_file_size = 0;
}

//---------------------------------------------------------------------------
//	@function:
//		CFileWriter::Close
//
//	@doc:
//		Close file after writing
//
//---------------------------------------------------------------------------
void CFileWriter::Close()
{
	CloseFile();
	m_file_size = 0;
}

//---------------------------------------------------------------------------
//	@function:
//		CFileWriter::Write
//
//	@doc:
//		Write bytes to file
//
//---------------------------------------------------------------------------
void CFileWriter::Write(const BYTE *read_buffer, const ULONG_PTR write_size)
{
	ULONG_PTR bytes_left_to_write = write_size;
	while (0 < bytes_left_to_write)
	{
		INT_PTR current_byte = -1;
		// write to file and check to simulate I/O error
		GPOS_CHECK_SIM_IO_ERR(&current_byte, ioutils::Write(GetFileDescriptor(), read_buffer, bytes_left_to_write));
		// check for error
		if (-1 == current_byte)
		{
			// in case an interrupt was received we retry
			if (EINTR == errno)
			{
				continue;
			}
			assert(false);
		}
		// increase file size
		m_file_size += current_byte;
		read_buffer += current_byte;
		bytes_left_to_write -= current_byte;
	}
}