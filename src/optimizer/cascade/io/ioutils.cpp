//---------------------------------------------------------------------------
//	@filename:
//		ioutils.cpp
//
//	@doc:
//		Implementation of I/O utilities
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/io/ioutils.h"
#include <errno.h>
#include <fcntl.h>
#include <sched.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <assert.h>
#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/common/clibwrapper.h"
#include "duckdb/optimizer/cascade/string/CStringStatic.h"

using namespace gpos;


//---------------------------------------------------------------------------
//	@function:
//		ioutils::CheckState
//
//	@doc:
//		Check state of file or directory
//
//---------------------------------------------------------------------------
void gpos::ioutils::CheckState(const CHAR *file_path, SFileStat *file_state)
{
	// reset file state
	(void) clib::Memset(file_state, 0, sizeof(*file_state));
	INT res = -1;
	if (0 != res)
	{
		assert(false);
	}
}

//---------------------------------------------------------------------------
//	@function:
//		ioutils::FStat
//
//	@doc:
//		Check state of file or directory by file descriptor
//
//---------------------------------------------------------------------------
void gpos::ioutils::CheckStateUsingFileDescriptor(const INT file_descriptor, SFileStat *file_state)
{
	// reset file state
	(void) clib::Memset(file_state, 0, sizeof(*file_state));
	INT res = -1;
	// check to simulate I/O error
	GPOS_CHECK_SIM_IO_ERR(&res, fstat(file_descriptor, file_state));
	if (0 != res)
	{
		assert(false);
	}
}

//---------------------------------------------------------------------------
//	@function:
//		ioutils::PathExists
//
//	@doc:
//		Check if path is mapped to an accessible file or directory
//
//---------------------------------------------------------------------------
BOOL gpos::ioutils::PathExists(const CHAR *file_path)
{
	SFileStat fs;
	INT res = stat(file_path, &fs);
	return (0 == res);
}

//---------------------------------------------------------------------------
//	@function:
//		ioutils::IsDir
//
//	@doc:
//		Check if path is directory
//
//---------------------------------------------------------------------------
BOOL
gpos::ioutils::IsDir(const CHAR *file_path)
{
	GPOS_ASSERT(NULL != file_path);

	SFileStat fs;
	CheckState(file_path, &fs);

	return S_ISDIR(fs.st_mode);
}


//---------------------------------------------------------------------------
//	@function:
//		ioutils::IsFile
//
//	@doc:
//		Check if path is file
//
//---------------------------------------------------------------------------
BOOL
gpos::ioutils::IsFile(const CHAR *file_path)
{
	GPOS_ASSERT(NULL != file_path);

	SFileStat fs;
	CheckState(file_path, &fs);

	return S_ISREG(fs.st_mode);
}


//---------------------------------------------------------------------------
//	@function:
//		ioutils::FileSize
//
//	@doc:
//		Get file size by file path
//
//---------------------------------------------------------------------------
ULLONG
gpos::ioutils::FileSize(const CHAR *file_path)
{
	GPOS_ASSERT(NULL != file_path);
	GPOS_ASSERT(IsFile(file_path));

	SFileStat fs;
	CheckState(file_path, &fs);

	return fs.st_size;
}


//---------------------------------------------------------------------------
//	@function:
//		ioutils::FileSize
//
//	@doc:
//		Get file size by file descriptor
//
//---------------------------------------------------------------------------
ULLONG
gpos::ioutils::FileSize(const INT file_descriptor)
{
	SFileStat fs;
	CheckStateUsingFileDescriptor(file_descriptor, &fs);

	return fs.st_size;
}


//---------------------------------------------------------------------------
//	@function:
//		ioutils::CheckFilePermissions
//
//	@doc:
//		Check permissions
//
//---------------------------------------------------------------------------
BOOL
gpos::ioutils::CheckFilePermissions(const CHAR *file_path,
									ULONG permission_bits)
{
	GPOS_ASSERT(NULL != file_path);

	SFileStat fs;
	CheckState(file_path, &fs);

	return (permission_bits == (fs.st_mode & permission_bits));
}


//---------------------------------------------------------------------------
//	@function:
//		ioutils::CreateDir
//
//	@doc:
//		Create directory with specific permissions
//
//---------------------------------------------------------------------------
void
gpos::ioutils::CreateDir(const CHAR *file_path, ULONG permission_bits)
{
	GPOS_ASSERT(NULL != file_path);

	INT res = -1;

	// check to simulate I/O error
	GPOS_CHECK_SIM_IO_ERR(&res, mkdir(file_path, (MODE_T) permission_bits));

	if (0 != res)
	{
		assert(false);
	}
}


//---------------------------------------------------------------------------
//	@function:
//		ioutils::RemoveDir
//
//	@doc:
//		Delete directory
//
//---------------------------------------------------------------------------
void
gpos::ioutils::RemoveDir(const CHAR *file_path)
{
	GPOS_ASSERT(NULL != file_path);
	GPOS_ASSERT(IsDir(file_path));

	INT res = -1;

	// delete existing directory and check to simulate I/O error
	GPOS_CHECK_SIM_IO_ERR(&res, rmdir(file_path));

	if (0 != res)
	{
		assert(false);
	}
}


//---------------------------------------------------------------------------
//	@function:
//		ioutils::Move
//
//	@doc:
//		Move file from old path to new path;
//		any file currently mapped to new path is deleted
//
//---------------------------------------------------------------------------
void gpos::ioutils::Move(const CHAR *old_path, const CHAR *szNew)
{
	// delete any existing file with the new path
	if (PathExists(szNew))
	{
		Unlink(szNew);
	}
	INT res = -1;
	// rename file and check to simulate I/O error
	GPOS_CHECK_SIM_IO_ERR(&res, rename(old_path, szNew));
	if (0 != res)
	{
		assert(false);
	}
}

//---------------------------------------------------------------------------
//	@function:
//		ioutils::Unlink
//
//	@doc:
//		Delete file
//
//---------------------------------------------------------------------------
void gpos::ioutils::Unlink(const CHAR *file_path)
{
	// delete existing file
	(void) unlink(file_path);
}

//---------------------------------------------------------------------------
//	@function:
//		ioutils::OpenFile
//
//	@doc:
//		Open a file;
//		It shall establish the connection between a file
//		and a file descriptor
//
//---------------------------------------------------------------------------
INT gpos::ioutils::OpenFile(const CHAR *file_path, INT mode, INT permission_bits)
{
	INT res = open(file_path, mode, permission_bits);
	return res;
}


//---------------------------------------------------------------------------
//	@function:
//		ioutils::CloseFile
//
//	@doc:
//		Close a file descriptor
//
//---------------------------------------------------------------------------
INT
gpos::ioutils::CloseFile(INT file_descriptor)
{
	INT res = close(file_descriptor);

	GPOS_ASSERT(0 == res || EBADF != errno);

	return res;
}


//---------------------------------------------------------------------------
//	@function:
//		ioutils::GetFileState
//
//	@doc:
//		Get file status
//
//---------------------------------------------------------------------------
INT
gpos::ioutils::GetFileState(INT file_descriptor, SFileStat *file_state)
{
	GPOS_ASSERT(NULL != file_state);

	INT res = fstat(file_descriptor, file_state);

	GPOS_ASSERT(0 == res || EBADF != errno);

	return res;
}


//---------------------------------------------------------------------------
//	@function:
//		ioutils::Write
//
//	@doc:
//		Write to a file descriptor
//
//---------------------------------------------------------------------------
INT_PTR
gpos::ioutils::Write(INT file_descriptor, const void *buffer,
					 const ULONG_PTR ulpCount)
{
	GPOS_ASSERT(NULL != buffer);
	GPOS_ASSERT(0 < ulpCount);
	GPOS_ASSERT(ULONG_PTR_MAX / 2 > ulpCount);

	SSIZE_T res = write(file_descriptor, buffer, ulpCount);

	GPOS_ASSERT((0 <= res) || EBADF != errno);

	return res;
}


//---------------------------------------------------------------------------
//	@function:
//		ioutils::Read
//
//	@doc:
//		Read from a file descriptor
//
//---------------------------------------------------------------------------
INT_PTR
gpos::ioutils::Read(INT file_descriptor, void *buffer, const ULONG_PTR ulpCount)
{
	GPOS_ASSERT(NULL != buffer);
	GPOS_ASSERT(0 < ulpCount);
	GPOS_ASSERT(ULONG_PTR_MAX / 2 > ulpCount);

	SSIZE_T res = read(file_descriptor, buffer, ulpCount);

	GPOS_ASSERT((0 <= res) || EBADF != errno);

	return res;
}

//---------------------------------------------------------------------------
//	@function:
//		ioutils::CreateTempDir
//
//	@doc:
//		Create a unique temporary directory
//
//---------------------------------------------------------------------------
void gpos::ioutils::CreateTempDir(CHAR *dir_path)
{
	CHAR *szRes = NULL;
#ifdef GPOS_SunOS
	// check to simulate I/O error
	GPOS_CHECK_SIM_IO_ERR(&szRes, mktemp(dir_path));
	ioutils::CreateDir(dir_path, S_IRUSR | S_IWUSR | S_IXUSR);
#else
	// check to simulate I/O error
	GPOS_CHECK_SIM_IO_ERR(&szRes, mkdtemp(dir_path));
#endif	// GPOS_SunOS
	if (NULL == szRes)
	{
		assert(false);
	}
	return;
}