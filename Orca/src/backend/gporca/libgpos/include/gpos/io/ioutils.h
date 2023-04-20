//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2010 Greenplum, Inc.
//
//	@filename:
//		ioutils.h
//
//	@doc:
//		I/O utilities;
//		generic I/O functions that are not associated with file descriptors
//---------------------------------------------------------------------------

#ifndef GPOS_ioutils_H
#define GPOS_ioutils_H

#include <dlfcn.h>
#include <unistd.h>

#include "gpos/io/iotypes.h"
#include "gpos/types.h"

namespace gpos
{
namespace ioutils
{
// check state of file or directory
void CheckState(const CHAR *file_path, SFileStat *file_state);

// check state of file or directory by file descriptor
void CheckStateUsingFileDescriptor(const INT file_descriptor,
								   SFileStat *file_state);

// check if path is mapped to an accessible file or directory
BOOL PathExists(const CHAR *file_path);

// get file size by file path
ULLONG FileSize(const CHAR *file_path);

// get file size by file descriptor
ULLONG FileSize(const INT file_descriptor);

// check if path is directory
BOOL IsDir(const CHAR *file_path);

// check if path is file
BOOL IsFile(const CHAR *file_path);

// check permissions
BOOL CheckFilePermissions(const CHAR *file_path, ULONG permission_bits);

// create directory with specific permissions
void CreateDir(const CHAR *file_path, ULONG permission_bits);

// delete file
void RemoveDir(const CHAR *file_path);

// delete file
void Unlink(const CHAR *file_path);

// open a file
INT OpenFile(const CHAR *file_path, INT mode, INT permission_bits);

// close a file descriptor
INT CloseFile(INT file_descriptor);

// get file status
INT GetFileState(INT file_descriptor, SFileStat *file_state);

// write to a file descriptor
INT_PTR Write(INT file_descriptor, const void *buffer,
			  const ULONG_PTR ulpCount);

// read from a file descriptor
INT_PTR Read(INT file_descriptor, void *buffer, const ULONG_PTR ulpCount);

// create a unique temporary directory
void CreateTempDir(CHAR *dir_path);

}  // namespace ioutils
}  // namespace gpos

#endif	// !GPOS_ioutils_H

// EOF
