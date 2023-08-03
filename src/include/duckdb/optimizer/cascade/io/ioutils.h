//---------------------------------------------------------------------------
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

#include "duckdb/optimizer/cascade/io/iotypes.h"
#include "duckdb/optimizer/cascade/types.h"

// macro for I/O error simulation
#ifdef GPOS_FPSIMULATOR
// simulate I/O error with specified address of returned error value,
// and specified errno
#define GPOS_CHECK_SIM_IO_ERR_CODE(return_value, error_no, IOFunc)      \
	do                                                                  \
	{                                                                   \
		if (!ioutils::SimulateIOError(return_value, error_no, __FILE__, \
									  __LINE__))                        \
		{                                                               \
			*return_value = IOFunc;                                     \
		}                                                               \
	} while (0)
#else
// execute the I/O function
#define GPOS_CHECK_SIM_IO_ERR_CODE(return_value, error_no, IOFunc) \
	do                                                             \
	{                                                              \
		*return_value = IOFunc;                                    \
	} while (0)
#endif	// GPOS_FPSIMULATOR

// simulate I/O error with specified address of returned error value
// and errno will set to 1 automatically
#define GPOS_CHECK_SIM_IO_ERR(return_value, IOFunc) \
	GPOS_CHECK_SIM_IO_ERR_CODE(return_value, 1, IOFunc)


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

// move file
void Move(const CHAR *old_path, const CHAR *szNew);

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

#ifdef GPOS_FPSIMULATOR
// inject I/O error for functions whose returned value type is INT
BOOL SimulateIOError(INT *return_value, INT error_no, const CHAR *file,
					 ULONG line_num);

#if defined(GPOS_64BIT) || defined(GPOS_Darwin)
// inject I/O error for functions whose returned value type is INT_PTR
inline BOOL
SimulateIOError(INT_PTR *return_value, INT error_no, const CHAR *file,
				ULONG line_num)
{
	return SimulateIOError((INT *) return_value, error_no, file, line_num);
}
#endif

// inject I/O error for functions whose returned value type is CHAR*
BOOL SimulateIOError(CHAR **return_value, INT error_no, const CHAR *file,
					 ULONG line_num);
#endif	// GPOS_FPSIMULATOR

}  // namespace ioutils
}  // namespace gpos

#endif
