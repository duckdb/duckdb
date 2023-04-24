//---------------------------------------------------------------------------
//	@filename:
//	       	clibwrapper.h
//
//	@doc:
//	       	Wrapper for functions in C library
//
//---------------------------------------------------------------------------

#ifndef GPOS_clibwrapper_H
#define GPOS_clibwrapper_H

#define VA_START(vaList, last) va_start(vaList, last);
#define VA_END(vaList) va_end(vaList)
#define VA_ARG(vaList, type) va_arg(vaList, type)

#include <unistd.h>

#include "duckdb/optimizer/cascade/common/clibtypes.h"
#include "duckdb/optimizer/cascade/types.h"

namespace gpos
{
namespace clib
{
typedef INT (*Comparator)(const void *, const void *);

#ifdef GPOS_sparc

#include <ucontext.h>

typedef INT (*Callback)(ULONG_PTR, INT, void *);

// get current user context
INT GetContext(ucontext_t *user_ctxt);

// call the user-supplied function callback for each routine found on
// the call stack and each signal handler invoked
INT WalkContext(const ucontext_t *user_ctxt, Callback callback, void *arg);

#endif

// get an environment variable
CHAR *GetEnv(const CHAR *name);

// compare a specified number of bytes of two regions of memory
INT Memcmp(const void *left, const void *right, SIZE_T num_bytes);

// sleep given number of microseconds
void USleep(ULONG usecs);

// compare two strings
INT Strcmp(const CHAR *left, const CHAR *right);

// compare two strings up to a specified number of characters
INT Strncmp(const CHAR *left, const CHAR *right, SIZE_T num_bytes);

// compare two strings up to a specified number of wide characters
INT Wcsncmp(const WCHAR *left, const WCHAR *right, SIZE_T num_bytes);

// copy two strings up to a specified number of wide characters
WCHAR *WcStrNCpy(WCHAR *dest, const WCHAR *src, SIZE_T num_bytes);

// copy a specified number of bytes between two memory areas
void *Memcpy(void *dest, const void *src, SIZE_T num_bytes);

// copy a specified number of wide characters
WCHAR *Wmemcpy(WCHAR *dest, const WCHAR *src, SIZE_T num_bytes);

// copy a specified number of characters
CHAR *Strncpy(CHAR *dest, const CHAR *src, SIZE_T num_bytes);

// find the first occurrence of the character c in src
CHAR *Strchr(const CHAR *src, INT c);

// set a specified number of bytes to a specified m_bytearray_value
void *Memset(void *dest, INT value, SIZE_T num_bytes);

// calculate the length of a wide-character string
ULONG Wcslen(const WCHAR *dest);

// calculate the length of a string
ULONG Strlen(const CHAR *buf);

// sort a specified number of elements
void Qsort(void *dest, SIZE_T num_bytes, SIZE_T size, Comparator fnComparator);

// parse command-line options
INT Getopt(INT argc, CHAR *const argv[], const CHAR *opt_string);

// convert string to long integer
LINT Strtol(const CHAR *val, CHAR **end, ULONG base);

// convert string to long long integer
LINT Strtoll(const CHAR *val, CHAR **end, ULONG base);

// convert string to double
DOUBLE Strtod(const CHAR *str);

// return a pseudo-random integer between 0 and RAND_MAX
ULONG Rand(ULONG *seed);

// format wide character output conversion
INT Vswprintf(WCHAR *wcstr, SIZE_T max_len, const WCHAR *format,
			  VA_LIST vaArgs);

// format string
INT Vsnprintf(CHAR *src, SIZE_T size, const CHAR *format, VA_LIST vaArgs);

// return string describing error number
void Strerror_r(INT errnum, CHAR *buf, SIZE_T buf_len);

// convert the calendar time time to broken-time representation
TIME *Localtime_r(const TIME_T *time, TIME *result);

// allocate dynamic memory
void *Malloc(SIZE_T size);

// free dynamic memory
void Free(void *src);

// convert a wide character to a multibyte sequence
INT Wctomb(CHAR *dest, WCHAR src);

// convert a wide-character string to a multi-byte string
LINT Wcstombs(CHAR *dest, WCHAR *src, ULONG_PTR dest_size);

// convert a multibyte sequence to wide character array
ULONG Mbstowcs(WCHAR *dest, const CHAR *src, SIZE_T len);

// return a pointer to the start of the NULL-terminated symbol
CHAR *Demangle(const CHAR *symbol, CHAR *buf, SIZE_T *len, INT *status);

// resolve symbol information from its address
void Dladdr(void *addr, DL_INFO *info);

}  //namespace clib
}  // namespace gpos

#endif
