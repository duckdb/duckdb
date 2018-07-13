/*--------------------------------------------------------------------
 * Symbols referenced in this file:
 * - pvsnprintf
 * - psprintf
 *--------------------------------------------------------------------
 */

/*-------------------------------------------------------------------------
 *
 * psprintf.c
 *		sprintf into an allocated-on-demand buffer
 *
 *
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/common/psprintf.c
 *
 *-------------------------------------------------------------------------
 */

#ifndef FRONTEND

#include "postgres.h"

#include "utils/memutils.h"

#else

#include "postgres_fe.h"

/* It's possible we could use a different value for this in frontend code */
#define MaxAllocSize	((Size) 0x3fffffff)		/* 1 gigabyte - 1 */

#endif


/*
 * psprintf
 *
 * Format text data under the control of fmt (an sprintf-style format string)
 * and return it in an allocated-on-demand buffer.  The buffer is allocated
 * with palloc in the backend, or malloc in frontend builds.  Caller is
 * responsible to free the buffer when no longer needed, if appropriate.
 *
 * Errors are not returned to the caller, but are reported via elog(ERROR)
 * in the backend, or printf-to-stderr-and-exit() in frontend builds.
 * One should therefore think twice about using this in libpq.
 */
char *
psprintf(const char *fmt,...)
{
	size_t		len = 128;		/* initial assumption about buffer size */

	for (;;)
	{
		char	   *result;
		va_list		args;
		size_t		newlen;

		/*
		 * Allocate result buffer.  Note that in frontend this maps to malloc
		 * with exit-on-error.
		 */
		result = (char *) palloc(len);

		/* Try to format the data. */
		va_start(args, fmt);
		newlen = pvsnprintf(result, len, fmt, args);
		va_end(args);

		if (newlen < len)
			return result;		/* success */

		/* Release buffer and loop around to try again with larger len. */
		pfree(result);
		len = newlen;
	}
}

/*
 * pvsnprintf
 *
 * Attempt to format text data under the control of fmt (an sprintf-style
 * format string) and insert it into buf (which has length len, len > 0).
 *
 * If successful, return the number of bytes emitted, not counting the
 * trailing zero byte.  This will always be strictly less than len.
 *
 * If there's not enough space in buf, return an estimate of the buffer size
 * needed to succeed (this *must* be more than the given len, else callers
 * might loop infinitely).
 *
 * Other error cases do not return, but exit via elog(ERROR) or exit().
 * Hence, this shouldn't be used inside libpq.
 *
 * This function exists mainly to centralize our workarounds for
 * non-C99-compliant vsnprintf implementations.  Generally, any call that
 * pays any attention to the return value should go through here rather
 * than calling snprintf or vsnprintf directly.
 *
 * Note that the semantics of the return value are not exactly C99's.
 * First, we don't promise that the estimated buffer size is exactly right;
 * callers must be prepared to loop multiple times to get the right size.
 * Second, we return the recommended buffer size, not one less than that;
 * this lets overflow concerns be handled here rather than in the callers.
 */
size_t
pvsnprintf(char *buf, size_t len, const char *fmt, va_list args)
{
	int			nprinted;

	Assert(len > 0);

	errno = 0;

	/*
	 * Assert check here is to catch buggy vsnprintf that overruns the
	 * specified buffer length.  Solaris 7 in 64-bit mode is an example of a
	 * platform with such a bug.
	 */
#ifdef USE_ASSERT_CHECKING
	buf[len - 1] = '\0';
#endif

	nprinted = vsnprintf(buf, len, fmt, args);

	Assert(buf[len - 1] == '\0');

	/*
	 * If vsnprintf reports an error other than ENOMEM, fail.  The possible
	 * causes of this are not user-facing errors, so elog should be enough.
	 */
	if (nprinted < 0 && errno != 0 && errno != ENOMEM)
	{
#ifndef FRONTEND
		elog(ERROR, "vsnprintf failed: %m");
#else
		fprintf(stderr, "vsnprintf failed: %s\n", strerror(errno));
		exit(EXIT_FAILURE);
#endif
	}

	/*
	 * Note: some versions of vsnprintf return the number of chars actually
	 * stored, not the total space needed as C99 specifies.  And at least one
	 * returns -1 on failure.  Be conservative about believing whether the
	 * print worked.
	 */
	if (nprinted >= 0 && (size_t) nprinted < len - 1)
	{
		/* Success.  Note nprinted does not include trailing null. */
		return (size_t) nprinted;
	}

	if (nprinted >= 0 && (size_t) nprinted > len)
	{
		/*
		 * This appears to be a C99-compliant vsnprintf, so believe its
		 * estimate of the required space.  (If it's wrong, the logic will
		 * still work, but we may loop multiple times.)  Note that the space
		 * needed should be only nprinted+1 bytes, but we'd better allocate
		 * one more than that so that the test above will succeed next time.
		 *
		 * In the corner case where the required space just barely overflows,
		 * fall through so that we'll error out below (possibly after
		 * looping).
		 */
		if ((size_t) nprinted <= MaxAllocSize - 2)
			return nprinted + 2;
	}

	/*
	 * Buffer overrun, and we don't know how much space is needed.  Estimate
	 * twice the previous buffer size, but not more than MaxAllocSize; if we
	 * are already at MaxAllocSize, choke.  Note we use this palloc-oriented
	 * overflow limit even when in frontend.
	 */
	if (len >= MaxAllocSize)
	{
#ifndef FRONTEND
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("out of memory")));
#else
		fprintf(stderr, _("out of memory\n"));
		exit(EXIT_FAILURE);
#endif
	}

	if (len >= MaxAllocSize / 2)
		return MaxAllocSize;

	return len * 2;
}
