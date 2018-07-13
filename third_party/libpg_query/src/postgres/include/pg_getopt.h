/*
 * Portions Copyright (c) 1987, 1993, 1994
 * The Regents of the University of California.  All rights reserved.
 *
 * Portions Copyright (c) 2003-2015, PostgreSQL Global Development Group
 *
 * src/include/pg_getopt.h
 */
#ifndef PG_GETOPT_H
#define PG_GETOPT_H

/* POSIX says getopt() is provided by unistd.h */
#include <unistd.h>

/* rely on the system's getopt.h if present */
#ifdef HAVE_GETOPT_H
#include <getopt.h>
#endif

/*
 * If we have <getopt.h>, assume it declares these variables, else do that
 * ourselves.  (We used to just declare them unconditionally, but Cygwin
 * doesn't like that.)
 */
#ifndef HAVE_GETOPT_H

extern char *optarg;
extern int	optind;
extern int	opterr;
extern int	optopt;

#endif   /* HAVE_GETOPT_H */

/*
 * Some platforms have optreset but fail to declare it in <getopt.h>, so cope.
 * Cygwin, however, doesn't like this either.
 */
#if defined(HAVE_INT_OPTRESET) && !defined(__CYGWIN__)
extern int	optreset;
#endif

#ifndef HAVE_GETOPT
extern int	getopt(int nargc, char *const * nargv, const char *ostr);
#endif

#endif   /* PG_GETOPT_H */
