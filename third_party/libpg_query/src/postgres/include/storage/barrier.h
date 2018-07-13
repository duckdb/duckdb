/*-------------------------------------------------------------------------
 *
 * barrier.h
 *	  Memory barrier operations.
 *
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/storage/barrier.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef BARRIER_H
#define BARRIER_H

/*
 * This used to be a separate file, full of compiler/architecture
 * dependent defines, but it's not included in the atomics.h
 * infrastructure and just kept for backward compatibility.
 */
#include "port/atomics.h"

#endif   /* BARRIER_H */
