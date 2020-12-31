/*
 * Copyright owned by the Transaction Processing Performance Council.
 *
 * A copy of the license is included under extension/tpch/dbgen/LICENSE
 * in this repository.
 *
 * You may not use this file except in compliance with the License.
 *
 * THE TPC SOFTWARE IS AVAILABLE WITHOUT CHARGE FROM TPC.
 */
/*
 * rnd.h -- header file for use withthe portable random number generator
 * provided by Frank Stephens of Unisys
 */

#pragma once

/* function protypes */
DSS_HUGE NextRand PROTO((DSS_HUGE));
DSS_HUGE UnifInt PROTO((DSS_HUGE, DSS_HUGE, long));

/*
 * macros to control RNG and assure reproducible multi-stream
 * runs without the need for seed files. Keep track of invocations of RNG
 * and always round-up to a known per-row boundary.
 */
/*
 * preferred solution, but not initializing correctly
 */
#define VSTR_MAX(len) (long)(len / 5 + (len % 5 == 0) ? 0 : 1 + 1)
