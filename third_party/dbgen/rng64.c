/*
 * $Id: rng64.c,v 1.7 2008/03/21 17:38:39 jms Exp $
 *
 * This software contains proprietary and confidential information of Gradient
 * Systems Inc.  By accepting transfer of this copy, Recipient agrees
 * to retain this software in confidence, to prevent disclosure to others, and
 * to make no use of this software other than that for which it was delivered.
 * This is an unpublished copyright work Gradient Systems, Inc.  Execpt as
 * permitted by federal law, 17 USC 117, copying is strictly prohibited.
 *
 * Gradient Systems Inc. CONFIDENTIAL - (Gradient Systems Inc. Confidential
 * when combined with the aggregated modules for this product)
 * OBJECT CODE ONLY SOURCE MATERIALS
 * (C) COPYRIGHT Gradient Systems Inc. 2003
 *
 * All Rights Reserved
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF GRADIENT SYSTEMS, INC.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Revision History
 * ===================
 * $Log: rng64.c,v $
 * Revision 1.7  2008/03/21 17:38:39  jms
 * changes for 2.6.3
 *
 * Revision 1.6  2006/04/26 23:20:05  jms
 * Data type clenaup for qgen
 *
 * Revision 1.5  2006/03/08 21:25:27  jms
 * change to RNG64 to address overflow/underflow issues
 *
 * Revision 1.4  2005/10/25 17:26:38  jms
 * check in integration between microsoft changes and baseline code
 *
 * Revision 1.3  2005/03/04 19:48:39  jms
 * Changes from Doug Johnson to address very large scale factors
 *
 * Revision 1.2  2005/01/03 20:08:59  jms
 * change line terminations
 *
 * Revision 1.1.1.1  2004/11/24 23:31:47  jms
 * re-establish external server
 *
 * Revision 1.2  2004/02/18 16:45:30  jms
 * remove C++ style comments for AIX compiler
 *
 * Revision 1.1.1.1  2003/08/08 21:57:34  jms
 * recreation after CVS crash
 *
 * Revision 1.1  2003/08/08 21:57:34  jms
 * first integration of rng64 for o_custkey and l_partkey
 *
 */
#include "rng64.h"

#include <stdio.h>
#include <stdlib.h>
extern double dM;

extern seed_t Seed[];

void dss_random64(DSS_HUGE *tgt, DSS_HUGE nLow, DSS_HUGE nHigh, long nStream) {
	DSS_HUGE nTemp;

	if (nStream < 0 || nStream > MAX_STREAM)
		nStream = 0;

	if (nLow > nHigh) {
		nTemp = nLow;
		nLow = nHigh;
		nHigh = nTemp;
	}

	Seed[nStream].value = NextRand64(Seed[nStream].value);
	nTemp = Seed[nStream].value;
	if (nTemp < 0)
		nTemp = -nTemp;
	nTemp %= (nHigh - nLow + 1);
	*tgt = nLow + nTemp;
	Seed[nStream].usage += 1;
#ifdef RNG_TEST
	Seed[nStream].nCalls += 1;
#endif

	return;
}

DSS_HUGE
NextRand64(DSS_HUGE nSeed) {

	DSS_HUGE a = (unsigned DSS_HUGE)RNG_A;
	DSS_HUGE c = (unsigned DSS_HUGE)RNG_C;
	nSeed = (nSeed * a + c); /* implicitely truncated to 64bits */

	return (nSeed);
}

DSS_HUGE AdvanceRand64(DSS_HUGE nSeed, DSS_HUGE nCount) {
	unsigned DSS_HUGE a = RNG_A;
	unsigned DSS_HUGE c = RNG_C;
	int nBit;
	unsigned DSS_HUGE Apow = a, Dsum = c;

	/* if nothing to do, do nothing ! */
	if (nCount == 0)
		return nSeed;

	/* Recursively compute X(n) = A * X(n-1) + C */
	/* */
	/* explicitely: */
	/* X(n) = A^n * X(0) + { A^(n-1) + A^(n-2) + ... A + 1 } * C */
	/* */
	/* we write this as: */
	/* X(n) = Apow(n) * X(0) + Dsum(n) * C */
	/* */
	/* we use the following relations: */
	/* Apow(n) = A^(n%2)*Apow(n/2)*Apow(n/2) */
	/* Dsum(n) =   (n%2)*Apow(n/2)*Apow(n/2) + (Apow(n/2) + 1) * Dsum(n/2) */
	/* */

	/* first get the highest non-zero bit */
	for (nBit = 0; (nCount >> nBit) != RNG_C; nBit++) {
	}

	/* go 1 bit at the time */
	while (--nBit >= 0) {
		Dsum *= (Apow + 1);
		Apow = Apow * Apow;
		if (((nCount >> nBit) % 2) == 1) { /* odd value */
			Dsum += Apow;
			Apow *= a;
		}
	}
	nSeed = nSeed * Apow + Dsum * c;
	return nSeed;
}
