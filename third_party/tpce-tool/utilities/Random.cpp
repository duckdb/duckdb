/*
 * Legal Notice
 *
 * This document and associated source code (the "Work") is a part of a
 * benchmark specification maintained by the TPC.
 *
 * The TPC reserves all right, title, and interest to the Work as provided
 * under U.S. and international laws, including without limitation all patent
 * and trademark rights therein.
 *
 * No Warranty
 *
 * 1.1 TO THE MAXIMUM EXTENT PERMITTED BY APPLICABLE LAW, THE INFORMATION
 *     CONTAINED HEREIN IS PROVIDED "AS IS" AND WITH ALL FAULTS, AND THE
 *     AUTHORS AND DEVELOPERS OF THE WORK HEREBY DISCLAIM ALL OTHER
 *     WARRANTIES AND CONDITIONS, EITHER EXPRESS, IMPLIED OR STATUTORY,
 *     INCLUDING, BUT NOT LIMITED TO, ANY (IF ANY) IMPLIED WARRANTIES,
 *     DUTIES OR CONDITIONS OF MERCHANTABILITY, OF FITNESS FOR A PARTICULAR
 *     PURPOSE, OF ACCURACY OR COMPLETENESS OF RESPONSES, OF RESULTS, OF
 *     WORKMANLIKE EFFORT, OF LACK OF VIRUSES, AND OF LACK OF NEGLIGENCE.
 *     ALSO, THERE IS NO WARRANTY OR CONDITION OF TITLE, QUIET ENJOYMENT,
 *     QUIET POSSESSION, CORRESPONDENCE TO DESCRIPTION OR NON-INFRINGEMENT
 *     WITH REGARD TO THE WORK.
 * 1.2 IN NO EVENT WILL ANY AUTHOR OR DEVELOPER OF THE WORK BE LIABLE TO
 *     ANY OTHER PARTY FOR ANY DAMAGES, INCLUDING BUT NOT LIMITED TO THE
 *     COST OF PROCURING SUBSTITUTE GOODS OR SERVICES, LOST PROFITS, LOSS
 *     OF USE, LOSS OF DATA, OR ANY INCIDENTAL, CONSEQUENTIAL, DIRECT,
 *     INDIRECT, OR SPECIAL DAMAGES WHETHER UNDER CONTRACT, TORT, WARRANTY,
 *     OR OTHERWISE, ARISING IN ANY WAY OUT OF THIS OR ANY OTHER AGREEMENT
 *     RELATING TO THE WORK, WHETHER OR NOT SUCH AUTHOR OR DEVELOPER HAD
 *     ADVANCE NOTICE OF THE POSSIBILITY OF SUCH DAMAGES.
 *
 * Contributors
 * - Charles Levine, Philip Durr, Doug Johnson, Cecil Reames, Matt Emmerton
 */

#include "utilities/Random.h"

#include <cmath>
#include <time.h>

#include "utilities/BigMath.h"
#include "utilities/MiscConsts.h"

using namespace TPCE;

inline RNGSEED CRandom::UInt64Rand(void) {

	UINT64 a = (UINT64)UInt64Rand_A_MULTIPLIER;
	UINT64 c = (UINT64)UInt64Rand_C_INCREMENT;
	m_seed = (m_seed * a + c); // implicitly truncated to 64bits

	return (m_seed);
}

RNGSEED CRandom::RndNthElement(RNGSEED nSeed, RNGSEED nCount) {
	UINT64 a = UInt64Rand_A_MULTIPLIER;
	UINT64 c = UInt64Rand_C_INCREMENT;
	int nBit;
	UINT64 Apow = a;
	UINT64 Dsum = UInt64Rand_ONE;

	// if nothing to do, do nothing !
	if (nCount == 0) {
		return nSeed;
	}

	// Recursively compute X(n) = A * X(n-1) + C
	//
	// explicitly:
	// X(n) = A^n * X(0) + { A^(n-1) + A^(n-2) + ... A + 1 } * C
	//
	// we write this as:
	// X(n) = Apow(n) * X(0) + Dsum(n) * C
	//
	// we use the following relations:
	// Apow(n) = A^(n%2)*Apow(n/2)*Apow(n/2)
	// Dsum(n) =   (n%2)*Apow(n/2)*Apow(n/2) + (Apow(n/2) + 1) * Dsum(n/2)
	//

	// first get the highest non-zero bit
	for (nBit = 0; (nCount >> nBit) != UInt64Rand_ONE; nBit++) {
	}

	// go 1 bit at the time
	while (--nBit >= 0) {
		Dsum *= (Apow + 1);
		Apow = Apow * Apow;
		if (((nCount >> nBit) % 2) == 1) { // odd value
			Dsum += Apow;
			Apow *= a;
		}
	}
	nSeed = nSeed * Apow + Dsum * c;
	return nSeed;
}

CRandom::CRandom(void) {
	do {
		// use portable way to get the seed
		m_seed = (RNGSEED)time(NULL);
	} while (m_seed == 0);
}

CRandom::CRandom(RNGSEED seed) {
	SetSeed(seed);
}

void CRandom::SetSeed(RNGSEED seed) {
	m_seed = seed;
}

#ifdef EGEN_USE_DEPRECATED_CODE

// returns a random value in the range [0 .. 0.99999999999999999994578989137572]
// care should be taken in casting the result as a float because of the
// potential loss of precision.
double CRandom::RndDouble(void) {
	return ((double)UInt64Rand()) * (double)UInt64Rand_RECIPROCAL_2_POWER_64;
}

#endif // EGEN_USE_DEPRECATED_CODE

int CRandom::RndIntRange(int min, int max) {
	if (max <= min)
		return min; // max <= min

	UINT range = (max - min + 1);

	if (range <= 1)
		return min; // overflow happened

	UInt64Rand(); // generate next seed value

	return (min + (int)Mul6432WithShiftRight64(m_seed, range));
}

INT64 CRandom::RndInt64Range(INT64 min, INT64 max) {
	if (max <= min)
		return min;

	UINT64 range = (max - min + 1);

	if (range <= 1)
		return min; // overflow happened

	UInt64Rand(); // generate next seed value

	return (min + (INT64)Mul6464WithShiftRight64(m_seed, range));
}

int CRandom::RndNthIntRange(RNGSEED Seed, RNGSEED N, int min, int max) {
	if (max <= min)
		return min; // max <= min

	UINT range = (max - min + 1);

	if (range <= 1)
		return min; // overflow happened

	RNGSEED nseed = RndNthElement(Seed, N); // generate next seed value

	return (min + (int)Mul6432WithShiftRight64(nseed, range));
}

// return Nth element in the sequence over the integer range
INT64 CRandom::RndNthInt64Range(RNGSEED Seed, RNGSEED N, INT64 min, INT64 max) {
	if (max <= min)
		return min;

	UINT64 range = (max - min + 1);

	if (range <= 1)
		return min; // overflow happened

	RNGSEED nseed = RndNthElement(Seed, N); // generate next seed value

	return (min + (INT64)Mul6464WithShiftRight64(nseed, range));
}

int CRandom::RndIntRangeExclude(int low, int high, int exclude) {
	int temp;

	temp = RndIntRange(low, high - 1);
	if (temp >= exclude)
		temp += 1;

	return temp;
}

INT64 CRandom::RndInt64RangeExclude(INT64 low, INT64 high, INT64 exclude) {
	INT64 temp;

	temp = RndInt64Range(low, high - 1);
	if (temp >= exclude)
		temp += 1;

	return temp;
}

#ifdef EGEN_USE_DEPRECATED_CODE

double CRandom::RndDoubleRange(double min, double max) {
	return min + RndDouble() * (max - min);
}

#endif // EGEN_USE_DEPRECATED_CODE

double CRandom::RndDoubleIncrRange(double min, double max, double incr) {
	INT64 width = (INT64)((max - min) / incr); // need [0..width], so no +1
	return min + ((double)RndInt64Range(0, width) * incr);
}

// returns a random double value from a negative exponential distribution with
// the given mean
double CRandom::RndDoubleNegExp(double mean) {
	return ((-1.0 * std::log(RndDoubleIncrRange(0.0, 1.0, 0.000000000001))) * mean);
}

/* Returns a non-uniform random 64-bit integer in range of [P .. Q].
 *
 *  NURnd is used to create a skewed data access pattern.  The function is
 *  similar to NURand in TPC-C.  (The two functions are identical when C=0
 *  and s=0.)
 *
 *  The parameter A must be of the form 2^k - 1, so that Rnd[0..A] will
 *  produce a k-bit field with all bits having 50/50 probability of being 0
 *  or 1.
 *
 *  With a k-bit A value, the weights range from 3^k down to 1 with the
 *  number of equal probability values given by C(k,i) = k! /(i!(k-i)!) for
 *  0 <= i <= k.  So a bigger A value from a larger k has much more skew.
 *
 *  Left shifting of Rnd[0..A] by "s" bits gets a larger interval without
 *  getting huge amounts of skew.  For example, when applied to elapsed time
 *  in milliseconds, s=10 effectively ignores the milliseconds, while s=16
 *  effectively ignores seconds and milliseconds, giving a granularity of
 *  just over 1 minute (65.536 seconds).  A smaller A value can then give
 *  the desired amount of skew at effectively one-minute resolution.
 */
INT64 CRandom::NURnd(INT64 P, INT64 Q, INT32 A, INT32 s) {
	return (((RndInt64Range(P, Q) | (RndInt64Range(0, A) << s)) % (Q - P + 1)) + P);
}

/*
 *   Returns an alphanumeric string in a specified format;
 */

void CRandom::RndAlphaNumFormatted(char *szReturnString, const char *szFormat) {
	while (szFormat && *szFormat) {
		switch (*szFormat) {
		case 'a':
			*szReturnString = UpperCaseLetters[RndIntRange(0, 25)]; // only uppercase
			break;
		case 'n':
			*szReturnString = Numerals[RndIntRange(0, 9)];
			break;
		default:
			*szReturnString = *szFormat;
		}

		++szFormat;
		++szReturnString;
	}
	*szReturnString = '\0';
}
