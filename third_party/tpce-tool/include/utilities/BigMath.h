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
 * - Cecil Reames, Matt Emmerton
 */

#ifndef BIGMATH_H
#define BIGMATH_H

#include "EGenStandardTypes.h"

namespace TPCE {

// For 128-bit integer multiplication
#define BIT63 UINT64_CONST(0x8000000000000000)
#define CARRY32 UINT64_CONST(0x100000000)
#define MASK32 UINT64_CONST(0xFFFFFFFF)
#define UPPER32 32

// Multiply 64-bit and 32-bit factors, followed by a right-shift of 64 bits
// (retaining upper 64-bit quantity) This is implemented as two 64-bit
// multiplications with summation of partial products.
inline UINT Mul6432WithShiftRight64(UINT64 seed, UINT range) {
	UINT64 SL = (seed & MASK32), // lower 32 bits of seed
	    SU = (seed >> UPPER32),  // upper 32 bits of seed
	    RL = range;              // range

	UINT64 p0 = (SL * RL), // partial products
	    p1 = (SU * RL), s;

	s = p0;
	s >>= UPPER32;
	s += p1;
	s >>= UPPER32;

	return (UINT)s;
}

// Multiply two 64-bit factors, followed by a right-shift of 64 bits (retaining
// upper 64-bit quantity) This is implemented as four 64-bit multiplications
// with summation of partial products and carry.
inline UINT64 Mul6464WithShiftRight64(UINT64 seed, UINT64 range) {
	UINT64 SL = (seed & MASK32), // lower 32 bits of seed
	    SU = (seed >> UPPER32),  // upper 32 bits of seed
	    RL = (range & MASK32),   // lower 32 bits of range
	    RU = (range >> UPPER32); // upper 32 bits of range

	UINT64 p0 = (SL * RL), // partial products
	    p1 = (SU * RL), p2 = (SL * RU), p3 = (SU * RU), p12_carry = 0, s;

	s = p0;
	s >>= UPPER32;
	s += p1;
	p12_carry = ((((p1 & BIT63) || (s & BIT63)) && (p2 & BIT63)) ? CARRY32 : 0);
	s += p2;
	s >>= UPPER32;
	s += p12_carry;
	s += p3;

	return s;
}

} // namespace TPCE

#endif // BIGMATH_H
