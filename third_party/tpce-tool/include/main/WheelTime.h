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
 * - Doug Johnson
 */

/******************************************************************************
 *   Description:        This class provides functionality for managing the
 *                       relationship between time and locations in a wheel.
 ******************************************************************************/

#ifndef WHEEL_TIME_H
#define WHEEL_TIME_H

#include "utilities/EGenUtilities_stdafx.h"
#include "Wheel.h"

namespace TPCE {

class CWheelTime {

private:
	PWheelConfig m_pWheelConfig; // Pointer to configuration info for the wheel
	INT32 m_Cycles;              // Number of completed cycles so far
	INT32 m_Index;               // Index into the current cycle

public:
	CWheelTime(PWheelConfig pWheelConfig);
	CWheelTime(PWheelConfig pWheelConfig, INT32 cycles, INT32 index);
	CWheelTime(PWheelConfig pWheelConfig, CDateTime &Base, CDateTime &Now, INT32 Offset);
	~CWheelTime(void);

	inline INT32 Cycles(void) {
		return m_Cycles;
	};
	inline INT32 Index(void) {
		return m_Index;
	};

	void Add(INT32 Interval);

	INT32 Offset(const CWheelTime &Time);

	void Set(INT32 cycles, INT32 index);
	void Set(CDateTime &Base, CDateTime &Now);
	void Set(CDateTime *pBase, CDateTime *pNow);

	CWheelTime &operator=(const CWheelTime &Time);
	bool operator<(const CWheelTime &Time);
	CWheelTime &operator+=(const INT32 &Interval);
	CWheelTime operator++(INT32);
};

} // namespace TPCE

#endif // WHEEL_TIME_H
