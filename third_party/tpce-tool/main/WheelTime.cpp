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
 *   Description:        Implementation of the CWheelTime class.
 *                       See WheelTime.h for a high level description.
 ******************************************************************************/

#include "main/WheelTime.h"

using namespace TPCE;

CWheelTime::CWheelTime(PWheelConfig pWheelConfig) : m_pWheelConfig(pWheelConfig), m_Cycles(0), m_Index(0) {
}

CWheelTime::CWheelTime(PWheelConfig pWheelConfig, INT32 cycles, INT32 index)
    : m_pWheelConfig(pWheelConfig), m_Cycles(cycles), m_Index(index) {
}

CWheelTime::CWheelTime(PWheelConfig pWheelConfig, CDateTime &Base, CDateTime &Now, INT32 offset)
    : m_pWheelConfig(pWheelConfig) {
	Set(Base, Now);
	Add(offset);
}

CWheelTime::~CWheelTime(void) {
}

void CWheelTime::Add(INT32 Interval) {
	// DJ - should throw error if Interval >= m_pWheelConfig->WheelSize?
	m_Cycles += Interval / m_pWheelConfig->WheelSize;
	m_Index += Interval % m_pWheelConfig->WheelSize;
	if (m_Index >= m_pWheelConfig->WheelSize) {
		// Handle wrapping in the wheel - assume we don't allow multi-cycle
		// intervals
		m_Cycles++;
		m_Index -= m_pWheelConfig->WheelSize;
	}
}

INT32 CWheelTime::Offset(const CWheelTime &Time) {
	INT32 Interval;

	Interval = (m_Cycles - Time.m_Cycles) * m_pWheelConfig->WheelSize;
	Interval += (m_Index - Time.m_Index);
	return (Interval);
}

void CWheelTime::Set(INT32 cycles, INT32 index) {
	m_Cycles = cycles;
	m_Index = index; // DJ - should throw error if Index >= m_pWheelConfig->WheelSize
}

// Set is overloaded. This version is used by the timer wheel.
void CWheelTime::Set(CDateTime &Base, CDateTime &Now) {
	INT32 offset; // offset from BaseTime in milliseconds

	// DJ - If Now < Base, then we should probably throw an exception

	offset = Now.DiffInMilliSeconds(Base) / m_pWheelConfig->WheelResolution; // convert based on wheel resolution
	m_Cycles = offset / m_pWheelConfig->WheelSize;
	m_Index = offset % m_pWheelConfig->WheelSize;
}

// Set is overloaded. This version is used by the event wheel.
void CWheelTime::Set(CDateTime *pBase, CDateTime *pNow) {
	INT32 offset; // offset from BaseTime in milliseconds

	// DJ - If Now < Base, then we should probably throw an exception

	offset = pNow->DiffInMilliSeconds(pBase) / m_pWheelConfig->WheelResolution; // convert based on wheel resolution
	m_Cycles = offset / m_pWheelConfig->WheelSize;
	m_Index = offset % m_pWheelConfig->WheelSize;
}

bool CWheelTime::operator<(const CWheelTime &Time) {
	return (m_Cycles == Time.m_Cycles) ? (m_Index < Time.m_Index) : (m_Cycles < Time.m_Cycles);
}

CWheelTime &CWheelTime::operator=(const CWheelTime &Time) {
	m_pWheelConfig = Time.m_pWheelConfig;
	m_Cycles = Time.m_Cycles;
	m_Index = Time.m_Index;

	return *this;
}

CWheelTime &CWheelTime::operator+=(const INT32 &Interval) {
	Add(Interval);
	return *this;
}

CWheelTime CWheelTime::operator++(INT32) {
	Add(1);
	return *this;
}
