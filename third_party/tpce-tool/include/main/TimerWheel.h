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
 *   Description:        Template for a Timer Wheel. This allows for efficient
 *                       handling of timers that have a known maximum period.
 ******************************************************************************/

#ifndef TIMER_WHEEL_H
#define TIMER_WHEEL_H

#include "utilities/EGenUtilities_stdafx.h"
#include "utilities/MiscConsts.h"
#include "Wheel.h"
#include "WheelTime.h"
#include "TimerWheelTimer.h"

namespace TPCE {

template <class T, class T2, INT32 Period, INT32 Resolution> class CTimerWheel {

private:
	CDateTime m_BaseTime;

	CWheelTime m_LastTime;
	CWheelTime m_CurrentTime;
	CWheelTime m_NextTime;

	TWheelConfig m_WheelConfig;

	list<CTimerWheelTimer<T, T2> *> m_TimerWheel[(Period * (MsPerSecond / Resolution))];

	INT32 m_NumberOfTimers;

	INT32 ExpiryProcessing(void);
	void ProcessTimerList(list<CTimerWheelTimer<T, T2> *> *pList);
	INT32 SetNextTime(void);

public:
	static const INT32 NO_OUTSTANDING_TIMERS = -1;

	CTimerWheel();
	~CTimerWheel(void);

	bool Empty(void);
	INT32 ProcessExpiredTimers(void);
	INT32 StartTimer(double Offset, T2 *pExpiryObject, void (T2::*pExpiryFunction)(T *), T *pExpiryData);

}; // class CTimerWheel

template <class T, class T2, INT32 Period, INT32 Resolution>
CTimerWheel<T, T2, Period, Resolution>::CTimerWheel()
    : m_BaseTime(), m_LastTime(&m_WheelConfig, 0, 0), m_CurrentTime(&m_WheelConfig, 0, 0),
      m_NextTime(&m_WheelConfig, MaxWheelCycles, (Period * (MsPerSecond / Resolution)) - 1),
      m_WheelConfig((Period * (MsPerSecond / Resolution)), Resolution), m_NumberOfTimers(0) {
	m_BaseTime.Set();
}

template <class T, class T2, INT32 Period, INT32 Resolution>
CTimerWheel<T, T2, Period, Resolution>::~CTimerWheel(void) {
	typename list<CTimerWheelTimer<T, T2> *>::iterator ExpiredTimer;

	for (INT32 ii = 0; ii < (Period * (MsPerSecond / Resolution)); ii++) {
		if (!m_TimerWheel[ii].empty()) {
			ExpiredTimer = m_TimerWheel[ii].begin();
			while (ExpiredTimer != m_TimerWheel[ii].end()) {
				delete *ExpiredTimer;
				m_NumberOfTimers--;
				ExpiredTimer++;
			}
			m_TimerWheel[ii].clear();
		}
	}
}

template <class T, class T2, INT32 Period, INT32 Resolution> bool CTimerWheel<T, T2, Period, Resolution>::Empty(void) {
	return (m_NumberOfTimers == 0 ? true : false);
}

template <class T, class T2, INT32 Period, INT32 Resolution>
INT32 CTimerWheel<T, T2, Period, Resolution>::StartTimer(double Offset, T2 *pExpiryObject,
                                                         void (T2::*pExpiryFunction)(T *), T *pExpiryData) {
	CDateTime Now;
	CWheelTime RequestedTime(&m_WheelConfig, m_BaseTime, Now, (INT32)(Offset * (MsPerSecond / Resolution)));
	CTimerWheelTimer<T, T2> *pNewTimer = new CTimerWheelTimer<T, T2>(pExpiryObject, pExpiryFunction, pExpiryData);

	// Update current wheel position
	m_CurrentTime.Set(m_BaseTime, Now);

	// Since the current time has been updated, we should make sure
	// any outstanding timers have been processed before proceeding.
	ExpiryProcessing();

	m_TimerWheel[RequestedTime.Index()].push_back(pNewTimer);
	m_NumberOfTimers++;

	if (RequestedTime < m_NextTime) {
		m_NextTime = RequestedTime;
	}

	return (m_NextTime.Offset(m_CurrentTime));
}

template <class T, class T2, INT32 Period, INT32 Resolution>
INT32 CTimerWheel<T, T2, Period, Resolution>::ProcessExpiredTimers(void) {
	CDateTime Now;

	// Update current wheel position
	m_CurrentTime.Set(m_BaseTime, Now);

	return (ExpiryProcessing());
}

template <class T, class T2, INT32 Period, INT32 Resolution>
INT32 CTimerWheel<T, T2, Period, Resolution>::ExpiryProcessing(void) {
	while (m_LastTime < m_CurrentTime) {
		m_LastTime++;
		if (!m_TimerWheel[m_LastTime.Index()].empty()) {
			ProcessTimerList(&m_TimerWheel[m_LastTime.Index()]);
		}
	}
	return (SetNextTime());
}

template <class T, class T2, INT32 Period, INT32 Resolution>
void CTimerWheel<T, T2, Period, Resolution>::ProcessTimerList(list<CTimerWheelTimer<T, T2> *> *pList) {
	typename list<CTimerWheelTimer<T, T2> *>::iterator ExpiredTimer;

	ExpiredTimer = pList->begin();
	while (ExpiredTimer != pList->end()) {
		(((*ExpiredTimer)->m_pExpiryObject)->*((*ExpiredTimer)->m_pExpiryFunction))((*ExpiredTimer)->m_pExpiryData);
		delete *ExpiredTimer;
		m_NumberOfTimers--;
		ExpiredTimer++;
	}
	pList->clear();
}

template <class T, class T2, INT32 Period, INT32 Resolution>
INT32 CTimerWheel<T, T2, Period, Resolution>::SetNextTime(void) {
	if (0 == m_NumberOfTimers) {
		m_NextTime.Set(MaxWheelCycles, (Period * (MsPerSecond / Resolution)) - 1);
		return (NO_OUTSTANDING_TIMERS);
	} else {
		m_NextTime = m_CurrentTime;
		while (m_TimerWheel[m_NextTime.Index()].empty()) {
			m_NextTime++;
		}
		return (m_NextTime.Offset(m_CurrentTime));
	}
}

} // namespace TPCE

#endif // TIMER_WHEEL_H
