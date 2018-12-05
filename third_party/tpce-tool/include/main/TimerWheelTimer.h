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
 *   Description:        Template for timers stored in the timer wheel.
 ******************************************************************************/

#ifndef TIMER_WHEEL_TIMER_H
#define TIMER_WHEEL_TIMER_H

namespace TPCE {

template <class T, class T2> class CTimerWheelTimer {

private:
public:
	T *m_pExpiryData;                   // The data to be passed back
	T2 *m_pExpiryObject;                // The object on which to call the function
	void (T2::*m_pExpiryFunction)(T *); // The function to call at expiration

	CTimerWheelTimer(T2 *pExpiryObject, void (T2::*pExpiryFunction)(T *), T *pExpiryData);

	~CTimerWheelTimer(void);

}; // class CTimerWheelTimer

template <class T, class T2>
CTimerWheelTimer<T, T2>::CTimerWheelTimer(T2 *pExpiryObject, void (T2::*pExpiryFunction)(T *), T *pExpiryData) {
	m_pExpiryData = pExpiryData;
	m_pExpiryObject = pExpiryObject;
	m_pExpiryFunction = pExpiryFunction;
}

template <class T, class T2> CTimerWheelTimer<T, T2>::~CTimerWheelTimer(void) {
}

} // namespace TPCE

#endif // TIMER_WHEEL_TIMER_H
