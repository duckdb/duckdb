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
 * - Sergey Vasilevskiy
 */

/******************************************************************************
 *   Description:        Money type that keeps all calculations in integer
 *                       number of cents. Needed for consistency of initial
 *                       database population.
 ******************************************************************************/

#ifndef MONEY_H
#define MONEY_H

#include "EGenStandardTypes.h"

namespace TPCE {

class CMoney {
	INT64 m_iAmountInCents; // dollar amount * 100

	// Define binary operators when CMoney is the right operand
	//
	friend CMoney operator*(int l_i, CMoney r_m);
	friend CMoney operator*(double l_f, CMoney r_m);
	friend double operator/(double l_f, CMoney r_m);

public:
	// Default constructor - initialize to $0
	//
	CMoney() : m_iAmountInCents(0) {
	}

	// Initialize from another CMoney
	//
	CMoney(CMoney *m) : m_iAmountInCents(m->m_iAmountInCents) {
	}

	// Initialize CMoney from double
	//
	CMoney(double fAmount)
	    : m_iAmountInCents((INT64)(100.0 * fAmount + 0.5)) // round floating-point number correctly
	{
	}

	// Return amount in integer dollars and fractional cents e.g. $123.99
	//
	double DollarAmount() {
		return m_iAmountInCents / 100.0;
	}

	// Return amount in integer cents e.g. 12399
	INT64 CentsAmount() {
		return m_iAmountInCents;
	}

	// Define arithmetic operations on CMoney and CMoney
	//
	CMoney operator+(const CMoney &m) {
		CMoney ret(this);

		ret.m_iAmountInCents += m.m_iAmountInCents;

		return ret;
	}

	CMoney &operator+=(const CMoney &m) {
		m_iAmountInCents += m.m_iAmountInCents;

		return *this;
	}

	CMoney operator-(const CMoney &m) {
		CMoney ret(this);

		ret.m_iAmountInCents -= m.m_iAmountInCents;

		return ret;
	}

	CMoney &operator-=(const CMoney &m) {
		m_iAmountInCents -= m.m_iAmountInCents;

		return *this;
	}

	CMoney operator*(const CMoney &m) {
		CMoney ret(this);

		ret.m_iAmountInCents *= m.m_iAmountInCents;

		return ret;
	}

	CMoney operator/(const CMoney &m) {
		CMoney ret(this);

		ret.m_iAmountInCents /= m.m_iAmountInCents;

		return ret;
	}

	// Define arithmetic operations on CMoney and int
	//
	CMoney operator*(int i) {
		CMoney ret(this);

		ret.m_iAmountInCents *= i;

		return ret;
	}

	// Define arithmetic operations on CMoney and double
	//
	CMoney operator+(double f) {
		CMoney ret(this);

		ret.m_iAmountInCents += (INT64)(100.0 * f + 0.5);

		return ret;
	}

	CMoney operator-(double f) {
		CMoney ret(this);

		ret.m_iAmountInCents -= (INT64)(100.0 * f + 0.5);

		return ret;
	}

	CMoney &operator-=(double f) {
		m_iAmountInCents -= (INT64)(100.0 * f + 0.5);

		return *this;
	}

	CMoney operator*(double f) {
		CMoney ret(this);

		// Do a trick for correct rounding. Can't use ceil or floor functions
		// because they do not round properly (e.g. down when < 0.5, up when >=
		// 0.5).
		//
		if (ret.m_iAmountInCents > 0) {
			ret.m_iAmountInCents = (INT64)(ret.m_iAmountInCents * f + 0.5);
		} else {
			ret.m_iAmountInCents = (INT64)(ret.m_iAmountInCents * f - 0.5);
		}

		return ret;
	}

	CMoney operator/(double f) {
		CMoney ret(this);

		if (ret.m_iAmountInCents > 0) {
			ret.m_iAmountInCents = (INT64)(ret.m_iAmountInCents / f + 0.5);
		} else {
			ret.m_iAmountInCents = (INT64)(ret.m_iAmountInCents / f - 0.5);
		}

		return ret;
	}

	// Assignment of a double (presumed fractional dollar amount e.g. in the
	// form $123.89)
	//
	CMoney &operator=(double f) {
		m_iAmountInCents = (INT64)(100.0 * f + 0.5);

		return *this;
	}

	// Comparison operators
	//
	bool operator==(const CMoney &m) {
		return m_iAmountInCents == m.m_iAmountInCents;
	}

	bool operator>(const CMoney &m) {
		return m_iAmountInCents > m.m_iAmountInCents;
	}
	bool operator>=(const CMoney &m) {
		return m_iAmountInCents >= m.m_iAmountInCents;
	}

	bool operator<(const CMoney &m) {
		return m_iAmountInCents < m.m_iAmountInCents;
	}
	bool operator<=(const CMoney &m) {
		return m_iAmountInCents <= m.m_iAmountInCents;
	}
};

} // namespace TPCE

#endif // #ifndef MONEY_H
