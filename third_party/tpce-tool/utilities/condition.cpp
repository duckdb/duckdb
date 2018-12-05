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
 * - Christopher Chan-Nui
 */

#include "utilities/condition.h"

#include <cstring>

#include <iostream>
#include <sstream>
#include <stdexcept>
#include <cerrno>
#include <cstdlib>
#ifndef WIN32
#include <sys/time.h>
#endif

#include "utilities/error.h"

using std::exit;
using std::strerror;

namespace TPCE {

#ifdef WIN32

CCondition::CCondition(CMutex &pairedmutex) : mutex_(pairedmutex), cond_() {
	InitializeConditionVariable(&cond_);
}

#else

CCondition::CCondition(CMutex &pairedmutex) : mutex_(pairedmutex), cond_() {
	int rc = pthread_cond_init(&cond_, NULL);
	if (rc != 0) {
		std::ostringstream strm;
		strm << "pthread_cond_init error: " << strerror(rc) << "(" << rc << ")";
		throw std::runtime_error(strm.str());
	}
}
#endif

void CCondition::lock() {
	mutex_.lock();
}

void CCondition::unlock() {
	mutex_.unlock();
}

#ifdef WIN32

void CCondition::wait() const {
	SleepConditionVariableCS(&cond_, mutex_.mutex(), INFINITE);
}

void CCondition::signal() {
	WakeConditionVariable(&cond_);
}

void CCondition::broadcast() {
	WakeAllConditionVariable(&cond_);
}

bool CCondition::timedwait(long timeout /*us*/) const {
	if (timeout < 0) {
		wait();
		return true;
	}

	int rc = SleepConditionVariableCS(&cond_, mutex_.mutex(), timeout / 1000);
	if (rc == 0) {
		int rc2 = GetLastError();
		if (rc2 == WAIT_TIMEOUT) {
			return false;
		} else {
			std::ostringstream strm;
			strm << "SleepConditionVariableCS error: " << strerror(rc) << "(" << rc << ")";
			throw std::runtime_error(strm.str());
		}
	}
	return true;
}

#else

void CCondition::wait() const {
	int rc = pthread_cond_wait(&cond_, mutex_.mutex());
	if (rc != 0) {
		std::ostringstream strm;
		strm << "pthread_cond_wait error: " << strerror(rc) << "(" << rc << ")";
		throw std::runtime_error(strm.str());
	}
}

void CCondition::signal() {
	int rc = pthread_cond_signal(&cond_);
	if (rc != 0) {
		std::ostringstream strm;
		strm << "pthread_cond_signal error: " << strerror(rc) << "(" << rc << ")";
		throw std::runtime_error(strm.str());
	}
}

void CCondition::broadcast() {
	int rc = pthread_cond_broadcast(&cond_);
	if (rc != 0) {
		std::ostringstream strm;
		strm << "pthread_cond_broadcast error: " << strerror(rc) << "(" << rc << ")";
		throw std::runtime_error(strm.str());
	}
}

bool CCondition::timedwait(const struct timespec &timeout) const {
	int rc = pthread_cond_timedwait(&cond_, mutex_.mutex(), &timeout);
	if (rc == ETIMEDOUT) {
		return false;
	} else if (rc != 0) {
		std::ostringstream strm;
		strm << "pthread_cond_timedwait error: " << strerror(rc) << "(" << rc << ")";
		throw std::runtime_error(strm.str());
	}
	return true;
}

bool CCondition::timedwait(long timeout /*us*/) const {
	if (timeout < 0) {
		wait();
		return true;
	}

	const int nsec_in_sec = 1000000000;
	const int usec_in_sec = 1000000;
	const int usec_in_nsec = 1000;
	struct timeval tv;
	struct timespec ts;

	gettimeofday(&tv, NULL);

	ts.tv_sec = tv.tv_sec + static_cast<long>(timeout / usec_in_sec);
	ts.tv_nsec = (tv.tv_usec + static_cast<long>(timeout % usec_in_sec) * usec_in_nsec);
	if (ts.tv_nsec > nsec_in_sec) {
		ts.tv_sec += ts.tv_nsec / nsec_in_sec;
		ts.tv_nsec = ts.tv_nsec % nsec_in_sec;
	}
	return timedwait(ts);
}
#endif

} // namespace TPCE
