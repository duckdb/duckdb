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
 * - Christopher Chan-Nui, Matt Emmerton
 */

#include "utilities/locking.h"

#include <cstring>

#ifndef WIN32
#include <iostream>
#include <sstream>
#include <stdexcept>
#include <cerrno>
#include <cstdlib>
#include <sys/time.h>

using std::exit;
using std::strerror;
#endif

#include "utilities/error.h"

namespace TPCE {

#ifdef WIN32

//////////////////////////////////////////////////////////
// Windows Implementation
//////////////////////////////////////////////////////////

CMutex::CMutex() : mutex_() {
	InitializeCriticalSection(&mutex_);
}

CMutex::~CMutex() {
	DeleteCriticalSection(&mutex_);
}

LPCRITICAL_SECTION CMutex::mutex() {
	return &mutex_;
}

void CMutex::lock() {
	EnterCriticalSection(&mutex_);
}

void CMutex::unlock() {
	LeaveCriticalSection(&mutex_);
}

#else

//////////////////////////////////////////////////////////
// Non-Windows Implementation (pthreads)
//////////////////////////////////////////////////////////

CMutex::CMutex() : mutex_() {
	int rc = pthread_mutex_init(&mutex_, NULL);
	if (rc != 0) {
		std::ostringstream strm;
		strm << "pthread_mutex_init error: " << strerror(rc) << "(" << rc << ")";
		throw std::runtime_error(strm.str());
	}
}

CMutex::~CMutex() {
	int rc = pthread_mutex_destroy(&mutex_);
	if (rc != 0) {
		std::ostringstream strm;
		strm << "pthread_mutex_destroy error: " << strerror(rc) << "(" << rc << ")";
	}
}

pthread_mutex_t *CMutex::mutex() {
	return &mutex_;
}

void CMutex::lock() {
	int rc = pthread_mutex_lock(&mutex_);
	if (rc != 0) {
		std::ostringstream strm;
		strm << "pthread_cond_wait error: " << strerror(rc) << "(" << rc << ")";
		throw std::runtime_error(strm.str());
	}
}

void CMutex::unlock() {
	int rc = pthread_mutex_unlock(&mutex_);
	if (rc != 0) {
		std::ostringstream strm;
		strm << "pthread_cond_wait error: " << strerror(rc) << "(" << rc << ")";
		throw std::runtime_error(strm.str());
	}
}

#endif

} // namespace TPCE
