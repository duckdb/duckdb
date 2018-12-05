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

#include "utilities/threading.h"

#include <iostream>

#include "main/progressmeter.h"
#include "main/strutil.h"

namespace TPCE {

// total - The total number of tasks to complete before the job is done.
ProgressMeter::ProgressMeter(int total_in, int verbosity, std::ostream *output)
    : m_total(total_in), m_current(0), m_display_interval(10), m_start_time(), m_last_time(m_start_time),
      m_output(output), m_verbosity(verbosity), m_mutex() {
}

// val - minimum number of seconds between automatic display updates
//       set to -1 to disable automatic displays
void ProgressMeter::set_display_interval(int val) {
	TPCE::Locker<ProgressMeter> locker(*this);
	m_display_interval = val;
}

int ProgressMeter::display_interval() const {
	TPCE::Locker<const ProgressMeter> locker(*this);
	return m_display_interval;
}

// Displays the current progress and an estimated time to finish
void ProgressMeter::display() const {
	TPCE::Locker<const ProgressMeter> locker(*this);
	display_message(*m_output);
	(*m_output) << std::endl;
}

// Displays a progress message to the specified output stream
// Lock progress meter before calling this function!
//
// output - output stream to display progress to
void ProgressMeter::display_message(std::ostream &out) const {
	CDateTime now;
	INT32 elapsed_in_ms = now.DiffInMilliSeconds(m_start_time);
	double rate = static_cast<double>(elapsed_in_ms) / static_cast<double>(m_current) / 1000.0;
	out << m_current << "/" << m_total << " (" << m_current * 100 / m_total << "%)"
	    << " [Remain: " << int64totimestr((int)(rate * (double)(m_total - m_current))) << ", "
	    << "Elapsed: " << int64totimestr(elapsed_in_ms / 1000) << "]";
}

// Notifies the progress meter that some tasks have been completed.  If there
// hasn't been a display update within display_interval seconds then an update
// will be emitted to the output stream.
//
// count  - number of tasks completed
// output - output stream to display progress to
void ProgressMeter::inc(int count) {
	CDateTime now;
	{
		TPCE::Locker<ProgressMeter> locker(*this);
		m_current += count;
		if (m_verbosity <= 0 || m_display_interval < 0 ||
		    now.DiffInMilliSeconds(m_last_time) <= m_display_interval * 1000) {
			return;
		}
		m_last_time.Set();
	}
	display();
}

// Return current count
int ProgressMeter::current() const {
	TPCE::Locker<const ProgressMeter> locker(*this);
	return m_current;
}

// Return total count
int ProgressMeter::total() const {
	TPCE::Locker<const ProgressMeter> locker(*this);
	return m_total;
}

// Display a message if the verbosity level is greater than message level
//
// mesg   - message to display
// level  - verbosity level to display at
void ProgressMeter::message(const std::string &mesg, int level) {
	TPCE::Locker<ProgressMeter> locker(*this);
	if (level >= m_verbosity) {
		return;
	}
	(*m_output) << mesg << std::endl;
}

void ProgressMeter::lock() const {
	m_mutex.lock();
}

void ProgressMeter::unlock() const {
	m_mutex.unlock();
}

} // namespace TPCE
