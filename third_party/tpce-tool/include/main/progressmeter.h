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

/*
 * Simple class to display the progress of long running job to the console.
 */

#ifndef PROGRESSMETER_H_INCLUDED
#define PROGRESSMETER_H_INCLUDED

#include <iostream>

#include "utilities/DateTime.h"
#include "progressmeterinterface.h"
#include "utilities/locking.h"

namespace TPCE {

class ProgressMeter : public ProgressMeterInterface {
private:
	int m_total;
	int m_current;
	int m_display_interval;
	CDateTime m_start_time;
	CDateTime m_last_time;
	std::ostream *m_output;
	int m_verbosity;
	mutable CMutex m_mutex;

public:
	// total - The total number of tasks to complete before the job is done.
	ProgressMeter(int total, int verbosity = 0, std::ostream *output = &std::cout);

	// val - minimum number of seconds between automatic display updates
	//       set to -1 to disable automatic displays
	void set_display_interval(int val);
	int display_interval() const;

	// Displays the current progress and an estimated time to finish onto
	// the provided output stream.
	virtual void display() const;

	// Displays a progress message to the specified output stream
	//
	// output - output stream to display progress to
	virtual void display_message(std::ostream &out) const;

	// Notifies the progress meter that some tasks have been completed.  If
	// there hasn't been a display update within display_interval seconds then
	// an update will be emitted to the output stream.
	//
	// count  - number of tasks completed
	// output - output stream to display progress to
	void inc(int count = 1);

	// Return current count
	int current() const;

	// Return total count
	int total() const;

	void lock() const;
	void unlock() const;

	// Output a message if it has the correct verbosity
	//
	// mesg   - message to display
	// level  - verbosity level to display at
	virtual void message(const std::string &mesg, int level = 0);
};

} // namespace TPCE

#endif // PROGRESSMETER_H_INCLUDED
