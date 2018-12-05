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

#include "input/CommissionRateDataFileRecord.h"

#include <sstream>
#include <stdexcept>
#include <cstdlib> // for atoi, atof
// #include <string> // C++11 for stoi, stod

#include "input/Utilities.h"

using namespace TPCE;

CommissionRateDataFileRecord::CommissionRateDataFileRecord(const std::deque<std::string> &fields) {
	if (fieldCount != fields.size()) {
		throw std::runtime_error("Incorrect field count.");
	}

	cr_c_tier = std::atoi(fields[0].c_str());
	// cr_c_tier = std::stoi(fields[0]); // C++11

	DFRStringInit(fields[1], cr_tt_id, cr_tt_idCStr, maxCr_tt_idLen);

	DFRStringInit(fields[2], cr_ex_id, cr_ex_idCStr, maxCr_ex_idLen);

	cr_from_qty = std::atoi(fields[3].c_str());
	// cr_from_qty = std::stoi(fields[3]); // C++11
	cr_to_qty = std::atoi(fields[4].c_str());
	// cr_to_qty = std::stoi(fields[4]); // C++11
	cr_rate = std::atof(fields[5].c_str());
	// cr_rate = std::stod(fields[5]); // C++11
}

int CommissionRateDataFileRecord::CR_C_TIER() const {
	return cr_c_tier;
}

const std::string &CommissionRateDataFileRecord::CR_TT_ID() const {
	return cr_tt_id;
}

const char *CommissionRateDataFileRecord::CR_TT_ID_CSTR() const {
	return cr_tt_idCStr;
}

const std::string &CommissionRateDataFileRecord::CR_EX_ID() const {
	return cr_ex_id;
}

const char *CommissionRateDataFileRecord::CR_EX_ID_CSTR() const {
	return cr_ex_idCStr;
}

int CommissionRateDataFileRecord::CR_FROM_QTY() const {
	return cr_from_qty;
}

int CommissionRateDataFileRecord::CR_TO_QTY() const {
	return cr_to_qty;
}

double CommissionRateDataFileRecord::CR_RATE() const {
	return cr_rate;
}

std::string CommissionRateDataFileRecord::ToString(char fieldSeparator) const {
	// Facilitate encapsulation by using public interface to fields.
	std::ostringstream msg;
	msg << CR_C_TIER() << fieldSeparator << CR_TT_ID() << fieldSeparator << CR_EX_ID() << fieldSeparator
	    << CR_FROM_QTY() << fieldSeparator << CR_TO_QTY() << fieldSeparator << CR_RATE();
	return msg.str();
}
