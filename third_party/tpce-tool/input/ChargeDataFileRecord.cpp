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

#include "input/ChargeDataFileRecord.h"

#include <sstream>
#include <stdexcept>
#include <cstdlib> // for atoi, atof
// #include <string> // C++11 for stoi, stod

#include "input/Utilities.h"

using namespace TPCE;

ChargeDataFileRecord::ChargeDataFileRecord(const std::deque<std::string> &fields) {
	if (fieldCount != fields.size()) {
		throw std::runtime_error("Incorrect field count.");
	}

	DFRStringInit(fields[0], ch_tt_id, ch_tt_idCStr, maxCh_tt_idLen);

	ch_c_tier = std::atoi(fields[1].c_str());
	// ch_c_tier = std::stoi(fields[1]); // C++11

	ch_chrg = std::atof(fields[2].c_str());
	// ch_chrg = std::stod(fields[2]); // C++11
}

const std::string &ChargeDataFileRecord::CH_TT_ID() const {
	return ch_tt_id;
}

const char *ChargeDataFileRecord::CH_TT_ID_CSTR() const {
	return ch_tt_idCStr;
}

int ChargeDataFileRecord::CH_C_TIER() const {
	return ch_c_tier;
}

double ChargeDataFileRecord::CH_CHRG() const {
	return ch_chrg;
}

std::string ChargeDataFileRecord::ToString(char fieldSeparator) const {
	// Facilitate encapsulation by using public interface to fields.
	std::ostringstream msg;
	msg << CH_TT_ID() << fieldSeparator << CH_C_TIER() << fieldSeparator << CH_CHRG();
	return msg.str();
}
