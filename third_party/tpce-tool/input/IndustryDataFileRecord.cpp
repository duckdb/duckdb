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

#include "input/IndustryDataFileRecord.h"

#include <sstream>
#include <stdexcept>

#include "input/Utilities.h"

using namespace TPCE;

IndustryDataFileRecord::IndustryDataFileRecord(const std::deque<std::string> &fields) {
	if (fieldCount != fields.size()) {
		throw std::runtime_error("Incorrect field count.");
	}

	DFRStringInit(fields[0], in_id, in_idCStr, maxIn_idLen);

	DFRStringInit(fields[1], in_name, in_nameCStr, maxIn_nameLen);

	DFRStringInit(fields[2], in_sc_id, in_sc_idCStr, maxIn_sc_idLen);
}

const std::string &IndustryDataFileRecord::IN_ID() const {
	return in_id;
}

const char *IndustryDataFileRecord::IN_ID_CSTR() const {
	return in_idCStr;
}

const std::string &IndustryDataFileRecord::IN_NAME() const {
	return in_name;
}

const char *IndustryDataFileRecord::IN_NAME_CSTR() const {
	return in_nameCStr;
}

const std::string &IndustryDataFileRecord::IN_SC_ID() const {
	return in_sc_id;
}

const char *IndustryDataFileRecord::IN_SC_ID_CSTR() const {
	return in_sc_idCStr;
}

std::string IndustryDataFileRecord::ToString(char fieldSeparator) const {
	// Facilitate encapsulation by using public interface to fields.
	std::ostringstream msg;
	msg << IN_ID() << fieldSeparator << IN_NAME() << fieldSeparator << IN_SC_ID();
	return msg.str();
}
