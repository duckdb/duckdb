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

#include "input/SecurityDataFileRecord.h"

#include <sstream>
#include <stdexcept>
#include <cstdlib> // for atoi
// #include <string> // C++11 for stoi

#include "input/Utilities.h"

using namespace TPCE;

SecurityDataFileRecord::SecurityDataFileRecord(const std::deque<std::string> &fields) {
	if (fieldCount != fields.size()) {
		throw std::runtime_error("Incorrect field count.");
	}

	s_id = std::atoi(fields[0].c_str());
	// s_id = std::stoi(fields[0]); // C++11

	DFRStringInit(fields[1], s_st_id, s_st_idCStr, maxS_st_idLen);

	DFRStringInit(fields[2], s_symb, s_symbCStr, maxS_symbLen);

	DFRStringInit(fields[3], s_issue, s_issueCStr, maxS_issueLen);

	DFRStringInit(fields[4], s_ex_id, s_ex_idCStr, maxS_ex_idLen);

	s_co_id = std::atoi(fields[5].c_str());
	// s_co_id = std::stoi(fields[5]); // C++11
}

TIdent SecurityDataFileRecord::S_ID() const {
	return s_id;
}

const std::string &SecurityDataFileRecord::S_ST_ID() const {
	return s_st_id;
}

const char *SecurityDataFileRecord::S_ST_ID_CSTR() const {
	return s_st_idCStr;
}

const std::string &SecurityDataFileRecord::S_SYMB() const {
	return s_symb;
}

const char *SecurityDataFileRecord::S_SYMB_CSTR() const {
	return s_symbCStr;
}

const std::string &SecurityDataFileRecord::S_ISSUE() const {
	return s_issue;
}

const char *SecurityDataFileRecord::S_ISSUE_CSTR() const {
	return s_issueCStr;
}

const std::string &SecurityDataFileRecord::S_EX_ID() const {
	return s_ex_id;
}

const char *SecurityDataFileRecord::S_EX_ID_CSTR() const {
	return s_ex_idCStr;
}

TIdent SecurityDataFileRecord::S_CO_ID() const {
	return s_co_id;
}

std::string SecurityDataFileRecord::ToString(char fieldSeparator) const {
	// Facilitate encapsulation by using public interface to fields.
	std::ostringstream msg;
	msg << S_ID() << fieldSeparator << S_ST_ID() << fieldSeparator << S_SYMB() << fieldSeparator << S_ISSUE()
	    << fieldSeparator << S_EX_ID() << fieldSeparator << S_CO_ID();
	return msg.str();
}
