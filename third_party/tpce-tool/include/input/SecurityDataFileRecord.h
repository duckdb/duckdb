#ifndef SECURITY_DATA_FILE_RECORD_H
#define SECURITY_DATA_FILE_RECORD_H

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

#include <deque>
#include <string>

#include "utilities/EGenStandardTypes.h"

namespace TPCE {
//
// Description:
// A class to represent a single record in the Security data file.
//
// Exception Safety:
// The Basic guarantee is provided.
//
// Copy Behavior:
// Copying is allowed.
//

class SecurityDataFileRecord {
private:
	TIdent s_id;

	static const int maxS_st_idLen = 4;
	char s_st_idCStr[maxS_st_idLen + 1];
	std::string s_st_id;

	static const int maxS_symbLen = 7 + 1 + 7; // base + separator + extended
	char s_symbCStr[maxS_symbLen + 1];
	std::string s_symb;

	static const int maxS_issueLen = 6;
	char s_issueCStr[maxS_issueLen + 1];
	std::string s_issue;

	static const int maxS_ex_idLen = 6;
	char s_ex_idCStr[maxS_ex_idLen + 1];
	std::string s_ex_id;

	TIdent s_co_id;

	static const unsigned int fieldCount = 6;

public:
	explicit SecurityDataFileRecord(const std::deque<std::string> &fields);

	//
	// Default copies and destructor are ok.
	//
	// ~SecurityDataFileRecord()
	// SecurityDataFileRecord(const SecurityDataFileRecord&);
	// SecurityDataFileRecord& operator=(const SecurityDataFileRecord&);
	//

	TIdent S_ID() const;

	const std::string &S_ST_ID() const;
	const char *S_ST_ID_CSTR() const;

	const std::string &S_SYMB() const;
	const char *S_SYMB_CSTR() const;

	const std::string &S_ISSUE() const;
	const char *S_ISSUE_CSTR() const;

	const std::string &S_EX_ID() const;
	const char *S_EX_ID_CSTR() const;

	TIdent S_CO_ID() const;

	std::string ToString(char fieldSeparator = '\t') const;
};

} // namespace TPCE
#endif // SECURITY_DATA_FILE_RECORD_H
