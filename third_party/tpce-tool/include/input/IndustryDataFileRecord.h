#ifndef INDUSTRY_DATA_FILE_RECORD_H
#define INDUSTRY_DATA_FILE_RECORD_H

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

namespace TPCE {
//
// Description:
// A class to represent a single record in the Industry data file.
//
// Exception Safety:
// The Basic guarantee is provided.
//
// Copy Behavior:
// Copying is allowed.
//

class IndustryDataFileRecord {
private:
	static const int maxIn_idLen = 2;
	char in_idCStr[maxIn_idLen + 1];
	std::string in_id;

	static const int maxIn_nameLen = 50;
	char in_nameCStr[maxIn_nameLen + 1];
	std::string in_name;

	static const int maxIn_sc_idLen = 2;
	char in_sc_idCStr[maxIn_sc_idLen + 1];
	std::string in_sc_id;

	static const unsigned int fieldCount = 3;

public:
	explicit IndustryDataFileRecord(const std::deque<std::string> &fields);

	//
	// Default copies and destructor are ok.
	//
	// ~IndustryDataFileRecord()
	// IndustryDataFileRecord(const IndustryDataFileRecord&);
	// IndustryDataFileRecord& operator=(const IndustryDataFileRecord&);
	//

	const std::string &IN_ID() const;
	const char *IN_ID_CSTR() const;

	const std::string &IN_NAME() const;
	const char *IN_NAME_CSTR() const;

	const std::string &IN_SC_ID() const;
	const char *IN_SC_ID_CSTR() const;

	std::string ToString(char fieldSeparator = '\t') const;
};

} // namespace TPCE
#endif // INDUSTRY_DATA_FILE_RECORD_H
