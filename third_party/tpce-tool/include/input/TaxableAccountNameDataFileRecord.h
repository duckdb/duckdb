#ifndef TAXABLE_ACCOUNT_NAME_DATA_FILE_RECORD_H
#define TAXABLE_ACCOUNT_NAME_DATA_FILE_RECORD_H

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
// A class to represent a single record in the TaxableAccountName data file.
//
// Exception Safety:
// The Basic guarantee is provided.
//
// Copy Behavior:
// Copying is allowed.
//

class TaxableAccountNameDataFileRecord {
private:
	static const int maxNameLen = 50;
	char nameCStr[maxNameLen + 1];
	std::string name;

	static const unsigned int fieldCount = 1;

public:
	explicit TaxableAccountNameDataFileRecord(const std::deque<std::string> &fields);

	//
	// Default copies and destructor are ok.
	//
	// ~TaxableAccountNameDataFileRecord()
	// TaxableAccountNameDataFileRecord(const
	// TaxableAccountNameDataFileRecord&); TaxableAccountNameDataFileRecord&
	// operator=(const TaxableAccountNameDataFileRecord&);
	//

	const std::string &NAME() const;
	const char *NAME_CSTR() const;

	std::string ToString() const;
};

} // namespace TPCE
#endif // TAXABLE_ACCOUNT_NAME_DATA_FILE_RECORD_H
