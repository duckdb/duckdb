#ifndef COMMISSION_RATE_DATA_FILE_RECORD_H
#define COMMISSION_RATE_DATA_FILE_RECORD_H

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
// A class to represent a single record in the CommissionRate data file.
//
// Exception Safety:
// The Basic guarantee is provided.
//
// Copy Behavior:
// Copying is allowed.
//

class CommissionRateDataFileRecord {
private:
	int cr_c_tier;

	static const int maxCr_tt_idLen = 3;
	char cr_tt_idCStr[maxCr_tt_idLen + 1];
	std::string cr_tt_id;

	static const int maxCr_ex_idLen = 6;
	char cr_ex_idCStr[maxCr_ex_idLen + 1];
	std::string cr_ex_id;

	int cr_from_qty;
	int cr_to_qty;
	double cr_rate;

	static const unsigned int fieldCount = 6;

public:
	explicit CommissionRateDataFileRecord(const std::deque<std::string> &fields);

	//
	// Default copies and destructor are ok.
	//
	// ~CommissionRateDataFileRecord()
	// CommissionRateDataFileRecord(const CommissionRateDataFileRecord&);
	// CommissionRateDataFileRecord& operator=(const
	// CommissionRateDataFileRecord&);
	//

	int CR_C_TIER() const;

	const std::string &CR_TT_ID() const;
	const char *CR_TT_ID_CSTR() const;

	const std::string &CR_EX_ID() const;
	const char *CR_EX_ID_CSTR() const;

	int CR_FROM_QTY() const;
	int CR_TO_QTY() const;
	double CR_RATE() const;

	std::string ToString(char fieldSeparator = '\t') const;
};

} // namespace TPCE
#endif // COMMISSION_RATE_DATA_FILE_RECORD_H
