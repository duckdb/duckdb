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

#include "input/TaxRateDivisionDataFileRecord.h"

#include <cstdlib>
#include <sstream>
#include <stdexcept>

#include "input/Utilities.h"

using namespace TPCE;

TaxRateDivisionDataFileRecord::TaxRateDivisionDataFileRecord(const std::deque<std::string> &fields) {
	if (fieldCount != fields.size()) {
		throw std::runtime_error("Incorrect field count.");
	}

	DFRStringInit(fields[0], tx_id, tx_idCStr, maxTx_idLen);

	DFRStringInit(fields[1], tx_name, tx_nameCStr, maxTx_nameLen);

	tx_rate = std::atof(fields[2].c_str());
}

const std::string &TaxRateDivisionDataFileRecord::TX_ID() const {
	return tx_id;
}

const char *TaxRateDivisionDataFileRecord::TX_ID_CSTR() const {
	return tx_idCStr;
}

const std::string &TaxRateDivisionDataFileRecord::TX_NAME() const {
	return tx_name;
}

const char *TaxRateDivisionDataFileRecord::TX_NAME_CSTR() const {
	return tx_nameCStr;
}

double TaxRateDivisionDataFileRecord::TX_RATE() const {
	return tx_rate;
}

std::string TaxRateDivisionDataFileRecord::ToString(char fieldSeparator) const {
	// Facilitate encapsulation by using public interface to fields.
	std::ostringstream msg;
	msg << TX_ID() << fieldSeparator << TX_NAME() << fieldSeparator << TX_RATE();
	return msg.str();
}
