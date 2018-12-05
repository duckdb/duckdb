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

#include "input/ZipCodeDataFileRecord.h"

#include <sstream>
#include <stdexcept>
#include <cstdlib> // for atoi
// #include <string> // C++11 for stoi

#include "input/Utilities.h"

using namespace TPCE;

ZipCodeDataFileRecord::ZipCodeDataFileRecord(const std::deque<std::string> &fields) {
	if (fieldCount != fields.size()) {
		throw std::runtime_error("Incorrect field count.");
	}

	divisionTaxKey = std::atoi(fields[0].c_str());
	// divisionTaxKey = std::stoi(fields[0]); // C++11

	DFRStringInit(fields[1], zc_code, zc_codeCStr, maxZc_codeLen);

	DFRStringInit(fields[2], zc_town, zc_townCStr, maxZc_townLen);

	DFRStringInit(fields[3], zc_div, zc_divCStr, maxZc_divLen);
}

int ZipCodeDataFileRecord::DivisionTaxKey() const {
	return divisionTaxKey;
}

const std::string &ZipCodeDataFileRecord::ZC_CODE() const {
	return zc_code;
}

const char *ZipCodeDataFileRecord::ZC_CODE_CSTR() const {
	return zc_codeCStr;
}

const std::string &ZipCodeDataFileRecord::ZC_TOWN() const {
	return zc_town;
}

const char *ZipCodeDataFileRecord::ZC_TOWN_CSTR() const {
	return zc_townCStr;
}

const std::string &ZipCodeDataFileRecord::ZC_DIV() const {
	return zc_div;
}

const char *ZipCodeDataFileRecord::ZC_DIV_CSTR() const {
	return zc_divCStr;
}

std::string ZipCodeDataFileRecord::ToString(char fieldSeparator) const {
	// Facilitate encapsulation by using public interface to fields.
	std::ostringstream msg;
	msg << DivisionTaxKey() << fieldSeparator << ZC_CODE() << fieldSeparator << ZC_TOWN() << fieldSeparator << ZC_DIV();
	return msg.str();
}
