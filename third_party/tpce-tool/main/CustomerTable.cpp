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
 * - Sergey Vasilevskiy
 * - Doug Johnson
 */

#include "main/EGenTables_stdafx.h"

using namespace TPCE;

namespace TPCE {
const char *szUSAreaCode = "011"; // USA/Canada phone area code
char EMAIL_DOMAINs[iNumEMAIL_DOMAINs][15] = {"@msn.com",     "@hotmail.com",   "@rr.com",
                                             "@netzero.com", "@earthlink.com", "@attbi.com"};
} // namespace TPCE

// Percentages used when generating C_TIER
const int iPercentCustomersInC_TIER_1 = 20;
const int iPercentCustomersInC_TIER_2 = 60;
const int iPercentCustomersInC_TIER_3 = 100 - iPercentCustomersInC_TIER_1 - iPercentCustomersInC_TIER_2;

// Percentages used when generating C_DOB
const int iPercentUnder18 = 5;
const int iPercentBetween19And24 = 16;
const int iPercentBetween25And34 = 17;
const int iPercentBetween35And44 = 19;
const int iPercentBetween45And54 = 16;
const int iPercentBetween55And64 = 11;
const int iPercentBetween65And74 = 8;
const int iPercentBetween75And84 = 7;
const int iPercentOver85 = 1;

// Number of RNG calls to skip for one row in order
// to not use any of the random values from the previous row.
const int iRNGSkipOneRowCustomer = 35; // real max count in v3.5: 29

/*
 *   CCustomerTable constructor
 */
CCustomerTable::CCustomerTable(const DataFileManager &dfm, TIdent iCustomerCount, TIdent iStartFromCustomer)
    : TableTemplate<CUSTOMER_ROW>(), m_iRowsToGenerate(iCustomerCount), m_person(dfm, iStartFromCustomer, true),
      m_Phones(dfm.AreaCodeDataFile()), m_iStartFromCustomer(iStartFromCustomer), m_iCustomerCount(iCustomerCount),
      m_StatusTypeFile(dfm.StatusTypeDataFile()), m_CustomerSelection() {
	m_iCompanyCount = dfm.CompanyFile().GetSize();
	m_iExchangeCount = dfm.ExchangeDataFile().size();
}

/*
 *   Reset the state for the next load unit
 */
void CCustomerTable::InitNextLoadUnit() {
	m_rnd.SetSeed(m_rnd.RndNthElement(RNGSeedTableDefault,
	                                  ((RNGSEED)m_iLastRowNumber + m_iStartFromCustomer - 1) * iRNGSkipOneRowCustomer));

	ClearRecord(); // this is needed for EGenTest to work

	m_person.InitNextLoadUnit();
}

/*
 *   Generates only the next Customer ID value
 */
TIdent CCustomerTable::GenerateNextC_ID() {
	if (m_iLastRowNumber % iDefaultLoadUnitSize == 0) {
		InitNextLoadUnit();
	}

	++m_iLastRowNumber; // increment state info
	m_bMoreRecords = m_iLastRowNumber < m_iRowsToGenerate;

	m_row.C_ID = m_iLastRowNumber + m_iStartFromCustomer - 1 + iTIdentShift;

	return m_row.C_ID;
}
/*
 *   Return current customer id.
 */
TIdent CCustomerTable::GetCurrentC_ID() {
	return m_iLastRowNumber + m_iStartFromCustomer - 1 + iTIdentShift;
}

/*
 *   Generate tax id.
 */
void CCustomerTable::GetC_TAX_ID(TIdent C_ID, char *szOutput) {
	m_person.GetTaxID(C_ID, szOutput);
}

/*
 *   Generate C_ST_ID.
 */
void CCustomerTable::GenerateC_ST_ID() {
	strncpy(m_row.C_ST_ID, m_StatusTypeFile[eActive].ST_ID_CSTR(), sizeof(m_row.C_ST_ID));
}

/*
 *   Generate first, last name, and the gender.
 */
void CCustomerTable::GeneratePersonInfo() {
	// Fill in the first name, last name, and the tax id
	m_person.GetFirstLastAndTaxID(m_row.C_ID, m_row.C_F_NAME, m_row.C_L_NAME, m_row.C_TAX_ID);
	// Fill in the gender
	m_row.C_GNDR = m_person.GetGender(m_row.C_ID);
	// Fill in the middle name
	m_row.C_M_NAME[0] = m_person.GetMiddleName(m_row.C_ID);
	m_row.C_M_NAME[1] = '\0';
}

/*
 *   Generate C_TIER.
 */
eCustomerTier CCustomerTable::GetC_TIER(TIdent C_ID) {
	return m_CustomerSelection.GetTier(C_ID);
}

/*
 *   Generate date of birth.
 */
void CCustomerTable::GenerateC_DOB() {
	// min and max age brackets limits in years
	static int age_brackets[] = {10, 19, 25, 35, 45, 55, 65, 75, 85, 100};
	int age_bracket;
	int dob_daysno_min, dob_daysno_max; // min and max date of birth in days
	int dob_in_days;                    // generated random date of birth in days

	int iThreshold = m_rnd.RndGenerateIntegerPercentage();

	// Determine customer age bracket according to the distribution.
	if (iThreshold <= iPercentUnder18)
		age_bracket = 0;
	else if (iThreshold <= iPercentUnder18 + iPercentBetween19And24)
		age_bracket = 1;
	else if (iThreshold <= iPercentUnder18 + iPercentBetween19And24 + iPercentBetween25And34)
		age_bracket = 2;
	else if (iThreshold <= iPercentUnder18 + iPercentBetween19And24 + iPercentBetween25And34 + iPercentBetween35And44)
		age_bracket = 3;
	else if (iThreshold <= iPercentUnder18 + iPercentBetween19And24 + iPercentBetween25And34 + iPercentBetween35And44 +
	                           iPercentBetween45And54)
		age_bracket = 4;
	else if (iThreshold <= iPercentUnder18 + iPercentBetween19And24 + iPercentBetween25And34 + iPercentBetween35And44 +
	                           iPercentBetween45And54 + iPercentBetween55And64)
		age_bracket = 5;
	else if (iThreshold <= iPercentUnder18 + iPercentBetween19And24 + iPercentBetween25And34 + iPercentBetween35And44 +
	                           iPercentBetween45And54 + iPercentBetween55And64 + iPercentBetween65And74)
		age_bracket = 6;
	else if (iThreshold <= iPercentUnder18 + iPercentBetween19And24 + iPercentBetween25And34 + iPercentBetween35And44 +
	                           iPercentBetween45And54 + iPercentBetween55And64 + iPercentBetween65And74 +
	                           iPercentBetween75And84)
		age_bracket = 7;
	else
		age_bracket = 8;
	assert(age_bracket < static_cast<int>(sizeof(age_brackets) / sizeof(age_brackets[0])));

	// Determine the range of valid day numbers for this person's birthday.
	dob_daysno_min = CDateTime::YMDtoDayno(InitialTradePopulationBaseYear - age_brackets[age_bracket + 1],
	                                       InitialTradePopulationBaseMonth, InitialTradePopulationBaseDay) +
	                 1;
	dob_daysno_max = CDateTime::YMDtoDayno(InitialTradePopulationBaseYear - age_brackets[age_bracket],
	                                       InitialTradePopulationBaseMonth, InitialTradePopulationBaseDay);

	// Generate the random age expressed in days that falls into the particular
	// range.
	dob_in_days = m_rnd.RndIntRange(dob_daysno_min, dob_daysno_max);

	m_row.C_DOB.Set(dob_in_days);
}

/*
 *   Generate C_AD_ID (address id).
 */
void CCustomerTable::GenerateC_AD_ID() {
	// Generate address id sequentially, allowing space for Exchanges and
	// Companies in the beggining of the address ids range.
	m_row.C_AD_ID = m_iExchangeCount + m_iCompanyCount + GetCurrentC_ID();
}

/*
 *   Generate C_CTRY_1.
 */
void CCustomerTable::GenerateC_CTRY_1() {
	strncpy(m_row.C_CTRY_1, szUSAreaCode, sizeof(m_row.C_CTRY_1));
}

/*
 *   Generate C_CTRY_2.
 */
void CCustomerTable::GenerateC_CTRY_2() {
	strncpy(m_row.C_CTRY_2, szUSAreaCode, sizeof(m_row.C_CTRY_2));
}

/*
 *   Generate C_CTRY_3.
 */
void CCustomerTable::GenerateC_CTRY_3() {
	strncpy(m_row.C_CTRY_3, szUSAreaCode, sizeof(m_row.C_CTRY_3));
}

/*
 *   Generate C_AREA_1
 */
void CCustomerTable::GenerateC_AREA_1() {
	RNGSEED OldSeed;
	int iThreshold;

	OldSeed = m_rnd.GetSeed();

	m_rnd.SetSeed(m_rnd.RndNthElement(RNGSeedBaseC_AREA_1, (RNGSEED)m_row.C_ID));

	// generate Threshold up to the value of the last key (first member in a
	// pair)
	iThreshold = m_rnd.RndIntRange(0, m_Phones.size() - 1);

	// copy the area code that corresponds to the Threshold
	strncpy(m_row.C_AREA_1, m_Phones[iThreshold].AREA_CODE_CSTR(), sizeof(m_row.C_AREA_1));

	m_rnd.SetSeed(OldSeed);
}

/*
 *   Generate C_AREA_2
 */
void CCustomerTable::GenerateC_AREA_2() {
	RNGSEED OldSeed;
	int iThreshold;

	OldSeed = m_rnd.GetSeed();

	m_rnd.SetSeed(m_rnd.RndNthElement(RNGSeedBaseC_AREA_2, (RNGSEED)m_row.C_ID));

	// generate Threshold up to the value of the last key (first member in a
	// pair)
	iThreshold = m_rnd.RndIntRange(0, m_Phones.size() - 1);

	// copy the area code that corresponds to the Threshold
	strncpy(m_row.C_AREA_2, m_Phones[iThreshold].AREA_CODE_CSTR(), sizeof(m_row.C_AREA_2));

	m_rnd.SetSeed(OldSeed);
}

/*
 *   Generate C_AREA_3
 */
void CCustomerTable::GenerateC_AREA_3() {
	RNGSEED OldSeed;
	int iThreshold;

	OldSeed = m_rnd.GetSeed();

	m_rnd.SetSeed(m_rnd.RndNthElement(RNGSeedBaseC_AREA_3, (RNGSEED)m_row.C_ID));

	// generate Threshold up to the value of the last key (first member in a
	// pair)
	iThreshold = m_rnd.RndIntRange(0, m_Phones.size() - 1);

	// copy the area code that corresponds to the Threshold
	strncpy(m_row.C_AREA_3, m_Phones[iThreshold].AREA_CODE_CSTR(), sizeof(m_row.C_AREA_3));

	m_rnd.SetSeed(OldSeed);
}

/*
 *   Generate C_LOCAL_1.
 */
void CCustomerTable::GenerateC_LOCAL_1() {
	m_rnd.RndAlphaNumFormatted(m_row.C_LOCAL_1,
	                           "nnnnnnn"); // 7-digit phone number
}

/*
 *   Generate C_LOCAL_2.
 */
void CCustomerTable::GenerateC_LOCAL_2() {
	m_rnd.RndAlphaNumFormatted(m_row.C_LOCAL_2,
	                           "nnnnnnn"); // 7-digit phone number
}

/*
 *   Generate C_LOCAL_3.
 */
void CCustomerTable::GenerateC_LOCAL_3() {
	m_rnd.RndAlphaNumFormatted(m_row.C_LOCAL_3,
	                           "nnnnnnn"); // 7-digit phone number
}

/*
 *   Generate C_EXT_1.
 */
void CCustomerTable::GenerateC_EXT_1() {
	int iThreshold = m_rnd.RndGenerateIntegerPercentage();

	if (iThreshold <= 25) {
		m_rnd.RndAlphaNumFormatted(m_row.C_EXT_1,
		                           "nnn"); // 3-digit phone extension
	} else {
		*m_row.C_EXT_1 = '\0'; // no extension
	}
}

/*
 *   Generate C_EXT_2.
 */
void CCustomerTable::GenerateC_EXT_2() {
	int iThreshold = m_rnd.RndGenerateIntegerPercentage();

	if (iThreshold <= 15) {
		m_rnd.RndAlphaNumFormatted(m_row.C_EXT_2,
		                           "nnn"); // 3-digit phone extension
	} else {
		*m_row.C_EXT_2 = '\0'; // no extension
	}
}

/*
 *   Generate C_EXT_3.
 */
void CCustomerTable::GenerateC_EXT_3() {
	int iThreshold = m_rnd.RndGenerateIntegerPercentage();

	if (iThreshold <= 5) {
		m_rnd.RndAlphaNumFormatted(m_row.C_EXT_3,
		                           "nnn"); // 3-digit phone extension
	} else {
		*m_row.C_EXT_3 = '\0'; // no extension
	}
}

/*
 *   Generate Email 1 and Email 2 that are guaranteed to be different.
 */
void CCustomerTable::GenerateC_EMAIL_1_and_C_EMAIL_2() {
	size_t iLen;
	int iEmail1Index;

	iEmail1Index = m_rnd.RndIntRange(0, iNumEMAIL_DOMAINs - 1);

	// Generate EMAIL_1
	iLen = strlen(m_row.C_L_NAME);
	m_row.C_EMAIL_1[0] = m_row.C_F_NAME[0];      // first char of the first name
	strncpy(&m_row.C_EMAIL_1[1], m_row.C_L_NAME, // last name
	        sizeof(m_row.C_EMAIL_1) - 1);
	strncpy(&m_row.C_EMAIL_1[1 + iLen],
	        EMAIL_DOMAINs[iEmail1Index], // domain name
	        sizeof(m_row.C_EMAIL_1) - iLen - 1);

	// Generate EMAIL_2 that is different from EMAIL_1
	m_row.C_EMAIL_2[0] = m_row.C_F_NAME[0]; // first char of the first name
	strncpy(&m_row.C_EMAIL_2[1], m_row.C_L_NAME,
	        sizeof(m_row.C_EMAIL_2) - 1); // last name
	strncpy(&m_row.C_EMAIL_2[1 + iLen],
	        EMAIL_DOMAINs[m_rnd.RndIntRangeExclude(0, iNumEMAIL_DOMAINs - 1, iEmail1Index)], // domain name
	        sizeof(m_row.C_EMAIL_2) - iLen - 1);
}

/*
 *   Generates all column values for the next row
 */
bool CCustomerTable::GenerateNextRecord() {
	GenerateNextC_ID();
	GenerateC_ST_ID();
	GeneratePersonInfo(); // generate last name, first name, gender and tax ID.
	m_row.C_TIER = (char)GetC_TIER(m_row.C_ID);
	GenerateC_DOB();
	GenerateC_AD_ID();
	GenerateC_CTRY_1();
	GenerateC_AREA_1();
	GenerateC_LOCAL_1();
	GenerateC_EXT_1();
	GenerateC_CTRY_2();
	GenerateC_AREA_2();
	GenerateC_LOCAL_2();
	GenerateC_EXT_2();
	GenerateC_CTRY_3();
	GenerateC_AREA_3();
	GenerateC_LOCAL_3();
	GenerateC_EXT_3();
	GenerateC_EMAIL_1_and_C_EMAIL_2();

	// Return false if all the rows have been generated
	return MoreRecords();
}
