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

// Percentages used in determining gender.
const int iPercentGenderIsMale = 49;

/*
 *   Initializes in-memory representation of names files.
 */
CPerson::CPerson(const DataFileManager &dfm, TIdent iStartFromCustomer, bool bCacheEnabled)
    : m_LastNames(dfm.LastNameDataFile()), m_MaleFirstNames(dfm.MaleFirstNameDataFile()),
      m_FemaleFirstNames(dfm.FemaleFirstNameDataFile()), m_bCacheEnabled(bCacheEnabled), m_iCacheSize(0),
      m_iCacheOffset(0), INVALID_NAME_CACHE_ENTRY(-1), INVALID_GENDER_CACHE_ENTRY('X') {
	if (m_bCacheEnabled) {
		m_iCacheSize = iDefaultLoadUnitSize;
		m_iCacheOffset = iTIdentShift + iStartFromCustomer;
		m_FirstNameCache = new int[m_iCacheSize];
		m_LastNameCache = new int[m_iCacheSize];
		m_GenderCache = new char[m_iCacheSize];
		for (int i = 0; i < m_iCacheSize; i++) {
			m_FirstNameCache[i] = INVALID_NAME_CACHE_ENTRY;
			m_LastNameCache[i] = INVALID_NAME_CACHE_ENTRY;
			m_GenderCache[i] = INVALID_GENDER_CACHE_ENTRY;
		}
	}
}

/*
 *   Deallocate in-memory representation of names files.
 */
CPerson::~CPerson() {
	if (m_bCacheEnabled) {
		delete[] m_FirstNameCache;
		delete[] m_LastNameCache;
		delete[] m_GenderCache;
	}
}

/*
 *   Resets the cache.
 */
void CPerson::InitNextLoadUnit(TIdent iCacheOffsetIncrement) {
	if (m_bCacheEnabled) {
		m_iCacheOffset += iCacheOffsetIncrement;
		for (int i = 0; i < m_iCacheSize; i++) {
			m_FirstNameCache[i] = INVALID_NAME_CACHE_ENTRY;
			m_LastNameCache[i] = INVALID_NAME_CACHE_ENTRY;
			m_GenderCache[i] = INVALID_GENDER_CACHE_ENTRY;
		}
	}
}

/*
 *   Returns the last name for a particular customer id.
 *   It'll always be the same for the same customer id.
 */
const string &CPerson::GetLastName(TIdent CID) {
	return getName<LastNameDataFile_t>(CID, m_LastNames, m_LastNameCache, RNGSeedBaseLastName);
}

/*
 *   Returns the first name for a particular customer id.
 *   Determines gender first.
 */
const string &CPerson::GetFirstName(TIdent CID) {
	if (IsMaleGender(CID)) {
		return getName<MaleFirstNameDataFile_t>(CID, m_MaleFirstNames, m_FirstNameCache, RNGSeedBaseFirstName);
	} else {
		return getName<FemaleFirstNameDataFile_t>(CID, m_FemaleFirstNames, m_FirstNameCache, RNGSeedBaseFirstName);
	}
}
/*
 *   Returns the middle name.
 */
char CPerson::GetMiddleName(TIdent CID) {
	RNGSEED OldSeed;
	char cMiddleInitial[2];

	OldSeed = m_rnd.GetSeed();
	m_rnd.SetSeed(m_rnd.RndNthElement(RNGSeedBaseMiddleInitial, (RNGSEED)CID));
	cMiddleInitial[1] = '\0';
	m_rnd.RndAlphaNumFormatted(cMiddleInitial, "a");
	m_rnd.SetSeed(OldSeed);
	return (cMiddleInitial[0]);
}

/*
 *   Returns the gender character for a particular customer id.
 */
char CPerson::GetGender(TIdent CID) {
	RNGSEED OldSeed;
	char cGender;

	// It is possible (and expected) to get CID values that are oustide the
	// current load unit. For example, AccountPermission CIDs and Broker IDs
	// can be outside the current load unit. These "out of bounds" CIDs are not
	// cached so we need to account for this.
	TIdent index = CID - m_iCacheOffset;
	bool bCheckCache = (index >= 0 && index < m_iCacheSize);

	// Use the cache if we can.
	if (m_bCacheEnabled && bCheckCache && (INVALID_GENDER_CACHE_ENTRY != m_GenderCache[index])) {
		return m_GenderCache[index];
	}

	// We couldn't use the cache.

	OldSeed = m_rnd.GetSeed();
	m_rnd.SetSeed(m_rnd.RndNthElement(RNGSeedBaseGender, (RNGSEED)CID));

	// Find out gender
	if (m_rnd.RndPercent(iPercentGenderIsMale)) {
		cGender = 'M';
	} else {
		cGender = 'F';
	}

	// Cache the result if appropriate.
	if (m_bCacheEnabled && bCheckCache) {
		m_GenderCache[index] = cGender;
	}

	// Restore the RNG
	m_rnd.SetSeed(OldSeed);
	return (cGender);
}

/*
 *   Returns TRUE is a customer id is male
 */
bool CPerson::IsMaleGender(TIdent CID) {
	return GetGender(CID) == 'M';
}

/*
 *   Generate tax id
 */
void CPerson::GetTaxID(TIdent CID, char *buf) {
	RNGSEED OldSeed;

	OldSeed = m_rnd.GetSeed();

	// NOTE: the call to RndAlphaNumFormatted "consumes" an RNG value
	// for EACH character in the format string. Therefore, to avoid getting
	// tax ID's that overlap N-1 out of N characters, multiply the offset into
	// the sequence by N to get a unique range of values.
	m_rnd.SetSeed(m_rnd.RndNthElement(RNGSeedBaseTaxID, ((RNGSEED)CID * TaxIDFmt_len)));
	m_rnd.RndAlphaNumFormatted(buf, TaxIDFmt);
	m_rnd.SetSeed(OldSeed);
}

/*
 *   Get first name, last name, and tax id.
 */
void CPerson::GetFirstLastAndTaxID(TIdent C_ID, char *szFirstName, char *szLastName, char *szTaxID) {
	// Fill in the last name
	strncpy(szLastName, GetLastName(C_ID).c_str(), cL_NAME_len);
	// Fill in the first name
	strncpy(szFirstName, GetFirstName(C_ID).c_str(), cF_NAME_len);
	// Fill in the tax id
	GetTaxID(C_ID, szTaxID);
}
