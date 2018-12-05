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

/*
 *   CPerson class for the Customer table.
 */
#ifndef PERSON_H
#define PERSON_H

#include <string>

#include "utilities/EGenStandardTypes.h"
#include "utilities/Random.h"

#include "input/DataFileManager.h"

namespace TPCE {

// Used for generating tax ID strings.
const int TaxIDFmt_len = 14;
const char TaxIDFmt[TaxIDFmt_len + 1] = "nnnaannnnaannn";

class CPerson {
private:
	const LastNameDataFile_t &m_LastNames;
	const MaleFirstNameDataFile_t &m_MaleFirstNames;
	const FemaleFirstNameDataFile_t &m_FemaleFirstNames;

	CRandom m_rnd;
	bool m_bCacheEnabled;
	int m_iCacheSize;
	TIdent m_iCacheOffset;
	int *m_FirstNameCache;
	int *m_LastNameCache;
	char *m_GenderCache;
	const int INVALID_NAME_CACHE_ENTRY;
	const char INVALID_GENDER_CACHE_ENTRY;

	template <class DataFileT> const string &getName(TIdent CID, const DataFileT &df, int *cache, RNGSEED seedBase) {
		// It is possible (and expected) to get CID values that are oustide the
		// current load unit. For example, AccountPermission CIDs and Broker IDs
		// can be outside the current load unit. These "out of bounds" CIDs are
		// not cached so we need to account for this.
		TIdent index = CID - m_iCacheOffset;
		bool bCheckCache = (index >= 0 && index < m_iCacheSize);

		// Use the cache if we can.
		if (m_bCacheEnabled && bCheckCache && (INVALID_NAME_CACHE_ENTRY != cache[index])) {
			return df[cache[index]].NAME();
		}

		// We couldn't use the cache.
		RNGSEED OldSeed;
		int iThreshold;

		OldSeed = m_rnd.GetSeed();

		m_rnd.SetSeed(m_rnd.RndNthElement(seedBase, (RNGSEED)CID));

		// First, generate the threshold.
		iThreshold = m_rnd.RndIntRange(0, df.size() - 1);

		// Cache the result if appropriate.
		if (m_bCacheEnabled && bCheckCache) {
			cache[index] = iThreshold;
		}

		// Restore the RNG
		m_rnd.SetSeed(OldSeed);

		return df[iThreshold].NAME();
	};

public:
	CPerson(const DataFileManager &dfm, TIdent iStartFromCustomer, bool bCacheEnabled = false);

	~CPerson();
	void InitNextLoadUnit(TIdent iCacheOffsetIncrement = iDefaultLoadUnitSize);

	const string &GetLastName(TIdent CID);
	const string &GetFirstName(TIdent CID);
	char GetMiddleName(TIdent CID);
	char GetGender(TIdent CID);    //'M' or 'F'
	bool IsMaleGender(TIdent CID); // TRUE if male, FALSE if female
	void GetTaxID(TIdent CID, char *buf);

	// get first name, last name, and tax id
	void GetFirstLastAndTaxID(TIdent C_ID, char *szFirstName, char *szLastName, char *szTaxID);
};

} // namespace TPCE

#endif // PERSON_H
