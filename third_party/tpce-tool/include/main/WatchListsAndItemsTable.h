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
 *   Class representing the Watch Lists table and Watch Items table.
 */
#ifndef WATCH_LIST_AND_ITEMS_TABLE_H
#define WATCH_LIST_AND_ITEMS_TABLE_H

#include "EGenTables_common.h"
#include "CustomerTable.h"

#include "input/DataFileManager.h"

namespace TPCE {

// Range of security ids indexes
const UINT iMinSecIdx = 0; // this should always be 0

// Min number of items in one watch list
const UINT iMinItemsInWL = 50;

// Max number of items in one watch list
const UINT iMaxItemsInWL = 150;

// Percentage of customers that have watch lists
const UINT iPercentWatchList = 100;

// Note: these parameters are dependent on the load unit size
static const UINT iWatchListIdPrime = 631;
static const UINT iWatchListIdOffset = 97;

// Number of RNG calls to skip for one row in order
// to not use any of the random values from the previous row.
const UINT iRNGSkipOneRowWatchListAndWatchItem = 15; // real max count in v3.5: 13

// Structure combining watch list row and watch items rows.
typedef struct WATCH_LIST_AND_ITEM_ROW {
	WATCH_LIST_ROW m_watch_list;
	WATCH_ITEM_ROW m_watch_items[iMaxItemsInWL + 1];
} * PWATCH_LIST_AND_ITEM_ROW;

typedef set<TIdent> IntSet; // set of integers with 'less' comparison function

class CWatchListsAndItemsTable : public TableTemplate<WATCH_LIST_AND_ITEM_ROW> {
	UINT m_iRowsToGenForWL;
	UINT m_iRowsGeneratedForWL;
	CCustomerTable m_cust;
	IntSet m_set;    // needed to generate random unique security ids
	UINT m_iWICount; //# of items for the last list
	TIdent m_iMinSecIdx;
	TIdent m_iMaxSecIdx;
	const CSecurityFile &m_SecurityFile;
	bool m_bInitNextLoadUnit;

	/*
	 *   Generate next Watch List ID
	 */
	void GenerateNextWL_ID() {
		TIdent iCustomerIdForCalc = m_cust.GetCurrentC_ID() - iDefaultStartFromCustomer;

		m_row.m_watch_list.WL_ID =
		    (iCustomerIdForCalc / iDefaultLoadUnitSize) * iDefaultLoadUnitSize + // strip last 3 digits
		    (iCustomerIdForCalc * iWatchListIdPrime + iWatchListIdOffset) % iDefaultLoadUnitSize +
		    iDefaultStartFromCustomer;
	}

	/*
	 *   Reset the state for the next load unit
	 */
	void InitNextLoadUnit() {
		m_rnd.SetSeed(m_rnd.RndNthElement(RNGSeedTableDefault,
		                                  (RNGSEED)m_cust.GetCurrentC_ID() * iRNGSkipOneRowWatchListAndWatchItem));

		ClearRecord(); // this is needed for EGenTest to work
	}

public:
	CWatchListsAndItemsTable(const DataFileManager &dfm, TIdent iCustomerCount, TIdent iStartFromCustomer)
	    : TableTemplate<WATCH_LIST_AND_ITEM_ROW>(), m_iRowsToGenForWL(0), m_iRowsGeneratedForWL(0),
	      m_cust(dfm, iCustomerCount, iStartFromCustomer), m_iWICount(0), m_iMinSecIdx(iMinSecIdx),
	      m_SecurityFile(dfm.SecurityFile()), m_bInitNextLoadUnit(false) {
		m_iMaxSecIdx = m_SecurityFile.GetConfiguredSecurityCount() - 1; // -1 because security indexes are 0-based

		// Initialize customer for the starting watch list id.
		// Iterate through customers to find the next one with a watch list
		do {
			if (m_cust.GetCurrentC_ID() % iDefaultLoadUnitSize == 0) {
				m_bInitNextLoadUnit = true; // delay calling InitNextLoadUnit until the start of
				                            // the row generation
			}

			m_cust.GenerateNextC_ID();

		} while (!m_rnd.RndPercent(iPercentWatchList) && m_cust.MoreRecords());
	};

	/*
	 *   Generates all column values for the next row.
	 */
	bool GenerateNextRecord() {
		TIdent iCustomerId;
		bool bRet = false;

		if (m_bInitNextLoadUnit) {
			InitNextLoadUnit();

			m_bInitNextLoadUnit = false;
		}

		++m_iLastRowNumber;

		iCustomerId = m_cust.GetCurrentC_ID();

		GenerateNextWL_ID();

		// Fill the customer ID
		m_row.m_watch_list.WL_C_ID = iCustomerId;

		// Now generate Watch Items for this Watch List
		m_iWICount = (UINT)m_rnd.RndIntRange(iMinItemsInWL,
		                                     iMaxItemsInWL); // number of items in the watch list
		for (m_set.clear(); m_set.size() < m_iWICount;) {
			// Generate random security id and insert into the set
			TIdent iSecurityIndex = m_rnd.RndInt64Range(m_iMinSecIdx, m_iMaxSecIdx);
			m_set.insert(iSecurityIndex);
		}

		int i;
		IntSet::iterator pos;
		// Now remove from the set and fill watch items rows
		for (i = 0, pos = m_set.begin(); pos != m_set.end(); ++i, ++pos) {
			m_row.m_watch_items[i].WI_WL_ID = m_row.m_watch_list.WL_ID; // same watch list id for all items
			// get the next element from the set
			m_SecurityFile.CreateSymbol(*pos, m_row.m_watch_items[i].WI_S_SYMB,
			                            static_cast<int>(sizeof(m_row.m_watch_items[i].WI_S_SYMB)));
		}

		bRet = false; // initialize for the case of currently processing the
		              // last customer

		// Iterate through customers to find the next one with a watch list
		while (!bRet && m_cust.MoreRecords()) {
			if (m_cust.GetCurrentC_ID() % iDefaultLoadUnitSize == 0) {
				m_bInitNextLoadUnit = true; // delay calling InitNextLoadUnit until the start of
				                            // the row generation
			}

			m_cust.GenerateNextC_ID();

			bRet = m_rnd.RndPercent(iPercentWatchList);
		}

		// If the new customer has a watch list, bRet was set to true.
		// If there are no more customers, bRet was initialized to false.
		if (bRet) {
			m_bMoreRecords = true;
		} else {
			m_bMoreRecords = false;
		}

		// Return false if all the rows have been generated
		return (MoreRecords());
	}

	const WATCH_LIST_ROW &GetWLRow() {
		return m_row.m_watch_list;
	}
	const WATCH_ITEM_ROW &GetWIRow(UINT i) {
		if (i < m_iWICount)
			return (m_row.m_watch_items[i]);
		else
			throw std::range_error("Watch Item row index out of bounds.");
	}
	UINT GetWICount() {
		return m_iWICount;
	}
};

} // namespace TPCE

#endif // WATCH_LIST_AND_ITEMS_TABLE_H
