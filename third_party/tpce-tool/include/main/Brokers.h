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
 *   Class representing the Brokers table.
 */
#ifndef BROKERS_H
#define BROKERS_H

#include <stdio.h> // for snprintf which is not part of the C++ headers
#include "EGenTables_common.h"
#include "CustomerAccountsAndPermissionsTable.h"
#include "input/DataFileManager.h"
#include "StatusTypeIDs.h"

namespace TPCE {

const TIdent iBrokerNameIDShift = 1000 * 1000; // starting ID to generate names from for brokers

const int iBrokerInitialTradesYTDMin = 10000;
const int iBrokerInitialTradesYTDMax = 100000;

const double fBrokerInitialCommissionYTDMin = 10000.0;
const double fBrokerInitialCommissionYTDMax = 100000.0;

class CBrokersTable : public TableTemplate<BROKER_ROW> {
	TIdent m_iTotalBrokers; // total number of brokers rows to generate
	TIdent m_iStartFromBroker;
	TIdent m_iStartFromCustomer;
	CPerson m_person;
	const StatusTypeDataFile_t &m_StatusTypeFile; // STATUS_TYPE table from the flat file
	int *m_pNumTrades;                            // array of B_NUM_TRADES values
	double *m_pCommTotal;                         // array of B_COMM_TOTAL values

public:
	/*
	 *  Constructor for the BROKER table class.
	 *
	 *  PARAMETERS:
	 *       IN  inputFiles              - input flat files loaded in memory
	 *       IN  iCustomerCount          - customer count
	 *       IN  iStartFromCustomer      - starting customer id (1-based)
	 *
	 *  RETURNS:
	 *       not applicable.
	 */
	CBrokersTable(const DataFileManager &dfm, TIdent iCustomerCount, TIdent iStartFromCustomer)
	    : TableTemplate<BROKER_ROW>(), m_iTotalBrokers(iCustomerCount / iBrokersDiv),
	      m_iStartFromBroker((iStartFromCustomer / iBrokersDiv) + iStartingBrokerID + iTIdentShift),
	      m_iStartFromCustomer(iStartFromCustomer), m_person(dfm, iBrokerNameIDShift, true),
	      m_StatusTypeFile(dfm.StatusTypeDataFile()), m_pNumTrades(NULL), m_pCommTotal(NULL){};

	/*
	 *  Destructor.
	 *
	 *  PARAMETERS:
	 *       not applicable.
	 *
	 *  RETURNS:
	 *       not applicable.
	 */
	~CBrokersTable() {
		if (m_pNumTrades != NULL) {
			delete[] m_pNumTrades;
		}

		if (m_pCommTotal != NULL) {
			delete[] m_pCommTotal;
		}
	}

	/*
	 *   Initialization method; required when generating data but not for
	 * run-time.
	 *
	 *   It is called by CTradeGen at the beginning of every load unit.
	 *
	 *   PARAMETERS:
	 *           IN  iCustomerCount      - new customer count
	 *           IN  iStartFromCustomer  - new starting customer id (1-based)
	 *
	 *   RETURNS:
	 *           none.
	 */
	void InitForGen(TIdent iCustomerCount, TIdent iStartFromCustomer) {
		TIdent i;

		if (m_iTotalBrokers != iCustomerCount / iBrokersDiv || m_pNumTrades == NULL || m_pCommTotal == NULL)

		{
			//  Reallocate arrays for the new number of brokers
			//

			m_iTotalBrokers = iCustomerCount / iBrokersDiv;

			if (m_pNumTrades != NULL) {
				delete[] m_pNumTrades;
			}

			m_pNumTrades = new int[(size_t)m_iTotalBrokers];

			if (m_pCommTotal != NULL) {
				delete[] m_pCommTotal;
			}

			m_pCommTotal = new double[(size_t)m_iTotalBrokers];
		}

		//  Initialize array to 0
		//
		if (m_pNumTrades != NULL) {
			for (i = 0; i < m_iTotalBrokers; ++i) {
				m_pNumTrades[i] = 0;
			}
		}

		//  Initialize array to 0
		//
		if (m_pCommTotal != NULL) {
			for (i = 0; i < m_iTotalBrokers; ++i) {
				m_pCommTotal[i] = 0.0;
			}
		}

		if (m_iStartFromBroker != ((iStartFromCustomer / iBrokersDiv) + iStartingBrokerID + iTIdentShift)) {
			// Multiplying by iBrokersDiv again to get 64-bit broker ids
			// with 4.3bln IDENT_T shift value.
			// Removing shift factor prior to arithmetic so that contiguous
			// B_IDs values are obtained, and then add it back so that we
			// get shifted values.
			//
			m_iStartFromBroker = (iStartFromCustomer / iBrokersDiv) + iStartingBrokerID + iTIdentShift;
		}

		m_iLastRowNumber = 0;

		ClearRecord(); // this is needed for EGenTest to work

		// Don't re-initialize the cache for the first load unit
		if (m_iStartFromCustomer != iStartFromCustomer) {
			m_person.InitNextLoadUnit(iDefaultLoadUnitSize / iBrokersDiv);
		}
	};

	/*
	 *   Increment year-to-date values for broker trades and commissions.
	 *   Used to preserve consistency with initial trades.
	 *
	 *   PARAMETERS:
	 *           IN  B_ID                    - broker for whom to update YTD
	 * values IN  iTradeIncrement         - number of trades to add IN
	 * fCommissionIncrement    - amount of commission to add
	 *
	 *   RETURNS:
	 *           none.
	 */
	void UpdateTradeAndCommissionYTD(TIdent B_ID, int iTradeIncrement, double fCommissionIncrement) {
		if ((B_ID >= m_iStartFromBroker) && (B_ID < (m_iStartFromBroker + m_iTotalBrokers))) {
			m_pNumTrades[B_ID - m_iStartFromBroker] += iTradeIncrement;
			m_pCommTotal[B_ID - m_iStartFromBroker] += fCommissionIncrement;
		}
	}

	/*
	 *   Generate random broker id.
	 *   Exposed mostly for the driver to ensure unique
	 *   broker names for Broker Volume.
	 *   External RNG object is used in order to honor CCE autoseed behavior.
	 *
	 *   PARAMETERS:
	 *           IN  rnd             - external RNG
	 *
	 *   RETURNS:
	 *           random broker id
	 */
	TIdent GenerateRandomBrokerId(CRandom *pRnd) {
		return pRnd->RndInt64Range(m_iStartFromBroker, m_iStartFromBroker + m_iTotalBrokers - 1);
	}

	/*
	 *   Generate broker name into the provided buffer.
	 *   Exposed mostly for the driver (Broker Volume).
	 *
	 *   PARAMETERS:
	 *           IN  B_ID                - broker id
	 *           IN  B_NAME              - buffer for broker name
	 *           IN  B_NAME_len          - length of the name buffer
	 *
	 *   RETURNS:
	 *           none.
	 */
	void GenerateBrokerName(TIdent B_ID, char *B_NAME, size_t B_NAME_len) {
		snprintf(B_NAME, B_NAME_len, "%s %c. %s", m_person.GetFirstName(B_ID + iBrokerNameIDShift).c_str(),
		         m_person.GetMiddleName(B_ID + iBrokerNameIDShift),
		         m_person.GetLastName(B_ID + iBrokerNameIDShift).c_str());
	}

	/*
	 *   Return total broker count.
	 *   Exposed mostly for the driver (Broker Volume).
	 *
	 *   PARAMETERS:
	 *           none.
	 *
	 *   RETURNS:
	 *           total number of brokers in the table
	 */
	TIdent GetBrokerCount() {
		return m_iTotalBrokers;
	}

	/*
	 *   Generates all column values for the next row
	 *   and store them in the internal record structure.
	 *   Increments the number of rows generated.
	 *
	 *   PARAMETERS:
	 *           none.
	 *
	 *   RETURNS:
	 *           TRUE, if there are more records in the ADDRESS table; FALSE
	 * othewise.
	 */
	bool GenerateNextRecord() {
		m_row.B_ID = m_iStartFromBroker + m_iLastRowNumber;
		strncpy(m_row.B_ST_ID, m_StatusTypeFile[eActive].ST_ID_CSTR(), sizeof(m_row.B_ST_ID));

		GenerateBrokerName(m_row.B_ID, m_row.B_NAME, static_cast<int>(sizeof(m_row.B_NAME)));

		m_row.B_NUM_TRADES = m_pNumTrades[m_row.B_ID - m_iStartFromBroker];
		m_row.B_COMM_TOTAL = m_pCommTotal[m_row.B_ID - m_iStartFromBroker];

		// Update state info
		++m_iLastRowNumber;
		m_bMoreRecords = m_iLastRowNumber < m_iTotalBrokers;

		// Return false if all the rows have been generated
		return (MoreRecords());
	}
};

} // namespace TPCE

#endif // BROKERS_H
