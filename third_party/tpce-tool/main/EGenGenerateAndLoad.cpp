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
 *   This file contains a class that acts as a client to the table
 *   generation classes (EGenTables) and to the loader classes (EGenBaseLoader).
 *   It provides routines for generating and loading the table data or its
 * subset.
 */

#include "main/EGenGenerateAndLoad_stdafx.h"
#include "main/DriverParamSettings.h"

#include "main/TableTypes.h"
#include "main/ChargeTable.h"
#include "main/CommissionRateTable.h"
#include "main/ExchangeTable.h"
#include "main/IndustryTable.h"
#include "main/SectorTable.h"
#include "main/StatusTypeTable.h"
#include "main/TaxRateTable.h"
#include "main/TradeTypeTable.h"
#include "main/ZipCodeTable.h"

using namespace TPCE;

/*
 *   Constructor.
 *
 *  PARAMETERS:
 *           IN  dfm                 - in-memory representation of input flat
 * files IN  inputFiles          - in-memory representation of input flat files
 *           IN  iCustomerCount      - number of customers to build (for this
 * class instance) IN  iStartFromCustomer  - first customer id IN
 * iTotalCustomers     - total number of customers in the database IN
 * iLoadUnitSize       - minimal number of customers that can be build (should
 * always be 1000) IN  iScaleFactor        - number of customers for 1tpsE IN
 * iDaysOfInitialTrades- number of 8-hour days of initial trades per customer IN
 * pLoaderFactory      - factory to create loader classes IN  pLogger -
 * parameter logging interface IN  pOutput             - interface to output
 * information to a user during the build process IN  szInDir             -
 * input flat file directory needed for tables loaded from flat files
 *
 *  RETURNS:
 *           not applicable.
 */
CGenerateAndLoad::CGenerateAndLoad(const DataFileManager &dfm, TIdent iCustomerCount, TIdent iStartFromCustomer,
                                   TIdent iTotalCustomers, UINT iLoadUnitSize, UINT iScaleFactor,
                                   UINT iDaysOfInitialTrades, CBaseLoaderFactory *pLoaderFactory, CBaseLogger *pLogger,
                                   CGenerateAndLoadBaseOutput *pOutput, bool bCacheEnabled)
    : m_dfm(dfm), m_iStartFromCustomer(iStartFromCustomer), m_iCustomerCount(iCustomerCount),
      m_iTotalCustomers(iTotalCustomers), m_iLoadUnitSize(iLoadUnitSize), m_iScaleFactor(iScaleFactor),
      m_iHoursOfInitialTrades(iDaysOfInitialTrades * HoursPerWorkDay), m_pLoaderFactory(pLoaderFactory),
      m_pOutput(pOutput), m_pLogger(pLogger), m_LoaderSettings(iTotalCustomers, iTotalCustomers, iStartFromCustomer,
                                                               iCustomerCount, iScaleFactor, iDaysOfInitialTrades),
      m_bCacheEnabled(bCacheEnabled) {
	// Copy input flat file directory needed for tables loaded from flat files.

	// Log Parameters
	m_pLogger->SendToLogger(m_LoaderSettings);
}

/*
 *   Generate and load ADDRESS table.
 *
 *   PARAMETERS:
 *           none.
 *
 *   RETURNS:
 *           none.
 */
void CGenerateAndLoad::GenerateAndLoadAddress() {
	bool bRet;
	CAddressTable Table(m_dfm, m_iCustomerCount, m_iStartFromCustomer,
	                    // do not generate exchange and company addresses
	                    // if the starting customer is not 1
	                    m_iStartFromCustomer != iDefaultStartFromCustomer);
	CBaseLoader<ADDRESS_ROW> *pLoad = m_pLoaderFactory->CreateAddressLoader();
	INT64 iCnt = 0;

	m_pOutput->OutputStart("Generating ADDRESS table...");

	pLoad->Init();

	do {
		bRet = Table.GenerateNextRecord();

		pLoad->WriteNextRecord(Table.GetRow());

		if (++iCnt % 20000 == 0) { // output progress
			m_pOutput->OutputProgress(".");
		}

	} while (bRet);

	pLoad->FinishLoad(); // commit
	delete pLoad;

	m_pOutput->OutputComplete("loaded.");
	m_pOutput->OutputNewline();
	m_pOutput->OutputNewline();
}

/*
 *   Generate and load CHARGE table.
 *
 *   PARAMETERS:
 *           none.
 *
 *   RETURNS:
 *           none.
 */
void CGenerateAndLoad::GenerateAndLoadCharge() {
	CChargeTable Table(m_dfm.ChargeDataFile());
	CBaseLoader<CHARGE_ROW> *pLoad = m_pLoaderFactory->CreateChargeLoader();

	m_pOutput->OutputStart("Generating CHARGE table...");

	pLoad->Init();

	while (Table.GenerateNextRecord()) {
		pLoad->WriteNextRecord(Table.GetRow());
	}
	pLoad->FinishLoad(); // commit
	delete pLoad;

	m_pOutput->OutputComplete("loaded.");
	m_pOutput->OutputNewline();
	m_pOutput->OutputNewline();
}

/*
 *   Generate and load COMMISSION_RATE table.
 *
 *   PARAMETERS:
 *           none.
 *
 *   RETURNS:
 *           none.
 */
void CGenerateAndLoad::GenerateAndLoadCommissionRate() {
	CCommissionRateTable Table(m_dfm.CommissionRateDataFile());
	CBaseLoader<COMMISSION_RATE_ROW> *pLoad = m_pLoaderFactory->CreateCommissionRateLoader();

	m_pOutput->OutputStart("Generating COMMISSION_RATE table...");

	pLoad->Init();

	while (Table.GenerateNextRecord()) {
		pLoad->WriteNextRecord(Table.GetRow());
	}
	pLoad->FinishLoad(); // commit
	delete pLoad;

	m_pOutput->OutputComplete("loaded.");
	m_pOutput->OutputNewline();
	m_pOutput->OutputNewline();
}

/*
 *   Generate and load COMPANY table.
 *
 *   PARAMETERS:
 *           none.
 *
 *   RETURNS:
 *           none.
 */
void CGenerateAndLoad::GenerateAndLoadCompany() {
	bool bRet;
	CCompanyTable Table(m_dfm, m_iCustomerCount, m_iStartFromCustomer);
	CBaseLoader<COMPANY_ROW> *pLoad = m_pLoaderFactory->CreateCompanyLoader();

	m_pOutput->OutputStart("Generating COMPANY table...");

	pLoad->Init();

	do {
		bRet = Table.GenerateNextRecord();

		pLoad->WriteNextRecord(Table.GetRow());

	} while (bRet);

	pLoad->FinishLoad(); // commit

	delete pLoad;

	m_pOutput->OutputComplete("loaded.");
	m_pOutput->OutputNewline();
	m_pOutput->OutputNewline();
}

/*
 *   Generate and load COMPANY_COMPETITOR table.
 *
 *   PARAMETERS:
 *           none.
 *
 *   RETURNS:
 *           none.
 */
void CGenerateAndLoad::GenerateAndLoadCompanyCompetitor() {
	bool bRet;
	CCompanyCompetitorTable Table(m_dfm, m_iCustomerCount, m_iStartFromCustomer);
	CBaseLoader<COMPANY_COMPETITOR_ROW> *pLoad = m_pLoaderFactory->CreateCompanyCompetitorLoader();

	m_pOutput->OutputStart("Generating COMPANY_COMPETITOR table...");

	pLoad->Init();

	do {
		bRet = Table.GenerateNextRecord();

		pLoad->WriteNextRecord(Table.GetRow());

	} while (bRet);

	pLoad->FinishLoad(); // commit

	delete pLoad;

	m_pOutput->OutputComplete("loaded.");
	m_pOutput->OutputNewline();
	m_pOutput->OutputNewline();
}

/*
 *   Generate and load CUSTOMER table.
 *
 *   PARAMETERS:
 *           none.
 *
 *   RETURNS:
 *           none.
 */
void CGenerateAndLoad::GenerateAndLoadCustomer() {
	bool bRet;
	CCustomerTable Table(m_dfm, m_iCustomerCount, m_iStartFromCustomer);
	CBaseLoader<CUSTOMER_ROW> *pLoad = m_pLoaderFactory->CreateCustomerLoader();
	INT64 iCnt = 0;

	m_pOutput->OutputStart("Generating CUSTOMER table...");

	pLoad->Init();

	do {
		bRet = Table.GenerateNextRecord();

		pLoad->WriteNextRecord(Table.GetRow());

		if (++iCnt % 20000 == 0) {
			m_pOutput->OutputProgress("."); // output progress
		}

	} while (bRet);

	pLoad->FinishLoad(); // commit

	delete pLoad;

	m_pOutput->OutputComplete("loaded.");
	m_pOutput->OutputNewline();
	m_pOutput->OutputNewline();
}

/*
 *   Generate and load CUSTOMER_ACCOUNT, ACCOUNT_PERMISSION table.
 *
 *   PARAMETERS:
 *           none.
 *
 *   RETURNS:
 *           none.
 */
void CGenerateAndLoad::GenerateAndLoadCustomerAccountAndAccountPermission() {
	bool bRet;
	CCustomerAccountsAndPermissionsTable Table(m_dfm, m_iLoadUnitSize, m_iCustomerCount, m_iStartFromCustomer);
	CBaseLoader<CUSTOMER_ACCOUNT_ROW> *pCALoad = m_pLoaderFactory->CreateCustomerAccountLoader();
	CBaseLoader<ACCOUNT_PERMISSION_ROW> *pAPLoad = m_pLoaderFactory->CreateAccountPermissionLoader();
	INT64 iCnt = 0;
	UINT i;

	m_pOutput->OutputStart("Generating CUSTOMER_ACCOUNT table and ACCOUNT_PERMISSION table...");

	pCALoad->Init();
	pAPLoad->Init();

	do {
		bRet = Table.GenerateNextRecord();

		pCALoad->WriteNextRecord(Table.GetCARow());

		for (i = 0; i < Table.GetCAPermsCount(); ++i) {

			pAPLoad->WriteNextRecord(Table.GetAPRow(i));
		}

		if (++iCnt % 10000 == 0) {
			m_pOutput->OutputProgress("."); // output progress
		}

		// Commit rows every so often
		if (iCnt % 10000 == 0) {
			pCALoad->Commit();
			pAPLoad->Commit();
		}

	} while (bRet);
	pCALoad->FinishLoad(); // commit
	pAPLoad->FinishLoad(); // commit
	delete pCALoad;
	delete pAPLoad;

	m_pOutput->OutputComplete("loaded.");
	m_pOutput->OutputNewline();
	m_pOutput->OutputNewline();
}

/*
 *   Generate and load CUSTOMER_TAXRATE table.
 *
 *   PARAMETERS:
 *           none.
 *
 *   RETURNS:
 *           none.
 */
void CGenerateAndLoad::GenerateAndLoadCustomerTaxrate() {
	bool bRet;
	CCustomerTaxRateTable Table(m_dfm, m_iCustomerCount, m_iStartFromCustomer);
	CBaseLoader<CUSTOMER_TAXRATE_ROW> *pLoad = m_pLoaderFactory->CreateCustomerTaxrateLoader();
	INT64 iCnt = 0;
	UINT i;

	m_pOutput->OutputStart("Generating CUSTOMER_TAX_RATE table...");

	pLoad->Init();

	do {
		bRet = Table.GenerateNextRecord();

		for (i = 0; i < Table.GetTaxRatesCount(); ++i) {
			pLoad->WriteNextRecord(Table.GetRowByIndex(i));

			if (++iCnt % 20000 == 0) {
				m_pOutput->OutputProgress("."); // output progress
			}
		}
	} while (bRet);

	pLoad->FinishLoad(); // commit

	delete pLoad;

	m_pOutput->OutputComplete("loaded.");
	m_pOutput->OutputNewline();
	m_pOutput->OutputNewline();
}

/*
 *   Generate and load DAILY_MARKET table.
 *
 *   PARAMETERS:
 *           none.
 *
 *   RETURNS:
 *           none.
 */
void CGenerateAndLoad::GenerateAndLoadDailyMarket() {
	bool bRet;
	CDailyMarketTable Table(m_dfm, m_iCustomerCount, m_iStartFromCustomer);
	CBaseLoader<DAILY_MARKET_ROW> *pLoad = m_pLoaderFactory->CreateDailyMarketLoader();
	INT64 iCnt = 0;

	m_pOutput->OutputStart("Generating DAILY_MARKET table...");

	pLoad->Init();

	do {
		bRet = Table.GenerateNextRecord();

		pLoad->WriteNextRecord(Table.GetRow());

		if (++iCnt % 20000 == 0) {
			m_pOutput->OutputProgress("."); // output progress
		}

		if (iCnt % 6525 == 0) {
			pLoad->Commit(); // commit rows every 5 securities (1305
			                 // rows/security)
		}

	} while (bRet);

	pLoad->FinishLoad(); // commit

	delete pLoad;

	m_pOutput->OutputComplete("loaded.");
	m_pOutput->OutputNewline();
	m_pOutput->OutputNewline();
}

/*
 *   Generate and load EXCHANGE table.
 *
 *   PARAMETERS:
 *           none.
 *
 *   RETURNS:
 *           none.
 */
void CGenerateAndLoad::GenerateAndLoadExchange() {
	CExchangeTable Table(m_dfm.ExchangeDataFile(), m_iCustomerCount);
	CBaseLoader<EXCHANGE_ROW> *pLoad = m_pLoaderFactory->CreateExchangeLoader();

	m_pOutput->OutputStart("Generating EXCHANGE table...");

	pLoad->Init();

	while (Table.GenerateNextRecord()) {
		pLoad->WriteNextRecord(Table.GetRow());
	}
	pLoad->FinishLoad(); // commit
	delete pLoad;

	m_pOutput->OutputComplete("loaded.");
	m_pOutput->OutputNewline();
	m_pOutput->OutputNewline();
}

/*
 *   Generate and load FINANCIAL table.
 *
 *   PARAMETERS:
 *           none.
 *
 *   RETURNS:
 *           none.
 */
void CGenerateAndLoad::GenerateAndLoadFinancial() {
	bool bRet;
	CFinancialTable Table(m_dfm, m_iCustomerCount, m_iStartFromCustomer);
	CBaseLoader<FINANCIAL_ROW> *pLoad = m_pLoaderFactory->CreateFinancialLoader();
	INT64 iCnt = 0;

	m_pOutput->OutputStart("Generating FINANCIAL table...");

	pLoad->Init();

	do {
		bRet = Table.GenerateNextRecord();

		pLoad->WriteNextRecord(Table.GetRow());

		if (++iCnt % 20000 == 0) {
			m_pOutput->OutputProgress("."); // output progress
		}

		if (iCnt % 5000 == 0) {
			pLoad->Commit(); // commit rows every 250 companies (20 rows/company)
		}

	} while (bRet);

	pLoad->FinishLoad(); // commit

	delete pLoad;

	m_pOutput->OutputComplete("loaded.");
	m_pOutput->OutputNewline();
	m_pOutput->OutputNewline();
}

/*
 *   Generate and load HOLDING, HOLDING_HISTORY, TRADE, TRADE_HISTORY,
 * SETTLEMENT, CASH_TRANSACTION, BROKER table.
 *
 *   PARAMETERS:
 *           none.
 *
 *   RETURNS:
 *           none.
 */
void CGenerateAndLoad::GenerateAndLoadHoldingAndTrade() {
	bool bRet;
	CTradeGen *pTradeGen;

	CBaseLoader<HOLDING_ROW> *pHoldingsLoad;
	CBaseLoader<HOLDING_HISTORY_ROW> *pHoldingHistoryLoad;
	CBaseLoader<HOLDING_SUMMARY_ROW> *pHoldingSummaryLoad;
	CBaseLoader<TRADE_ROW> *pTradesLoad;
	// Not loading TRADE_REQUEST table (it'll be quickly populated during a run)
	// CBaseLoader<TRADE_REQUEST_ROW>*       pRequestsLoad;
	CBaseLoader<SETTLEMENT_ROW> *pSettlementLoad;
	CBaseLoader<TRADE_HISTORY_ROW> *pHistoryLoad;
	CBaseLoader<CASH_TRANSACTION_ROW> *pCashLoad;
	CBaseLoader<BROKER_ROW> *pBrokerLoad;
	int iCnt = 0;
	int i;
	int iCurrentLoadUnit = 1;
	char szCurrentLoadUnit[11];

	pHoldingsLoad = m_pLoaderFactory->CreateHoldingLoader();
	pHoldingHistoryLoad = m_pLoaderFactory->CreateHoldingHistoryLoader();
	pHoldingSummaryLoad = m_pLoaderFactory->CreateHoldingSummaryLoader();
	pTradesLoad = m_pLoaderFactory->CreateTradeLoader();
	// Not loading TRADE_REQUEST table (it'll be quickly populated during a run)
	// pRequestsLoad = m_pLoaderFactory->CreateTradeRequestLoader();
	pSettlementLoad = m_pLoaderFactory->CreateSettlementLoader();
	pHistoryLoad = m_pLoaderFactory->CreateTradeHistoryLoader();
	pCashLoad = m_pLoaderFactory->CreateCashTransactionLoader();
	pBrokerLoad = m_pLoaderFactory->CreateBrokerLoader();

	m_pOutput->OutputStart("Generating TRADE, SETTLEMENT, TRADE HISTORY, CASH TRANSACTION, "
	                       "HOLDING_HISTORY, HOLDING_SUMMARY, HOLDING, and BROKER tables...");

	pTradeGen = new CTradeGen(m_dfm, m_iCustomerCount, m_iStartFromCustomer, m_iTotalCustomers, m_iLoadUnitSize,
	                          m_iScaleFactor, m_iHoursOfInitialTrades, m_bCacheEnabled);

	// Generate and load one load unit at a time.
	//
	do {
		pTradesLoad->Init();
		pSettlementLoad->Init();
		pHistoryLoad->Init();
		pCashLoad->Init();
		pBrokerLoad->Init();
		pHoldingHistoryLoad->Init();
		pHoldingsLoad->Init();
		pHoldingSummaryLoad->Init();
		// Not loading TRADE_REQUEST table
		// pRequestsLoad->Init();

		// Generate and load trades for this load unit.
		//
		do {
			bRet = pTradeGen->GenerateNextTrade();

			pTradesLoad->WriteNextRecord(pTradeGen->GetTradeRow());

			for (i = 0; i < pTradeGen->GetTradeHistoryRowCount(); ++i) {
				pHistoryLoad->WriteNextRecord(pTradeGen->GetTradeHistoryRow(i));
			}

			if (pTradeGen->GetSettlementRowCount()) {
				pSettlementLoad->WriteNextRecord(pTradeGen->GetSettlementRow());
			}

			if (pTradeGen->GetCashTransactionRowCount()) {
				pCashLoad->WriteNextRecord(pTradeGen->GetCashTransactionRow());
			}

			for (i = 0; i < pTradeGen->GetHoldingHistoryRowCount(); ++i) {
				pHoldingHistoryLoad->WriteNextRecord(pTradeGen->GetHoldingHistoryRow(i));
			}

			/*if ((pTradeGen->GetTradeRow())->m_iTradeStatus == eCompleted) //
			Not loading TRADE_REQUEST table
			{
			pRequestsLoad->WriteNextRecord(pTradeGen->GetTradeRequestRow());
			}*/

			if (++iCnt % 10000 == 0) {
				m_pOutput->OutputProgress("."); // output progress
			}

			// Commit rows every so often
			if (iCnt % 10000 == 0) {
				pTradesLoad->Commit();     // commit
				pSettlementLoad->Commit(); // commit
				pHistoryLoad->Commit();    // commit
				pCashLoad->Commit();
				pHoldingHistoryLoad->Commit(); // commit
				                               // Not loading TRADE_REQUEST table
				                               // pRequestsLoad->Commit();      //commit
			}

		} while (bRet);

		// After trades generate and load BROKER table.
		//
		do {
			bRet = pTradeGen->GenerateNextBrokerRecord();

			pBrokerLoad->WriteNextRecord(pTradeGen->GetBrokerRow());

			// Commit rows every so often
			if (++iCnt % 10000 == 0) {
				pBrokerLoad->Commit(); // commit
			}
		} while (bRet);

		m_pOutput->OutputProgress("t");

		//  Now generate and load HOLDING_SUMMARY rows for this load unit.
		//
		do {
			bRet = pTradeGen->GenerateNextHoldingSummaryRow();

			pHoldingSummaryLoad->WriteNextRecord(pTradeGen->GetHoldingSummaryRow());

			if (++iCnt % 10000 == 0) {
				m_pOutput->OutputProgress("."); // output progress
			}

			// Commit rows every so often
			if (iCnt % 10000 == 0) {
				pHoldingSummaryLoad->Commit(); // commit
			}
		} while (bRet);

		//  Now generate and load holdings for this load unit.
		//
		do {
			bRet = pTradeGen->GenerateNextHolding();

			pHoldingsLoad->WriteNextRecord(pTradeGen->GetHoldingRow());

			if (++iCnt % 10000 == 0) {
				m_pOutput->OutputProgress("."); // output progress
			}

			// Commit rows every so often
			if (iCnt % 10000 == 0) {
				pHoldingsLoad->Commit(); // commit
			}
		} while (bRet);

		pTradesLoad->FinishLoad();         // commit
		pSettlementLoad->FinishLoad();     // commit
		pHistoryLoad->FinishLoad();        // commit
		pCashLoad->FinishLoad();           // commit
		pBrokerLoad->FinishLoad();         // commit
		pHoldingHistoryLoad->FinishLoad(); // commit
		pHoldingsLoad->FinishLoad();       // commit
		pHoldingSummaryLoad->FinishLoad(); // commit
		// Not loading TRADE_REQUEST table
		// pRequestsLoad->FinishLoad();      //commit

		// Output unit number for information
		snprintf(szCurrentLoadUnit, sizeof(szCurrentLoadUnit), "%d", iCurrentLoadUnit++);

		m_pOutput->OutputProgress(szCurrentLoadUnit);

	} while (pTradeGen->InitNextLoadUnit());

	delete pHoldingsLoad;
	delete pHoldingHistoryLoad;
	delete pHoldingSummaryLoad;
	delete pTradesLoad;
	// Not loading TRADE_REQUEST table
	// delete pRequestsLoad;
	delete pSettlementLoad;
	delete pHistoryLoad;
	delete pCashLoad;
	delete pBrokerLoad;

	delete pTradeGen;

	m_pOutput->OutputComplete(".loaded.");
	m_pOutput->OutputNewline();
	m_pOutput->OutputNewline();
}

/*
 *   Generate and load INDUSTRY table.
 *
 *   PARAMETERS:
 *           none.
 *
 *   RETURNS:
 *           none.
 */
void CGenerateAndLoad::GenerateAndLoadIndustry() {
	CIndustryTable Table(m_dfm.IndustryDataFile());
	CBaseLoader<INDUSTRY_ROW> *pLoad = m_pLoaderFactory->CreateIndustryLoader();

	m_pOutput->OutputStart("Generating INDUSTRY table...");

	pLoad->Init();

	while (Table.GenerateNextRecord()) {
		pLoad->WriteNextRecord(Table.GetRow());
	}
	pLoad->FinishLoad(); // commit
	delete pLoad;

	m_pOutput->OutputComplete("loaded.");
	m_pOutput->OutputNewline();
	m_pOutput->OutputNewline();
}

/*
 *   Generate and load LAST_TRADE table.
 *
 *   PARAMETERS:
 *           none.
 *
 *   RETURNS:
 *           none.
 */
void CGenerateAndLoad::GenerateAndLoadLastTrade() {
	bool bRet;
	CLastTradeTable Table(m_dfm, m_iCustomerCount, m_iStartFromCustomer, m_iHoursOfInitialTrades);
	CBaseLoader<LAST_TRADE_ROW> *pLoad = m_pLoaderFactory->CreateLastTradeLoader();

	m_pOutput->OutputStart("Generating LAST TRADE table...");

	pLoad->Init();

	do {
		bRet = Table.GenerateNextRecord();

		pLoad->WriteNextRecord(Table.GetRow());

	} while (bRet);

	pLoad->FinishLoad(); // commit

	delete pLoad;

	m_pOutput->OutputComplete("loaded.");
	m_pOutput->OutputNewline();
	m_pOutput->OutputNewline();
}

/*
 *   Generate and load NEWS_ITEM, NEWS_XREF table.
 *
 *   PARAMETERS:
 *           none.
 *
 *   RETURNS:
 *           none.
 */
void CGenerateAndLoad::GenerateAndLoadNewsItemAndNewsXRef() {
	bool bRet;
	// allocated on the heap because contains 100KB item
	CNewsItemAndXRefTable *pTable =
	    new CNewsItemAndXRefTable(m_dfm, m_iCustomerCount, m_iStartFromCustomer, m_iHoursOfInitialTrades);
	CBaseLoader<NEWS_ITEM_ROW> *pNewsItemLoad = m_pLoaderFactory->CreateNewsItemLoader();
	CBaseLoader<NEWS_XREF_ROW> *pNewsXRefLoad = m_pLoaderFactory->CreateNewsXRefLoader();
	INT64 iCnt = 0;

	m_pOutput->OutputStart("Generating NEWS_ITEM and NEWS_XREF table...");

	pNewsItemLoad->Init();
	pNewsXRefLoad->Init();

	do {
		bRet = pTable->GenerateNextRecord();

		pNewsItemLoad->WriteNextRecord(pTable->GetNewsItemRow());

		pNewsXRefLoad->WriteNextRecord(pTable->GetNewsXRefRow());

		if (++iCnt % 1000 == 0) // output progress every 1000 rows because each
		                        // row generation takes a lot of time
		{
			m_pOutput->OutputProgress("."); // output progress

			pNewsItemLoad->Commit();
			pNewsXRefLoad->Commit();
		}

	} while (bRet);
	pNewsItemLoad->FinishLoad(); // commit
	pNewsXRefLoad->FinishLoad(); // commit
	delete pNewsItemLoad;
	delete pNewsXRefLoad;
	delete pTable;

	m_pOutput->OutputComplete("loaded.");
	m_pOutput->OutputNewline();
	m_pOutput->OutputNewline();
}

/*
 *   Generate and load SECTOR table.
 *
 *   PARAMETERS:
 *           none.
 *
 *   RETURNS:
 *           none.
 */
void CGenerateAndLoad::GenerateAndLoadSector() {
	CSectorTable Table(m_dfm.SectorDataFile());
	CBaseLoader<SECTOR_ROW> *pLoad = m_pLoaderFactory->CreateSectorLoader();

	m_pOutput->OutputStart("Generating SECTOR table...");

	pLoad->Init();

	while (Table.GenerateNextRecord()) {
		pLoad->WriteNextRecord(Table.GetRow());
	}
	pLoad->FinishLoad(); // commit
	delete pLoad;

	m_pOutput->OutputComplete("loaded.");
	m_pOutput->OutputNewline();
	m_pOutput->OutputNewline();
}

/*
 *   Generate and load SECURITY table.
 *
 *   PARAMETERS:
 *           none.
 *
 *   RETURNS:
 *           none.
 */
void CGenerateAndLoad::GenerateAndLoadSecurity() {
	bool bRet;
	CSecurityTable Table(m_dfm, m_iCustomerCount, m_iStartFromCustomer);
	CBaseLoader<SECURITY_ROW> *pLoad = m_pLoaderFactory->CreateSecurityLoader();
	INT64 iCnt = 0;

	m_pOutput->OutputStart("Generating SECURITY table...");

	pLoad->Init();

	do {
		bRet = Table.GenerateNextRecord();

		pLoad->WriteNextRecord(Table.GetRow());

		if (++iCnt % 20000 == 0) {
			m_pOutput->OutputProgress("."); // output progress
		}

	} while (bRet);

	pLoad->FinishLoad(); // commit

	delete pLoad;

	m_pOutput->OutputComplete("loaded.");
	m_pOutput->OutputNewline();
	m_pOutput->OutputNewline();
}

/*
 *   Generate and load STATUS_TYPE table.
 *
 *   PARAMETERS:
 *           none.
 *
 *   RETURNS:
 *           none.
 */
void CGenerateAndLoad::GenerateAndLoadStatusType() {
	CStatusTypeTable Table(m_dfm.StatusTypeDataFile());
	CBaseLoader<STATUS_TYPE_ROW> *pLoad = m_pLoaderFactory->CreateStatusTypeLoader();

	m_pOutput->OutputStart("Generating STATUS_TYPE table...");

	pLoad->Init();

	while (Table.GenerateNextRecord()) {
		pLoad->WriteNextRecord(Table.GetRow());
	}
	pLoad->FinishLoad(); // commit
	delete pLoad;

	m_pOutput->OutputComplete("loaded.");
	m_pOutput->OutputNewline();
	m_pOutput->OutputNewline();
}

/*
 *   Generate and load TAXRATE table.
 *
 *   PARAMETERS:
 *           none.
 *
 *   RETURNS:
 *           none.
 */
void CGenerateAndLoad::GenerateAndLoadTaxrate() {
	// TaxRateFile df(m_dfm.TaxRatesCountryDataFile(),
	// m_dfm.TaxRatesDivisionDataFile()); TaxRateTable_t Table(df);
	TaxRateTable Table(m_dfm.TaxRateFile());
	CBaseLoader<TAX_RATE_ROW> *pLoad = m_pLoaderFactory->CreateTaxRateLoader();

	m_pOutput->OutputStart("Generating TAXRATE table...");

	pLoad->Init();

	while (Table.GenerateNextRecord()) {
		pLoad->WriteNextRecord(Table.GetRow());
	}
	pLoad->FinishLoad(); // commit
	delete pLoad;

	m_pOutput->OutputComplete("loaded.");
	m_pOutput->OutputNewline();
	m_pOutput->OutputNewline();
}

/*
 *   Generate and load TRADE_TYPE table.
 *
 *   PARAMETERS:
 *           none.
 *
 *   RETURNS:
 *           none.
 */
void CGenerateAndLoad::GenerateAndLoadTradeType() {
	CTradeTypeTable Table(m_dfm.TradeTypeDataFile());
	CBaseLoader<TRADE_TYPE_ROW> *pLoad = m_pLoaderFactory->CreateTradeTypeLoader();

	m_pOutput->OutputStart("Generating TRADE_TYPE table...");

	pLoad->Init();

	while (Table.GenerateNextRecord()) {
		pLoad->WriteNextRecord(Table.GetRow());
	}
	pLoad->FinishLoad(); // commit
	delete pLoad;

	m_pOutput->OutputComplete("loaded.");
	m_pOutput->OutputNewline();
	m_pOutput->OutputNewline();
}

/*
 *   Generate and load WATCH_LIST, WATCH_ITEM table.
 *
 *   PARAMETERS:
 *           none.
 *
 *   RETURNS:
 *           none.
 */
void CGenerateAndLoad::GenerateAndLoadWatchListAndWatchItem() {
	bool bRet;
	CWatchListsAndItemsTable Table(m_dfm, m_iCustomerCount, m_iStartFromCustomer);
	CBaseLoader<WATCH_LIST_ROW> *pWatchListsLoad = m_pLoaderFactory->CreateWatchListLoader();
	CBaseLoader<WATCH_ITEM_ROW> *pWatchItemsLoad = m_pLoaderFactory->CreateWatchItemLoader();
	INT64 iCnt = 0;
	UINT i;

	m_pOutput->OutputStart("Generating WATCH_LIST table and WATCH_ITEM table...");

	pWatchListsLoad->Init();
	pWatchItemsLoad->Init();

	do {
		bRet = Table.GenerateNextRecord();

		pWatchListsLoad->WriteNextRecord(Table.GetWLRow());

		for (i = 0; i < Table.GetWICount(); ++i) {
			pWatchItemsLoad->WriteNextRecord(Table.GetWIRow(i));

			if (++iCnt % 20000 == 0) {
				m_pOutput->OutputProgress("."); // output progress

				pWatchListsLoad->Commit(); // commit
				pWatchItemsLoad->Commit(); // commit
			}
		}
	} while (bRet);

	pWatchListsLoad->FinishLoad(); // commit
	pWatchItemsLoad->FinishLoad(); // commit

	delete pWatchListsLoad;
	delete pWatchItemsLoad;

	m_pOutput->OutputComplete("loaded.");
	m_pOutput->OutputNewline();
	m_pOutput->OutputNewline();
}

/*
 *   Generate and load ZIP_CODE table.
 *
 *   PARAMETERS:
 *           none.
 *
 *   RETURNS:
 *           none.
 */
void CGenerateAndLoad::GenerateAndLoadZipCode() {
	CZipCodeTable Table(m_dfm.ZipCodeDataFile());
	CBaseLoader<ZIP_CODE_ROW> *pLoad = m_pLoaderFactory->CreateZipCodeLoader();

	m_pOutput->OutputStart("Generating ZIP_CODE table...");

	pLoad->Init();

	while (Table.GenerateNextRecord()) {
		pLoad->WriteNextRecord(Table.GetRow());
	}
	pLoad->FinishLoad(); // commit
	delete pLoad;

	m_pOutput->OutputComplete("loaded.");
	m_pOutput->OutputNewline();
	m_pOutput->OutputNewline();
}

/*
 *   Generate and load All tables that are constant in size.
 *
 *   Spec definition: Fixed tables.
 *
 *   PARAMETERS:
 *           none.
 *
 *   RETURNS:
 *           none.
 */
void CGenerateAndLoad::GenerateAndLoadFixedTables() {
	GenerateAndLoadCharge();
	GenerateAndLoadCommissionRate();
	GenerateAndLoadExchange();
	GenerateAndLoadIndustry();
	GenerateAndLoadSector();
	GenerateAndLoadStatusType();
	GenerateAndLoadTaxrate();
	GenerateAndLoadTradeType();
	GenerateAndLoadZipCode();
}

/*
 *   Generate and load All tables (except BROKER) that scale with the size of
 *   the CUSTOMER table, but do not grow in runtime.
 *
 *   Spec definition: Scaling tables.
 *
 *   PARAMETERS:
 *           none.
 *
 *   RETURNS:
 *           none.
 */
void CGenerateAndLoad::GenerateAndLoadScalingTables() {
	// Customer-related tables
	//
	GenerateAndLoadAddress();
	GenerateAndLoadCustomer();
	GenerateAndLoadCustomerAccountAndAccountPermission();
	GenerateAndLoadCustomerTaxrate();
	GenerateAndLoadWatchListAndWatchItem();

	// Now security/company related tables
	//
	GenerateAndLoadCompany();
	GenerateAndLoadCompanyCompetitor();
	GenerateAndLoadDailyMarket();
	GenerateAndLoadFinancial();
	GenerateAndLoadLastTrade();
	GenerateAndLoadNewsItemAndNewsXRef();
	GenerateAndLoadSecurity();
}

/*
 *   Generate and load All trade related tables and BROKER (included here to
 *   facilitate generation of a consistent database).
 *
 *   Spec definition: Growing tables.
 *
 *   PARAMETERS:
 *           none.
 *
 *   RETURNS:
 *           none.
 */
void CGenerateAndLoad::GenerateAndLoadGrowingTables() {
	GenerateAndLoadHoldingAndTrade();
}
