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
 *   ODBC loader class factory for MS SQL Server.
 *   This class instantiates particular table loader classes.
 */
#ifndef ODBC_LOADER_FACTORY_H
#define ODBC_LOADER_FACTORY_H

namespace TPCE {

class CODBCLoaderFactory : public CBaseLoaderFactory {
	char m_szServer[iMaxHostname]; // database server name
	char m_szDB[iMaxDBName];       // database name
	char m_szLoaderParams[1024];   // optional parameters

public:
	// Constructor
	CODBCLoaderFactory(char *szServer, char *szDatabase, char *szLoaderParams) {
		assert(szServer);
		assert(szDatabase);

		strncpy(m_szServer, szServer, sizeof(m_szServer));
		strncpy(m_szDB, szDatabase, sizeof(m_szDB));
		strncpy(m_szLoaderParams, szLoaderParams, sizeof(m_szLoaderParams));
	}

	// Functions to create loader classes for individual tables.

	virtual CBaseLoader<ACCOUNT_PERMISSION_ROW> *CreateAccountPermissionLoader() {
		return new CODBCAccountPermissionLoad(m_szServer, m_szDB, m_szLoaderParams);
	};
	virtual CBaseLoader<ADDRESS_ROW> *CreateAddressLoader() {
		return new CODBCAddressLoad(m_szServer, m_szDB, m_szLoaderParams);
	};
	virtual CBaseLoader<BROKER_ROW> *CreateBrokerLoader() {
		return new CODBCBrokerLoad(m_szServer, m_szDB, m_szLoaderParams);
	};
	virtual CBaseLoader<CASH_TRANSACTION_ROW> *CreateCashTransactionLoader() {
		return new CODBCCashTransactionLoad(m_szServer, m_szDB, m_szLoaderParams);
	};
	virtual CBaseLoader<CHARGE_ROW> *CreateChargeLoader() {
		return new CODBCChargeLoad(m_szServer, m_szDB, m_szLoaderParams);
	};
	virtual CBaseLoader<COMMISSION_RATE_ROW> *CreateCommissionRateLoader() {
		return new CODBCCommissionRateLoad(m_szServer, m_szDB, m_szLoaderParams);
	};
	virtual CBaseLoader<COMPANY_COMPETITOR_ROW> *CreateCompanyCompetitorLoader() {
		return new CODBCCompanyCompetitorLoad(m_szServer, m_szDB, m_szLoaderParams);
	};
	virtual CBaseLoader<COMPANY_ROW> *CreateCompanyLoader() {
		return new CODBCCompanyLoad(m_szServer, m_szDB, m_szLoaderParams);
	};
	virtual CBaseLoader<CUSTOMER_ACCOUNT_ROW> *CreateCustomerAccountLoader() {
		return new CODBCCustomerAccountLoad(m_szServer, m_szDB, m_szLoaderParams);
	};
	virtual CBaseLoader<CUSTOMER_ROW> *CreateCustomerLoader() {
		return new CODBCCustomerLoad(m_szServer, m_szDB, m_szLoaderParams);
	};
	virtual CBaseLoader<CUSTOMER_TAXRATE_ROW> *CreateCustomerTaxrateLoader() {
		return new CODBCCustomerTaxRateLoad(m_szServer, m_szDB, m_szLoaderParams);
	};
	virtual CBaseLoader<DAILY_MARKET_ROW> *CreateDailyMarketLoader() {
		return new CODBCDailyMarketLoad(m_szServer, m_szDB, m_szLoaderParams);
	};
	virtual CBaseLoader<EXCHANGE_ROW> *CreateExchangeLoader() {
		return new CODBCExchangeLoad(m_szServer, m_szDB, m_szLoaderParams);
	};
	virtual CBaseLoader<FINANCIAL_ROW> *CreateFinancialLoader() {
		return new CODBCFinancialLoad(m_szServer, m_szDB, m_szLoaderParams);
	};
	virtual CBaseLoader<HOLDING_ROW> *CreateHoldingLoader() {
		return new CODBCHoldingLoad(m_szServer, m_szDB, m_szLoaderParams);
	};
	virtual CBaseLoader<HOLDING_HISTORY_ROW> *CreateHoldingHistoryLoader() {
		return new CODBCHoldingHistoryLoad(m_szServer, m_szDB, m_szLoaderParams);
	};
	virtual CBaseLoader<HOLDING_SUMMARY_ROW> *CreateHoldingSummaryLoader() {
		return new CODBCHoldingSummaryLoad(m_szServer, m_szDB, m_szLoaderParams);
	};
	virtual CBaseLoader<INDUSTRY_ROW> *CreateIndustryLoader() {
		return new CODBCIndustryLoad(m_szServer, m_szDB, m_szLoaderParams);
	};
	virtual CBaseLoader<LAST_TRADE_ROW> *CreateLastTradeLoader() {
		return new CODBCLastTradeLoad(m_szServer, m_szDB, m_szLoaderParams);
	};
	virtual CBaseLoader<NEWS_ITEM_ROW> *CreateNewsItemLoader() {
		return new CODBCNewsItemLoad(m_szServer, m_szDB, m_szLoaderParams);
	};
	virtual CBaseLoader<NEWS_XREF_ROW> *CreateNewsXRefLoader() {
		return new CODBCNewsXRefLoad(m_szServer, m_szDB, m_szLoaderParams);
	};
	virtual CBaseLoader<SECTOR_ROW> *CreateSectorLoader() {
		return new CODBCSectorLoad(m_szServer, m_szDB, m_szLoaderParams);
	};
	virtual CBaseLoader<SECURITY_ROW> *CreateSecurityLoader() {
		return new CODBCSecurityLoad(m_szServer, m_szDB, m_szLoaderParams);
	};
	virtual CBaseLoader<SETTLEMENT_ROW> *CreateSettlementLoader() {
		return new CODBCSettlementLoad(m_szServer, m_szDB, m_szLoaderParams);
	};
	virtual CBaseLoader<STATUS_TYPE_ROW> *CreateStatusTypeLoader() {
		return new CODBCStatusTypeLoad(m_szServer, m_szDB, m_szLoaderParams);
	};
	virtual CBaseLoader<TAX_RATE_ROW> *CreateTaxRateLoader() {
		return new CODBCTaxRateLoad(m_szServer, m_szDB, m_szLoaderParams);
	};
	virtual CBaseLoader<TRADE_HISTORY_ROW> *CreateTradeHistoryLoader() {
		return new CODBCTradeHistoryLoad(m_szServer, m_szDB, m_szLoaderParams);
	};
	virtual CBaseLoader<TRADE_ROW> *CreateTradeLoader() {
		return new CODBCTradeLoad(m_szServer, m_szDB, m_szLoaderParams);
	};
	virtual CBaseLoader<TRADE_REQUEST_ROW> *CreateTradeRequestLoader() {
		return new CODBCTradeRequestLoad(m_szServer, m_szDB, m_szLoaderParams);
	};
	virtual CBaseLoader<TRADE_TYPE_ROW> *CreateTradeTypeLoader() {
		return new CODBCTradeTypeLoad(m_szServer, m_szDB, m_szLoaderParams);
	};
	virtual CBaseLoader<WATCH_ITEM_ROW> *CreateWatchItemLoader() {
		return new CODBCWatchItemLoad(m_szServer, m_szDB, m_szLoaderParams);
	};
	virtual CBaseLoader<WATCH_LIST_ROW> *CreateWatchListLoader() {
		return new CODBCWatchListLoad(m_szServer, m_szDB, m_szLoaderParams);
	};
	virtual CBaseLoader<ZIP_CODE_ROW> *CreateZipCodeLoader() {
		return new CODBCZipCodeLoad(m_szServer, m_szDB, m_szLoaderParams);
	};
};

} // namespace TPCE

#endif // ODBC_LOADER_FACTORY_H
