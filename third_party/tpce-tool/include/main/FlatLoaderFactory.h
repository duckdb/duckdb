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
 *   Flat file loader class factory.
 *   This class instantiates particular table loader classes.
 */
#ifndef FLAT_LOADER_FACTORY_H
#define FLAT_LOADER_FACTORY_H

#include <cstring>

#include "FlatFileLoad_stdafx.h"

namespace TPCE {

class CFlatLoaderFactory : public CBaseLoaderFactory {
	char m_szOutDir[iMaxPath];
	char m_szFullFileName[iMaxPath];
	FlatFileOutputModes m_eOutputMode; // overwrite/append

	void SetFileName(const char *szFileName) {
		snprintf(m_szFullFileName, sizeof(m_szFullFileName), "%s%s", m_szOutDir, szFileName);
	}

public:
	// Constructor
	CFlatLoaderFactory(char *szOutDir, FlatFileOutputModes eOutputMode) : m_eOutputMode(eOutputMode) {
		assert(szOutDir);

		strncpy(m_szOutDir, szOutDir, sizeof(m_szOutDir));
	};

	// Functions to create loader classes for individual tables.

	virtual CBaseLoader<ACCOUNT_PERMISSION_ROW> *CreateAccountPermissionLoader() {
		SetFileName("AccountPermission.txt");

		return new CFlatAccountPermissionLoad(m_szFullFileName, m_eOutputMode);
	};
	virtual CBaseLoader<ADDRESS_ROW> *CreateAddressLoader() {
		SetFileName("Address.txt");

		return new CFlatAddressLoad(m_szFullFileName, m_eOutputMode);
	};
	virtual CBaseLoader<BROKER_ROW> *CreateBrokerLoader() {
		SetFileName("Broker.txt");

		return new CFlatBrokerLoad(m_szFullFileName, m_eOutputMode);
	};
	virtual CBaseLoader<CASH_TRANSACTION_ROW> *CreateCashTransactionLoader() {
		SetFileName("CashTransaction.txt");

		return new CFlatCashTransactionLoad(m_szFullFileName, m_eOutputMode);
	};
	virtual CBaseLoader<CHARGE_ROW> *CreateChargeLoader() {
		SetFileName("Charge.txt");

		return new CFlatChargeLoad(m_szFullFileName, m_eOutputMode);
	};

	virtual CBaseLoader<COMMISSION_RATE_ROW> *CreateCommissionRateLoader() {
		SetFileName("CommissionRate.txt");

		return new CFlatCommissionRateLoad(m_szFullFileName, m_eOutputMode);
	};

	virtual CBaseLoader<COMPANY_COMPETITOR_ROW> *CreateCompanyCompetitorLoader() {
		SetFileName("CompanyCompetitor.txt");

		return new CFlatCompanyCompetitorLoad(m_szFullFileName, m_eOutputMode);
	};

	virtual CBaseLoader<COMPANY_ROW> *CreateCompanyLoader() {
		SetFileName("Company.txt");

		return new CFlatCompanyLoad(m_szFullFileName, m_eOutputMode);
	};

	virtual CBaseLoader<CUSTOMER_ACCOUNT_ROW> *CreateCustomerAccountLoader() {
		SetFileName("CustomerAccount.txt");

		return new CFlatCustomerAccountLoad(m_szFullFileName, m_eOutputMode);
	};

	virtual CBaseLoader<CUSTOMER_ROW> *CreateCustomerLoader() {
		SetFileName("Customer.txt");

		return new CFlatCustomerLoad(m_szFullFileName, m_eOutputMode);
	};
	virtual CBaseLoader<CUSTOMER_TAXRATE_ROW> *CreateCustomerTaxrateLoader() {
		SetFileName("CustomerTaxrate.txt");

		return new CFlatCustomerTaxrateLoad(m_szFullFileName, m_eOutputMode);
	};
	virtual CBaseLoader<DAILY_MARKET_ROW> *CreateDailyMarketLoader() {
		SetFileName("DailyMarket.txt");

		return new CFlatDailyMarketLoad(m_szFullFileName, m_eOutputMode);
	};
	virtual CBaseLoader<EXCHANGE_ROW> *CreateExchangeLoader() {
		SetFileName("Exchange.txt");

		return new CFlatExchangeLoad(m_szFullFileName, m_eOutputMode);
	};
	virtual CBaseLoader<FINANCIAL_ROW> *CreateFinancialLoader() {
		SetFileName("Financial.txt");

		return new CFlatFinancialLoad(m_szFullFileName, m_eOutputMode);
	};
	virtual CBaseLoader<HOLDING_ROW> *CreateHoldingLoader() {
		SetFileName("Holding.txt");

		return new CFlatHoldingLoad(m_szFullFileName, m_eOutputMode);
	};
	virtual CBaseLoader<HOLDING_HISTORY_ROW> *CreateHoldingHistoryLoader() {
		SetFileName("HoldingHistory.txt");

		return new CFlatHoldingHistoryLoad(m_szFullFileName, m_eOutputMode);
	};
	virtual CBaseLoader<HOLDING_SUMMARY_ROW> *CreateHoldingSummaryLoader() {
		SetFileName("HoldingSummary.txt");

		return new CFlatHoldingSummaryLoad(m_szFullFileName, m_eOutputMode);
	};
	virtual CBaseLoader<INDUSTRY_ROW> *CreateIndustryLoader() {
		SetFileName("Industry.txt");

		return new CFlatIndustryLoad(m_szFullFileName, m_eOutputMode);
	};
	virtual CBaseLoader<LAST_TRADE_ROW> *CreateLastTradeLoader() {
		SetFileName("LastTrade.txt");

		return new CFlatLastTradeLoad(m_szFullFileName, m_eOutputMode);
	};
	virtual CBaseLoader<NEWS_ITEM_ROW> *CreateNewsItemLoader() {
		SetFileName("NewsItem.txt");

		return new CFlatNewsItemLoad(m_szFullFileName, m_eOutputMode);
	};
	virtual CBaseLoader<NEWS_XREF_ROW> *CreateNewsXRefLoader() {
		SetFileName("NewsXRef.txt");

		return new CFlatNewsXRefLoad(m_szFullFileName, m_eOutputMode);
	};
	virtual CBaseLoader<SECTOR_ROW> *CreateSectorLoader() {
		SetFileName("Sector.txt");

		return new CFlatSectorLoad(m_szFullFileName, m_eOutputMode);
	};
	virtual CBaseLoader<SECURITY_ROW> *CreateSecurityLoader() {
		SetFileName("Security.txt");

		return new CFlatSecurityLoad(m_szFullFileName, m_eOutputMode);
	};
	virtual CBaseLoader<SETTLEMENT_ROW> *CreateSettlementLoader() {
		SetFileName("Settlement.txt");

		return new CFlatSettlementLoad(m_szFullFileName, m_eOutputMode);
	};
	virtual CBaseLoader<STATUS_TYPE_ROW> *CreateStatusTypeLoader() {
		SetFileName("StatusType.txt");

		return new CFlatStatusTypeLoad(m_szFullFileName, m_eOutputMode);
	};
	virtual CBaseLoader<TAX_RATE_ROW> *CreateTaxRateLoader() {
		SetFileName("TaxRate.txt");

		return new CFlatTaxRateLoad(m_szFullFileName, m_eOutputMode);
	};
	virtual CBaseLoader<TRADE_HISTORY_ROW> *CreateTradeHistoryLoader() {
		SetFileName("TradeHistory.txt");

		return new CFlatTradeHistoryLoad(m_szFullFileName, m_eOutputMode);
	};
	virtual CBaseLoader<TRADE_ROW> *CreateTradeLoader() {
		SetFileName("Trade.txt");

		return new CFlatTradeLoad(m_szFullFileName, m_eOutputMode);
	};
	virtual CBaseLoader<TRADE_REQUEST_ROW> *CreateTradeRequestLoader() {
		SetFileName("TradeRequest.txt");

		return new CFlatTradeRequestLoad(m_szFullFileName, m_eOutputMode);
	};
	virtual CBaseLoader<TRADE_TYPE_ROW> *CreateTradeTypeLoader() {
		SetFileName("TradeType.txt");

		return new CFlatTradeTypeLoad(m_szFullFileName, m_eOutputMode);
	};
	virtual CBaseLoader<WATCH_ITEM_ROW> *CreateWatchItemLoader() {
		SetFileName("WatchItem.txt");

		return new CFlatWatchItemLoad(m_szFullFileName, m_eOutputMode);
	};

	virtual CBaseLoader<WATCH_LIST_ROW> *CreateWatchListLoader() {
		SetFileName("WatchList.txt");

		return new CFlatWatchListLoad(m_szFullFileName, m_eOutputMode);
	};

	virtual CBaseLoader<ZIP_CODE_ROW> *CreateZipCodeLoader() {
		SetFileName("ZipCode.txt");

		return new CFlatZipCodeLoad(m_szFullFileName, m_eOutputMode);
	};
};

} // namespace TPCE

#endif // FLAT_LOADER_FACTORY_H
