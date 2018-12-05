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
 *   Null loader class factory.
 *   This class instantiates particular table loader classes.
 */
#ifndef NULL_LOADER_FACTORY_H
#define NULL_LOADER_FACTORY_H

#include "BaseLoader.h"
#include "BaseLoaderFactory.h"
#include "NullLoader.h"
#include "TableRows.h"

namespace TPCE {

class CNullLoaderFactory : public CBaseLoaderFactory {

public:
	// Functions to create loader classes for individual tables.

	virtual CBaseLoader<ACCOUNT_PERMISSION_ROW> *CreateAccountPermissionLoader() {
		return new CNullLoader<ACCOUNT_PERMISSION_ROW>();
	};

	virtual CBaseLoader<ADDRESS_ROW> *CreateAddressLoader() {
		return new CNullLoader<ADDRESS_ROW>();
	};

	virtual CBaseLoader<BROKER_ROW> *CreateBrokerLoader() {
		return new CNullLoader<BROKER_ROW>();
	};

	virtual CBaseLoader<CASH_TRANSACTION_ROW> *CreateCashTransactionLoader() {
		return new CNullLoader<CASH_TRANSACTION_ROW>();
	};

	virtual CBaseLoader<CHARGE_ROW> *CreateChargeLoader() {
		return new CNullLoader<CHARGE_ROW>();
	};

	virtual CBaseLoader<COMMISSION_RATE_ROW> *CreateCommissionRateLoader() {
		return new CNullLoader<COMMISSION_RATE_ROW>();
	};

	virtual CBaseLoader<COMPANY_COMPETITOR_ROW> *CreateCompanyCompetitorLoader() {
		return new CNullLoader<COMPANY_COMPETITOR_ROW>();
	};

	virtual CBaseLoader<COMPANY_ROW> *CreateCompanyLoader() {
		return new CNullLoader<COMPANY_ROW>();
	};

	virtual CBaseLoader<CUSTOMER_ACCOUNT_ROW> *CreateCustomerAccountLoader() {
		return new CNullLoader<CUSTOMER_ACCOUNT_ROW>();
	};

	virtual CBaseLoader<CUSTOMER_ROW> *CreateCustomerLoader() {
		return new CNullLoader<CUSTOMER_ROW>();
	};
	virtual CBaseLoader<CUSTOMER_TAXRATE_ROW> *CreateCustomerTaxrateLoader() {
		return new CNullLoader<CUSTOMER_TAXRATE_ROW>();
	};
	virtual CBaseLoader<DAILY_MARKET_ROW> *CreateDailyMarketLoader() {
		return new CNullLoader<DAILY_MARKET_ROW>();
	};
	virtual CBaseLoader<EXCHANGE_ROW> *CreateExchangeLoader() {
		return new CNullLoader<EXCHANGE_ROW>();
	};
	virtual CBaseLoader<FINANCIAL_ROW> *CreateFinancialLoader() {
		return new CNullLoader<FINANCIAL_ROW>();
	};
	virtual CBaseLoader<HOLDING_ROW> *CreateHoldingLoader() {
		return new CNullLoader<HOLDING_ROW>();
	};
	virtual CBaseLoader<HOLDING_HISTORY_ROW> *CreateHoldingHistoryLoader() {
		return new CNullLoader<HOLDING_HISTORY_ROW>();
	};
	virtual CBaseLoader<HOLDING_SUMMARY_ROW> *CreateHoldingSummaryLoader() {
		return new CNullLoader<HOLDING_SUMMARY_ROW>();
	};
	virtual CBaseLoader<INDUSTRY_ROW> *CreateIndustryLoader() {
		return new CNullLoader<INDUSTRY_ROW>();
	};
	virtual CBaseLoader<LAST_TRADE_ROW> *CreateLastTradeLoader() {
		return new CNullLoader<LAST_TRADE_ROW>();
	};
	virtual CBaseLoader<NEWS_ITEM_ROW> *CreateNewsItemLoader() {
		return new CNullLoader<NEWS_ITEM_ROW>();
	};
	virtual CBaseLoader<NEWS_XREF_ROW> *CreateNewsXRefLoader() {
		return new CNullLoader<NEWS_XREF_ROW>();
	};
	virtual CBaseLoader<SECTOR_ROW> *CreateSectorLoader() {
		return new CNullLoader<SECTOR_ROW>();
	};
	virtual CBaseLoader<SECURITY_ROW> *CreateSecurityLoader() {
		return new CNullLoader<SECURITY_ROW>();
	};
	virtual CBaseLoader<SETTLEMENT_ROW> *CreateSettlementLoader() {
		return new CNullLoader<SETTLEMENT_ROW>();
	};
	virtual CBaseLoader<STATUS_TYPE_ROW> *CreateStatusTypeLoader() {
		return new CNullLoader<STATUS_TYPE_ROW>();
	};
	virtual CBaseLoader<TAX_RATE_ROW> *CreateTaxRateLoader() {
		return new CNullLoader<TAX_RATE_ROW>();
	};
	virtual CBaseLoader<TRADE_HISTORY_ROW> *CreateTradeHistoryLoader() {
		return new CNullLoader<TRADE_HISTORY_ROW>();
	};
	virtual CBaseLoader<TRADE_ROW> *CreateTradeLoader() {
		return new CNullLoader<TRADE_ROW>();
	};
	virtual CBaseLoader<TRADE_REQUEST_ROW> *CreateTradeRequestLoader() {
		return new CNullLoader<TRADE_REQUEST_ROW>();
	};
	virtual CBaseLoader<TRADE_TYPE_ROW> *CreateTradeTypeLoader() {
		return new CNullLoader<TRADE_TYPE_ROW>();
	};
	virtual CBaseLoader<WATCH_ITEM_ROW> *CreateWatchItemLoader() {
		return new CNullLoader<WATCH_ITEM_ROW>();
	};

	virtual CBaseLoader<WATCH_LIST_ROW> *CreateWatchListLoader() {
		return new CNullLoader<WATCH_LIST_ROW>();
	};

	virtual CBaseLoader<ZIP_CODE_ROW> *CreateZipCodeLoader() {
		return new CNullLoader<ZIP_CODE_ROW>();
	};
};

} // namespace TPCE

#endif // NULL_LOADER_FACTORY_H
