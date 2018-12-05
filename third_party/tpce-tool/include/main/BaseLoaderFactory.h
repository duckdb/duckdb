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
 *   Base interface for a loader class factory.
 *   This class instantiates particular table loader classs.
 */

#ifndef BASE_LOADER_FACTORY_H
#define BASE_LOADER_FACTORY_H

#include "BaseLoader.h"
#include "TableRows.h"

namespace TPCE {

class CBaseLoaderFactory {

public:
	/*
	 *  Virtual destructor. Provided so that a sponsor-specific
	 *  destructor can be called on destruction from the base-class pointer.
	 *
	 *  PARAMETERS:
	 *           none.
	 *
	 *  RETURNS:
	 *           not applicable.
	 */
	virtual ~CBaseLoaderFactory(){};

	/*
	 *  Create loader class for ACCOUNT_PERMISSION table.
	 *  Should be defined in a subclass according to the subclass load type.
	 *
	 *  PARAMETERS:
	 *           none.
	 *
	 *  RETURNS:
	 *           Reference to the loader class.
	 */
	virtual CBaseLoader<ACCOUNT_PERMISSION_ROW> *CreateAccountPermissionLoader() = 0;

	/*
	 *  Create loader class for ADDRESS table.
	 *  Should be defined in a subclass according to the subclass load type.
	 *
	 *  PARAMETERS:
	 *           none.
	 *
	 *  RETURNS:
	 *           Reference to the loader class.
	 */
	virtual CBaseLoader<ADDRESS_ROW> *CreateAddressLoader() = 0;

	/*
	 *  Create loader class for BROKER table.
	 *  Should be defined in a subclass according to the subclass load type.
	 *
	 *  PARAMETERS:
	 *           none.
	 *
	 *  RETURNS:
	 *           Reference to the loader class.
	 */
	virtual CBaseLoader<BROKER_ROW> *CreateBrokerLoader() = 0;

	/*
	 *  Create loader class for CASH_TRANSACTION table.
	 *  Should be defined in a subclass according to the subclass load type.
	 *
	 *  PARAMETERS:
	 *           none.
	 *
	 *  RETURNS:
	 *           Reference to the loader class.
	 */
	virtual CBaseLoader<CASH_TRANSACTION_ROW> *CreateCashTransactionLoader() = 0;

	/*
	 *  Create loader class for CHARGE table.
	 *  Should be defined in a subclass according to the subclass load type.
	 *
	 *  PARAMETERS:
	 *           none.
	 *
	 *  RETURNS:
	 *           Reference to the loader class.
	 */
	virtual CBaseLoader<CHARGE_ROW> *CreateChargeLoader() = 0;

	/*
	 *  Create loader class for COMMISSION_RATE table.
	 *  Should be defined in a subclass according to the subclass load type.
	 *
	 *  PARAMETERS:
	 *           none.
	 *
	 *  RETURNS:
	 *           Reference to the loader class.
	 */
	virtual CBaseLoader<COMMISSION_RATE_ROW> *CreateCommissionRateLoader() = 0;

	/*
	 *  Create loader class for COMPANY_COMPETITOR table.
	 *  Should be defined in a subclass according to the subclass load type.
	 *
	 *  PARAMETERS:
	 *           none.
	 *
	 *  RETURNS:
	 *           Reference to the loader class.
	 */
	virtual CBaseLoader<COMPANY_COMPETITOR_ROW> *CreateCompanyCompetitorLoader() = 0;

	/*
	 *  Create loader class for COMPANY table.
	 *  Should be defined in a subclass according to the subclass load type.
	 *
	 *  PARAMETERS:
	 *           none.
	 *
	 *  RETURNS:
	 *           Reference to the loader class.
	 */
	virtual CBaseLoader<COMPANY_ROW> *CreateCompanyLoader() = 0;

	/*
	 *  Create loader class for CUSTOMER_ACCOUNT table.
	 *  Should be defined in a subclass according to the subclass load type.
	 *
	 *  PARAMETERS:
	 *           none.
	 *
	 *  RETURNS:
	 *           Reference to the loader class.
	 */
	virtual CBaseLoader<CUSTOMER_ACCOUNT_ROW> *CreateCustomerAccountLoader() = 0;

	/*
	 *  Create loader class for CUSTOMER table.
	 *  Should be defined in a subclass according to the subclass load type.
	 *
	 *  PARAMETERS:
	 *           none.
	 *
	 *  RETURNS:
	 *           Reference to the loader class.
	 */
	virtual CBaseLoader<CUSTOMER_ROW> *CreateCustomerLoader() = 0;

	/*
	 *  Create loader class for CUSTOMER_TAXRATE table.
	 *  Should be defined in a subclass according to the subclass load type.
	 *
	 *  PARAMETERS:
	 *           none.
	 *
	 *  RETURNS:
	 *           Reference to the loader class.
	 */
	virtual CBaseLoader<CUSTOMER_TAXRATE_ROW> *CreateCustomerTaxrateLoader() = 0;

	/*
	 *  Create loader class for DAILY_MARKET table.
	 *  Should be defined in a subclass according to the subclass load type.
	 *
	 *  PARAMETERS:
	 *           none.
	 *
	 *  RETURNS:
	 *           Reference to the loader class.
	 */
	virtual CBaseLoader<DAILY_MARKET_ROW> *CreateDailyMarketLoader() = 0;

	/*
	 *  Create loader class for EXCHANGE table.
	 *  Should be defined in a subclass according to the subclass load type.
	 *
	 *  PARAMETERS:
	 *           none.
	 *
	 *  RETURNS:
	 *           Reference to the loader class.
	 */
	virtual CBaseLoader<EXCHANGE_ROW> *CreateExchangeLoader() = 0;

	/*
	 *  Create loader class for FINANCIAL table.
	 *  Should be defined in a subclass according to the subclass load type.
	 *
	 *  PARAMETERS:
	 *           none.
	 *
	 *  RETURNS:
	 *           Reference to the loader class.
	 */
	virtual CBaseLoader<FINANCIAL_ROW> *CreateFinancialLoader() = 0;

	/*
	 *  Create loader class for HOLDING table.
	 *  Should be defined in a subclass according to the subclass load type.
	 *
	 *  PARAMETERS:
	 *           none.
	 *
	 *  RETURNS:
	 *           Reference to the loader class.
	 */
	virtual CBaseLoader<HOLDING_ROW> *CreateHoldingLoader() = 0;

	/*
	 *  Create loader class for HOLDING_HISTORY table.
	 *  Should be defined in a subclass according to the subclass load type.
	 *
	 *  PARAMETERS:
	 *           none.
	 *
	 *  RETURNS:
	 *           Reference to the loader class.
	 */
	virtual CBaseLoader<HOLDING_HISTORY_ROW> *CreateHoldingHistoryLoader() = 0;

	/*
	 *  Create loader class for HOLDING_SUMMARY table.
	 *  Should be defined in a subclass according to the subclass load type.
	 *
	 *  PARAMETERS:
	 *           none.
	 *
	 *  RETURNS:
	 *           Reference to the loader class.
	 */
	virtual CBaseLoader<HOLDING_SUMMARY_ROW> *CreateHoldingSummaryLoader() = 0;

	/*
	 *  Create loader class for INDUSTRY table.
	 *  Should be defined in a subclass according to the subclass load type.
	 *
	 *  PARAMETERS:
	 *           none.
	 *
	 *  RETURNS:
	 *           Reference to the loader class.
	 */
	virtual CBaseLoader<INDUSTRY_ROW> *CreateIndustryLoader() = 0;

	/*
	 *  Create loader class for LAST_TRADE table.
	 *  Should be defined in a subclass according to the subclass load type.
	 *
	 *  PARAMETERS:
	 *           none.
	 *
	 *  RETURNS:
	 *           Reference to the loader class.
	 */
	virtual CBaseLoader<LAST_TRADE_ROW> *CreateLastTradeLoader() = 0;

	/*
	 *  Create loader class for NEWS_ITEM table.
	 *  Should be defined in a subclass according to the subclass load type.
	 *
	 *  PARAMETERS:
	 *           none.
	 *
	 *  RETURNS:
	 *           Reference to the loader class.
	 */
	virtual CBaseLoader<NEWS_ITEM_ROW> *CreateNewsItemLoader() = 0;

	/*
	 *  Create loader class for NEWS_XREF table.
	 *  Should be defined in a subclass according to the subclass load type.
	 *
	 *  PARAMETERS:
	 *           none.
	 *
	 *  RETURNS:
	 *           Reference to the loader class.
	 */
	virtual CBaseLoader<NEWS_XREF_ROW> *CreateNewsXRefLoader() = 0;

	/*
	 *  Create loader class for SECTOR table.
	 *  Should be defined in a subclass according to the subclass load type.
	 *
	 *  PARAMETERS:
	 *           none.
	 *
	 *  RETURNS:
	 *           Reference to the loader class.
	 */
	virtual CBaseLoader<SECTOR_ROW> *CreateSectorLoader() = 0;

	/*
	 *  Create loader class for SECURITY table.
	 *  Should be defined in a subclass according to the subclass load type.
	 *
	 *  PARAMETERS:
	 *           none.
	 *
	 *  RETURNS:
	 *           Reference to the loader class.
	 */
	virtual CBaseLoader<SECURITY_ROW> *CreateSecurityLoader() = 0;

	/*
	 *  Create loader class for SETTLEMENT table.
	 *  Should be defined in a subclass according to the subclass load type.
	 *
	 *  PARAMETERS:
	 *           none.
	 *
	 *  RETURNS:
	 *           Reference to the loader class.
	 */
	virtual CBaseLoader<SETTLEMENT_ROW> *CreateSettlementLoader() = 0;

	/*
	 *  Create loader class for STATUS_TYPE table.
	 *  Should be defined in a subclass according to the subclass load type.
	 *
	 *  PARAMETERS:
	 *           none.
	 *
	 *  RETURNS:
	 *           Reference to the loader class.
	 */
	virtual CBaseLoader<STATUS_TYPE_ROW> *CreateStatusTypeLoader() = 0;

	/*
	 *  Create loader class for TAXRATE table.
	 *  Should be defined in a subclass according to the subclass load type.
	 *
	 *  PARAMETERS:
	 *           none.
	 *
	 *  RETURNS:
	 *           Reference to the loader class.
	 */
	virtual CBaseLoader<TAX_RATE_ROW> *CreateTaxRateLoader() = 0;

	/*
	 *  Create loader class for TRADE_HISTORY table.
	 *  Should be defined in a subclass according to the subclass load type.
	 *
	 *  PARAMETERS:
	 *           none.
	 *
	 *  RETURNS:
	 *           Reference to the loader class.
	 */
	virtual CBaseLoader<TRADE_HISTORY_ROW> *CreateTradeHistoryLoader() = 0;

	/*
	 *  Create loader class for TRADE table.
	 *  Should be defined in a subclass according to the subclass load type.
	 *
	 *  PARAMETERS:
	 *           none.
	 *
	 *  RETURNS:
	 *           Reference to the loader class.
	 */
	virtual CBaseLoader<TRADE_ROW> *CreateTradeLoader() = 0;

	/*
	 *  Create loader class for TRADE_REQUEST table.
	 *  Should be defined in a subclass according to the subclass load type.
	 *
	 *  PARAMETERS:
	 *           none.
	 *
	 *  RETURNS:
	 *           Reference to the loader class.
	 */
	virtual CBaseLoader<TRADE_REQUEST_ROW> *CreateTradeRequestLoader() = 0;

	/*
	 *  Create loader class for TRADE_TYPE table.
	 *  Should be defined in a subclass according to the subclass load type.
	 *
	 *  PARAMETERS:
	 *           none.
	 *
	 *  RETURNS:
	 *           Reference to the loader class.
	 */
	virtual CBaseLoader<TRADE_TYPE_ROW> *CreateTradeTypeLoader() = 0;

	/*
	 *  Create loader class for WATCH_ITEM table.
	 *  Should be defined in a subclass according to the subclass load type.
	 *
	 *  PARAMETERS:
	 *           none.
	 *
	 *  RETURNS:
	 *           Reference to the loader class.
	 */
	virtual CBaseLoader<WATCH_ITEM_ROW> *CreateWatchItemLoader() = 0;

	/*
	 *  Create loader class for WATCH_LIST table.
	 *  Should be defined in a subclass according to the subclass load type.
	 *
	 *  PARAMETERS:
	 *           none.
	 *
	 *  RETURNS:
	 *           Reference to the loader class.
	 */
	virtual CBaseLoader<WATCH_LIST_ROW> *CreateWatchListLoader() = 0;

	/*
	 *  Create loader class for ZIP_CODE table.
	 *  Should be defined in a subclass according to the subclass load type.
	 *
	 *  PARAMETERS:
	 *           none.
	 *
	 *  RETURNS:
	 *           Reference to the loader class.
	 */
	virtual CBaseLoader<ZIP_CODE_ROW> *CreateZipCodeLoader() = 0;
};

} // namespace TPCE

#endif // #ifndef BASE_LOADER_FACTORY_H
