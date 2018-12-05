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
 */

/*
 *   Contains class definitions for all transactions.
 */
#ifndef TXN_HARNESS_STRUCTS_H
#define TXN_HARNESS_STRUCTS_H

#include "utilities/EGenStandardTypes.h"
#include "utilities/DateTime.h"
#include "utilities/MiscConsts.h"
#include "utilities/TableConsts.h"
#include "MEETradeRequestActions.h"
#include "CustomerAccountsAndPermissionsTable.h" // for iMaxAccountsPerCustomer
#include "utilities/BufferFiller.h"

namespace TPCE {

// declare the < operator for timestamps
bool operator<(const TIMESTAMP_STRUCT &ts1, const TIMESTAMP_STRUCT &ts2);

const INT32 iFinYears = 5;
const INT32 iFinQtrPerYear = 4;
const INT32 iMaxDailyHistory = 10;
const INT32 iMaxNews = 10;

// Broker-Volume
const INT32 min_broker_list_len = 20;
const INT32 max_broker_list_len = 40;

// Customer-Position
const INT32 max_acct_len = iMaxAccountsPerCust;
const INT32 min_hist_len = 10 * 1;
const INT32 max_hist_len = 10 * 3;

// Market-Feed
const INT32 max_feed_len = 20;

// Security-Detail
const INT32 min_day_len = 5;
const INT32 max_day_len = 20;
const INT32 max_fin_len = 20;
const INT32 max_news_len = 2;
const INT32 max_comp_len = 3;

// Trade-Status
const INT32 max_trade_status_len = 50;

// Data-Maintenance
const INT32 max_table_name = 30;

/*
 * Macros for harness validation
 */

#define TXN_HARNESS_PROPAGATE_STATUS(code)                                                                             \
	if ((pTxnOutput->status >= 0) && ((code) < 0)) {                                                                   \
		/* propagate error over existing ok/warn status */                                                             \
		pTxnOutput->status = (code);                                                                                   \
	} else if ((pTxnOutput->status == 0) && ((code) > 0)) {                                                            \
		/* propagate warning over existing ok status */                                                                \
		pTxnOutput->status = (code);                                                                                   \
	}

#define TXN_HARNESS_SET_STATUS_SUCCESS pTxnOutput->status = CBaseTxnErr::SUCCESS;

#define TXN_HARNESS_EARLY_EXIT_ON_ERROR                                                                                \
	if (pTxnOutput->status < 0) {                                                                                      \
		return;                                                                                                        \
	}

/*
 *   Broker-Volume
 */
typedef class TBrokerVolumeTxnInput {
public:
	// Transaction level inputs
	char broker_list[max_broker_list_len][cB_NAME_len + 1];
	char sector_name[cSC_NAME_len + 1];

#ifdef ENABLE_DEFAULT_CONSTRUCTOR_ON_TXN_INPUT_OUTPUT
	TBrokerVolumeTxnInput() {
		Clear();
	}
#endif

	inline void Clear() {
		BufferFiller::Clear(this, sizeof(*this));
	}

} * PBrokerVolumeTxnInput,
    TBrokerVolumeFrame1Input,  // Single-Frame transaction
    *PBrokerVolumeFrame1Input; // Single-Frame transaction

typedef class TBrokerVolumeTxnOutput {
public:
	// Transaction level outputs
	double volume[max_broker_list_len];
	INT32 list_len;
	INT32 status;

#ifdef ENABLE_DEFAULT_CONSTRUCTOR_ON_TXN_INPUT_OUTPUT
	TBrokerVolumeTxnOutput() {
		Clear();
	}
#endif

	inline void Clear() {
		BufferFiller::Clear(this, sizeof(*this));
	}

} * PBrokerVolumeTxnOutput;

typedef class TBrokerVolumeFrame1Output {
public:
	// Frame level outputs
	double volume[max_broker_list_len];
	INT32 list_len;
	char broker_name[max_broker_list_len][cB_NAME_len + 1];

	TBrokerVolumeFrame1Output() {
		Clear();
	}

	inline void Clear() {
		BufferFiller::Clear(this, sizeof(*this));
	}

} * PBrokerVolumeFrame1Output;

/*
 *   Customer-Position
 */
typedef class TCustomerPositionTxnInput {
public:
	TIdent acct_id_idx;
	TIdent cust_id;
	bool get_history;
	char tax_id[cTAX_ID_len + 1];

#ifdef ENABLE_DEFAULT_CONSTRUCTOR_ON_TXN_INPUT_OUTPUT
	TCustomerPositionTxnInput() {
		Clear();
	}
#endif

	inline void Clear() {
		BufferFiller::Clear(this, sizeof(*this));
	}

} * PCustomerPositionTxnInput;

typedef class TCustomerPositionTxnOutput {
public:
	double asset_total[max_acct_len];
	double cash_bal[max_acct_len];
	TIdent acct_id[max_acct_len];
	TTrade trade_id[max_hist_len];
	TIdent c_ad_id;
	INT32 qty[max_hist_len];
	INT32 acct_len;
	INT32 hist_len;
	INT32 status;
	TIMESTAMP_STRUCT hist_dts[max_hist_len];
	TIMESTAMP_STRUCT c_dob;
	char symbol[max_hist_len][cSYMBOL_len + 1];
	char trade_status[max_hist_len][cST_NAME_len + 1];
	char c_area_1[cAREA_len + 1];
	char c_area_2[cAREA_len + 1];
	char c_area_3[cAREA_len + 1];
	char c_ctry_1[cCTRY_len + 1];
	char c_ctry_2[cCTRY_len + 1];
	char c_ctry_3[cCTRY_len + 1];
	char c_email_1[cEMAIL_len + 1];
	char c_email_2[cEMAIL_len + 1];
	char c_ext_1[cEXT_len + 1];
	char c_ext_2[cEXT_len + 1];
	char c_ext_3[cEXT_len + 1];
	char c_f_name[cF_NAME_len + 1];
	char c_gndr[cGNDR_len + 1];
	char c_l_name[cL_NAME_len + 1];
	char c_local_1[cLOCAL_len + 1];
	char c_local_2[cLOCAL_len + 1];
	char c_local_3[cLOCAL_len + 1];
	char c_m_name[cM_NAME_len + 1];
	char c_st_id[cST_ID_len + 1];
	char c_tier;

#ifdef ENABLE_DEFAULT_CONSTRUCTOR_ON_TXN_INPUT_OUTPUT
	TCustomerPositionTxnOutput() {
		Clear();
	}
#endif

	inline void Clear() {
		BufferFiller::Clear(this, sizeof(*this));
	}

} * PCustomerPositionTxnOutput;

typedef class TCustomerPositionFrame1Input {
public:
	TIdent cust_id;
	char tax_id[cTAX_ID_len + 1];

	TCustomerPositionFrame1Input() {
		Clear();
	}

	inline void Clear() {
		BufferFiller::Clear(this, sizeof(*this));
	}

} * PCustomerPositionFrame1Input;

typedef class TCustomerPositionFrame1Output {
public:
	double asset_total[max_acct_len];
	double cash_bal[max_acct_len];
	TIdent acct_id[max_acct_len];
	TIdent c_ad_id;
	TIdent cust_id;
	INT32 acct_len;
	TIMESTAMP_STRUCT c_dob;
	char c_area_1[cAREA_len + 1];
	char c_area_2[cAREA_len + 1];
	char c_area_3[cAREA_len + 1];
	char c_ctry_1[cCTRY_len + 1];
	char c_ctry_2[cCTRY_len + 1];
	char c_ctry_3[cCTRY_len + 1];
	char c_email_1[cEMAIL_len + 1];
	char c_email_2[cEMAIL_len + 1];
	char c_ext_1[cEXT_len + 1];
	char c_ext_2[cEXT_len + 1];
	char c_ext_3[cEXT_len + 1];
	char c_f_name[cF_NAME_len + 1];
	char c_gndr[cGNDR_len + 1];
	char c_l_name[cL_NAME_len + 1];
	char c_local_1[cLOCAL_len + 1];
	char c_local_2[cLOCAL_len + 1];
	char c_local_3[cLOCAL_len + 1];
	char c_m_name[cM_NAME_len + 1];
	char c_st_id[cST_ID_len + 1];
	char c_tier;

	TCustomerPositionFrame1Output() {
		Clear();
	}

	inline void Clear() {
		BufferFiller::Clear(this, sizeof(*this));
	}

} * PCustomerPositionFrame1Output;

typedef class TCustomerPositionFrame2Input {
public:
	TIdent acct_id;

	TCustomerPositionFrame2Input() {
		Clear();
	}

	inline void Clear() {
		BufferFiller::Clear(this, sizeof(*this));
	}

} * PCustomerPositionFrame2Input;

typedef class TCustomerPositionFrame2Output {
public:
	TTrade trade_id[max_hist_len];
	INT32 qty[max_hist_len];
	INT32 hist_len;
	TIMESTAMP_STRUCT hist_dts[max_hist_len];
	char symbol[max_hist_len][cSYMBOL_len + 1];
	char trade_status[max_hist_len][cST_NAME_len + 1];

	TCustomerPositionFrame2Output() {
		Clear();
	}

	inline void Clear() {
		BufferFiller::Clear(this, sizeof(*this));
	}

} * PCustomerPositionFrame2Output;

/*
 *   Data-Maintenance
 */
typedef class TDataMaintenanceTxnInput {
public:
	TIdent acct_id;
	TIdent c_id;
	TIdent co_id;
	INT32 day_of_month;
	INT32 vol_incr;
	char symbol[cSYMBOL_len + 1];
	char table_name[max_table_name + 1];
	char tx_id[cTX_ID_len + 1];

#ifdef ENABLE_DEFAULT_CONSTRUCTOR_ON_TXN_INPUT_OUTPUT
	TDataMaintenanceTxnInput() {
		Clear();
	}
#endif

	inline void Clear() {
		BufferFiller::Clear(this, sizeof(*this));
	}

} * PDataMaintenanceTxnInput,
    TDataMaintenanceFrame1Input,  // Single-Frame transaction
    *PDataMaintenanceFrame1Input; // Single-Frame transaction

typedef class TDataMaintenanceTxnOutput {
public:
	INT32 status;

#ifdef ENABLE_DEFAULT_CONSTRUCTOR_ON_TXN_INPUT_OUTPUT
	TDataMaintenanceTxnOutput() {
		Clear();
	}
#endif

	inline void Clear() {
		BufferFiller::Clear(this, sizeof(*this));
	}

} * PDataMaintenanceTxnOutput;

/*
 *   Market-Feed
 */
// MEE populates this class
typedef class TStatusAndTradeType {
public:
	char status_submitted[cST_ID_len + 1];
	char type_limit_buy[cTT_ID_len + 1];
	char type_limit_sell[cTT_ID_len + 1];
	char type_stop_loss[cTT_ID_len + 1];

#ifdef ENABLE_DEFAULT_CONSTRUCTOR_ON_TXN_INPUT_OUTPUT
	TStatusAndTradeType() {
		Clear();
	}
#endif

	inline void Clear() {
		BufferFiller::Clear(this, sizeof(*this));
	}

} * PTStatusAndTradeType;

// Incoming order from SendToMarket interface.
typedef class TTradeRequest {
public:
	double price_quote;
	TTrade trade_id;
	INT32 trade_qty;
	eMEETradeRequestAction eAction;
	char symbol[cSYMBOL_len + 1];
	char trade_type_id[cTT_ID_len + 1];

#ifdef ENABLE_DEFAULT_CONSTRUCTOR_ON_TXN_INPUT_OUTPUT
	TTradeRequest() {
		Clear();
	}
#endif

	inline void Clear() {
		BufferFiller::Clear(this, sizeof(*this));
	}

} * PTradeRequest;

// A single entry on the ticker tape feed.
typedef class TTickerEntry {
public:
	double price_quote;
	INT32 trade_qty;
	char symbol[cSYMBOL_len + 1];

#ifdef ENABLE_DEFAULT_CONSTRUCTOR_ON_TXN_INPUT_OUTPUT
	TTickerEntry() {
		Clear();
	}
#endif

	inline void Clear() {
		BufferFiller::Clear(this, sizeof(*this));
	}

} * PTickerEntry;

// Market-Feed data sent from MEE to sponsor provided SUT interface
typedef class TMarketFeedTxnInput {
public:
	INT32 unique_symbols;
	char zz_padding1[4];
	TStatusAndTradeType StatusAndTradeType;
	char zz_padding2[4];
	TTickerEntry Entries[max_feed_len];

#ifdef ENABLE_DEFAULT_CONSTRUCTOR_ON_TXN_INPUT_OUTPUT
	TMarketFeedTxnInput() {
		Clear();
	}
#endif

	inline void Clear() {
		BufferFiller::Clear(this, sizeof(*this));
	}

} * PMarketFeedTxnInput;

typedef class TMarketFeedTxnOutput {
public:
	INT32 send_len;
	INT32 status;

#ifdef ENABLE_DEFAULT_CONSTRUCTOR_ON_TXN_INPUT_OUTPUT
	TMarketFeedTxnOutput() {
		Clear();
	}
#endif

	inline void Clear() {
		BufferFiller::Clear(this, sizeof(*this));
	}

} * PMarketFeedTxnOutput;

typedef class TMarketFeedFrame1Input {
public:
	TStatusAndTradeType StatusAndTradeType;
	char zz_padding[4];
	TTickerEntry Entries[max_feed_len];

	TMarketFeedFrame1Input() {
		Clear();
	}

	inline void Clear() {
		BufferFiller::Clear(this, sizeof(*this));
	}

} * PMarketFeedFrame1Input;

typedef class TMarketFeedFrame1Output {
public:
	INT32 num_updated;
	INT32 send_len;

	TMarketFeedFrame1Output() {
		Clear();
	}

	inline void Clear() {
		BufferFiller::Clear(this, sizeof(*this));
	}

} * PMarketFeedFrame1Output;

/*
 *   Market-Watch
 */
typedef class TMarketWatchTxnInput {
public:
	TIdent acct_id;
	TIdent c_id;
	TIdent ending_co_id;
	TIdent starting_co_id;
	TIMESTAMP_STRUCT start_day;
	char industry_name[cIN_NAME_len + 1];

#ifdef ENABLE_DEFAULT_CONSTRUCTOR_ON_TXN_INPUT_OUTPUT
	TMarketWatchTxnInput() {
		Clear();
	}
#endif

	inline void Clear() {
		BufferFiller::Clear(this, sizeof(*this));
	}

} * PMarketWatchTxnInput,
    TMarketWatchFrame1Input,  // Single-Frame transaction
    *PMarketWatchFrame1Input; // Single-Frame transaction

typedef class TMarketWatchTxnOutput {
public:
	double pct_change;
	INT32 status;

#ifdef ENABLE_DEFAULT_CONSTRUCTOR_ON_TXN_INPUT_OUTPUT
	TMarketWatchTxnOutput() {
		Clear();
	}
#endif

	inline void Clear() {
		BufferFiller::Clear(this, sizeof(*this));
	}

} * PMarketWatchTxnOutput;

typedef class TMarketWatchFrame1Output {
public:
	double pct_change;

	TMarketWatchFrame1Output() {
		Clear();
	}

	inline void Clear() {
		BufferFiller::Clear(this, sizeof(*this));
	}

} * PMarketWatchFrame1Output;

/*
 *   Security-Detail
 */
typedef class TFinInfo {
public:
	double assets;
	double basic_eps;
	double dilut_eps;
	double invent;
	double liab;
	double margin;
	double net_earn;
	INT64 out_basic;
	INT64 out_dilut;
	double rev;
	INT32 qtr;
	INT32 year;
	TIMESTAMP_STRUCT start_date;
	DB_INDICATOR assets_ind;
	DB_INDICATOR basic_eps_ind;
	DB_INDICATOR dilut_eps_ind;
	DB_INDICATOR invent_ind;
	DB_INDICATOR liab_ind;
	DB_INDICATOR margin_ind;
	DB_INDICATOR net_earn_ind;
	DB_INDICATOR out_basic_ind;
	DB_INDICATOR out_dilut_ind;
	DB_INDICATOR qtr_ind;
	DB_INDICATOR rev_ind;
	DB_INDICATOR start_date_ind;
	DB_INDICATOR year_ind;

	TFinInfo() {
		Clear();
	}

	inline void Clear() {
		BufferFiller::Clear(this, sizeof(*this));
	}

} * PFinInfo;

typedef class TDailyHistory {
public:
	double close;
	double high;
	double low;
	INT64 vol;
	TIMESTAMP_STRUCT date;
	DB_INDICATOR close_ind;
	DB_INDICATOR date_ind;
	DB_INDICATOR high_ind;
	DB_INDICATOR low_ind;
	DB_INDICATOR vol_ind;

	TDailyHistory() {
		Clear();
	}

	inline void Clear() {
		BufferFiller::Clear(this, sizeof(*this));
	}

} * PDailyHistory;

typedef class TNews {
public:
	TIMESTAMP_STRUCT dts;
	char auth[cNI_AUTHOR_len + 1];
	char headline[cNI_HEADLINE_len + 1];
	char item[cNI_ITEM_len + 1];
	char src[cNI_SOURCE_len + 1];
	char summary[cNI_SUMMARY_len + 1];
	DB_INDICATOR auth_ind;

	TNews() {
		Clear();
	}

	inline void Clear() {
		BufferFiller::Clear(&dts, sizeof(dts));
		BufferFiller::Clear(auth, sizeof(auth));
		BufferFiller::Clear(headline, sizeof(headline));

		// Purposely don't clear item since it is so large.
		// BufferFiller::Clear( item, sizeof( item ));

		BufferFiller::Clear(src, sizeof(src));
		BufferFiller::Clear(summary, sizeof(summary));
		BufferFiller::Clear(&auth_ind, sizeof(auth_ind));
	}

} * PNews;

typedef class TSecurityDetailTxnInput {
public:
	INT32 max_rows_to_return;
	bool access_lob_flag;
	TIMESTAMP_STRUCT start_day;
	char symbol[cSYMBOL_len + 1];

#ifdef ENABLE_DEFAULT_CONSTRUCTOR_ON_TXN_INPUT_OUTPUT
	TSecurityDetailTxnInput() {
		Clear();
	}
#endif

	inline void Clear() {
		BufferFiller::Clear(this, sizeof(*this));
	}

} * PSecurityDetailTxnInput,
    TSecurityDetailFrame1Input,  // Single-Frame transaction
    *PSecurityDetailFrame1Input; // Single-Frame transaction

typedef class TSecurityDetailTxnOutput {
public:
	INT64 last_vol;
	INT32 news_len;
	INT32 status;

#ifdef ENABLE_DEFAULT_CONSTRUCTOR_ON_TXN_INPUT_OUTPUT
	TSecurityDetailTxnOutput() {
		Clear();
	}
#endif

	inline void Clear() {
		BufferFiller::Clear(this, sizeof(*this));
	}

} * PSecurityDetailTxnOutput;

typedef class TSecurityDetailFrame1Output {
public:
	double divid;
	double last_open;
	double last_price;
	double pe_ratio;
	double s52_wk_high;
	double s52_wk_low;
	double yield;
	INT64 last_vol;
	INT64 num_out;
	INT32 day_len;
	INT32 ex_close;
	INT32 ex_num_symb;
	INT32 ex_open;
	INT32 fin_len;
	INT32 news_len;
	TIMESTAMP_STRUCT ex_date;
	TIMESTAMP_STRUCT open_date;
	TIMESTAMP_STRUCT s52_wk_high_date;
	TIMESTAMP_STRUCT s52_wk_low_date;
	TIMESTAMP_STRUCT start_date;
	TDailyHistory day[max_day_len];
	TFinInfo fin[max_fin_len];
	TNews news[max_news_len];
	char cp_co_name[max_comp_len][cCO_NAME_len + 1];
	char cp_in_name[max_comp_len][cIN_NAME_len + 1];
	char ceo_name[cCEO_NAME_len + 1];
	char co_ad_cty[cAD_CTRY_len + 1];
	char co_ad_div[cAD_DIV_len + 1];
	char co_ad_line1[cAD_LINE_len + 1];
	char co_ad_line2[cAD_LINE_len + 1];
	char co_ad_town[cAD_TOWN_len + 1];
	char co_ad_zip[cAD_ZIP_len + 1];
	char co_desc[cCO_DESC_len + 1];
	char co_name[cCO_NAME_len + 1];
	char co_st_id[cST_ID_len + 1];
	char ex_ad_cty[cAD_CTRY_len + 1];
	char ex_ad_div[cAD_DIV_len + 1];
	char ex_ad_line1[cAD_LINE_len + 1];
	char ex_ad_line2[cAD_LINE_len + 1];
	char ex_ad_town[cAD_TOWN_len + 1];
	char ex_ad_zip[cAD_ZIP_len + 1];
	char ex_desc[cEX_DESC_len + 1];
	char ex_name[cEX_NAME_len + 1];
	char s_name[cS_NAME_len + 1];
	char sp_rate[cSP_RATE_len + 1];

	TSecurityDetailFrame1Output() {
		Clear();
	}

	inline void Clear() {
		// Using  BufferFiller::Clear( this, sizeof( *this ))  would incur
		// the overhead of clearing two LOB items in news array.
		// So instead, clear members individually and call Clear on
		// elements of news array to avoid clearing LOB space.
		BufferFiller::Clear(&divid, sizeof(divid));
		BufferFiller::Clear(&last_open, sizeof(last_open));
		BufferFiller::Clear(&last_price, sizeof(last_price));
		BufferFiller::Clear(&pe_ratio, sizeof(pe_ratio));
		BufferFiller::Clear(&s52_wk_high, sizeof(s52_wk_high));
		BufferFiller::Clear(&s52_wk_low, sizeof(s52_wk_low));
		BufferFiller::Clear(&yield, sizeof(yield));
		BufferFiller::Clear(&last_vol, sizeof(last_vol));
		BufferFiller::Clear(&num_out, sizeof(num_out));
		BufferFiller::Clear(&day_len, sizeof(day_len));
		BufferFiller::Clear(&ex_close, sizeof(ex_close));
		BufferFiller::Clear(&ex_num_symb, sizeof(ex_num_symb));
		BufferFiller::Clear(&ex_open, sizeof(ex_open));
		BufferFiller::Clear(&fin_len, sizeof(fin_len));
		BufferFiller::Clear(&news_len, sizeof(news_len));
		BufferFiller::Clear(&ex_date, sizeof(ex_date));
		BufferFiller::Clear(&open_date, sizeof(open_date));
		BufferFiller::Clear(&s52_wk_high_date, sizeof(s52_wk_high_date));
		BufferFiller::Clear(&s52_wk_low_date, sizeof(s52_wk_low_date));
		BufferFiller::Clear(&start_date, sizeof(start_date));
		BufferFiller::Clear(day, sizeof(day));
		BufferFiller::Clear(fin, sizeof(fin));
		for (int ii = 0; ii < sizeof(news) / sizeof(news[0]); ++ii) {
			news[ii].Clear();
		}
		BufferFiller::Clear(cp_co_name, sizeof(cp_co_name));
		BufferFiller::Clear(cp_in_name, sizeof(cp_in_name));
		BufferFiller::Clear(ceo_name, sizeof(ceo_name));
		BufferFiller::Clear(co_ad_cty, sizeof(co_ad_cty));
		BufferFiller::Clear(co_ad_div, sizeof(co_ad_div));
		BufferFiller::Clear(co_ad_line1, sizeof(co_ad_line1));
		BufferFiller::Clear(co_ad_line2, sizeof(co_ad_line2));
		BufferFiller::Clear(co_ad_town, sizeof(co_ad_town));
		BufferFiller::Clear(co_ad_zip, sizeof(co_ad_zip));
		BufferFiller::Clear(co_desc, sizeof(co_desc));
		BufferFiller::Clear(co_name, sizeof(co_name));
		BufferFiller::Clear(co_st_id, sizeof(co_st_id));
		BufferFiller::Clear(ex_ad_cty, sizeof(co_ad_cty));
		BufferFiller::Clear(ex_ad_div, sizeof(co_ad_div));
		BufferFiller::Clear(ex_ad_line1, sizeof(co_ad_line1));
		BufferFiller::Clear(ex_ad_line2, sizeof(co_ad_line2));
		BufferFiller::Clear(ex_ad_town, sizeof(co_ad_town));
		BufferFiller::Clear(ex_ad_zip, sizeof(co_ad_zip));
		BufferFiller::Clear(ex_desc, sizeof(co_desc));
		BufferFiller::Clear(ex_name, sizeof(co_name));
		BufferFiller::Clear(s_name, sizeof(s_name));
		BufferFiller::Clear(sp_rate, sizeof(sp_rate));
	}

} * PSecurityDetailFrame1Output;

/*
 *   Trade-Lookup
 */
typedef class TTradeLookupTxnInput {
public:
	TTrade trade_id[TradeLookupFrame1MaxRows];
	TIdent acct_id;
	TIdent max_acct_id;
	INT32 frame_to_execute; // which of the frames to execute
	INT32 max_trades;
	TIMESTAMP_STRUCT end_trade_dts;
	TIMESTAMP_STRUCT start_trade_dts;
	char symbol[cSYMBOL_len + 1];

#ifdef ENABLE_DEFAULT_CONSTRUCTOR_ON_TXN_INPUT_OUTPUT
	TTradeLookupTxnInput() {
		Clear();
	}
#endif

	inline void Clear() {
		BufferFiller::Clear(this, sizeof(*this));
	}

} * PTradeLookupTxnInput;

typedef class TTradeLookupTxnOutput {
public:
	TTrade trade_list[TradeLookupMaxRows];
	INT32 frame_executed; // confirmation of which frame was executed
	INT32 num_found;
	INT32 status;
	bool is_cash[TradeLookupMaxRows];
	bool is_market[TradeLookupMaxRows];

#ifdef ENABLE_DEFAULT_CONSTRUCTOR_ON_TXN_INPUT_OUTPUT
	TTradeLookupTxnOutput() {
		Clear();
	}
#endif

	inline void Clear() {
		BufferFiller::Clear(this, sizeof(*this));
	}

} * PTradeLookupTxnOutput;

typedef class TTradeLookupFrame1Input {
public:
	TTrade trade_id[TradeLookupFrame1MaxRows];
	INT32 max_trades;

	TTradeLookupFrame1Input() {
		Clear();
	}

	inline void Clear() {
		BufferFiller::Clear(this, sizeof(*this));
	}

} * PTradeLookupFrame1Input;

// Class to hold one trade information row
//
typedef class TTradeLookupFrame1TradeInfo {
public:
	double bid_price;
	double cash_transaction_amount;
	double settlement_amount;
	double trade_price;
	bool is_cash;
	bool is_market;
	TIMESTAMP_STRUCT trade_history_dts[TradeLookupMaxTradeHistoryRowsReturned];
	TIMESTAMP_STRUCT cash_transaction_dts;
	TIMESTAMP_STRUCT settlement_cash_due_date;
	char trade_history_status_id[TradeLookupMaxTradeHistoryRowsReturned][cTH_ST_ID_len + 1];
	char cash_transaction_name[cCT_NAME_len + 1];
	char exec_name[cEXEC_NAME_len + 1];
	char settlement_cash_type[cSE_CASH_TYPE_len + 1];
	DB_INDICATOR trade_history_dts_ind[TradeLookupMaxTradeHistoryRowsReturned];
	DB_INDICATOR
	trade_history_status_id_ind[TradeLookupMaxTradeHistoryRowsReturned];
	DB_INDICATOR bid_price_ind;
	DB_INDICATOR cash_transaction_amount_ind;
	DB_INDICATOR cash_transaction_dts_ind;
	DB_INDICATOR cash_transaction_name_ind;
	DB_INDICATOR exec_name_ind;
	DB_INDICATOR is_cash_ind;
	DB_INDICATOR is_market_ind;
	DB_INDICATOR settlement_amount_ind;
	DB_INDICATOR settlement_cash_due_date_ind;
	DB_INDICATOR settlement_cash_type_ind;
	DB_INDICATOR trade_price_ind;

	TTradeLookupFrame1TradeInfo() {
		Clear();
	}

	inline void Clear() {
		BufferFiller::Clear(this, sizeof(*this));
	}

} * PTradeLookupFrame1TradeInfo;

typedef class TTradeLookupFrame1Output {
public:
	INT32 num_found;
	TTradeLookupFrame1TradeInfo trade_info[TradeLookupFrame1MaxRows];

	TTradeLookupFrame1Output() {
		Clear();
	}

	inline void Clear() {
		BufferFiller::Clear(this, sizeof(*this));
	}

} * PTradeLookupFrame1Output;

typedef class TTradeLookupFrame2Input {
public:
	TIdent acct_id;
	INT32 max_trades;
	TIMESTAMP_STRUCT end_trade_dts;
	TIMESTAMP_STRUCT start_trade_dts;

	TTradeLookupFrame2Input() {
		Clear();
	}

	inline void Clear() {
		BufferFiller::Clear(this, sizeof(*this));
	}

} * PTradeLookupFrame2Input;

// Class to hold one trade information row
//
typedef class TTradeLookupFrame2TradeInfo {
public:
	double bid_price;
	double cash_transaction_amount;
	double settlement_amount;
	double trade_price;
	TTrade trade_id;
	bool is_cash;
	TIMESTAMP_STRUCT trade_history_dts[TradeLookupMaxTradeHistoryRowsReturned];
	TIMESTAMP_STRUCT cash_transaction_dts;
	TIMESTAMP_STRUCT settlement_cash_due_date;
	char trade_history_status_id[TradeLookupMaxTradeHistoryRowsReturned][cTH_ST_ID_len + 1];
	char cash_transaction_name[cCT_NAME_len + 1];
	char exec_name[cEXEC_NAME_len + 1];
	char settlement_cash_type[cSE_CASH_TYPE_len + 1];
	DB_INDICATOR trade_history_dts_ind[TradeLookupMaxTradeHistoryRowsReturned];
	DB_INDICATOR
	trade_history_status_id_ind[TradeLookupMaxTradeHistoryRowsReturned];
	DB_INDICATOR bid_price_ind;
	DB_INDICATOR cash_transaction_amount_ind;
	DB_INDICATOR cash_transaction_dts_ind;
	DB_INDICATOR cash_transaction_name_ind;
	DB_INDICATOR exec_name_ind;
	DB_INDICATOR is_cash_ind;
	DB_INDICATOR settlement_amount_ind;
	DB_INDICATOR settlement_cash_due_date_ind;
	DB_INDICATOR settlement_cash_type_ind;
	DB_INDICATOR trade_id_ind;
	DB_INDICATOR trade_price_ind;

	TTradeLookupFrame2TradeInfo() {
		Clear();
	}

	inline void Clear() {
		BufferFiller::Clear(this, sizeof(*this));
	}

} * PTradeLookupFrame2TradeInfo;

typedef class TTradeLookupFrame2Output {
public:
	INT32 num_found;
	TTradeLookupFrame2TradeInfo trade_info[TradeLookupFrame2MaxRows];

	TTradeLookupFrame2Output() {
		Clear();
	}

	inline void Clear() {
		BufferFiller::Clear(this, sizeof(*this));
	}

} * PTradeLookupFrame2Output;

typedef class TTradeLookupFrame3Input {
public:
	TIdent max_acct_id;
	INT32 max_trades;
	TIMESTAMP_STRUCT end_trade_dts;
	TIMESTAMP_STRUCT start_trade_dts;
	char symbol[cSYMBOL_len + 1];

	TTradeLookupFrame3Input() {
		Clear();
	}

	inline void Clear() {
		BufferFiller::Clear(this, sizeof(*this));
	}

} * PTradeLookupFrame3Input;

// Class to hold one trade information row
//
typedef class TTradeLookupFrame3TradeInfo {
public:
	double cash_transaction_amount;
	double price;
	double settlement_amount;
	TIdent acct_id;
	TTrade trade_id;
	INT32 quantity;
	bool is_cash;
	TIMESTAMP_STRUCT trade_history_dts[TradeLookupMaxTradeHistoryRowsReturned];
	TIMESTAMP_STRUCT cash_transaction_dts;
	TIMESTAMP_STRUCT settlement_cash_due_date;
	TIMESTAMP_STRUCT trade_dts;
	char trade_history_status_id[TradeLookupMaxTradeHistoryRowsReturned][cTH_ST_ID_len + 1];
	char cash_transaction_name[cCT_NAME_len + 1];
	char exec_name[cEXEC_NAME_len + 1];
	char settlement_cash_type[cSE_CASH_TYPE_len + 1];
	char trade_type[cTT_ID_len + 1];
	DB_INDICATOR trade_history_dts_ind[TradeLookupMaxTradeHistoryRowsReturned];
	DB_INDICATOR
	trade_history_status_id_ind[TradeLookupMaxTradeHistoryRowsReturned];
	DB_INDICATOR acct_id_ind;
	DB_INDICATOR cash_transaction_amount_ind;
	DB_INDICATOR cash_transaction_dts_ind;
	DB_INDICATOR cash_transaction_name_ind;
	DB_INDICATOR exec_name_ind;
	DB_INDICATOR is_cash_ind;
	DB_INDICATOR price_ind;
	DB_INDICATOR quantity_ind;
	DB_INDICATOR settlement_amount_ind;
	DB_INDICATOR settlement_cash_due_date_ind;
	DB_INDICATOR settlement_cash_type_ind;
	DB_INDICATOR trade_dts_ind;
	DB_INDICATOR trade_id_ind;
	DB_INDICATOR trade_type_ind;

	TTradeLookupFrame3TradeInfo() {
		Clear();
	}

	inline void Clear() {
		BufferFiller::Clear(this, sizeof(*this));
	}

} * PTradeLookupFrame3TradeInfo;

typedef class TTradeLookupFrame3Output {
public:
	INT32 num_found;
	TTradeLookupFrame3TradeInfo trade_info[TradeLookupFrame3MaxRows];

	TTradeLookupFrame3Output() {
		Clear();
	}

	inline void Clear() {
		BufferFiller::Clear(this, sizeof(*this));
	}

} * PTradeLookupFrame3Output;

typedef class TTradeLookupFrame4Input {
public:
	TIdent acct_id;
	TIMESTAMP_STRUCT trade_dts;

	TTradeLookupFrame4Input() {
		Clear();
	}

	inline void Clear() {
		BufferFiller::Clear(this, sizeof(*this));
	}

} * PTradeLookupFrame4Input;

// Class to hold one trade information row
//
typedef class TTradeLookupFrame4TradeInfo {
public:
	TTrade holding_history_id;
	TTrade holding_history_trade_id;
	INT32 quantity_after;
	INT32 quantity_before;
	DB_INDICATOR holding_history_id_ind;
	DB_INDICATOR holding_history_trade_id_ind;
	DB_INDICATOR quantity_after_ind;
	DB_INDICATOR quantity_before_ind;

	TTradeLookupFrame4TradeInfo() {
		Clear();
	}

	inline void Clear() {
		BufferFiller::Clear(this, sizeof(*this));
	}

} * PTradeLookupFrame4TradeInfo;

typedef class TTradeLookupFrame4Output {
public:
	TTrade trade_id;
	INT32 num_found;
	INT32 num_trades_found;
	TTradeLookupFrame4TradeInfo trade_info[TradeLookupFrame4MaxRows];

	TTradeLookupFrame4Output() {
		Clear();
	}

	inline void Clear() {
		BufferFiller::Clear(this, sizeof(*this));
	}

} * PTradeLookupFrame4Output;

/*
 *   Trade-Order
 */
typedef class TTradeOrderTxnInput {
public:
	double requested_price;
	TIdent acct_id;
	INT32 is_lifo;
	INT32 roll_it_back;
	INT32 trade_qty;
	INT32 type_is_margin;
	char co_name[cCO_NAME_len + 1];
	char exec_f_name[cF_NAME_len + 1];
	char exec_l_name[cL_NAME_len + 1];
	char exec_tax_id[cTAX_ID_len + 1];
	char issue[cS_ISSUE_len + 1];
	char st_pending_id[cST_ID_len + 1];
	char st_submitted_id[cST_ID_len + 1];
	char symbol[cSYMBOL_len + 1];
	char trade_type_id[cTT_ID_len + 1];

#ifdef ENABLE_DEFAULT_CONSTRUCTOR_ON_TXN_INPUT_OUTPUT
	TTradeOrderTxnInput() {
		Clear();
	}
#endif

	inline void Clear() {
		BufferFiller::Clear(this, sizeof(*this));
	}

} * PTradeOrderTxnInput;

typedef class TTradeOrderTxnOutput {
public:
	double buy_value;
	double sell_value;
	double tax_amount;
	TTrade trade_id;
	INT32 status;

#ifdef ENABLE_DEFAULT_CONSTRUCTOR_ON_TXN_INPUT_OUTPUT
	TTradeOrderTxnOutput() {
		Clear();
	}
#endif

	inline void Clear() {
		BufferFiller::Clear(this, sizeof(*this));
	}

} * PTradeOrderTxnOutput;

typedef class TTradeOrderFrame1Input {
public:
	TIdent acct_id;

	TTradeOrderFrame1Input() {
		Clear();
	}

	inline void Clear() {
		BufferFiller::Clear(this, sizeof(*this));
	}

} * PTradeOrderFrame1Input;

typedef class TTradeOrderFrame1Output {
public:
	TIdent broker_id;
	TIdent cust_id;
	INT32 cust_tier;
	INT32 num_found;
	INT32 tax_status;
	char acct_name[cCA_NAME_len + 1];
	char broker_name[cB_NAME_len + 1];
	char cust_f_name[cF_NAME_len + 1];
	char cust_l_name[cL_NAME_len + 1];
	char tax_id[cTAX_ID_len + 1];

	TTradeOrderFrame1Output() {
		Clear();
	}

	inline void Clear() {
		BufferFiller::Clear(this, sizeof(*this));
	}

} * PTradeOrderFrame1Output;

typedef class TTradeOrderFrame2Input {
public:
	TIdent acct_id;
	char exec_f_name[cF_NAME_len + 1];
	char exec_l_name[cL_NAME_len + 1];
	char exec_tax_id[cTAX_ID_len + 1];

	TTradeOrderFrame2Input() {
		Clear();
	}

	inline void Clear() {
		BufferFiller::Clear(this, sizeof(*this));
	}

} * PTradeOrderFrame2Input;

typedef class TTradeOrderFrame2Output {
public:
	char ap_acl[cACL_len + 1];

	TTradeOrderFrame2Output() {
		Clear();
	}

	inline void Clear() {
		BufferFiller::Clear(this, sizeof(*this));
	}

} * PTradeOrderFrame2Output;

typedef class TTradeOrderFrame3Input {
public:
	double requested_price; // IN-OUT parameter
	TIdent acct_id;
	TIdent cust_id;
	INT32 cust_tier;
	INT32 is_lifo;
	INT32 tax_status;
	INT32 trade_qty;
	INT32 type_is_margin;
	char co_name[cCO_NAME_len + 1]; // IN-OUT parameter
	char issue[cS_ISSUE_len + 1];
	char st_pending_id[cST_ID_len + 1];
	char st_submitted_id[cST_ID_len + 1];
	char symbol[cSYMBOL_len + 1]; // IN-OUT parameter
	char trade_type_id[cTT_ID_len + 1];

	TTradeOrderFrame3Input() {
		Clear();
	}

	inline void Clear() {
		BufferFiller::Clear(this, sizeof(*this));
	}

} * PTradeOrderFrame3Input;

typedef class TTradeOrderFrame3Output {
public:
	double acct_assets;
	double buy_value;
	double charge_amount;
	double comm_rate;
	double market_price;
	double requested_price; // IN-OUT parameter
	double sell_value;
	double tax_amount;
	INT32 type_is_market;
	INT32 type_is_sell;
	char co_name[cCO_NAME_len + 1]; // IN-OUT parameter
	char s_name[cS_NAME_len + 1];
	char status_id[cST_ID_len + 1];
	char symbol[cSYMBOL_len + 1]; // IN-OUT parameter

	TTradeOrderFrame3Output() {
		Clear();
	}

	inline void Clear() {
		BufferFiller::Clear(this, sizeof(*this));
	}

} * PTradeOrderFrame3Output;

typedef class TTradeOrderFrame4Input {
public:
	double charge_amount;
	double comm_amount;
	double requested_price;
	TIdent acct_id;
	TIdent broker_id;
	INT32 is_cash;
	INT32 is_lifo;
	INT32 trade_qty;
	INT32 type_is_market;
	char exec_name[cEXEC_NAME_len + 1];
	char status_id[cST_ID_len + 1];
	char symbol[cSYMBOL_len + 1];
	char trade_type_id[cTT_ID_len + 1];

	TTradeOrderFrame4Input() {
		Clear();
	}

	inline void Clear() {
		BufferFiller::Clear(this, sizeof(*this));
	}

} * PTradeOrderFrame4Input;

typedef class TTradeOrderFrame4Output {
public:
	TTrade trade_id;

	TTradeOrderFrame4Output() {
		Clear();
	}

	inline void Clear() {
		BufferFiller::Clear(this, sizeof(*this));
	}

} * PTradeOrderFrame4Output;

/*
 *   Trade-Result
 */
// Trade-Result data sent from MEE to sponsor provided SUT interface
typedef class TTradeResultTxnInput {
public:
	double trade_price;
	TTrade trade_id;

#ifdef ENABLE_DEFAULT_CONSTRUCTOR_ON_TXN_INPUT_OUTPUT
	TTradeResultTxnInput() {
		Clear();
	}
#endif

	inline void Clear() {
		BufferFiller::Clear(this, sizeof(*this));
	}

} * PTradeResultTxnInput;

typedef class TTradeResultTxnOutput {
public:
	double acct_bal;
	TIdent acct_id;
	INT32 load_unit;
	INT32 status;

#ifdef ENABLE_DEFAULT_CONSTRUCTOR_ON_TXN_INPUT_OUTPUT
	TTradeResultTxnOutput() {
		Clear();
	}
#endif

	inline void Clear() {
		BufferFiller::Clear(this, sizeof(*this));
	}

} * PTradeResultTxnOutput;

typedef class TTradeResultFrame1Input {
public:
	TTrade trade_id;

	TTradeResultFrame1Input() {
		Clear();
	}

	inline void Clear() {
		BufferFiller::Clear(this, sizeof(*this));
	}

} * PTradeResultFrame1Input;

typedef class TTradeResultFrame1Output {
public:
	double charge;
	TIdent acct_id;
	INT32 hs_qty;
	INT32 is_lifo;
	INT32 num_found;
	INT32 trade_is_cash;
	INT32 trade_qty;
	INT32 type_is_market;
	INT32 type_is_sell;
	char symbol[cSYMBOL_len + 1];
	char type_id[cTT_ID_len + 1];
	char type_name[cTT_NAME_len + 1];

	TTradeResultFrame1Output() {
		Clear();
	}

	inline void Clear() {
		BufferFiller::Clear(this, sizeof(*this));
	}

} * PTradeResultFrame1Output;

typedef class TTradeResultFrame2Input {
public:
	double trade_price;
	TIdent acct_id;
	TTrade trade_id;
	INT32 hs_qty;
	INT32 is_lifo;
	INT32 trade_qty;
	INT32 type_is_sell;
	char symbol[cSYMBOL_len + 1];

	TTradeResultFrame2Input() {
		Clear();
	}

	inline void Clear() {
		BufferFiller::Clear(this, sizeof(*this));
	}

} * PTradeResultFrame2Input;

typedef class TTradeResultFrame2Output {
public:
	double buy_value;
	double sell_value;
	TIdent broker_id;
	TIdent cust_id;
	INT32 tax_status;
	TIMESTAMP_STRUCT trade_dts;

	TTradeResultFrame2Output() {
		Clear();
	}

	inline void Clear() {
		BufferFiller::Clear(this, sizeof(*this));
	}

} * PTradeResultFrame2Output;

typedef class TTradeResultFrame3Input {
public:
	double buy_value;
	double sell_value;
	TIdent cust_id;
	TTrade trade_id;

	TTradeResultFrame3Input() {
		Clear();
	}

	inline void Clear() {
		BufferFiller::Clear(this, sizeof(*this));
	}

} * PTradeResultFrame3Input;

typedef class TTradeResultFrame3Output {
public:
	double tax_amount;

	TTradeResultFrame3Output() {
		Clear();
	}

	inline void Clear() {
		BufferFiller::Clear(this, sizeof(*this));
	}

} * PTradeResultFrame3Output;

typedef class TTradeResultFrame4Input {
public:
	TIdent cust_id;
	INT32 trade_qty;
	char symbol[cSYMBOL_len + 1];
	char type_id[cTT_ID_len + 1];

	TTradeResultFrame4Input() {
		Clear();
	}

	inline void Clear() {
		BufferFiller::Clear(this, sizeof(*this));
	}

} * PTradeResultFrame4Input;

typedef class TTradeResultFrame4Output {
public:
	double comm_rate;
	char s_name[cS_NAME_len + 1];

	TTradeResultFrame4Output() {
		Clear();
	}

	inline void Clear() {
		BufferFiller::Clear(this, sizeof(*this));
	}

} * PTradeResultFrame4Output;

typedef class TTradeResultFrame5Input {
public:
	double comm_amount;
	double trade_price;
	TIdent broker_id;
	TTrade trade_id;
	TIMESTAMP_STRUCT trade_dts;
	char st_completed_id[cST_ID_len + 1];

	TTradeResultFrame5Input() {
		Clear();
	}

	inline void Clear() {
		BufferFiller::Clear(this, sizeof(*this));
	}

} * PTradeResultFrame5Input;

typedef class TTradeResultFrame6Input {
public:
	double se_amount;
	TIdent acct_id;
	TTrade trade_id;
	INT32 trade_is_cash;
	INT32 trade_qty;
	TIMESTAMP_STRUCT due_date;
	TIMESTAMP_STRUCT trade_dts;
	char s_name[cS_NAME_len + 1];
	char type_name[cTT_NAME_len + 1];

	TTradeResultFrame6Input() {
		Clear();
	}

	inline void Clear() {
		BufferFiller::Clear(this, sizeof(*this));
	}

} * PTradeResultFrame6Input;

typedef class TTradeResultFrame6Output {
public:
	double acct_bal;

	TTradeResultFrame6Output() {
		Clear();
	}

	inline void Clear() {
		BufferFiller::Clear(this, sizeof(*this));
	}

} * PTradeResultFrame6Output;

/*
 *   Trade-Status
 */
typedef class TTradeStatusTxnInput {
public:
	TIdent acct_id;

#ifdef ENABLE_DEFAULT_CONSTRUCTOR_ON_TXN_INPUT_OUTPUT
	TTradeStatusTxnInput() {
		Clear();
	}
#endif

	inline void Clear() {
		BufferFiller::Clear(this, sizeof(*this));
	}

} * PTradeStatusTxnInput,
    TTradeStatusFrame1Input,  // Single-Frame transaction
    *PTradeStatusFrame1Input; // Single-Frame transaction

typedef class TTradeStatusTxnOutput {
public:
	TTrade trade_id[max_trade_status_len];
	INT32 status;
	char status_name[max_trade_status_len][cST_NAME_len + 1];

#ifdef ENABLE_DEFAULT_CONSTRUCTOR_ON_TXN_INPUT_OUTPUT
	TTradeStatusTxnOutput() {
		Clear();
	}
#endif

	inline void Clear() {
		BufferFiller::Clear(this, sizeof(*this));
	}

} * PTradeStatusTxnOutput;

typedef class TTradeStatusFrame1Output {
public:
	double charge[max_trade_status_len];
	TTrade trade_id[max_trade_status_len];
	INT32 trade_qty[max_trade_status_len];
	INT32 num_found;
	TIMESTAMP_STRUCT trade_dts[max_trade_status_len];
	char ex_name[max_trade_status_len][cEX_NAME_len + 1];
	char exec_name[max_trade_status_len][cEXEC_NAME_len + 1];
	char s_name[max_trade_status_len][cS_NAME_len + 1];
	char status_name[max_trade_status_len][cST_NAME_len + 1];
	char symbol[max_trade_status_len][cSYMBOL_len + 1];
	char type_name[max_trade_status_len][cTT_NAME_len + 1];
	char broker_name[cB_NAME_len + 1];
	char cust_f_name[cF_NAME_len + 1];
	char cust_l_name[cL_NAME_len + 1];

	TTradeStatusFrame1Output() {
		Clear();
	}

	inline void Clear() {
		BufferFiller::Clear(this, sizeof(*this));
	}

} * PTradeStatusFrame1Output;

/*
 *   Trade-Update
 */
typedef class TTradeUpdateTxnInput {
public:
	TTrade trade_id[TradeUpdateFrame1MaxRows];
	TIdent acct_id;
	TIdent max_acct_id;
	INT32 frame_to_execute; // which of the frames to execute
	INT32 max_trades;
	INT32 max_updates;
	TIMESTAMP_STRUCT end_trade_dts;
	TIMESTAMP_STRUCT start_trade_dts;
	char symbol[cSYMBOL_len + 1];

#ifdef ENABLE_DEFAULT_CONSTRUCTOR_ON_TXN_INPUT_OUTPUT
	TTradeUpdateTxnInput() {
		Clear();
	}
#endif

	inline void Clear() {
		BufferFiller::Clear(this, sizeof(*this));
	}

} * PTradeUpdateTxnInput;

typedef class TTradeUpdateTxnOutput {
public:
	TTrade trade_list[TradeUpdateMaxRows];
	INT32 frame_executed; // confirmation of which frame was executed
	INT32 num_found;
	INT32 num_updated;
	INT32 status;
	bool is_cash[TradeUpdateMaxRows];
	bool is_market[TradeUpdateMaxRows];

#ifdef ENABLE_DEFAULT_CONSTRUCTOR_ON_TXN_INPUT_OUTPUT
	TTradeUpdateTxnOutput() {
		Clear();
	}
#endif

	inline void Clear() {
		BufferFiller::Clear(this, sizeof(*this));
	}

} * PTradeUpdateTxnOutput;

typedef class TTradeUpdateFrame1Input {
public:
	TTrade trade_id[TradeUpdateFrame1MaxRows];
	INT32 max_trades;
	INT32 max_updates;

	TTradeUpdateFrame1Input() {
		Clear();
	}

	inline void Clear() {
		BufferFiller::Clear(this, sizeof(*this));
	}

} * PTradeUpdateFrame1Input;

typedef class TTradeUpdateFrame1TradeInfo {
public:
	double bid_price;
	double cash_transaction_amount;
	double settlement_amount;
	double trade_price;
	bool is_cash;
	bool is_market;
	TIMESTAMP_STRUCT trade_history_dts[TradeUpdateMaxTradeHistoryRowsReturned];
	TIMESTAMP_STRUCT cash_transaction_dts;
	TIMESTAMP_STRUCT settlement_cash_due_date;
	char trade_history_status_id[TradeUpdateMaxTradeHistoryRowsReturned][cTH_ST_ID_len + 1];
	char cash_transaction_name[cCT_NAME_len + 1];
	char exec_name[cEXEC_NAME_len + 1];
	char settlement_cash_type[cSE_CASH_TYPE_len + 1];
	DB_INDICATOR trade_history_dts_ind[TradeUpdateMaxTradeHistoryRowsReturned];
	DB_INDICATOR
	trade_history_status_id_ind[TradeUpdateMaxTradeHistoryRowsReturned];
	DB_INDICATOR bid_price_ind;
	DB_INDICATOR cash_transaction_amount_ind;
	DB_INDICATOR cash_transaction_dts_ind;
	DB_INDICATOR cash_transaction_name_ind;
	DB_INDICATOR exec_name_ind;
	DB_INDICATOR is_cash_ind;
	DB_INDICATOR is_market_ind;
	DB_INDICATOR settlement_amount_ind;
	DB_INDICATOR settlement_cash_due_date_ind;
	DB_INDICATOR settlement_cash_type_ind;
	DB_INDICATOR trade_price_ind;

	TTradeUpdateFrame1TradeInfo() {
		Clear();
	}

	inline void Clear() {
		BufferFiller::Clear(this, sizeof(*this));
	}

} * PTradeUpdateFrame1TradeInfo;

typedef class TTradeUpdateFrame1Output {
public:
	INT32 num_found;
	INT32 num_updated;
	TTradeUpdateFrame1TradeInfo trade_info[TradeUpdateFrame1MaxRows];

	TTradeUpdateFrame1Output() {
		Clear();
	}

	inline void Clear() {
		BufferFiller::Clear(this, sizeof(*this));
	}

} * PTradeUpdateFrame1Output;

typedef class TTradeUpdateFrame2Input {
public:
	TIdent acct_id;
	INT32 max_trades;
	INT32 max_updates;
	TIMESTAMP_STRUCT end_trade_dts;
	TIMESTAMP_STRUCT start_trade_dts;

	TTradeUpdateFrame2Input() {
		Clear();
	}

	inline void Clear() {
		BufferFiller::Clear(this, sizeof(*this));
	}

} * PTradeUpdateFrame2Input;

typedef class TTradeUpdateFrame2TradeInfo {
public:
	double bid_price;
	double cash_transaction_amount;
	double settlement_amount;
	double trade_price;
	TTrade trade_id;
	bool is_cash;
	TIMESTAMP_STRUCT trade_history_dts[TradeUpdateMaxTradeHistoryRowsReturned];
	TIMESTAMP_STRUCT cash_transaction_dts;
	TIMESTAMP_STRUCT settlement_cash_due_date;
	char trade_history_status_id[TradeUpdateMaxTradeHistoryRowsReturned][cTH_ST_ID_len + 1];
	char cash_transaction_name[cCT_NAME_len + 1];
	char exec_name[cEXEC_NAME_len + 1];
	char settlement_cash_type[cSE_CASH_TYPE_len + 1];
	DB_INDICATOR trade_history_dts_ind[TradeUpdateMaxTradeHistoryRowsReturned];
	DB_INDICATOR
	trade_history_status_id_ind[TradeUpdateMaxTradeHistoryRowsReturned];
	DB_INDICATOR bid_price_ind;
	DB_INDICATOR cash_transaction_amount_ind;
	DB_INDICATOR cash_transaction_dts_ind;
	DB_INDICATOR cash_transaction_name_ind;
	DB_INDICATOR exec_name_ind;
	DB_INDICATOR is_cash_ind;
	DB_INDICATOR settlement_amount_ind;
	DB_INDICATOR settlement_cash_due_date_ind;
	DB_INDICATOR settlement_cash_type_ind;
	DB_INDICATOR trade_id_ind;
	DB_INDICATOR trade_price_ind;

	TTradeUpdateFrame2TradeInfo() {
		Clear();
	}

	inline void Clear() {
		BufferFiller::Clear(this, sizeof(*this));
	}

} * PTradeUpdateFrame2TradeInfo;

typedef class TTradeUpdateFrame2Output {
public:
	INT32 num_found;
	INT32 num_updated;
	TTradeUpdateFrame2TradeInfo trade_info[TradeUpdateFrame2MaxRows];

	TTradeUpdateFrame2Output() {
		Clear();
	}

	inline void Clear() {
		BufferFiller::Clear(this, sizeof(*this));
	}

} * PTradeUpdateFrame2Output;

typedef class TTradeUpdateFrame3Input {
public:
	TIdent max_acct_id;
	INT32 max_trades;
	INT32 max_updates;
	TIMESTAMP_STRUCT end_trade_dts;
	TIMESTAMP_STRUCT start_trade_dts;
	char symbol[cSYMBOL_len + 1];

	TTradeUpdateFrame3Input() {
		Clear();
	}

	inline void Clear() {
		BufferFiller::Clear(this, sizeof(*this));
	}

} * PTradeUpdateFrame3Input;

typedef class TTradeUpdateFrame3TradeInfo {
public:
	double cash_transaction_amount;
	double price;
	double settlement_amount;
	TIdent acct_id;
	TTrade trade_id;
	INT32 quantity;
	bool is_cash;
	TIMESTAMP_STRUCT trade_history_dts[TradeUpdateMaxTradeHistoryRowsReturned];
	TIMESTAMP_STRUCT cash_transaction_dts;
	TIMESTAMP_STRUCT settlement_cash_due_date;
	TIMESTAMP_STRUCT trade_dts;
	char trade_history_status_id[TradeUpdateMaxTradeHistoryRowsReturned][cTH_ST_ID_len + 1];
	char cash_transaction_name[cCT_NAME_len + 1];
	char exec_name[cEXEC_NAME_len + 1];
	char s_name[cS_NAME_len + 1];
	char settlement_cash_type[cSE_CASH_TYPE_len + 1];
	char trade_type[cTT_ID_len + 1];
	char type_name[cTT_NAME_len + 1];
	DB_INDICATOR trade_history_dts_ind[TradeUpdateMaxTradeHistoryRowsReturned];
	DB_INDICATOR
	trade_history_status_id_ind[TradeUpdateMaxTradeHistoryRowsReturned];
	DB_INDICATOR acct_id_ind;
	DB_INDICATOR cash_transaction_amount_ind;
	DB_INDICATOR cash_transaction_dts_ind;
	DB_INDICATOR cash_transaction_name_ind;
	DB_INDICATOR exec_name_ind;
	DB_INDICATOR is_cash_ind;
	DB_INDICATOR price_ind;
	DB_INDICATOR quantity_ind;
	DB_INDICATOR s_name_ind;
	DB_INDICATOR settlement_amount_ind;
	DB_INDICATOR settlement_cash_due_date_ind;
	DB_INDICATOR settlement_cash_type_ind;
	DB_INDICATOR trade_dts_ind;
	DB_INDICATOR trade_id_ind;
	DB_INDICATOR trade_type_ind;
	DB_INDICATOR type_name_ind;

	TTradeUpdateFrame3TradeInfo() {
		Clear();
	}

	inline void Clear() {
		BufferFiller::Clear(this, sizeof(*this));
	}

} * PTradeUpdateFrame3TradeInfo;

typedef class TTradeUpdateFrame3Output {
public:
	INT32 num_found;
	INT32 num_updated;
	TTradeUpdateFrame3TradeInfo trade_info[TradeUpdateFrame3MaxRows];

	TTradeUpdateFrame3Output() {
		Clear();
	}

	inline void Clear() {
		BufferFiller::Clear(this, sizeof(*this));
	}

} * PTradeUpdateFrame3Output;

/*
 *   Trade-Cleanup
 */
typedef class TTradeCleanupTxnInput {
public:
	TTrade start_trade_id;
	char st_canceled_id[cST_ID_len + 1];
	char st_pending_id[cST_ID_len + 1];
	char st_submitted_id[cST_ID_len + 1];

#ifdef ENABLE_DEFAULT_CONSTRUCTOR_ON_TXN_INPUT_OUTPUT
	TTradeCleanupTxnInput() {
		Clear();
	}
#endif

	inline void Clear() {
		BufferFiller::Clear(this, sizeof(*this));
	}

} * PTradeCleanupTxnInput,
    TTradeCleanupFrame1Input,  // Single-Frame transaction
    *PTradeCleanupFrame1Input; // Single-Frame transaction

typedef class TTradeCleanupTxnOutput {
public:
	INT32 status;

#ifdef ENABLE_DEFAULT_CONSTRUCTOR_ON_TXN_INPUT_OUTPUT
	TTradeCleanupTxnOutput() {
		Clear();
	}
#endif

	inline void Clear() {
		BufferFiller::Clear(this, sizeof(*this));
	}

} * PTradeCleanupTxnOutput;

} // namespace TPCE

#endif // #ifndef TXN_HARNESS_STRUCTS_H
