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
 * - Sergey Vasilevskiy, Doug Johnson, Matt Emmerton, Christopher Chan-Nui
 */

/******************************************************************************
 *   Description:        This file is used to "expose" the configurable driver
 *                       parameters. These values may be set by test sponsors
 *                       for testing and prototyping. The default values
 *                       represent those that must be used for a compliant run.
 ******************************************************************************/

#ifndef DRIVER_PARAM_SETTINGS_H
#define DRIVER_PARAM_SETTINGS_H

#include <iostream>
#include <iomanip> // for log message formatting
#include <sstream> // for log message construction

#include "utilities/EGenUtilities_stdafx.h"

namespace TPCE {

// CHECK tests for CheckValid and CheckCompliant, use macros so we can get a
// textual representation of the tested values, these tests could be
// significantly condensed but we're trying for readibility here...
#define DRIVERPARAM_CHECK_EQUAL(name, lhs, rhs)                                                                        \
	if ((lhs) != (rhs)) {                                                                                              \
		std::ostringstream strm;                                                                                       \
		strm << #lhs << "(" << (lhs) << ") != " << #rhs << "(" << (rhs) << ")";                                        \
		throw CCheckErr(name, strm.str());                                                                             \
	}

#define DRIVERPARAM_CHECK_GE(name, lhs, rhs)                                                                           \
	if ((lhs) < (rhs)) {                                                                                               \
		std::ostringstream strm;                                                                                       \
		strm << #lhs << "(" << (lhs) << ") < " << #rhs << "(" << (rhs) << ")";                                         \
		throw CCheckErr(name, strm.str());                                                                             \
	}

#define DRIVERPARAM_CHECK_LE(name, lhs, rhs)                                                                           \
	if ((lhs) > (rhs)) {                                                                                               \
		std::ostringstream strm;                                                                                       \
		strm << #lhs << "(" << (lhs) << ") > " << #rhs << "(" << (rhs) << ")";                                         \
		throw CCheckErr(name, strm.str());                                                                             \
	}

#define DRIVERPARAM_CHECK_BETWEEN(name, lhs, minval, maxval)                                                           \
	DRIVERPARAM_CHECK_GE(name, lhs, minval)                                                                            \
	DRIVERPARAM_CHECK_LE(name, lhs, maxval)

#define DRIVERPARAM_CHECK_DEFAULT(name)                                                                                \
	if (cur.name != dft.name) {                                                                                        \
		std::ostringstream strm;                                                                                       \
		strm << #name << "(" << cur.name << ") != " << dft.name;                                                       \
		throw CCheckErr(#name, strm.str());                                                                            \
	}

/******************************************************************************
 *   Parameter Base Class Template
 ******************************************************************************/
template <typename T> class CParametersWithoutDefaults {
public:
	T cur;

	CParametersWithoutDefaults() {
	}
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
	virtual ~CParametersWithoutDefaults(){};

	virtual void CheckValid(void) = 0;
	virtual void CheckCompliant(void) = 0;
	bool IsValid(void) {
		try {
			CheckValid();
			return true;
		} catch (CCheckErr) {
			throw;
			return false;
		}
	}

	bool IsCompliant(void) {
		// This will always return true for the classes defined in this file
		try {
			CheckCompliant();
			return true;
		} catch (CCheckErr) {
			throw;
			return false;
		}
	}
};

template <typename T, typename T2> class CParametersWithDefaults {
	// protected:
public:
	T dft;
	T2 state;

public:
	T cur;

	CParametersWithDefaults() {
	}
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
	virtual ~CParametersWithDefaults(){};

	void Initialize(void) {
		InitializeDefaults();
		SetToDefaults();
	}

	void SetToDefaults(void) {
		cur = dft;
		CheckDefaults();
	}

	virtual void InitializeDefaults(void) {
	}
	virtual void CheckDefaults(void) {
	}
	virtual void CheckValid(void) = 0;
	virtual void CheckCompliant(void) = 0;
	bool IsValid(void) {
		try {
			CheckValid();
			return true;
		} catch (CCheckErr) {
			throw;
			return false;
		}
	}

	bool IsCompliant(void) {
		try {
			CheckCompliant();
			return true;
		} catch (CCheckErr) {
			throw;
			return false;
		}
	}
};

/******************************************************************************
 *   Parameter Structures (Data)
 ******************************************************************************/
typedef struct TBrokerVolumeSettings {
} * PBrokerVolumeSettings;

typedef struct TCustomerPositionSettings {
	INT32 by_cust_id;  // percentage
	INT32 by_tax_id;   // percentage
	INT32 get_history; // percentage
} * PCustomerPositionSettings;

typedef struct TMarketWatchSettings {
	INT32 by_acct_id;    // percentage
	INT32 by_industry;   // percentage
	INT32 by_watch_list; // percentage
} * PMarketWatchSettings;

typedef struct TSecurityDetailSettings {
	INT32 LOBAccessPercentage;
} * PSecurityDetailSettings;

typedef struct TTradeLookupSettings {
	INT32 do_frame1; // percentage
	INT32 do_frame2; // percentage
	INT32 do_frame3; // percentage
	INT32 do_frame4; // percentage

	INT32 MaxRowsFrame1; // Max number of trades for frame

	INT32 BackOffFromEndTimeFrame2; // Used to cap time interval generated.
	INT32 MaxRowsFrame2;            // Max number of trades for frame

	INT32 BackOffFromEndTimeFrame3; // Used to cap time interval generated.
	INT32 MaxRowsFrame3;            // Max number of trades for frame

	INT32 BackOffFromEndTimeFrame4; // Used to cap time interval generated.
	INT32 MaxRowsFrame4;            // Max number of rows for frame
} * PTradeLookupSettings;

typedef struct TTradeOrderSettings {
	INT32 market;
	INT32 limit;
	INT32 stop_loss;
	INT32 security_by_name;
	INT32 security_by_symbol;
	INT32 buy_orders;
	INT32 sell_orders;
	INT32 lifo;
	INT32 exec_is_owner;
	INT32 rollback;
	INT32 type_is_margin;
} * PTradeOrderSettings;

typedef struct TTradeUpdateSettings {
	INT32 do_frame1; // percentage
	INT32 do_frame2; // percentage
	INT32 do_frame3; // percentage

	INT32 MaxRowsFrame1;         // Max number of trades for frame
	INT32 MaxRowsToUpdateFrame1; // Max number of rows to update

	INT32 BackOffFromEndTimeFrame2; // Used to cap time interval generated.
	INT32 MaxRowsFrame2;            // Max number of trades for frame
	INT32 MaxRowsToUpdateFrame2;    // Max number of rows to update

	INT32 BackOffFromEndTimeFrame3; // Used to cap time interval generated.
	INT32 MaxRowsFrame3;            // Max number of trades for frame
	INT32 MaxRowsToUpdateFrame3;    // Max number of rows to update
} * PTradeUpdateSettings;

typedef struct TTxnMixGeneratorSettings {
	// Market-Feed and Trade-Result settings don't really alter the mix.
	// They are done as a by-product of Trade-Orders. However, the values
	// still need to be set correctly because they get used when generating
	// the random number for selecting the other transaction types.
	INT32 BrokerVolumeMixLevel;
	INT32 CustomerPositionMixLevel;
	INT32 MarketFeedMixLevel;
	INT32 MarketWatchMixLevel;
	INT32 SecurityDetailMixLevel;
	INT32 TradeLookupMixLevel;
	INT32 TradeOrderMixLevel;
	INT32 TradeResultMixLevel;
	INT32 TradeStatusMixLevel;
	INT32 TradeUpdateMixLevel;

	// Transaction mix levels are expressed out of a total of 1000.
	INT32 TransactionMixTotal;
} * PTxnMixGeneratorSettings;

typedef struct TLoaderSettings {
	TIdent iConfiguredCustomerCount;
	TIdent iActiveCustomerCount;
	INT32 iScaleFactor;
	INT32 iDaysOfInitialTrades;
	TIdent iStartingCustomer;
	TIdent iCustomerCount;
} * pLoaderSettings;

typedef struct TDriverGlobalSettings {
	TIdent iConfiguredCustomerCount;
	TIdent iActiveCustomerCount;
	INT32 iScaleFactor;
	INT32 iDaysOfInitialTrades;
} * PDriverGlobalSettings;

typedef struct TDriverCESettings {
	UINT32 UniqueId;
	RNGSEED TxnMixRNGSeed;
	RNGSEED TxnInputRNGSeed;
} * PDriverCESettings;

typedef struct TDriverCEPartitionSettings {
	TIdent iMyStartingCustomerId;
	TIdent iMyCustomerCount;
	INT32 iPartitionPercent;
} * PDriverCEPartitionSettings;

typedef struct TDriverMEESettings {
	UINT32 UniqueId;
	RNGSEED RNGSeed;
	RNGSEED TickerTapeRNGSeed;
	RNGSEED TradingFloorRNGSeed;
} * PDriverMEESettings;

typedef struct TDriverDMSettings {
	UINT32 UniqueId;
	RNGSEED RNGSeed;
} * PDriverDMSettings;

/******************************************************************************
 *   Parameter Structures (Boolean "Is Default" State)
 ******************************************************************************/
struct TBrokerVolumeSettingsState {};

struct TCustomerPositionSettingsState {
	bool by_cust_id;  // percentage
	bool by_tax_id;   // percentage
	bool get_history; // percentage
};

struct TMarketWatchSettingsState {
	bool by_acct_id;    // percentage
	bool by_industry;   // percentage
	bool by_watch_list; // percentage
};

struct TSecurityDetailSettingsState {
	bool LOBAccessPercentage;
};

struct TTradeLookupSettingsState {
	bool do_frame1; // percentage
	bool do_frame2; // percentage
	bool do_frame3; // percentage
	bool do_frame4; // percentage

	bool MaxRowsFrame1; // Max number of trades for frame

	bool BackOffFromEndTimeFrame2; // Used to cap time interval generated.
	bool MaxRowsFrame2;            // Max number of trades for frame

	bool BackOffFromEndTimeFrame3; // Used to cap time interval generated.
	bool MaxRowsFrame3;            // Max number of trades for frame

	bool BackOffFromEndTimeFrame4; // Used to cap time interval generated.
	bool MaxRowsFrame4;            // Max number of rows for frame
};

struct TTradeOrderSettingsState {
	bool market;
	bool limit;
	bool stop_loss;
	bool security_by_name;
	bool security_by_symbol;
	bool buy_orders;
	bool sell_orders;
	bool lifo;
	bool exec_is_owner;
	bool rollback;
	bool type_is_margin;
};

struct TTradeUpdateSettingsState {
	bool do_frame1; // percentage
	bool do_frame2; // percentage
	bool do_frame3; // percentage

	bool MaxRowsFrame1;         // Max number of trades for frame
	bool MaxRowsToUpdateFrame1; // Max number of rows to update

	bool BackOffFromEndTimeFrame2; // Used to cap time interval generated.
	bool MaxRowsFrame2;            // Max number of trades for frame
	bool MaxRowsToUpdateFrame2;    // Max number of rows to update

	bool BackOffFromEndTimeFrame3; // Used to cap time interval generated.
	bool MaxRowsFrame3;            // Max number of trades for frame
	bool MaxRowsToUpdateFrame3;    // Max number of rows to update
};

struct TTxnMixGeneratorSettingsState {
	bool BrokerVolumeMixLevel;
	bool CustomerPositionMixLevel;
	bool MarketWatchMixLevel;
	bool SecurityDetailMixLevel;
	bool TradeLookupMixLevel;
	bool TradeOrderMixLevel;
	bool TradeStatusMixLevel;
	bool TradeUpdateMixLevel;
	bool TransactionMixTotal;
};

struct TLoaderSettingsState {
	bool iConfiguredCustomerCount;
	bool iActiveCustomerCount;
	bool iScaleFactor;
	bool iDaysOfInitialTrades;
	bool iStartingCustomer;
	bool iCustomerCount;
};

struct TDriverCEPartitionSettingsState {
	bool iPartitionPercent;
};

struct TDriverGlobalSettingsState {
	bool iConfiguredCustomerCount;
	bool iActiveCustomerCount;
	bool iScaleFactor;
	bool iDaysOfInitialTrades;
};

/******************************************************************************
 *   Parameter Derived Class / Template Instantiation
 ******************************************************************************/
class CBrokerVolumeSettings
    : public CParametersWithDefaults<struct TBrokerVolumeSettings, struct TBrokerVolumeSettingsState> {
public:
	CBrokerVolumeSettings() {
		Initialize();
	}

	void InitializeDefaults(void) {
	}

	void CheckDefaults(void) {
	}

	void CheckValid(void) {
	}

	void CheckCompliant(void) {
	}
};

class CCustomerPositionSettings
    : public CParametersWithDefaults<struct TCustomerPositionSettings, struct TCustomerPositionSettingsState> {
public:
	CCustomerPositionSettings() {
		Initialize();
	}

	void InitializeDefaults(void) {
		dft.by_cust_id = 50;
		dft.by_tax_id = 50;
		dft.get_history = 50;
	}

	void CheckDefaults(void) {
		state.by_cust_id = (cur.by_cust_id == dft.by_cust_id);
		state.by_tax_id = (cur.by_tax_id == dft.by_tax_id);
		state.get_history = (cur.get_history == dft.get_history);
	}

	void CheckValid(void) {
		DRIVERPARAM_CHECK_BETWEEN("by_cust_id", cur.by_cust_id, 0, 100);
		DRIVERPARAM_CHECK_BETWEEN("by_tax_id", cur.by_tax_id, 0, 100);
		DRIVERPARAM_CHECK_EQUAL("by_*_id total", cur.by_cust_id + cur.by_tax_id, 100);
		DRIVERPARAM_CHECK_BETWEEN("get_history", cur.get_history, 0, 100);
	}

	void CheckCompliant(void) {
		CheckValid();
		DRIVERPARAM_CHECK_DEFAULT(by_cust_id);
		DRIVERPARAM_CHECK_DEFAULT(by_tax_id);
		DRIVERPARAM_CHECK_DEFAULT(get_history);
	}
};

class CMarketWatchSettings
    : public CParametersWithDefaults<struct TMarketWatchSettings, struct TMarketWatchSettingsState> {
public:
	CMarketWatchSettings() {
		Initialize();
	}

	void InitializeDefaults(void) {
		dft.by_acct_id = 35;
		dft.by_industry = 5;
		dft.by_watch_list = 60;
	}

	void CheckDefaults(void) {
		state.by_acct_id = (cur.by_acct_id == dft.by_acct_id);
		state.by_industry = (cur.by_industry == dft.by_industry);
		state.by_watch_list = (cur.by_watch_list == dft.by_watch_list);
	}

	void CheckValid(void) {
		DRIVERPARAM_CHECK_BETWEEN("by_acct_id", cur.by_acct_id, 0, 100);
		DRIVERPARAM_CHECK_BETWEEN("by_industry", cur.by_industry, 0, 100);
		DRIVERPARAM_CHECK_BETWEEN("by_watch_list", cur.by_watch_list, 0, 100);
		DRIVERPARAM_CHECK_EQUAL("by_* total", cur.by_acct_id + cur.by_industry + cur.by_watch_list, 100);
	}

	void CheckCompliant(void) {
		CheckValid();
		DRIVERPARAM_CHECK_DEFAULT(by_acct_id);
		DRIVERPARAM_CHECK_DEFAULT(by_industry);
		DRIVERPARAM_CHECK_DEFAULT(by_watch_list);
	}
};

class CSecurityDetailSettings
    : public CParametersWithDefaults<struct TSecurityDetailSettings, struct TSecurityDetailSettingsState> {
public:
	CSecurityDetailSettings() {
		Initialize();
	}

	void InitializeDefaults(void) {
		dft.LOBAccessPercentage = 1;
	}

	void CheckDefaults(void) {
		state.LOBAccessPercentage = (cur.LOBAccessPercentage == dft.LOBAccessPercentage);
	}

	void CheckValid(void) {
		DRIVERPARAM_CHECK_BETWEEN("LOBAccessPercentage", cur.LOBAccessPercentage, 0, 100);
	}

	void CheckCompliant(void) {
		CheckValid();
		DRIVERPARAM_CHECK_DEFAULT(LOBAccessPercentage);
	}
};

class CTradeLookupSettings
    : public CParametersWithDefaults<struct TTradeLookupSettings, struct TTradeLookupSettingsState> {
public:
	CTradeLookupSettings() {
		Initialize();
	}

	void InitializeDefaults(void) {
		dft.do_frame1 = 30;
		dft.do_frame2 = 30;
		dft.do_frame3 = 30;
		dft.do_frame4 = 10;
		dft.MaxRowsFrame1 = 20;
		dft.BackOffFromEndTimeFrame2 = 4 * 8 * 3600; // four 8-hour days or 32 hours
		dft.MaxRowsFrame2 = 20;
		dft.BackOffFromEndTimeFrame3 = 200 * 60; // 200 minutes
		dft.MaxRowsFrame3 = 20;
		dft.BackOffFromEndTimeFrame4 = 500 * 60; // 30,000 seconds
		dft.MaxRowsFrame4 = 20;
	}

	void CheckDefaults(void) {
		state.do_frame1 = (cur.do_frame1 == dft.do_frame1);
		state.do_frame2 = (cur.do_frame2 == dft.do_frame2);
		state.do_frame3 = (cur.do_frame3 == dft.do_frame3);
		state.do_frame4 = (cur.do_frame4 == dft.do_frame4);
		state.MaxRowsFrame1 = (cur.MaxRowsFrame1 == dft.MaxRowsFrame1);
		state.BackOffFromEndTimeFrame2 = (cur.BackOffFromEndTimeFrame2 == dft.BackOffFromEndTimeFrame2);
		state.MaxRowsFrame2 = (cur.MaxRowsFrame2 == dft.MaxRowsFrame2);
		state.BackOffFromEndTimeFrame3 = (cur.BackOffFromEndTimeFrame3 == dft.BackOffFromEndTimeFrame3);
		state.MaxRowsFrame3 = (cur.MaxRowsFrame3 == dft.MaxRowsFrame3);
		state.BackOffFromEndTimeFrame4 = (cur.BackOffFromEndTimeFrame4 == dft.BackOffFromEndTimeFrame4);
		state.MaxRowsFrame4 = (cur.MaxRowsFrame4 == dft.MaxRowsFrame4);
	}

	void CheckValid(void) {
		DRIVERPARAM_CHECK_BETWEEN("do_frame1", cur.do_frame1, 0, 100);
		DRIVERPARAM_CHECK_BETWEEN("do_frame2", cur.do_frame2, 0, 100);
		DRIVERPARAM_CHECK_BETWEEN("do_frame3", cur.do_frame3, 0, 100);
		DRIVERPARAM_CHECK_BETWEEN("do_frame4", cur.do_frame4, 0, 100);
		DRIVERPARAM_CHECK_EQUAL("do_frame* total", cur.do_frame1 + cur.do_frame2 + cur.do_frame3 + cur.do_frame4, 100);
		DRIVERPARAM_CHECK_LE("MaxRowsFrame1", cur.MaxRowsFrame1, TradeLookupFrame1MaxRows);
		DRIVERPARAM_CHECK_LE("MaxRowsFrame2", cur.MaxRowsFrame2, TradeLookupFrame2MaxRows);
		DRIVERPARAM_CHECK_LE("MaxRowsFrame3", cur.MaxRowsFrame3, TradeLookupFrame3MaxRows);
		DRIVERPARAM_CHECK_LE("MaxRowsFrame4", cur.MaxRowsFrame4, TradeLookupFrame4MaxRows);
	}

	void CheckCompliant(void) {
		CheckValid();
		DRIVERPARAM_CHECK_DEFAULT(do_frame1);
		DRIVERPARAM_CHECK_DEFAULT(do_frame2);
		DRIVERPARAM_CHECK_DEFAULT(do_frame3);
		DRIVERPARAM_CHECK_DEFAULT(do_frame4);
		DRIVERPARAM_CHECK_DEFAULT(MaxRowsFrame1);
		DRIVERPARAM_CHECK_DEFAULT(BackOffFromEndTimeFrame2);
		DRIVERPARAM_CHECK_DEFAULT(MaxRowsFrame2);
		DRIVERPARAM_CHECK_DEFAULT(BackOffFromEndTimeFrame3);
		DRIVERPARAM_CHECK_DEFAULT(MaxRowsFrame3);
		DRIVERPARAM_CHECK_DEFAULT(BackOffFromEndTimeFrame4);
		DRIVERPARAM_CHECK_DEFAULT(MaxRowsFrame4);
	}
};

class CTradeOrderSettings
    : public CParametersWithDefaults<struct TTradeOrderSettings, struct TTradeOrderSettingsState> {
public:
	CTradeOrderSettings() {
		Initialize();
	}

	void InitializeDefaults(void) {
		dft.market = 60;
		dft.limit = 40;
		dft.stop_loss = 50;
		dft.security_by_name = 40;
		dft.security_by_symbol = 60;
		dft.buy_orders = 50;
		dft.sell_orders = 50;
		dft.lifo = 35;
		dft.exec_is_owner = 90;
		dft.rollback = 1;
		dft.type_is_margin = 8;
	}

	void CheckDefaults(void) {
		state.market = (cur.market == dft.market);
		state.limit = (cur.limit == dft.limit);
		state.stop_loss = (cur.stop_loss == dft.stop_loss);
		state.security_by_name = (cur.security_by_name == dft.security_by_name);
		state.security_by_symbol = (cur.security_by_symbol == dft.security_by_symbol);
		state.buy_orders = (cur.buy_orders == dft.buy_orders);
		state.sell_orders = (cur.sell_orders == dft.sell_orders);
		state.lifo = (cur.lifo == dft.lifo);
		state.exec_is_owner = (cur.exec_is_owner == dft.exec_is_owner);
		state.rollback = (cur.rollback == dft.rollback);
		state.type_is_margin = (cur.type_is_margin == dft.type_is_margin);
	}

	void CheckValid(void) {
		DRIVERPARAM_CHECK_BETWEEN("market", cur.market, 0, 100);
		DRIVERPARAM_CHECK_BETWEEN("limit", cur.limit, 0, 100);
		DRIVERPARAM_CHECK_EQUAL("market or limit total", cur.market + cur.limit, 100);
		DRIVERPARAM_CHECK_BETWEEN("stop_loss", cur.stop_loss, 0, 100);
		DRIVERPARAM_CHECK_BETWEEN("security_by_name", cur.security_by_name, 0, 100);
		DRIVERPARAM_CHECK_BETWEEN("security_by_symbol", cur.security_by_symbol, 0, 100);
		DRIVERPARAM_CHECK_EQUAL("security_by_* total", cur.security_by_name + cur.security_by_symbol, 100);
		DRIVERPARAM_CHECK_BETWEEN("buy_orders", cur.buy_orders, 0, 100);
		DRIVERPARAM_CHECK_BETWEEN("sell_orders", cur.sell_orders, 0, 100);
		DRIVERPARAM_CHECK_EQUAL("*_orders total", cur.buy_orders + cur.sell_orders, 100);
		DRIVERPARAM_CHECK_BETWEEN("lifo", cur.lifo, 0, 100);
		DRIVERPARAM_CHECK_BETWEEN("exec_is_owner", cur.exec_is_owner, 0, 100);
		DRIVERPARAM_CHECK_BETWEEN("rollback", cur.rollback, 0, 100);
		DRIVERPARAM_CHECK_BETWEEN("type_is_margin", cur.type_is_margin, 0, 100);
	}

	void CheckCompliant(void) {
		CheckValid();
		DRIVERPARAM_CHECK_BETWEEN("exec_is_owner", cur.exec_is_owner, 60, 100);
		DRIVERPARAM_CHECK_DEFAULT(market);
		DRIVERPARAM_CHECK_DEFAULT(limit);
		DRIVERPARAM_CHECK_DEFAULT(stop_loss);
		DRIVERPARAM_CHECK_DEFAULT(security_by_name);
		DRIVERPARAM_CHECK_DEFAULT(security_by_symbol);
		DRIVERPARAM_CHECK_DEFAULT(buy_orders);
		DRIVERPARAM_CHECK_DEFAULT(sell_orders);
		DRIVERPARAM_CHECK_DEFAULT(lifo);
		DRIVERPARAM_CHECK_DEFAULT(exec_is_owner);
		DRIVERPARAM_CHECK_DEFAULT(rollback);
		DRIVERPARAM_CHECK_DEFAULT(type_is_margin);
	}
};

class CTradeUpdateSettings
    : public CParametersWithDefaults<struct TTradeUpdateSettings, struct TTradeUpdateSettingsState> {
public:
	CTradeUpdateSettings() {
		Initialize();
	}

	void InitializeDefaults(void) {
		dft.do_frame1 = 33;
		dft.do_frame2 = 33;
		dft.do_frame3 = 34;
		dft.MaxRowsFrame1 = 20;
		dft.MaxRowsToUpdateFrame1 = 20;
		dft.MaxRowsFrame2 = 20;
		dft.MaxRowsToUpdateFrame2 = 20;
		dft.BackOffFromEndTimeFrame2 = 4 * 8 * 3600; // four 8-hour days or 32 hours
		dft.MaxRowsFrame3 = 20;
		dft.MaxRowsToUpdateFrame3 = 20;
		dft.BackOffFromEndTimeFrame3 = 200 * 60; // 200 minutes
	}

	void CheckDefaults(void) {
		state.do_frame1 = (cur.do_frame1 == dft.do_frame1);
		state.do_frame2 = (cur.do_frame2 == dft.do_frame2);
		state.do_frame3 = (cur.do_frame3 == dft.do_frame3);
		state.MaxRowsFrame1 = (cur.MaxRowsFrame1 == dft.MaxRowsFrame1);
		state.MaxRowsToUpdateFrame1 = (cur.MaxRowsToUpdateFrame1 == dft.MaxRowsToUpdateFrame1);
		state.MaxRowsFrame2 = (cur.MaxRowsFrame2 == dft.MaxRowsFrame2);
		state.MaxRowsToUpdateFrame2 = (cur.MaxRowsToUpdateFrame2 == dft.MaxRowsToUpdateFrame2);
		state.BackOffFromEndTimeFrame2 = (cur.BackOffFromEndTimeFrame2 == dft.BackOffFromEndTimeFrame2);
		state.MaxRowsFrame3 = (cur.MaxRowsFrame3 == dft.MaxRowsFrame3);
		state.MaxRowsToUpdateFrame3 = (cur.MaxRowsToUpdateFrame3 == dft.MaxRowsToUpdateFrame3);
		state.BackOffFromEndTimeFrame3 = (cur.BackOffFromEndTimeFrame3 == dft.BackOffFromEndTimeFrame3);
	}

	void CheckValid(void) {
		DRIVERPARAM_CHECK_BETWEEN("do_frame1", cur.do_frame1, 0, 100);
		DRIVERPARAM_CHECK_BETWEEN("do_frame2", cur.do_frame2, 0, 100);
		DRIVERPARAM_CHECK_BETWEEN("do_frame3", cur.do_frame3, 0, 100);
		DRIVERPARAM_CHECK_EQUAL("do_frame* total", cur.do_frame1 + cur.do_frame2 + cur.do_frame3, 100);
		DRIVERPARAM_CHECK_LE("MaxRowsFrame1", cur.MaxRowsFrame1, TradeUpdateFrame1MaxRows);
		DRIVERPARAM_CHECK_LE("MaxRowsFrame2", cur.MaxRowsFrame2, TradeUpdateFrame2MaxRows);
		DRIVERPARAM_CHECK_LE("MaxRowsFrame3", cur.MaxRowsFrame3, TradeUpdateFrame3MaxRows);
		DRIVERPARAM_CHECK_LE("MaxRowsToUpdateFrame1", cur.MaxRowsToUpdateFrame1, TradeUpdateFrame1MaxRows);
		DRIVERPARAM_CHECK_LE("MaxRowsToUpdateFrame2", cur.MaxRowsToUpdateFrame2, TradeUpdateFrame2MaxRows);
		DRIVERPARAM_CHECK_LE("MaxRowsToUpdateFrame3", cur.MaxRowsToUpdateFrame3, TradeUpdateFrame3MaxRows);
	}

	void CheckCompliant(void) {
		CheckValid();

		DRIVERPARAM_CHECK_DEFAULT(do_frame1);
		DRIVERPARAM_CHECK_DEFAULT(do_frame2);
		DRIVERPARAM_CHECK_DEFAULT(do_frame3);
		DRIVERPARAM_CHECK_DEFAULT(MaxRowsFrame1);
		DRIVERPARAM_CHECK_DEFAULT(MaxRowsToUpdateFrame1);
		DRIVERPARAM_CHECK_DEFAULT(MaxRowsFrame2);
		DRIVERPARAM_CHECK_DEFAULT(MaxRowsToUpdateFrame2);
		DRIVERPARAM_CHECK_DEFAULT(BackOffFromEndTimeFrame2);
		DRIVERPARAM_CHECK_DEFAULT(MaxRowsFrame3);
		DRIVERPARAM_CHECK_DEFAULT(MaxRowsToUpdateFrame3);
		DRIVERPARAM_CHECK_DEFAULT(BackOffFromEndTimeFrame3);
	}
};

class CTxnMixGeneratorSettings
    : public CParametersWithDefaults<struct TTxnMixGeneratorSettings, struct TTxnMixGeneratorSettingsState> {
public:
	CTxnMixGeneratorSettings() {
		Initialize();
	}

	void InitializeDefaults(void) {
		dft.BrokerVolumeMixLevel = 49;
		dft.CustomerPositionMixLevel = 130;
		dft.MarketWatchMixLevel = 180;
		dft.SecurityDetailMixLevel = 140;
		dft.TradeLookupMixLevel = 80;
		dft.TradeOrderMixLevel = 101;
		dft.TradeStatusMixLevel = 190;
		dft.TradeUpdateMixLevel = 20;
	}

	void CheckDefaults(void) {
		state.BrokerVolumeMixLevel = (cur.BrokerVolumeMixLevel == dft.BrokerVolumeMixLevel);
		state.CustomerPositionMixLevel = (cur.CustomerPositionMixLevel == dft.CustomerPositionMixLevel);
		state.MarketWatchMixLevel = (cur.MarketWatchMixLevel == dft.MarketWatchMixLevel);
		state.SecurityDetailMixLevel = (cur.SecurityDetailMixLevel == dft.SecurityDetailMixLevel);
		state.TradeLookupMixLevel = (cur.TradeLookupMixLevel == dft.TradeLookupMixLevel);
		state.TradeOrderMixLevel = (cur.TradeOrderMixLevel == dft.TradeOrderMixLevel);
		state.TradeStatusMixLevel = (cur.TradeStatusMixLevel == dft.TradeStatusMixLevel);
		state.TradeUpdateMixLevel = (cur.TradeUpdateMixLevel == dft.TradeUpdateMixLevel);
	}

	void CheckValid(void) {
		DRIVERPARAM_CHECK_GE("BrokerVolumeMixLevel", cur.BrokerVolumeMixLevel, 0);
		DRIVERPARAM_CHECK_GE("CustomerPositionMixLevel", cur.CustomerPositionMixLevel, 0);
		DRIVERPARAM_CHECK_GE("MarketWatchMixLevel", cur.MarketWatchMixLevel, 0);
		DRIVERPARAM_CHECK_GE("SecurityDetailMixLevel", cur.SecurityDetailMixLevel, 0);
		DRIVERPARAM_CHECK_GE("TradeLookupMixLevel", cur.TradeLookupMixLevel, 0);
		DRIVERPARAM_CHECK_GE("TradeOrderMixLevel", cur.TradeOrderMixLevel, 0);
		DRIVERPARAM_CHECK_GE("TradeStatusMixLevel", cur.TradeStatusMixLevel, 0);
		DRIVERPARAM_CHECK_GE("TradeUpdateMixLevel", cur.TradeUpdateMixLevel, 0);
	}

	void CheckCompliant(void) {
		CheckValid();
		DRIVERPARAM_CHECK_DEFAULT(BrokerVolumeMixLevel);
		DRIVERPARAM_CHECK_DEFAULT(CustomerPositionMixLevel);
		DRIVERPARAM_CHECK_DEFAULT(MarketWatchMixLevel);
		DRIVERPARAM_CHECK_DEFAULT(SecurityDetailMixLevel);
		DRIVERPARAM_CHECK_DEFAULT(TradeLookupMixLevel);
		DRIVERPARAM_CHECK_DEFAULT(TradeOrderMixLevel);
		DRIVERPARAM_CHECK_DEFAULT(TradeStatusMixLevel);
		DRIVERPARAM_CHECK_DEFAULT(TradeUpdateMixLevel);
	}
};

class CLoaderSettings : public CParametersWithDefaults<struct TLoaderSettings, struct TLoaderSettingsState> {
public:
	CLoaderSettings(TIdent iConfiguredCustomerCount, TIdent iActiveCustomerCount, TIdent iStartingCustomer,
	                TIdent iCustomerCount, INT32 iScaleFactor, INT32 iDaysOfInitialTrades) {
		Initialize();

		cur.iConfiguredCustomerCount = iConfiguredCustomerCount;
		cur.iActiveCustomerCount = iActiveCustomerCount;
		cur.iStartingCustomer = iStartingCustomer;
		cur.iCustomerCount = iCustomerCount;
		cur.iScaleFactor = iScaleFactor;
		cur.iDaysOfInitialTrades = iDaysOfInitialTrades;

		CheckDefaults();
	}

	CLoaderSettings() {
		Initialize();
	}

	void InitializeDefaults(void) {
		// NOTE: All of these parameters should match the default values hard-
		// coded in src/EGenLoader.cpp via the variable names listed below.
		dft.iConfiguredCustomerCount = 5000; // iDefaultCustomerCount
		dft.iActiveCustomerCount = 5000;     // iDefaultCustomerCount
		dft.iStartingCustomer = 1;           // iDefaultStartFromCustomer
		dft.iCustomerCount = 5000;           // iDefaultCustomerCount
		dft.iScaleFactor = 500;              // iScaleFactor
		dft.iDaysOfInitialTrades = 300;      // iDaysOfInitialTrades
	}

	void CheckDefaults(void) {
		state.iConfiguredCustomerCount = true;
		state.iActiveCustomerCount = true;
		state.iStartingCustomer = true;
		state.iCustomerCount = true;
		state.iScaleFactor = (cur.iScaleFactor == dft.iScaleFactor);
		state.iDaysOfInitialTrades = (cur.iDaysOfInitialTrades == dft.iDaysOfInitialTrades);
	}

	void CheckValid(void) {
		DRIVERPARAM_CHECK_GE("iConfiguredCustomerCount", cur.iConfiguredCustomerCount, 1000);
		DRIVERPARAM_CHECK_GE("iActiveCustomerCount", cur.iActiveCustomerCount, 1000);
		DRIVERPARAM_CHECK_LE("iActiveCustomerCount", cur.iActiveCustomerCount, cur.iConfiguredCustomerCount);
		DRIVERPARAM_CHECK_EQUAL("iConfiguredCustomerCount", cur.iConfiguredCustomerCount % 1000, 0);
		DRIVERPARAM_CHECK_GE("iStartingCustomer", cur.iStartingCustomer, 1)
		DRIVERPARAM_CHECK_EQUAL("iStartingCustomer", cur.iStartingCustomer % 1000, 1);
		DRIVERPARAM_CHECK_EQUAL("iCustomerCount", cur.iCustomerCount % 1000, 0);
		DRIVERPARAM_CHECK_LE("iCustomerCount", cur.iCustomerCount + cur.iStartingCustomer - 1,
		                     cur.iConfiguredCustomerCount);
	}

	void CheckCompliant(void) {
		CheckValid();
		DRIVERPARAM_CHECK_GE("iConfiguredCustomerCount", cur.iConfiguredCustomerCount, 5000);
		DRIVERPARAM_CHECK_GE("iActiveCustomerCount", cur.iActiveCustomerCount, 5000);
		DRIVERPARAM_CHECK_EQUAL("iActiveCustomerCount", cur.iActiveCustomerCount, cur.iConfiguredCustomerCount);
		DRIVERPARAM_CHECK_DEFAULT(iScaleFactor);
		DRIVERPARAM_CHECK_DEFAULT(iDaysOfInitialTrades);
	}
};

class CDriverGlobalSettings
    : public CParametersWithDefaults<struct TDriverGlobalSettings, struct TDriverGlobalSettingsState> {
public:
	CDriverGlobalSettings(TIdent iConfiguredCustomerCount, TIdent iActiveCustomerCount, INT32 iScaleFactor,
	                      INT32 iDaysOfInitialTrades) {
		Initialize();

		cur.iConfiguredCustomerCount = iConfiguredCustomerCount;
		cur.iActiveCustomerCount = iActiveCustomerCount;
		cur.iScaleFactor = iScaleFactor;
		cur.iDaysOfInitialTrades = iDaysOfInitialTrades;

		CheckDefaults();
	}

	CDriverGlobalSettings() {
		Initialize();
	}

	void InitializeDefaults(void) {
		// NOTE: All of these parameters should match the default values hard-
		// coded in src/EGenLoader.cpp via the variable names listed below,
		// as these are the minimum build (and therefore run) values.
		dft.iConfiguredCustomerCount = 5000; // iDefaultLoadUnitSize
		dft.iActiveCustomerCount = 5000;     // iDefaultLoadUnitSize
		dft.iScaleFactor = 500;              // iScaleFactor
		dft.iDaysOfInitialTrades = 300;      // iDaysOfInitialTrades
	}

	void CheckDefaults(void) {
		state.iConfiguredCustomerCount = true;
		state.iActiveCustomerCount = true;
		state.iScaleFactor = (cur.iScaleFactor == dft.iScaleFactor);
		state.iDaysOfInitialTrades = (cur.iDaysOfInitialTrades == dft.iDaysOfInitialTrades);
	}

	void CheckValid(void) {
		DRIVERPARAM_CHECK_GE("iConfiguredCustomerCount", cur.iConfiguredCustomerCount, 1000);
		DRIVERPARAM_CHECK_GE("iActiveCustomerCount", cur.iActiveCustomerCount, 1000);
		DRIVERPARAM_CHECK_LE("iActiveCustomerCount", cur.iActiveCustomerCount, cur.iConfiguredCustomerCount);
		DRIVERPARAM_CHECK_EQUAL("iConfiguredCustomerCount", cur.iConfiguredCustomerCount % 1000, 0);
	}

	void CheckCompliant(void) {
		CheckValid();
		DRIVERPARAM_CHECK_GE("iConfiguredCustomerCount", cur.iConfiguredCustomerCount, 5000);
		DRIVERPARAM_CHECK_GE("iActiveCustomerCount", cur.iActiveCustomerCount, 5000);
		DRIVERPARAM_CHECK_EQUAL("iActiveCustomerCount", cur.iActiveCustomerCount, cur.iConfiguredCustomerCount);
		DRIVERPARAM_CHECK_DEFAULT(iScaleFactor);
		DRIVERPARAM_CHECK_DEFAULT(iDaysOfInitialTrades);
	}
};

class CDriverCESettings : public CParametersWithoutDefaults<struct TDriverCESettings> {
public:
	CDriverCESettings(UINT32 UniqueId, RNGSEED TxnMixRNGSeed, RNGSEED TxnInputRNGSeed) {
		cur.UniqueId = UniqueId;
		cur.TxnMixRNGSeed = TxnMixRNGSeed;
		cur.TxnInputRNGSeed = TxnInputRNGSeed;
	}

	CDriverCESettings(){};

	void CheckValid(void) {
	}

	void CheckCompliant(void) {
	}
};

class CDriverCEPartitionSettings
    : public CParametersWithDefaults<struct TDriverCEPartitionSettings, struct TDriverCEPartitionSettingsState> {
public:
	CDriverCEPartitionSettings(TIdent iMyStartingCustomerId, TIdent iMyCustomerCount, INT32 iPartitionPercent) {
		Initialize();

		cur.iMyStartingCustomerId = iMyStartingCustomerId;
		cur.iMyCustomerCount = iMyCustomerCount;
		cur.iPartitionPercent = iPartitionPercent;

		CheckDefaults();
	}

	// Default constructor neccessary for CE instantiation in the
	// non-partitioned case In thise case we set the current values to 0 to
	// indicate that they are unused.
	CDriverCEPartitionSettings() {
		Initialize();

		cur.iMyStartingCustomerId = 0;
		cur.iMyCustomerCount = 0;
		cur.iPartitionPercent = 0;

		CheckDefaults();
	}

	void InitializeDefaults(void) {
		dft.iMyStartingCustomerId = 1; // Spec 6.4.3.1: Minimum possible starting C_ID
		dft.iMyCustomerCount = 5000;   // Spec 6.4.3.1: Minimum partition size
		dft.iPartitionPercent = 50;    // Spec 6.4.3.1: Required partition percentage
	}

	void CheckDefaults(void) {
		state.iPartitionPercent = (cur.iPartitionPercent == dft.iPartitionPercent);
	}

	void CheckValid(void) {
		DRIVERPARAM_CHECK_BETWEEN("iPartitionPercent", cur.iPartitionPercent, 0, 100);
		if (cur.iMyStartingCustomerId == 0 && cur.iMyCustomerCount == 0 && cur.iPartitionPercent == 0) {
			// Partitioning Disabled:
			// - in this case, the default constructor would have been used and
			// all values
			//   are set to 0.  This must be considered valid.
		} else {
			// Partitioning Enabled:
			// Spec clause 6.4.3.1 has many requirements, these are the ones
			// that we validate here:
			// - minimum C_ID in a subrange is the starting C_ID for a LU
			// - minimum C_ID size of a subrange is 5000
			// - size of a subrange must be an integral multiple of LU
			DRIVERPARAM_CHECK_EQUAL("iMyStartingCustomerId", cur.iMyStartingCustomerId % 1000, 1);
			DRIVERPARAM_CHECK_GE("iMyCustomerCount", cur.iMyCustomerCount, 1000);
			DRIVERPARAM_CHECK_EQUAL("iMyCustomerCount", cur.iMyCustomerCount % 1000, 0);
		}
	}

	void CheckCompliant(void) {
		CheckValid();

		if (cur.iMyStartingCustomerId == 0 && cur.iMyCustomerCount == 0 && cur.iPartitionPercent == 0) {
			// Partitioning Disabled
		} else {
			// - CE partition is used 50% of the time
			DRIVERPARAM_CHECK_DEFAULT(iPartitionPercent);
		}
	}
};

class CDriverMEESettings : public CParametersWithoutDefaults<struct TDriverMEESettings> {
public:
	CDriverMEESettings(UINT32 UniqueId, RNGSEED RNGSeed, RNGSEED TickerTapeRNGSeed, RNGSEED TradingFloorRNGSeed) {
		cur.UniqueId = UniqueId;
		cur.RNGSeed = RNGSeed;
		cur.TickerTapeRNGSeed = TickerTapeRNGSeed;
		cur.TradingFloorRNGSeed = TradingFloorRNGSeed;
	}

	CDriverMEESettings(){};

	void CheckValid(void) {
	}

	void CheckCompliant(void) {
	}
};

class CDriverDMSettings : public CParametersWithoutDefaults<struct TDriverDMSettings> {
public:
	CDriverDMSettings(UINT32 UniqueId, RNGSEED RNGSeed) {
		cur.UniqueId = UniqueId;
		cur.RNGSeed = RNGSeed;
	}

	CDriverDMSettings(){};

	void CheckValid(void) {
	}

	void CheckCompliant(void) {
	}
};

typedef struct TDriverCETxnSettings {
	CBrokerVolumeSettings BV_settings;
	CCustomerPositionSettings CP_settings;
	CMarketWatchSettings MW_settings;
	CSecurityDetailSettings SD_settings;
	CTradeLookupSettings TL_settings;
	CTradeOrderSettings TO_settings;
	CTradeUpdateSettings TU_settings;

	CTxnMixGeneratorSettings TxnMixGenerator_settings;

	bool IsValid(void) {
		try {
			CheckValid();
			return true;
		} catch (CCheckErr) {
			throw;
			return false;
		}
	}

	bool IsCompliant(void) {
		try {
			CheckCompliant();
			return true;
		} catch (CCheckErr) {
			throw;
			return false;
		}
	}

	void CheckValid(void) {
		BV_settings.CheckValid();
		CP_settings.CheckValid();
		MW_settings.CheckValid();
		SD_settings.CheckValid();
		TL_settings.CheckValid();
		TO_settings.CheckValid();
		TU_settings.CheckValid();
		TxnMixGenerator_settings.CheckValid();
	}

	void CheckCompliant(void) {
		BV_settings.CheckCompliant();
		CP_settings.CheckCompliant();
		MW_settings.CheckCompliant();
		SD_settings.CheckCompliant();
		TL_settings.CheckCompliant();
		TO_settings.CheckCompliant();
		TU_settings.CheckCompliant();
		TxnMixGenerator_settings.CheckCompliant();
	}
} * PDriverCETxnSettings;

} // namespace TPCE

#endif //#ifndef DRIVER_PARAM_SETTINGS_H
