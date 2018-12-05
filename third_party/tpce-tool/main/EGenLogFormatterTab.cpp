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
 * - Matt Emmerton
 */

/******************************************************************************
 *   Description:        This file implements the methods for formatting
 *                       log entries in TSV or CSV format.
 ******************************************************************************/

#include "main/EGenLogFormatterTab.h"
#include "main/DriverTypes.h"

using namespace TPCE;

////////////////////////////////////////////////////////////////
// CE Transaction Settings
////////////////////////////////////////////////////////////////

string CLogFormatTab::GetLogOutput(CBrokerVolumeSettings &parms) {
	// Compare against defaults
	parms.CheckDefaults();

	// Construct Log String
	logmsg.str(emptyString);
	logmsg << "Broker Volume Parameters:  NONE" << endl;

	// Send String to Logger
	return (logmsg.str());
}

string CLogFormatTab::GetLogOutput(CCustomerPositionSettings &parms) {
	// Compare against defaults
	parms.CheckDefaults();

	// Construct Log String
	logmsg.str(emptyString);
	logmsg << "Customer Position Parameters:" << endl;
	logmsg << left << setw(40) << "Parameter"
	       << "Default"
	       << "\t"
	       << "Current"
	       << "\t"
	       << "Default?" << endl;
	logmsg << left << setw(40) << "By Cust ID: " << parms.dft.by_cust_id << "\t" << parms.cur.by_cust_id << "\t"
	       << (parms.state.by_cust_id ? "YES" : "NO") << endl;
	logmsg << left << setw(40) << "By Tax ID: " << parms.dft.by_tax_id << "\t" << parms.cur.by_tax_id << "\t"
	       << (parms.state.by_tax_id ? "YES" : "NO") << endl;
	logmsg << left << setw(40) << "Get History: " << parms.dft.get_history << "\t" << parms.cur.get_history << "\t"
	       << (parms.state.get_history ? "YES" : "NO") << endl;

	// Send String to Logger
	return (logmsg.str());
}

string CLogFormatTab::GetLogOutput(CMarketWatchSettings &parms) {
	// Compare against defaults
	parms.CheckDefaults();

	// Construct Log String
	logmsg.str(emptyString);
	logmsg << "Market Watch Parameters:" << endl;
	logmsg << left << setw(40) << "Parameter"
	       << "Default"
	       << "\t"
	       << "Current"
	       << "\t"
	       << "Default?" << endl;
	logmsg << left << setw(40) << "By Account ID: " << parms.dft.by_acct_id << "\t" << parms.cur.by_acct_id << "\t"
	       << (parms.state.by_acct_id ? "YES" : "NO") << endl;
	logmsg << left << setw(40) << "By Industry: " << parms.dft.by_industry << "\t" << parms.cur.by_industry << "\t"
	       << (parms.state.by_industry ? "YES" : "NO") << endl;
	logmsg << left << setw(40) << "By Watch List: " << parms.dft.by_watch_list << "\t" << parms.cur.by_watch_list
	       << "\t" << (parms.state.by_watch_list ? "YES" : "NO") << endl;

	// Send String to Logger
	return (logmsg.str());
}

string CLogFormatTab::GetLogOutput(CSecurityDetailSettings &parms) {
	// Compare against defaults
	parms.CheckDefaults();

	// Construct Log String
	logmsg.str(emptyString);
	logmsg << "Security Detail Parameters:" << endl;
	logmsg << left << setw(40) << "Parameter"
	       << "Default"
	       << "\t"
	       << "Current"
	       << "\t"
	       << "Default?" << endl;
	logmsg << left << setw(40) << "LOB Access Pct: " << parms.dft.LOBAccessPercentage << "\t"
	       << parms.cur.LOBAccessPercentage << "\t" << (parms.state.LOBAccessPercentage ? "YES" : "NO") << endl;

	// Send String to Logger
	return (logmsg.str());
}

string CLogFormatTab::GetLogOutput(CTradeLookupSettings &parms) {
	// Compare against defaults
	parms.CheckDefaults();

	// Construct Log String
	logmsg.str(emptyString);
	logmsg << "Trade Lookup Parameters:" << endl;
	logmsg << left << setw(40) << "Parameter"
	       << "Default"
	       << "\t"
	       << "Current"
	       << "\t"
	       << "Default?" << endl;
	logmsg << left << setw(40) << "Frame 1 Pct:" << parms.dft.do_frame1 << "\t" << parms.cur.do_frame1 << "\t"
	       << (parms.state.do_frame1 ? "YES" : "NO") << endl;
	logmsg << left << setw(40) << "Frame 2 Pct:" << parms.dft.do_frame2 << "\t" << parms.cur.do_frame2 << "\t"
	       << (parms.state.do_frame2 ? "YES" : "NO") << endl;
	logmsg << left << setw(40) << "Frame 3 Pct:" << parms.dft.do_frame3 << "\t" << parms.cur.do_frame3 << "\t"
	       << (parms.state.do_frame3 ? "YES" : "NO") << endl;
	logmsg << left << setw(40) << "Frame 4 Pct:" << parms.dft.do_frame4 << "\t" << parms.cur.do_frame4 << "\t"
	       << (parms.state.do_frame4 ? "YES" : "NO") << endl;
	logmsg << left << setw(40) << "Frame 1 MaxRows:" << parms.dft.MaxRowsFrame1 << "\t" << parms.cur.MaxRowsFrame1
	       << "\t" << (parms.state.MaxRowsFrame1 ? "YES" : "NO") << endl;
	logmsg << left << setw(40) << "Frame 2 MaxRows:" << parms.dft.MaxRowsFrame2 << "\t" << parms.cur.MaxRowsFrame2
	       << "\t" << (parms.state.MaxRowsFrame2 ? "YES" : "NO") << endl;
	logmsg << left << setw(40) << "Frame 3 MaxRows:" << parms.dft.MaxRowsFrame3 << "\t" << parms.cur.MaxRowsFrame3
	       << "\t" << (parms.state.MaxRowsFrame3 ? "YES" : "NO") << endl;
	logmsg << left << setw(40) << "Frame 4 MaxRows:" << parms.dft.MaxRowsFrame4 << "\t" << parms.cur.MaxRowsFrame4
	       << "\t" << (parms.state.MaxRowsFrame4 ? "YES" : "NO") << endl;
	logmsg << left << setw(40) << "BackOffFromEndTimeFrame2:" << parms.dft.BackOffFromEndTimeFrame2 << "\t"
	       << parms.cur.BackOffFromEndTimeFrame2 << "\t" << (parms.state.BackOffFromEndTimeFrame2 ? "YES" : "NO")
	       << endl;
	logmsg << left << setw(40) << "BackOffFromEndTimeFrame3:" << parms.dft.BackOffFromEndTimeFrame3 << "\t"
	       << parms.cur.BackOffFromEndTimeFrame3 << "\t" << (parms.state.BackOffFromEndTimeFrame3 ? "YES" : "NO")
	       << endl;
	logmsg << left << setw(40) << "BackOffFromEndTimeFrame4:" << parms.dft.BackOffFromEndTimeFrame4 << "\t"
	       << parms.cur.BackOffFromEndTimeFrame4 << "\t" << (parms.state.BackOffFromEndTimeFrame4 ? "YES" : "NO")
	       << endl;

	// Send String to Logger
	return (logmsg.str());
}

string CLogFormatTab::GetLogOutput(CTradeOrderSettings &parms) {
	// Compare against defaults
	parms.CheckDefaults();

	// Construct Log String
	logmsg.str(emptyString);
	logmsg << "Trade Order Parameters:" << endl;
	logmsg << left << setw(40) << "Parameter"
	       << "Default"
	       << "\t"
	       << "Current"
	       << "\t"
	       << "Default?" << endl;
	logmsg << left << setw(40) << "Market Trade Pct:" << parms.dft.market << "\t" << parms.cur.market << "\t"
	       << (parms.state.market ? "YES" : "NO") << endl;
	logmsg << left << setw(40) << "Limit Trade Pct:" << parms.dft.limit << "\t" << parms.cur.limit << "\t"
	       << (parms.state.limit ? "YES" : "NO") << endl;
	logmsg << left << setw(40) << "Stop Loss Pct:" << parms.dft.stop_loss << "\t" << parms.cur.stop_loss << "\t"
	       << (parms.state.stop_loss ? "YES" : "NO") << endl;
	logmsg << left << setw(40) << "Security by Name Pct:" << parms.dft.security_by_name << "\t"
	       << parms.cur.security_by_name << "\t" << (parms.state.security_by_name ? "YES" : "NO") << endl;
	logmsg << left << setw(40) << "Security by Symbol Pct:" << parms.dft.security_by_symbol << "\t"
	       << parms.cur.security_by_symbol << "\t" << (parms.state.security_by_symbol ? "YES" : "NO") << endl;
	logmsg << left << setw(40) << "Buy Order Pct:" << parms.dft.buy_orders << "\t" << parms.cur.buy_orders << "\t"
	       << (parms.state.buy_orders ? "YES" : "NO") << endl;
	logmsg << left << setw(40) << "Sell Order Pct:" << parms.dft.sell_orders << "\t" << parms.cur.sell_orders << "\t"
	       << (parms.state.sell_orders ? "YES" : "NO") << endl;
	logmsg << left << setw(40) << "LIFO Pct:" << parms.dft.lifo << "\t" << parms.cur.lifo << "\t"
	       << (parms.state.lifo ? "YES" : "NO") << endl;
	logmsg << left << setw(40) << "Margin Trade Pct:" << parms.dft.type_is_margin << "\t" << parms.cur.type_is_margin
	       << "\t" << (parms.state.type_is_margin ? "YES" : "NO") << endl;
	logmsg << left << setw(40) << "Executor as Owner Pct:" << parms.dft.exec_is_owner << "\t" << parms.cur.exec_is_owner
	       << "\t" << (parms.state.exec_is_owner ? "YES" : "NO") << endl;
	logmsg << left << setw(40) << "Rollback Pct:" << parms.dft.rollback << "\t" << parms.cur.rollback << "\t"
	       << (parms.state.rollback ? "YES" : "NO") << endl;

	// Send String to Logger
	return (logmsg.str());
}

string CLogFormatTab::GetLogOutput(CTradeUpdateSettings &parms) {
	// Compare against defaults
	parms.CheckDefaults();

	// Construct Log String
	logmsg.str(emptyString);
	logmsg << "Trade Update Parameters:" << endl;
	logmsg << left << setw(40) << "Parameter"
	       << "Default"
	       << "\t"
	       << "Current"
	       << "\t"
	       << "Default?" << endl;
	logmsg << left << setw(40) << "Frame 1 Pct:" << parms.dft.do_frame1 << "\t" << parms.cur.do_frame1 << "\t"
	       << (parms.state.do_frame1 ? "YES" : "NO") << endl;
	logmsg << left << setw(40) << "Frame 2 Pct:" << parms.dft.do_frame2 << "\t" << parms.cur.do_frame2 << "\t"
	       << (parms.state.do_frame2 ? "YES" : "NO") << endl;
	logmsg << left << setw(40) << "Frame 3 Pct:" << parms.dft.do_frame3 << "\t" << parms.cur.do_frame3 << "\t"
	       << (parms.state.do_frame3 ? "YES" : "NO") << endl;
	logmsg << left << setw(40) << "Frame 1 MaxRows:" << parms.dft.MaxRowsFrame1 << "\t" << parms.cur.MaxRowsFrame1
	       << "\t" << (parms.state.MaxRowsFrame1 ? "YES" : "NO") << endl;
	logmsg << left << setw(40) << "Frame 2 MaxRows:" << parms.dft.MaxRowsFrame2 << "\t" << parms.cur.MaxRowsFrame2
	       << "\t" << (parms.state.MaxRowsFrame2 ? "YES" : "NO") << endl;
	logmsg << left << setw(40) << "Frame 3 MaxRows:" << parms.dft.MaxRowsFrame3 << "\t" << parms.cur.MaxRowsFrame3
	       << "\t" << (parms.state.MaxRowsFrame3 ? "YES" : "NO") << endl;
	logmsg << left << setw(40) << "Frame 1 MaxRowsToUpdate:" << parms.dft.MaxRowsToUpdateFrame1 << "\t"
	       << parms.cur.MaxRowsToUpdateFrame1 << "\t" << (parms.state.MaxRowsToUpdateFrame1 ? "YES" : "NO") << endl;
	logmsg << left << setw(40) << "Frame 2 MaxRowsToUpdate:" << parms.dft.MaxRowsToUpdateFrame2 << "\t"
	       << parms.cur.MaxRowsToUpdateFrame2 << "\t" << (parms.state.MaxRowsToUpdateFrame2 ? "YES" : "NO") << endl;
	logmsg << left << setw(40) << "Frame 3 MaxRowsToUpdate:" << parms.dft.MaxRowsToUpdateFrame3 << "\t"
	       << parms.cur.MaxRowsToUpdateFrame3 << "\t" << (parms.state.MaxRowsToUpdateFrame3 ? "YES" : "NO") << endl;
	logmsg << left << setw(40) << "BackOffFromEndTimeFrame2:" << parms.dft.BackOffFromEndTimeFrame2 << "\t"
	       << parms.cur.BackOffFromEndTimeFrame2 << "\t" << (parms.state.BackOffFromEndTimeFrame2 ? "YES" : "NO")
	       << endl;
	logmsg << left << setw(40) << "BackOffFromEndTimeFrame3:" << parms.dft.BackOffFromEndTimeFrame3 << "\t"
	       << parms.cur.BackOffFromEndTimeFrame3 << "\t" << (parms.state.BackOffFromEndTimeFrame3 ? "YES" : "NO")
	       << endl;

	// Send String to Logger
	return (logmsg.str());
}

////////////////////////////////////////////////////////////////
// CE Transaction Mix Settings
////////////////////////////////////////////////////////////////

string CLogFormatTab::GetLogOutput(CTxnMixGeneratorSettings &parms) {
	// Compare against defaults
	parms.CheckDefaults();

	// Construct Log String
	logmsg.str(emptyString);
	logmsg << "Transaction Mixes:" << endl;
	logmsg << left << setw(40) << "Parameter"
	       << "Default"
	       << "\t"
	       << "Current"
	       << "\t"
	       << "Default?" << endl;
	logmsg << left << setw(40) << "Broker Volume: " << parms.dft.BrokerVolumeMixLevel << "\t"
	       << parms.cur.BrokerVolumeMixLevel << "\t" << (parms.state.BrokerVolumeMixLevel ? "YES" : "NO") << endl;
	logmsg << left << setw(40) << "Customer Position: " << parms.dft.CustomerPositionMixLevel << "\t"
	       << parms.cur.CustomerPositionMixLevel << "\t" << (parms.state.CustomerPositionMixLevel ? "YES" : "NO")
	       << endl;
	logmsg << left << setw(40) << "Market Watch: " << parms.dft.MarketWatchMixLevel << "\t"
	       << parms.cur.MarketWatchMixLevel << "\t" << (parms.state.MarketWatchMixLevel ? "YES" : "NO") << endl;
	logmsg << left << setw(40) << "Security Detail: " << parms.dft.SecurityDetailMixLevel << "\t"
	       << parms.cur.SecurityDetailMixLevel << "\t" << (parms.state.SecurityDetailMixLevel ? "YES" : "NO") << endl;
	logmsg << left << setw(40) << "Trade Lookup: " << parms.dft.TradeLookupMixLevel << "\t"
	       << parms.cur.TradeLookupMixLevel << "\t" << (parms.state.TradeLookupMixLevel ? "YES" : "NO") << endl;
	logmsg << left << setw(40) << "Trade Order: " << parms.dft.TradeOrderMixLevel << "\t"
	       << parms.cur.TradeOrderMixLevel << "\t" << (parms.state.TradeOrderMixLevel ? "YES" : "NO") << endl;
	logmsg << left << setw(40) << "Trade Status: " << parms.dft.TradeStatusMixLevel << "\t"
	       << parms.cur.TradeStatusMixLevel << "\t" << (parms.state.TradeStatusMixLevel ? "YES" : "NO") << endl;
	logmsg << left << setw(40) << "Trade Update: " << parms.dft.TradeUpdateMixLevel << "\t"
	       << parms.cur.TradeUpdateMixLevel << "\t" << (parms.state.TradeUpdateMixLevel ? "YES" : "NO") << endl;

	// Send String to Logger
	return (logmsg.str());
}

////////////////////////////////////////////////////////////////
// Loader Settings
////////////////////////////////////////////////////////////////

string CLogFormatTab::GetLogOutput(CLoaderSettings &parms) {
	// Compare against defaults
	parms.CheckDefaults();

	// Construct Log String
	logmsg.str(emptyString);
	logmsg << "Loader Settings:" << endl;
	logmsg << left << setw(40) << "Parameter"
	       << "Default"
	       << "\t"
	       << "Current"
	       << "\t"
	       << "Default?" << endl;
	logmsg << left << setw(40) << "Configured Customers:" << parms.dft.iConfiguredCustomerCount << "\t"
	       << parms.cur.iConfiguredCustomerCount << endl;
	if (parms.cur.iConfiguredCustomerCount != parms.cur.iActiveCustomerCount) {
		logmsg << left << setw(40) << "Active Customers:" << parms.dft.iActiveCustomerCount << "\t"
		       << parms.cur.iActiveCustomerCount << endl;
	}
	logmsg << left << setw(40) << "Starting Customer:" << parms.dft.iStartingCustomer << "\t"
	       << parms.cur.iStartingCustomer << endl;
	logmsg << left << setw(40) << "Customer Count:" << parms.dft.iCustomerCount << "\t" << parms.cur.iCustomerCount
	       << endl;
	logmsg << left << setw(40) << "Scale Factor:" << parms.dft.iScaleFactor << "\t" << parms.cur.iScaleFactor << "\t"
	       << (parms.state.iScaleFactor ? "YES" : "NO") << endl;
	logmsg << left << setw(40) << "Days of Initial Trades:" << parms.dft.iDaysOfInitialTrades << "\t"
	       << parms.cur.iDaysOfInitialTrades << "\t" << (parms.state.iDaysOfInitialTrades ? "YES" : "NO") << endl;

	return (logmsg.str());
}

////////////////////////////////////////////////////////////////
// Driver Settings
////////////////////////////////////////////////////////////////

string CLogFormatTab::GetLogOutput(CDriverGlobalSettings &parms) {
	// Compare against defaults
	parms.CheckDefaults();

	// Construct Log String
	logmsg.str(emptyString);
	logmsg << "Driver Global Settings:" << endl;
	logmsg << left << setw(40) << "Parameter"
	       << "Default"
	       << "\t"
	       << "Current"
	       << "\t"
	       << "Default?" << endl;
	logmsg << left << setw(40) << "Configured Customers:" << parms.dft.iConfiguredCustomerCount << "\t"
	       << parms.cur.iConfiguredCustomerCount << endl;
	logmsg << left << setw(40) << "Active Customers:" << parms.dft.iActiveCustomerCount << "\t"
	       << parms.cur.iActiveCustomerCount << endl;
	logmsg << left << setw(40) << "Scale Factor:" << parms.dft.iScaleFactor << "\t" << parms.cur.iScaleFactor << "\t"
	       << (parms.state.iScaleFactor ? "YES" : "NO") << endl;
	logmsg << left << setw(40) << "Days of Initial Trades:" << parms.dft.iDaysOfInitialTrades << "\t"
	       << parms.cur.iDaysOfInitialTrades << "\t" << (parms.state.iDaysOfInitialTrades ? "YES" : "NO") << endl;

	return (logmsg.str());
}

string CLogFormatTab::GetLogOutput(CDriverCESettings &parms) {
	// Construct Log String
	logmsg.str(emptyString);
	logmsg << "Driver CE Settings:" << endl;
	logmsg << left << setw(40) << "Parameter"
	       << "Value" << endl;
	logmsg << left << setw(40) << "Unique ID:" << parms.cur.UniqueId << endl;
	logmsg << left << setw(40) << "Txn Mix RNGSeed:" << parms.cur.TxnMixRNGSeed << endl;
	logmsg << left << setw(40) << "Txn Input RNGSeed:" << parms.cur.TxnInputRNGSeed << endl;

	// Send String to Logger
	return (logmsg.str());
}

string CLogFormatTab::GetLogOutput(CDriverCEPartitionSettings &parms) {
	// Compare against defaults
	parms.CheckDefaults();

	// Construct Log String
	logmsg.str(emptyString);
	logmsg << "Driver CE Partition Settings:" << endl;
	logmsg << left << setw(40) << "Parameter"
	       << "Default"
	       << "\t"
	       << "Current"
	       << "\t"
	       << "Default?" << endl;
	logmsg << left << setw(40) << "Partition Starting Customer ID:" << parms.dft.iMyStartingCustomerId << "\t"
	       << parms.cur.iMyStartingCustomerId << endl;
	logmsg << left << setw(40) << "Partition Customer Count:" << parms.dft.iMyCustomerCount << "\t"
	       << parms.cur.iMyCustomerCount << endl;
	logmsg << left << setw(40) << "Partition Percent:" << parms.dft.iPartitionPercent << "\t"
	       << parms.cur.iPartitionPercent << "\t" << (parms.state.iPartitionPercent ? "YES" : "NO") << endl;

	// Send String to Logger
	return (logmsg.str());
}

string CLogFormatTab::GetLogOutput(CDriverMEESettings &parms) {
	// Construct Log String
	logmsg.str(emptyString);
	logmsg << "Driver MEE Settings:" << endl;
	logmsg << left << setw(40) << "Parameter"
	       << "Value" << endl;
	logmsg << left << setw(40) << "Unique ID:" << parms.cur.UniqueId << endl;
	logmsg << left << setw(40) << "Ticker Tape RNGSeed:" << parms.cur.TickerTapeRNGSeed << endl;
	logmsg << left << setw(40) << "Trading Floor RNGSeed:" << parms.cur.TradingFloorRNGSeed << endl;

	// Send String to Logger
	return (logmsg.str());
}

string CLogFormatTab::GetLogOutput(CDriverDMSettings &parms) {
	// Construct Log String
	logmsg.str(emptyString);
	logmsg << "Driver DM Settings:" << endl;
	logmsg << left << setw(40) << "Parameter"
	       << "Value" << endl;
	logmsg << left << setw(40) << "Unique ID:" << parms.cur.UniqueId << endl;
	logmsg << left << setw(40) << "RNGSeed:" << parms.cur.RNGSeed << endl;

	// Send String to Logger
	return (logmsg.str());
}
