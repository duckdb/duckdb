#include "main/TxnHarnessDBInterface.h"

using namespace duckdb;
using namespace TPCE;

// implementation according to TPC-E spec
// see:
// http://hstore.cs.brown.edu/wordpress/wp-content/uploads/2011/05/TPCE-v0.32.2g.pdf

void CTradeOrderDBInterface::DoTradeOrderFrame1(const TTradeOrderFrame1Input *pIn, TTradeOrderFrame1Output *pOut) {
	// auto id = pIn->acct_id;

	// con.Query("BEGIN TRANSACTION");
	// auto result = con.Query("select CA_NAME, CA_B_ID, CA_C_ID, CA_TAX_ST, C_F_NAME, C_L_NAME, "
	//                         "C_TIER, C_TAX_ID, B_NAME from CUSTOMER_ACCOUNT, CUSTOMER, BROKER "
	//                         "where CA_C_ID=C_ID AND CA_B_ID=B_ID AND CA_ID = " +
	//                         to_string(id));
	// pOut->num_found = result->collection.count;
	// if (pOut->num_found > 0) {
	// 	assert(pOut->num_found == 1);

	// 	strcpy(pOut->acct_name, result->GetValue<const char *>(0, 0));
	// 	pOut->broker_id = result->GetValue<int32_t>(1, 0);
	// 	pOut->cust_id = result->GetValue<int32_t>(2, 0);
	// 	pOut->tax_status = result->GetValue<int32_t>(3, 0);
	// 	strcpy(pOut->cust_f_name, result->GetValue<const char *>(4, 0));
	// 	strcpy(pOut->cust_l_name, result->GetValue<const char *>(5, 0));
	// 	pOut->cust_tier = result->GetValue<int>(6, 0);
	// 	strcpy(pOut->tax_id, result->GetValue<const char *>(7, 0));
	// 	strcpy(pOut->broker_name, result->GetValue<const char *>(8, 0));
	// }
}

void CTradeOrderDBInterface::DoTradeOrderFrame2(const TTradeOrderFrame2Input *pIn, TTradeOrderFrame2Output *pOut) {
	// auto result =
	//     con.Query("select AP_ACL from ACCOUNT_PERMISSION where AP_CA_ID = " + to_string(pIn->acct_id) +
	//               " and AP_F_NAME = " + string(pIn->exec_f_name) + " and AP_L_NAME = " + string(pIn->exec_l_name) +
	//               " and AP_TAX_ID = " + string(pIn->exec_tax_id));
	// if (result->collection.count > 0) {
	// 	assert(result->collection.count == 1);
	// 	strcpy(pOut->ap_acl, result->GetValue<const char *>(0, 0));
	// } else {
	// 	pOut->ap_acl[0] = '\0';
	// }
}

void CTradeOrderDBInterface::DoTradeOrderFrame3(const TTradeOrderFrame3Input *pIn, TTradeOrderFrame3Output *pOut) {
	// int exch_id;
	// string symbol;
	// if (pIn->symbol[0] == '\0') {
	// 	// empty symbol
	// 	auto result = con.Query("select S_EX_ID, S_NAME, S_SYMB from COMPANY, "
	// 	                        "SECURITY where S_CO_ID=CO_ID AND CO_NAME = " +
	// 	                        string(pIn->co_name) + " AND S_ISSUE=" + string(pIn->issue));
	// 	assert(result->collection.count == 1);
	// 	exch_id = result->GetValue<int>(0, 0);
	// 	strcpy(pOut->s_name, result->GetValue<const char *>(1, 0));
	// 	symbol = string(result->GetValue<const char *>(2, 0));
	// 	strcpy(pOut->symbol, symbol.c_str());
	// } else {
	// 	symbol = string(pIn->symbol);

	// 	auto result = con.Query("select CO_NAME, S_EX_ID, S_NAME from SECURITY, COMPANY "
	// 	                        "where CO_ID = S_CO_ID AND S_SYMB = " +
	// 	                        symbol);
	// 	assert(result->collection.count == 1);
	// 	strcpy(pOut->co_name, result->GetValue<const char *>(0, 0));
	// 	exch_id = result->GetValue<int>(1, 0);
	// 	strcpy(pOut->s_name, result->GetValue<const char *>(2, 0));
	// }
	// // Get current pricing information for the security select
	// auto result = con.Query("SELECT market_price = LT_PRICE from LAST_TRADE where LT_S_SYMB = " + symbol);
	// pOut->market_price = result->GetValue<double>(0, 0);

	// // Set trade characteristics based on the type of trade. select
	// result = con.Query("SELECT TT_IS_MRKT, TT_IS_SELL from TRADE_TYPE where TT_ID = " + string(pIn->trade_type_id));
	// pOut->type_is_market = result->GetValue<bool>(0, 0);
	// pOut->type_is_sell = result->GetValue<bool>(1, 0);

	// // If this is a limit-order, then the requested_price was passed in to us,
	// // but if this this a market-order, then we need to set the requested_price
	// // to the current market price.
	// if (pOut->type_is_market) {
	// 	pOut->requested_price = pOut->market_price;
	// }

	// // Local frame variables used when estimating impact of this trade on
	// // any current holdings of the same security.
	// double buy_value = 0.0;
	// double sell_value = 0.0;
	// int needed_qty = pIn->trade_qty;

	// result = con.Query("select HS_QTY from HOLDING_SUMMARY where HS_CA_ID = " + to_string(pIn->acct_id) +
	//                    " and HS_S_SYMB = " + symbol);
	// int hs_qty = result->GetValue<int>(0, 0);

	// if (pOut->type_is_sell) {
	// 	// This is a sell transaction, so estimate the impact to any currently
	// 	// held long positions in the security.
	// 	if (hs_qty > 0) {
	// 		if (pIn->is_lifo) {
	// 			// Estimates will be based on closing most recently acquired
	// 			// holdings Could return 0, 1 or many rows
	// 			result = con.Query("select H_QTY, H_PRICE from HOLDING where H_CA_ID = " + to_string(pIn->acct_id) +
	// 			                   " and H_S_SYMB = " + symbol + " order by H_DTS desc");
	// 		} else {
	// 			// Estimates will be based on closing oldest holdings
	// 			// Could return 0, 1 or many rows
	// 			result = con.Query("select H_QTY, H_PRICE from HOLDING where H_CA_ID = " + to_string(pIn->acct_id) +
	// 			                   " and H_S_SYMB = " + symbol + " order by H_DTS asc");
	// 		}

	// 		// Estimate, based on the requested price, any profit that may be
	// 		// realized by selling current holdings for this security. The
	// 		// customer may have multiple holdings for this security
	// 		// (representing different purchases of this security at different
	// 		// times and therefore, most likely, different prices).
	// 		for (size_t i = 0; needed_qty != 0 || i < result->size(); i++) {
	// 			int hold_qty = result->GetValue<int>(0, i);
	// 			double hold_price = result->GetValue<double>(1, i);
	// 			if (hold_qty > needed_qty) {
	// 				// Only a portion of this holding would be sold as a result
	// 				// of the // trade.
	// 				buy_value += needed_qty * hold_price;
	// 				sell_value += needed_qty * pIn->requested_price;
	// 				needed_qty = 0;
	// 			} else {
	// 				// All of this holding would be sold as a result of this
	// 				// trade.
	// 				buy_value += hold_qty * hold_price;
	// 				sell_value += hold_qty * pIn->requested_price;
	// 				needed_qty = needed_qty - hold_qty;
	// 			}
	// 		}
	// 	}
	// 	// NOTE: If needed_qty is still greater than 0 at this point, then the
	// 	// customer would be liquidating all current holdings for this security,
	// 	// and then short-selling this remaining balance for the transaction.
	// } else {
	// 	// This is a buy transaction, so estimate the impact to any currently
	// 	// held short positions in the security. These are represented as
	// 	// negative H_QTY holdings. Short postions will be covered before
	// 	// opening a long postion in // this security.
	// 	if (hs_qty < 0) {
	// 		if (pIn->is_lifo) {
	// 			// Estimates will be based on closing most recently acquired
	// 			// holdings Could return 0, 1 or many rows
	// 			result = con.Query("select H_QTY, H_PRICE from HOLDING where H_CA_ID = " + to_string(pIn->acct_id) +
	// 			                   " and H_S_SYMB = " + symbol + " order by H_DTS desc");
	// 		} else {
	// 			// Estimates will be based on closing oldest holdings
	// 			// Could return 0, 1 or many rows
	// 			result = con.Query("select H_QTY, H_PRICE from HOLDING where H_CA_ID = " + to_string(pIn->acct_id) +
	// 			                   " and H_S_SYMB = " + symbol + " order by H_DTS asc");
	// 		}
	// 		// Estimate, based on the requested price, any profit that may be
	// 		// realized by covering short postions currently held for this
	// 		// security. The customer may have multiple holdings for this
	// 		// security (representing different purchases of this security at
	// 		// different times and therefore, most likely, different prices).
	// 		for (size_t i = 0; needed_qty != 0 || i < result->size(); i++) {
	// 			int hold_qty = result->GetValue<int>(0, i);
	// 			double hold_price = result->GetValue<double>(1, i);
	// 			if (hold_qty + needed_qty < 0) {
	// 				// Only a portion of this holding would be covered (bought
	// 				// back) as a result of this trade.
	// 				sell_value += needed_qty * hold_price;
	// 				buy_value += needed_qty * pIn->requested_price;
	// 				needed_qty = 0;
	// 			} else {
	// 				// All of this holding would be covered (bought back) as
	// 				// a result of this trade.
	// 				// NOTE: Local variable hold_qty is made positive for easy
	// 				// calculations
	// 				hold_qty = -hold_qty;
	// 				sell_value += hold_qty * hold_price;
	// 				buy_value += hold_qty * pIn->requested_price;
	// 				needed_qty = needed_qty - hold_qty;
	// 			}
	// 		}
	// 		// NOTE: If needed_qty is still greater than 0 at this point, then
	// 		// the customer would cover all current short positions for this
	// 		// security, (if any) and then open a new long position for the
	// 		// remaining balance // of this transaction.
	// 	}
	// }
	// // Estimate any capital gains tax that would be incurred as a result of this
	// // // transaction.
	// pOut->tax_amount = 0.0;
	// if ((sell_value > buy_value) && ((pIn->tax_status == 1) || (pIn->tax_status == 2))) {
	// 	// Customer’s can be (are) subject to more than one tax rate.
	// 	// For example, a state tax rate and a federal tax rate. Therefore,
	// 	// get all tax rates the customer is subject to, and estimate overall
	// 	// amount of tax that would result from this order.
	// 	auto result = con.Query("select sum(TX_RATE) from TAXRATE where TX_ID in ( "
	// 	                        "select CX_TX_ID from CUSTOMER_TAXRATE where CX_C_ID = " +
	// 	                        to_string(pIn->cust_id) + ")");
	// 	pOut->tax_amount = (sell_value - buy_value) * result->GetValue<double>(0, 0);
	// }
	// // Get administrative fees (e.g. trading charge, commision rate)
	// result = con.Query("select CR_RATE from COMMISSION_RATE where CR_C_TIER = " + to_string(pIn->cust_tier) +
	//                    " and CR_TT_ID = " + string(pIn->trade_type_id) + " and CR_EX_ID = " + to_string(exch_id) +
	//                    " and CR_FROM_QTY <= " + to_string(pIn->trade_qty) +
	//                    " and CR_TO_QTY >= " + to_string(pIn->trade_qty));
	// pOut->comm_rate = result->GetValue<double>(0, 0);

	// result = con.Query("select CH_CHRG from CHARGE where CH_C_TIER = " + to_string(pIn->cust_tier) +
	//                    " and CH_TT_ID = " + string(pIn->trade_type_id));
	// pOut->charge_amount = result->GetValue<double>(0, 0);

	// pOut->acct_assets = 0;
	// if (pIn->type_is_margin) {
	// 	// get the current account balance
	// 	result = con.Query("select CA_BAL from CUSTOMER_ACCOUNT where CA_ID = acct_id");
	// 	double acct_bal = 0;
	// 	if (result->collection.count > 0) {
	// 		assert(result->collection.count == 1);
	// 		acct_bal = result->GetValue<double>(0, 0);
	// 	}
	// 	// update the account balance with the new price
	// 	result = con.Query("select sum(HS_QTY * LT_PRICE) from HOLDING_SUMMARY, LAST_TRADE "
	// 	                   "where HS_CA_ID = acct_id and LT_S_SYMB = HS_S_SYMB");
	// 	if (result->ValueIsNull(0, 0)) {
	// 		/* account currently has no holdings */
	// 		pOut->acct_assets = acct_bal;
	// 	} else {
	// 		pOut->acct_assets = result->GetValue<double>(0, 0) + acct_bal;
	// 	}
	// }
	// if (pOut->type_is_market) {
	// 	strcpy(pOut->status_id, pIn->st_submitted_id);
	// } else {
	// 	strcpy(pOut->status_id, pIn->st_pending_id);
	// }
}

void CTradeOrderDBInterface::DoTradeOrderFrame4(const TTradeOrderFrame4Input *pIn, TTradeOrderFrame4Output *pOut) {
	// // FIXME: auto increment column
	// // FIXME: NOW() function
	// // FIXME: get auto increment column value from insert?
	// con.Query("insert into TRADE (T_DTS, T_ST_ID, T_TT_ID, T_IS_CASH, T_S_SYMB, "
	//           "T_QTY, T_BID_PRICE, T_CA_ID, T_EXEC_NAME, T_TRADE_PRICE, T_CHRG, "
	//           "T_COMM, T_TAX, T_LIFO) VALUES (NOW(), " +
	//           string(pIn->status_id) + ", " + string(pIn->trade_type_id) + ", " + to_string(pIn->is_cash) + ", " +
	//           string(pIn->symbol) + ", " + to_string(pIn->trade_qty) + ", " + to_string(pIn->requested_price) + ", "
	//           + to_string(pIn->acct_id) + ", " + string(pIn->exec_name) + ", NULL, " + to_string(pIn->charge_amount)
	//           +
	//           ", " + to_string(pIn->comm_amount) + ", 0, " + to_string(pIn->is_lifo) + ");");
	// if (!pIn->type_is_market) {
	// 	// con.Query("INSERT INTO TRADE_REQUEST (TR_T_ID, TR_TT_ID, TR_S_SYMB,
	// 	// TR_QTY, TR_BID_PRICE, TR_CA_ID) VALUES ()");
	// }
	// assert(0);
}

void CTradeOrderDBInterface::DoTradeOrderFrame5(void) {
	// con.Query("ROLLBACK");
}

void CTradeOrderDBInterface::DoTradeOrderFrame6(void) {
	// con.Query("COMMIT");
}
