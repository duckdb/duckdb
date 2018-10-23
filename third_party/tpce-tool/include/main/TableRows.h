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
 * - Sergey Vasilevskiy, Doug Johnson, Larry Loen
 */

/******************************************************************************
 *   Description: Simple behaviorless structs representing a row in a table.
 *                These are what is emitted by EGen to database loaders.
 ******************************************************************************/

#ifndef TABLE_ROWS_H
#define TABLE_ROWS_H

#include "utilities/EGenStandardTypes.h"
#include "utilities/TableConsts.h"
#include "utilities/DateTime.h"

using namespace std;

namespace TPCE {

// ACCOUNT_PERMISSION table
typedef struct ACCOUNT_PERMISSION_ROW {
	TIdent AP_CA_ID;
	char AP_ACL[cACL_len + 1]; // binary column in the table
	char AP_TAX_ID[cTAX_ID_len + 1];
	char AP_L_NAME[cL_NAME_len + 1];
	char AP_F_NAME[cF_NAME_len + 1];
} * PACCOUNT_PERMISSION_ROW;

// ADDRESS table
typedef struct ADDRESS_ROW {
	TIdent AD_ID;
	char AD_LINE1[cAD_LINE_len + 1];
	char AD_LINE2[cAD_LINE_len + 1];
	char AD_ZC_CODE[cAD_ZIP_len + 1];
	char AD_CTRY[cAD_CTRY_len + 1];
} * PADDRESS_ROW;

// BROKER table
typedef struct BROKER_ROW {
	TIdent B_ID;
	char B_ST_ID[cST_ID_len + 1];
	char B_NAME[cB_NAME_len + 1];
	int B_NUM_TRADES;
	double B_COMM_TOTAL;
} * PBROKER_ROW;

// CASH_TRANSACTION table
typedef struct CASH_TRANSACTION_ROW {
	TTrade CT_T_ID;
	CDateTime CT_DTS;
	double CT_AMT;
	char CT_NAME[cCT_NAME_len + 1];
} * PCASH_TRANSACTION_ROW;

// CHARGE table
typedef struct CHARGE_ROW {
	char CH_TT_ID[cTT_ID_len + 1];
	int CH_C_TIER;
	double CH_CHRG;
} * PCHARGE_ROW;

// COMMISSION_RATE table
typedef struct COMMISSION_RATE_ROW {
	int CR_C_TIER;
	char CR_TT_ID[cTT_ID_len + 1];
	char CR_EX_ID[cEX_ID_len + 1];
	int CR_FROM_QTY;
	int CR_TO_QTY;
	double CR_RATE;
} * PCOMMISSION_RATE_ROW;

// COMPANY table
typedef struct COMPANY_ROW {
	TIdent CO_ID;
	char CO_ST_ID[cST_ID_len + 1];
	char CO_NAME[cCO_NAME_len + 1];
	char CO_IN_ID[cIN_ID_len + 1];
	char CO_SP_RATE[cSP_RATE_len + 1];
	char CO_CEO[cCEO_NAME_len + 1];
	TIdent CO_AD_ID;
	char CO_DESC[cCO_DESC_len + 1];
	CDateTime CO_OPEN_DATE;
} * PCOMPANY_ROW;

// COMPANY_COMPETITOR table
typedef struct COMPANY_COMPETITOR_ROW {
	TIdent CP_CO_ID;
	TIdent CP_COMP_CO_ID;
	char CP_IN_ID[cIN_ID_len + 1];
} * PCOMPANY_COMPETITOR_ROW;

// CUSTOMER table
typedef struct CUSTOMER_ROW {
	TIdent C_ID;
	char C_TAX_ID[cTAX_ID_len + 1];
	char C_ST_ID[cST_ID_len + 1];
	char C_L_NAME[cL_NAME_len + 1];
	char C_F_NAME[cF_NAME_len + 1];
	char C_M_NAME[cM_NAME_len + 1];
	char C_GNDR;
	char C_TIER;
	CDateTime C_DOB;
	TIdent C_AD_ID;
	char C_CTRY_1[cCTRY_len + 1];
	char C_AREA_1[cAREA_len + 1];
	char C_LOCAL_1[cLOCAL_len + 1];
	char C_EXT_1[cEXT_len + 1];
	char C_CTRY_2[cCTRY_len + 1];
	char C_AREA_2[cAREA_len + 1];
	char C_LOCAL_2[cLOCAL_len + 1];
	char C_EXT_2[cEXT_len + 1];
	char C_CTRY_3[cCTRY_len + 1];
	char C_AREA_3[cAREA_len + 1];
	char C_LOCAL_3[cLOCAL_len + 1];
	char C_EXT_3[cEXT_len + 1];
	char C_EMAIL_1[cEMAIL_len + 1];
	char C_EMAIL_2[cEMAIL_len + 1];

	CUSTOMER_ROW() : C_ID(0){};

} * PCUSTOMER_ROW;

// CUSTOMER_ACCOUNT table
typedef struct CUSTOMER_ACCOUNT_ROW {
	TIdent CA_ID;
	TIdent CA_B_ID;
	TIdent CA_C_ID;
	char CA_NAME[cCA_NAME_len + 1];
	char CA_TAX_ST;
	double CA_BAL;
} * PCUSTOMER_ACCOUNT_ROW;

// CUSTOMER_TAXRATE table
typedef struct CUSTOMER_TAXRATE_ROW {
	char CX_TX_ID[cTX_ID_len + 1];
	TIdent CX_C_ID;
} * PCUSTOMER_TAXRATE_ROW;

// DAILY_MARKET table
typedef struct DAILY_MARKET_ROW {
	CDateTime DM_DATE;
	char DM_S_SYMB[cSYMBOL_len + 1];
	double DM_CLOSE;
	double DM_HIGH;
	double DM_LOW;
	INT64 DM_VOL;
} * PDAILY_MARKET_ROW;

// EXCHANGE table
typedef struct EXCHANGE_ROW {
	char EX_ID[cEX_ID_len + 1];
	char EX_NAME[cEX_NAME_len + 1];
	int EX_NUM_SYMB;
	int EX_OPEN;
	int EX_CLOSE;
	char EX_DESC[cEX_DESC_len + 1];
	TIdent EX_AD_ID;
} * PEXCHANGE_ROW;

// FINANCIAL table
typedef struct FINANCIAL_ROW {
	TIdent FI_CO_ID;
	int FI_YEAR;
	int FI_QTR;
	CDateTime FI_QTR_START_DATE;
	double FI_REVENUE;
	double FI_NET_EARN;
	double FI_BASIC_EPS;
	double FI_DILUT_EPS;
	double FI_MARGIN;
	double FI_INVENTORY;
	double FI_ASSETS;
	double FI_LIABILITY;
	INT64 FI_OUT_BASIC;
	INT64 FI_OUT_DILUT;
} * PFINANCIAL_ROW;

// HOLDING table
typedef struct HOLDING_ROW {
	TTrade H_T_ID;
	TIdent H_CA_ID;
	char H_S_SYMB[cSYMBOL_len + 1];
	CDateTime H_DTS;
	double H_PRICE;
	int H_QTY;
} * PHOLDING_ROW;

// HOLDING_HISTORY table
typedef struct HOLDING_HISTORY_ROW {
	TTrade HH_H_T_ID;
	TTrade HH_T_ID;
	int HH_BEFORE_QTY;
	int HH_AFTER_QTY;
} * PHOLDING_HISTORY_ROW;

// HOLDING_SUMMARY table
typedef struct HOLDING_SUMMARY_ROW {
	TIdent HS_CA_ID;
	char HS_S_SYMB[cSYMBOL_len + 1];
	int HS_QTY;
} * PHOLDING_SUMMARY_ROW;

// INDUSTRY table
typedef struct INDUSTRY_ROW {
	char IN_ID[cIN_ID_len + 1];
	char IN_NAME[cIN_NAME_len + 1];
	char IN_SC_ID[cSC_ID_len + 1];
} * PINDUSTRY_ROW;

// LAST_TRADE table
typedef struct LAST_TRADE_ROW {
	char LT_S_SYMB[cSYMBOL_len + 1];
	CDateTime LT_DTS;
	double LT_PRICE;
	double LT_OPEN_PRICE;
	INT64 LT_VOL;
} * PLAST_TRADE_ROW;

// NEWS_ITEM table
typedef struct NEWS_ITEM_ROW {
	TIdent NI_ID;
	char NI_HEADLINE[cNI_HEADLINE_len + 1];
	char NI_SUMMARY[cNI_SUMMARY_len + 1];
	char NI_ITEM[cNI_ITEM_len + 1];
	CDateTime NI_DTS;
	char NI_SOURCE[cNI_SOURCE_len + 1];
	char NI_AUTHOR[cNI_AUTHOR_len + 1];
} * PNEWS_ITEM_ROW;

// NEWS_XREF table
typedef struct NEWS_XREF_ROW {
	TIdent NX_NI_ID;
	TIdent NX_CO_ID;
} * PNEWS_XREF_ROW;

// SECTOR table
typedef struct SECTOR_ROW {
	char SC_ID[cSC_ID_len + 1];
	char SC_NAME[cSC_NAME_len + 1];
} * PSECTOR_ROW;

// SECURITY table
typedef struct SECURITY_ROW {
	char S_SYMB[cSYMBOL_len + 1];
	char S_ISSUE[cS_ISSUE_len + 1];
	char S_ST_ID[cST_ID_len + 1];
	char S_NAME[cS_NAME_len + 1];
	char S_EX_ID[cEX_ID_len + 1];
	TIdent S_CO_ID;
	INT64 S_NUM_OUT;
	CDateTime S_START_DATE;
	CDateTime S_EXCH_DATE;
	double S_PE;
	float S_52WK_HIGH;
	CDateTime S_52WK_HIGH_DATE;
	float S_52WK_LOW;
	CDateTime S_52WK_LOW_DATE;
	double S_DIVIDEND;
	double S_YIELD;
} * PSECURITY_ROW;

// SETTLEMENT table
typedef struct SETTLEMENT_ROW {
	TTrade SE_T_ID;
	char SE_CASH_TYPE[cSE_CASH_TYPE_len + 1];
	CDateTime SE_CASH_DUE_DATE;
	double SE_AMT;
} * PSETTLEMENT_ROW;

// STATUS_TYPE table
typedef struct STATUS_TYPE_ROW {
	char ST_ID[cST_ID_len + 1];
	char ST_NAME[cST_NAME_len + 1];
} * PSTATUS_TYPE_ROW;

// TAXRATE table
typedef struct TAX_RATE_ROW {
	char TX_ID[cTX_ID_len + 1];
	char TX_NAME[cTX_NAME_len + 1];
	double TX_RATE;
} * PTAX_RATE_ROW;

// TRADE table
typedef struct TRADE_ROW {
	TTrade T_ID;
	CDateTime T_DTS;
	char T_ST_ID[cST_ID_len + 1];
	char T_TT_ID[cTT_ID_len + 1];
	bool T_IS_CASH;
	char T_S_SYMB[cSYMBOL_len + 1];
	int T_QTY;
	double T_BID_PRICE;
	TIdent T_CA_ID;
	char T_EXEC_NAME[cEXEC_NAME_len + 1];
	double T_TRADE_PRICE;
	double T_CHRG;
	double T_COMM;
	double T_TAX;
	bool T_LIFO;
} * PTRADE_ROW;

// TRADE_HISTORY table
typedef struct TRADE_HISTORY_ROW {
	TTrade TH_T_ID;
	CDateTime TH_DTS;
	char TH_ST_ID[cST_ID_len + 1];
} * PTRADE_HISTORY_ROW;

// TRADE_REQUEST table
typedef struct TRADE_REQUEST_ROW {
	TTrade TR_T_ID;
	char TR_TT_ID[cTT_ID_len + 1];
	char TR_S_SYMB[cSYMBOL_len + 1];
	int TR_QTY;
	double TR_BID_PRICE;
	TIdent TR_B_ID;
} * PTRADE_REQUEST_ROW;

// TRADE_TYPE table
typedef struct TRADE_TYPE_ROW {
	char TT_ID[cTT_ID_len + 1];
	char TT_NAME[cTT_NAME_len + 1];
	bool TT_IS_SELL;
	bool TT_IS_MRKT;
} * PTRADE_TYPE_ROW;

// WATCH_ITEM table
typedef struct WATCH_ITEM_ROW {
	TIdent WI_WL_ID;
	char WI_S_SYMB[cSYMBOL_len + 1];
} * PWATCH_ITEM_ROW;

// WATCH_LIST table
typedef struct WATCH_LIST_ROW {
	TIdent WL_ID;
	TIdent WL_C_ID;
} * PWATCH_LIST_ROW;

// ZIP_CODE table
typedef struct ZIP_CODE_ROW {
	char ZC_CODE[cZC_CODE_len + 1];
	char ZC_TOWN[cZC_TOWN_len + 1];
	char ZC_DIV[cZC_DIV_len + 1];
} * PZIP_CODE_ROW;

} // namespace TPCE

#endif // #ifndef TABLE_ROWS_H
