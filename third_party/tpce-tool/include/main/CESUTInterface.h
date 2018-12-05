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
 * - Doug Johnson
 */

/******************************************************************************
 *   Description:        Interface base class to be used for deriving a sponsor
 *                       specific class for commmunicating with the SUT for all
 *                       customer-initiated transactions.
 ******************************************************************************/

#ifndef CE_SUT_INTERFACE_H
#define CE_SUT_INTERFACE_H

#include "TxnHarnessStructs.h"

namespace TPCE {

class CCESUTInterface {
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
	virtual ~CCESUTInterface(){};

	virtual bool BrokerVolume(PBrokerVolumeTxnInput pTxnInput) = 0;         // return whether it was successful
	virtual bool CustomerPosition(PCustomerPositionTxnInput pTxnInput) = 0; // return whether it was successful
	virtual bool MarketWatch(PMarketWatchTxnInput pTxnInput) = 0;           // return whether it was successful
	virtual bool SecurityDetail(PSecurityDetailTxnInput pTxnInput) = 0;     // return whether it was successful
	virtual bool TradeLookup(PTradeLookupTxnInput pTxnInput) = 0;           // return whether it was successful
	virtual bool TradeOrder(PTradeOrderTxnInput pTxnInput, INT32 iTradeType,
	                        bool bExecutorIsAccountOwner) = 0;    // return whether it was successful
	virtual bool TradeStatus(PTradeStatusTxnInput pTxnInput) = 0; // return whether it was successful
	virtual bool TradeUpdate(PTradeUpdateTxnInput pTxnInput) = 0; // return whether it was successful
};

} // namespace TPCE

#endif // CE_SUT_INTERFACE_H
