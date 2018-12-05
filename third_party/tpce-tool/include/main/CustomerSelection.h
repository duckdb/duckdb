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
 * - Sergey Vasilevskiy, Doug Johnson
 */

/******************************************************************************
 *   Description:        This class encapsulates customer tier distribution
 *                       functions and provides functionality to:
 *                       - Generate customer tier based on customer ID
 *                       - Generate non-uniform customer ID
 *                       - Generate customer IDs in a specified partition, and
 *                         outside the specified partition a set percentage of
 *                         the time.
 ******************************************************************************/

#ifndef CUSTOMER_SELECTION_H
#define CUSTOMER_SELECTION_H

#include "utilities/Random.h"

namespace TPCE {

/*
 *   Define customer tier type.
 */
enum eCustomerTier { eCustomerTierOne = 1, eCustomerTierTwo, eCustomerTierThree };

class CCustomerSelection {
	CRandom *m_pRND; // external random number generator

	TIdent m_iStartFromCustomer;
	TIdent m_iCustomerCount;

	/*
	 *   Used when partitioning by C_ID.
	 */
	bool m_bPartitionByCID;
	int m_iPartitionPercent;
	TIdent m_iMyStartFromCustomer;
	TIdent m_iMyCustomerCount;

	/*
	 *   Forward permutation (used to convert ordinal C_ID into real C_ID).
	 */
	TIdent Permute(TIdent iLow, TIdent iHigh);

	/*
	 *   Inverse permutation (used to convert real C_ID into it's ordinal
	 * number).
	 */
	TIdent InversePermute(TIdent iLow, TIdent iHigh);

	/*
	 *   Get lower 3 digits.
	 */
	inline TIdent CLow(TIdent C_ID) {
		return ((C_ID - 1) % 1000);
	}

	/*
	 *   Get higher digits.
	 */
	inline TIdent CHigh(TIdent C_ID) {
		return ((C_ID - 1) / 1000);
	}

public:
	/*
	 *   Default constructor.
	 */
	CCustomerSelection();

	/*
	 *   Constructor to set the customer range.
	 */
	CCustomerSelection(CRandom *pRND, TIdent iStartFromCustomer, TIdent iCustomerCount);

	/*
	 *   Constructor to set subrange when paritioning by C_ID.
	 */
	CCustomerSelection(CRandom *pRND, TIdent iStartFromCustomer, TIdent iCustomerCount, int iPartitionPercent,
	                   TIdent iMyStartFromCustomer, TIdent iMyCustomerCount);

	/*
	 *   Re-set the customer range for the parition.
	 */
	void SetPartitionRange(TIdent iStartFromCustomer, TIdent iCustomerCount);

	/*
	 *   Return scrambled inverse customer id.
	 */
	UINT GetInverseCID(TIdent C_ID);

	/*
	 *   Return customer tier.
	 */
	eCustomerTier GetTier(TIdent C_ID);

	/*
	 *   Return a non-uniform random customer and the associated tier.
	 */
	void GenerateRandomCustomer(TIdent &C_ID, eCustomerTier &C_TIER);
};

} // namespace TPCE

#endif // CUSTOMER_SELECTION_H
