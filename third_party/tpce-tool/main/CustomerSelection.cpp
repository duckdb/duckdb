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
 *   Description:        Implementation of the CustomerSelection class.
 *                       (see CustomerSelction.h for details)
 ******************************************************************************/

#include "main/EGenTables_stdafx.h"

using namespace TPCE;

/*
 *   Default constructor.
 */
CCustomerSelection::CCustomerSelection()
    : m_pRND(NULL), m_iStartFromCustomer(0 + iTIdentShift), m_iCustomerCount(0), m_bPartitionByCID(false),
      m_iPartitionPercent(0), m_iMyStartFromCustomer(0 + iTIdentShift), m_iMyCustomerCount(0) {
}

/*
 *   Constructor to set the customer range when not partitioining
 */
CCustomerSelection::CCustomerSelection(CRandom *pRND, TIdent iStartFromCustomer, TIdent iCustomerCount)
    : m_pRND(pRND), m_iStartFromCustomer(iStartFromCustomer + iTIdentShift), m_iCustomerCount(iCustomerCount),
      m_bPartitionByCID(false), m_iPartitionPercent(0), m_iMyStartFromCustomer(0 + iTIdentShift),
      m_iMyCustomerCount(0) {
}

/*
 *   Constructor to set subrange when partitioning by C_ID.
 */
CCustomerSelection::CCustomerSelection(CRandom *pRND, TIdent iStartFromCustomer, TIdent iCustomerCount,
                                       int iPartitionPercent, TIdent iMyStartFromCustomer, TIdent iMyCustomerCount)
    : m_pRND(pRND), m_iStartFromCustomer(iStartFromCustomer + iTIdentShift), m_iCustomerCount(iCustomerCount)

      ,
      m_bPartitionByCID(true), m_iPartitionPercent(iPartitionPercent),
      m_iMyStartFromCustomer(iMyStartFromCustomer + iTIdentShift), m_iMyCustomerCount(iMyCustomerCount) {
	if ((iStartFromCustomer == iMyStartFromCustomer) && (iCustomerCount == iMyCustomerCount)) {
		// Even though the partitioning constructor was called, we're apparently
		// not really partitioning.
		m_bPartitionByCID = false;
	}
}

/*
 *   Re-set the customer range for the partition.
 */
void CCustomerSelection::SetPartitionRange(TIdent iStartFromCustomer, TIdent iCustomerCount) {
	if (m_bPartitionByCID) {
		m_iMyStartFromCustomer = iStartFromCustomer;
		m_iMyCustomerCount = iCustomerCount;
	}
}

/*
 *   Forward permutation.
 */
TIdent CCustomerSelection::Permute(TIdent iLow, TIdent iHigh) {
	return ((677 * iLow + 33 * (iHigh + 1)) % 1000);
}

/*
 *   Inverse permutation.
 */
TIdent CCustomerSelection::InversePermute(TIdent iLow, TIdent iHigh) {
	// Extra mod to make the result always positive
	//
	return (((((613 * (iLow - 33 * (iHigh + 1))) % 1000) + 1000) % 1000));
}

/*
 *   Return scrambled inverse customer id in range of 0 to 999.
 */
UINT CCustomerSelection::GetInverseCID(TIdent C_ID) {
	UINT iCHigh = (UINT)CHigh(C_ID);
	UINT iInverseCID = (UINT)InversePermute(CLow(C_ID), iCHigh);

	if (iInverseCID < 200) // Tier 1: value 0 to 199
	{
		return ((3 * iInverseCID + (iCHigh + 1)) % 200);
	} else {
		if (iInverseCID < 800) // Tier 2: value 200 to 799
		{
			return (((59 * iInverseCID + 47 * (iCHigh + 1)) % 600) + 200);
		} else // Tier 3: value 800 to 999
		{
			return (((23 * iInverseCID + 17 * (iCHigh + 1)) % 200) + 800);
		}
	}
}

/*
 *   Return customer tier.
 */
eCustomerTier CCustomerSelection::GetTier(TIdent C_ID) {
	TIdent iRevC_ID = InversePermute(CLow(C_ID), CHigh(C_ID));

	if (iRevC_ID < 200) {
		return eCustomerTierOne;
	} else {
		if (iRevC_ID < 800) {
			return eCustomerTierTwo;
		} else {
			return eCustomerTierThree;
		}
	}
}

/*
 *   Return a non-uniform random customer and the associated tier.
 */
void CCustomerSelection::GenerateRandomCustomer(TIdent &C_ID, eCustomerTier &C_TIER) {
	// Can't use this function if there is no external RNG.
	//
	if (m_pRND == NULL) {
		return;
	}

	double fCW = m_pRND->RndDoubleIncrRange(0.0001, 2000, 0.000000001);

	// Uniformly select the higher portion of the C_ID.
	// Use "short-circuit" logic to avoid unnecessary call to RNG.
	TIdent iCHigh;
	if (m_bPartitionByCID && m_pRND->RndPercent(m_iPartitionPercent)) {
		// Generate a load unit inside the partition.
		iCHigh = (m_pRND->RndInt64Range(m_iMyStartFromCustomer,
		                                m_iMyStartFromCustomer + m_iMyCustomerCount - 1) -
		          1) // minus 1 for the upper boundary case
		         / 1000;
	} else {
		// Generate a load unit across the entire range
		iCHigh = (m_pRND->RndInt64Range(m_iStartFromCustomer,
		                                m_iStartFromCustomer + m_iCustomerCount - 1) -
		          1) // minus 1 for the upper boundary case
		         / 1000;
	}

	// Non-uniformly select the lower portion of the C_ID.
	//
	int iCLow;

	if (fCW <= 200) {
		// tier one
		//
		iCLow = (int)ceil(sqrt(22500 + 500 * fCW) - 151);

		C_TIER = eCustomerTierOne;
	} else {
		if (fCW <= 1400) {
			// tier two
			//
			iCLow = (int)ceil(sqrt(290000 + 1000 * fCW) - 501);

			C_TIER = eCustomerTierTwo;
		} else {
			// tier three
			//
			iCLow = (int)ceil(149 + sqrt(500 * fCW - 277500));

			C_TIER = eCustomerTierThree;
		}
	}

	C_ID = iCHigh * 1000 + Permute(iCLow, iCHigh) + 1;
}

/////////*
////////*   Return a non-uniform random customer and tier.
////////*/
////////void CCustomerSelection::GenerateCustomerIdAndTier(TIdent &C_ID,
/// eCustomerTier &C_TIER, bool bAcrossEntireRange)
////////{
////////    // Can't use this function if there is no external RNG.
////////    //
////////    if (m_pRND == NULL)
////////    {
////////        return;
////////    }
////////
////////    double fCW = m_pRND->RndDoubleRange(0.0001, 2000);
////////
////////    // Select uniformly higher portion of the Customer ID.
////////    //
////////    TIdent iCHigh;
////////    if (bAcrossEntireRange)
////////    {
////////        // Generate a load unit across the entire range
////////        iCHigh = (m_pRND->RndInt64Range(1,
/// m_iAdjustedTotalCustomerCount) - 1) // minus 1 for the upper boundary case
////////                                    / 1000;
////////        if( iCHigh >= ( m_iStartFromCustomer / 1000 ))
////////        {
////////            iCHigh += ( m_iCustomerCount / 1000 );
////////        }
////////    }
////////    else
////////    {
////////        // Generate a load unit inside the parition.
////////        iCHigh = (m_pRND->RndInt64Range(m_iStartFromCustomer,
////////                                    m_iStartFromCustomer +
/// m_iCustomerCount - 1) - 1) // minus 1 for the upper boundary case
////////                                    / 1000;
////////    }
////////
////////    // Select non-uniformly the lower portion of the Customer ID.
////////    //
////////    int iCLow;
////////
////////    if (fCW <= 200)
////////    {
////////        // tier one
////////        //
////////        iCLow = (int) ceil( sqrt(22500 + 500 * fCW) - 151 );
////////
////////        C_TIER = eCustomerTierOne;
////////    }
////////    else
////////    {
////////        if (fCW <=1400)
////////        {
////////            // tier two
////////            //
////////            iCLow = (int) ceil( sqrt(290000 + 1000 * fCW) - 501 );
////////
////////            C_TIER = eCustomerTierTwo;
////////        }
////////        else
////////        {
////////            // tier three
////////            //
////////            iCLow = (int) ceil( 149 + sqrt(500 * fCW - 277500) );
////////
////////            C_TIER = eCustomerTierThree;
////////        }
////////    }
////////
////////    C_ID = iCHigh * 1000 + Permute(iCLow, iCHigh) + 1;
////////}
