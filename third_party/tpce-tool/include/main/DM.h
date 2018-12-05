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
 *   Description:        This class provides Data-Maintenance functionality.
 *                       It generates all necessary inputs for the
 *                       Data-Maintenance transaction. These inputs are then
 *                       made available to a sponsor provided callback interface
 *                       to the SUT (see DMSUTInterface.h).
 *
 *                       The constructor to this class accepts the following
 *                       parameters.
 *
 *                       - pSUT: a pointer to an instance of a sponsor provided
 *                       subclassing of the CCESUTInterface class.
 *                                               - pLogger: a pointer to an
 *instance of CEGenLogger or a sponsor provided subclassing of the CBaseLogger
 *class.
 *                       - dfm: a reference to an instance of the
 *                       CInputFiles class containing all input files loaded
 *                       into memory.
 *                       - iActiveCustomerCount: the total number of customers
 *                       to emulate. C_IDs will be generated in the range of
 *                       1 to iActiveCustomerCount.
 *                       - RandomSeed: seed to be used for the RNG.
 *
 *                       The DM class provides the following entry point.
 *
 *                       - DoTxn: this entry point will generate all required
 *                       inputs and provide those inputs to sponsor code at the
 *                       - DoCleanupTxn: this entry point will execute the
 *                       Trade-Cleanup transaction. This must be run at the
 *                       start of each measurement run before any CE or MEE
 *                       transactions are executed.
 ******************************************************************************/

#ifndef DM_H
#define DM_H

#include "utilities/EGenUtilities_stdafx.h"
#include "TxnHarnessStructs.h"
#include "DMSUTInterface.h"
#include "BaseLogger.h"
#include "DriverParamSettings.h"

#include "input/DataFileManager.h"

namespace TPCE {

class CDM {
private:
	CDriverGlobalSettings m_DriverGlobalSettings;
	CDriverDMSettings m_DriverDMSettings;

	CRandom m_rnd;
	CCustomerSelection m_CustomerSelection;
	CCustomerAccountsAndPermissionsTable m_AccsAndPerms;
	const CSecurityFile &m_Securities;
	const CCompanyFile &m_Companies;
	const TaxRateDivisionDataFile_t &m_TaxRatesDivision;
	const StatusTypeDataFile_t &m_StatusType;
	TIdent m_iSecurityCount;
	TIdent m_iCompanyCount;
	TIdent m_iStartFromCompany;
	INT32 m_iDivisionTaxCount;
	TIdent m_iStartFromCustomer;

	INT32 m_DataMaintenanceTableNum;

	TDataMaintenanceTxnInput m_TxnInput;
	TTradeCleanupTxnInput m_CleanupTxnInput;
	CDMSUTInterface *m_pSUT;
	CBaseLogger *m_pLogger;

	// Automatically generate unique RNG seeds
	void AutoSetRNGSeeds(UINT32 UniqueId);

	TIdent GenerateRandomCustomerId();

	TIdent GenerateRandomCustomerAccountId();

	TIdent GenerateRandomCompanyId();

	TIdent GenerateRandomSecurityId();

	// Initialization that is common for all constructors.
	void Initialize();

public:
	// Constructor - automatice RNG seed generation
	CDM(CDMSUTInterface *pSUT, CBaseLogger *pLogger, const DataFileManager &dfm, TIdent iConfiguredCustomerCount,
	    TIdent iActiveCustomerCount, INT32 iScaleFactor, INT32 iDaysOfInitialTrades, UINT32 UniqueId);

	// Constructor - RNG seed provided
	CDM(CDMSUTInterface *pSUT, CBaseLogger *pLogger, const DataFileManager &dfm, TIdent iConfiguredCustomerCount,
	    TIdent iActiveCustomerCount, INT32 iScaleFactor, INT32 iDaysOfInitialTrades, UINT32 UniqueId, RNGSEED RNGSeed);

	~CDM(void);

	RNGSEED GetRNGSeed(void);
	void DoTxn(void);
	void DoCleanupTxn(void);
};

} // namespace TPCE

#endif // #ifndef DM_H
