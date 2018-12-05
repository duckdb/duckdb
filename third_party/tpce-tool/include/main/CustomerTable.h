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
 *   Contains class definition to generate Customer table.
 */
#ifndef CUSTOMER_TABLE_H
#define CUSTOMER_TABLE_H

#include "EGenTables_common.h"
#include "input/DataFileManager.h"

namespace TPCE {
const int iNumEMAIL_DOMAINs = 6;

class CCustomerTable : public TableTemplate<CUSTOMER_ROW> {
private:
	TIdent m_iRowsToGenerate; // total # of rows to generate
	CPerson m_person;
	const AreaCodeDataFile_t &m_Phones;
	TIdent m_iStartFromCustomer;
	TIdent m_iCustomerCount;
	const StatusTypeDataFile_t &m_StatusTypeFile; // STATUS_TYPE table from the flat file
	CCustomerSelection m_CustomerSelection;
	TIdent m_iCompanyCount;        // number of Companies
	unsigned int m_iExchangeCount; // number of Exchanges

	void GenerateC_ST_ID();
	void GeneratePersonInfo(); // generate last name, first name, and gender.
	void GenerateC_DOB();
	void GenerateC_AD_ID();
	void GenerateC_CTRY_1();
	void GenerateC_LOCAL_1();
	void GenerateC_AREA_1();
	void GenerateC_EXT_1();
	void GenerateC_CTRY_2();
	void GenerateC_LOCAL_2();
	void GenerateC_AREA_2();
	void GenerateC_EXT_2();
	void GenerateC_CTRY_3();
	void GenerateC_LOCAL_3();
	void GenerateC_AREA_3();
	void GenerateC_EXT_3();
	void GenerateC_EMAIL_1_and_C_EMAIL_2();

	/*
	 *   Reset the state for the next load unit
	 */
	void InitNextLoadUnit();

public:
	/*
	 *  Constructor for the CUSTOMER table class.
	 *
	 *  PARAMETERS:
	 *       IN  dfm                 - input flat files loaded in memory
	 *       IN  iCustomerCount      - number of customers to generate
	 *       IN  iStartFromCustomer  - ordinal position of the first customer in
	 * the sequence (Note: 1-based)
	 */
	CCustomerTable(const DataFileManager &dfm, TIdent iCustomerCount, TIdent iStartFromCustomer);

	TIdent GenerateNextC_ID(); // generate C_ID and store state information;
	                           // return false if all ids are generated
	TIdent GetCurrentC_ID();   // return current customer id

	void GetC_TAX_ID(TIdent C_ID,
	                 char *szOutput);     // return tax id (ala Social Security number)
	eCustomerTier GetC_TIER(TIdent C_ID); // returns unique C_TIER for a given customer id

	bool GenerateNextRecord(); // generates the next table row
};

} // namespace TPCE

#endif // CUSTOMER_TABLE_H
