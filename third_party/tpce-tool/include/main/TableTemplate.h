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
 *   This template represents a general table class.
 *   The type specified is the type of the table row.
 */
#ifndef TABLE_TEMPLATE_H
#define TABLE_TEMPLATE_H

#include <cstring>

#include "utilities/EGenStandardTypes.h"
#include "utilities/Random.h"
#include "utilities/RNGSeeds.h"

namespace TPCE {

template <typename T> class TableTemplate {
protected:
	T m_row;                 // private row for generation
	TIdent m_iLastRowNumber; // sequential last row number
	bool m_bMoreRecords;     // true if more records can be generated, otherwise
	                         // false
	CRandom m_rnd;           // random generator - present in all tables
public:
	/*
	 *  The Constructor - just initializes member variables.
	 *
	 *  PARAMETERS:
	 *           not applicable.
	 *
	 *  RETURNS:
	 *           not applicable.
	 */
	TableTemplate()
	    : m_iLastRowNumber(0), m_bMoreRecords(false) // assume
	{
		ClearRecord(); // zero the row

		m_rnd.SetSeed(RNGSeedTableDefault);
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
	virtual ~TableTemplate(){};

	/*
	 *  Generate the next record (row).
	 *
	 *  PARAMETERS:
	 *           none.
	 *
	 *  RETURNS:
	 *           true    - if more records are available in the table.
	 *           false   - if no more records can be generated.
	 */
	virtual bool GenerateNextRecord() = 0; // abstract class

	/*
	 *  Return whether all records in this table have been generated.
	 *
	 *  PARAMETERS:
	 *           none.
	 *
	 *  RETURNS:
	 *           true    - if more records are available in the table.
	 *           false   - if no more records can be generated.
	 */
	bool MoreRecords() {
		return m_bMoreRecords;
	}

	/*
	 *   Return a reference to the data row.
	 *
	 *   PARAMETERS:
	 *           none.
	 *
	 *   RETURNS:
	 *           typed reference to internal row buffer.
	 */
	// T const * GetRow() { return &m_row; }
	const T &GetRow() {
		return m_row;
	}

	/*
	 *   Set the internal record buffer to zero.
	 *
	 *   This routine allows bitwise comparison between two versions of a row,
	 *   because it clears the unused bytes in string values
	 *   (past the NULL terminators).
	 *
	 *   Note: the record cannot contain any inter-record state information,
	 *   or this information will be lost.
	 *
	 *   PARAMETERS:
	 *           none.
	 *
	 *   RETURNS:
	 *           none.
	 */
	void ClearRecord() {
		memset(&m_row, 0, sizeof(m_row));
	}
};

} // namespace TPCE

#endif // TABLE_TEMPLATE_H
