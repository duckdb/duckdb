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
 *   Base interface for all loader classes.
 */

#ifndef BASE_LOADER_H
#define BASE_LOADER_H

namespace TPCE {

template <typename T> class CBaseLoader {

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
	virtual ~CBaseLoader(){};

	/*
	 *  This function is provided to reset the loader to
	 *  a clean state. "Clean state" here means a sequence of
	 *  WriteNextRecord(), followed by Commit(), followed by FinishLoad()
	 *  can be called.
	 *
	 *  PARAMETERS:
	 *           none.
	 *
	 *  RETURNS:
	 *           none.
	 */
	virtual void Init(){}; // default implementation is empty

	/*
	 *  Receive a new record to be loaded into the database.
	 *  This is the main function that is called
	 *  for every generated record.
	 *
	 *  Note: the records are not guaranteed to actually be inserted
	 *  into the database after this call. It is Commit() that ensures that
	 *  all records received by WriteNextRecord are persisted in the database.
	 *
	 *  PARAMETERS:
	 *           IN  next_record          - a pointer to a structure, containing
	 * the record
	 *
	 *  RETURNS:
	 *           none.
	 */
	virtual void WriteNextRecord(const T &next_record) {
		printf("BaseLoader - const ref\n");
	};

	/*
	 *  Commit any records that might have been kept in temporary
	 *  storage to the database. After this call, all records received
	 *  by WriteNextRecord should be present in the database.
	 *
	 *  PARAMETERS:
	 *           none.
	 *
	 *  RETURNS:
	 *           none.
	 */
	virtual void Commit(){}; // default implementation is empty

	/*
	 *  Release any resources held for loading.
	 *  Also, commit any records that might have been kept in temporary
	 *  storage to the database.
	 *  This function is called once, after the last record has been loaded.
	 *
	 *  PARAMETERS:
	 *           none.
	 *
	 *  RETURNS:
	 *           none.
	 */
	virtual void FinishLoad() = 0; // must be defined in subclasses
};

} // namespace TPCE

#endif // #ifndef BASE_LOADER_H
