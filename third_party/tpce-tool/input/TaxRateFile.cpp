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

#include "input/TaxRateFile.h"

using namespace TPCE;

CTaxRateFile::CTaxRateFile(const TaxRateCountryDataFile_t &countryDataFile,
                           const TaxRateDivisionDataFile_t &divisionDataFile)
    : cdf(&countryDataFile), ddf(&divisionDataFile) {
	locations.reserve(cdf->size() + ddf->size());

	int maxBucketIdx = cdf->size(cdf->BucketsOnly);
	for (int bucketIdx = 0; bucketIdx < maxBucketIdx; ++bucketIdx) {
		int maxRecordIdx = (*cdf)[bucketIdx].size();
		for (int recordIdx = 0; recordIdx < maxRecordIdx; ++recordIdx) {
			locations.push_back(RecordLocation(bucketIdx, recordIdx));
		}
	}

	maxBucketIdx = ddf->size(ddf->BucketsOnly);
	for (int bucketIdx = 0; bucketIdx < maxBucketIdx; ++bucketIdx) {
		int maxRecordIdx = (*ddf)[bucketIdx].size();
		for (int recordIdx = 0; recordIdx < maxRecordIdx; ++recordIdx) {
			locations.push_back(RecordLocation(bucketIdx, recordIdx));
		}
	}
}

// Provide range-checked access to the records.
const ITaxRateFileRecord &CTaxRateFile::operator[](unsigned int idx) const {
	if (0 == size()) {
		// This is an attempt to access an empty container.
		throw std::out_of_range("Attemt to index into an emtpy DataFile.");
	}

	unsigned int maxIdx = size() - 1;

	if (maxIdx < idx) {
		std::ostringstream msg;
		msg << "Provided index (" << idx << ") is outside the legal range (0:" << maxIdx << ").";
		throw std::out_of_range(msg.str());
	}

	if (idx < cdf->size()) {
		return (*cdf)[locations[idx].bucketIdx][locations[idx].recordIdx];
	} else {
		return (*ddf)[locations[idx].bucketIdx][locations[idx].recordIdx];
	}
}

CTaxRateFile::size_type CTaxRateFile::size() const {
	return locations.size();
}
