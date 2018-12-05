#ifndef BUCKETED_DATA_FILE_H
#define BUCKETED_DATA_FILE_H

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

#include <vector>

//#include <string> // for stoi C++11
#include <cstdlib> // for atoi

#include "ITextSplitter.h"
#include "ShrinkToFit.h"

namespace TPCE {
//
// Description:
// A template class for converting a series of text records into a
// bucketed binary in-memory structure for quick easy access.
//
// Exception Safety:
// The Basic guarantee is provided.
//
// Copy Behavior:
// Copying is allowed.
//
//
// Assumptions:
// - bucket IDs start at 1.
// - records are sorted by bucket ID smallest to largest.
//
template <class T> class BucketedDataFile {
public:
	// Leverage the size type of our underlying storage container but
	// insulate clients from the implementation particulars by creating
	// our own type.
	// Set this first so we can use it for recordCount.
	typedef typename std::vector<T>::size_type size_type;

private:
	std::vector<std::vector<T>> buckets;
	size_type recordCount;

public:
	enum SizeFilter { AllRecords, BucketsOnly };

	explicit BucketedDataFile(ITextSplitter &splitter) : recordCount(0) {
		// eof only returns true after trying to read the end, so
		// "prime the pump" by doing an initial read.
		std::deque<std::string> fields = splitter.getNextRecord();

		// Process each record.
		while (!splitter.eof()) {
			if (1 == fields.size() && "" == fields[0]) {
				// We found a blank line so skip it and move on.
				fields = splitter.getNextRecord();
				continue;
			}

			// The first field is the bucket ID for this record.
			// int bucketID = std::stoi(fields[0]); // C++11
			unsigned int bucketID = std::atoi(fields[0].c_str());
			fields.pop_front();

			if (buckets.size() == bucketID - 1) {
				// First record of a new bucket so add the bucket.
				buckets.push_back(std::vector<T>());
			}

			// Now we know the bucket exists so go ahead and add the record.
			buckets[bucketID - 1].push_back(T(fields));
			++recordCount;

			// Move on to the next record.
			fields = splitter.getNextRecord();
		}

		// Now that everything has been loaded tighten up our storage.
		// NOTE: shrinking the outer bucket vector has the side effect of
		// shrinking all the internal bucket vectors.
		shrink_to_fit<std::vector<std::vector<T>>>(buckets);
		// buckets.shrink_to_fit(); // C++11
	}

	//
	// Default copies and destructor are ok.
	//
	// ~BucketedDataFile();
	// BucketedDataFile(const BucketedDataFile&);
	// BucketedDataFile& operator=(const BucketedDataFile&);
	//

	size_type size(SizeFilter filter = AllRecords) const {
		return (filter == AllRecords ? recordCount : buckets.size());
	}

	// Provide 0-based access to the buckets.
	const std::vector<T> &operator[](size_type idx) const {
		return buckets[idx];
	}

	// Provide range-checked 0-based access to the buckets.
	const std::vector<T> &at(size_type idx) const {
		return buckets.at(idx);
	}

	// Provide range-checked bucket-ID-based access by to the buckets.
	const std::vector<T> &getBucket(size_type bucketID, bool rangeCheckedAccess = false) const {
		size_type idx = bucketID - 1;
		return (rangeCheckedAccess ? buckets.at(idx) : buckets[idx]);
	}
};

} // namespace TPCE
#endif // BUCKETED_DATA_FILE_H
