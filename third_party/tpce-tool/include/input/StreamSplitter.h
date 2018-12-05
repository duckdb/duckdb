#ifndef STREAM_SPLITTER_H
#define STREAM_SPLITTER_H

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

#include <istream>
#include <deque>
#include <string>

#include "ITextSplitter.h"

namespace TPCE {

//
// Description:
// A class to split a text stream into a series of records made up
// of fields.
//
// Exception Safety:
// The Basic guarantee is provided.
//
// Copy Behavior:
// Copying is disallowed.
//

class StreamSplitter : public ITextSplitter {
private:
	char recordDelim;
	char fieldDelim;
	std::istream &stream;

	// Copying a StreamSplitter doesn't make sense so disallow it.
	StreamSplitter(const StreamSplitter &);
	StreamSplitter &operator=(const StreamSplitter &);

public:
	static const char DEFAULT_RECORD_DELIMITER = '\n';
	static const char DEFAULT_FIELD_DELIMITER = '\t';

	explicit StreamSplitter(std::istream &textStream, char recordDelimiter = DEFAULT_RECORD_DELIMITER,
	                        char fieldDelimiter = DEFAULT_FIELD_DELIMITER);
	//~StreamSplitter() - default destructor is ok.

	// ITextSplitter implementation
	bool eof() const;
	std::deque<std::string> getNextRecord();
};

} // namespace TPCE
#endif // STREAM_SPLITTER_H
