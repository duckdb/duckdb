#ifndef TEXT_FILE_SPLITTER_H
#define TEXT_FILE_SPLITTER_H

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

#include <sstream>

#include "ITextSplitter.h"
#include "StreamSplitter.h"

namespace TPCE {

//
// Description:
// A class for managing a text file stream and splitting it
// into records made up of fields.
//
// Exception Safety:
// The Basic guarantee is provided.
//
// Copy Behavior:
// Copying is disallowed.
//

class TextFileSplitter : public ITextSplitter {
private:
	std::istringstream file;
	StreamSplitter splitter;

	// Copying a StreamSplitter doesn't make sense so disallow it.
	TextFileSplitter(const TextFileSplitter &);
	TextFileSplitter &operator=(const TextFileSplitter &);

public:
	static const char DEFAULT_RECORD_DELIMITER = StreamSplitter::DEFAULT_RECORD_DELIMITER;
	static const char DEFAULT_FIELD_DELIMITER = StreamSplitter::DEFAULT_FIELD_DELIMITER;

	explicit TextFileSplitter(const std::string &string, char recordDelimiter = DEFAULT_RECORD_DELIMITER,
	                          char fieldDelimiter = DEFAULT_FIELD_DELIMITER);
	~TextFileSplitter();

	// ITextSplitter implementation
	bool eof() const;
	std::deque<std::string> getNextRecord();
};

} // namespace TPCE
#endif // TEXT_FILE_SPLITTER_H
