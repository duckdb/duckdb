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
 * - Christopher Chan-Nui
 */

#include <iomanip>
#include <sstream>
#include <stdexcept>

#include <cstdlib>
#include <cerrno>
#include <stdlib.h> // needed for strtoull which is not in the C++ headers

#include "utilities/EGenStandardTypes.h"

#ifdef WIN32
#define strtoull _strtoui64
#endif

using std::strtod;
using std::strtoul;

namespace TPCE {

// Converts an ASCII string into a 64 bit integer.  Accepts a scaling factor of
// 'K', 'M', or 'G' on values.
// ptr - string to convert
// Returns the integral 64 bit representation
INT64 strtoint64(const char *ptr) {
	INT64 val;
	char *endp;
	errno = 0;
	val = strtoull(ptr, &endp, 0);
	if (errno != 0) {
		std::ostringstream strm;
		strm << "Unable to parse integer '" << ptr << "'" << std::endl;
		throw std::runtime_error(strm.str());
	}
	switch (*endp) {
	case 'G':
		val *= 1000 * 1000 * 1000;
		break;
	case 'M':
		val *= 1000 * 1000;
		break;
	case 'K':
		val *= 1000;
		break;
	case '\0':
		endp--;
		break;
	default:
		std::ostringstream strm;
		strm << "Unable to parse invalid scale factor on integer '" << ptr << "'" << std::endl;
		throw std::runtime_error(strm.str());
	}
	if (*++endp != '\0') {
		std::ostringstream strm;
		strm << "Unable to parse trailing characters on integer '" << ptr << "'" << std::endl;
		throw std::runtime_error(strm.str());
	}
	return val;
}

// Converts an ASCII string into a double.  Accepts a scaling factor of
// 'K', 'M', or 'G' on values.
// ptr - string to convert
// Returns the double representation
double strtodbl(const char *ptr) {
	double val;
	char *endp;
	errno = 0;
	val = strtod(ptr, &endp);
	if (errno != 0) {
		std::ostringstream strm;
		strm << "Unable to parse floating point number '" << ptr << "'" << std::endl;
		throw std::runtime_error(strm.str());
	}
	switch (*endp) {
	case 'G':
		val *= 1000.0 * 1000.0 * 1000.0;
		break;
	case 'M':
		val *= 1000.0 * 1000.0;
		break;
	case 'K':
		val *= 1000.0;
		break;
	case '\0':
		endp--;
		break;
	default:
		std::ostringstream strm;
		strm << "Unable to parse invalid scale factor on floating point number '" << ptr << "'" << std::endl;
		throw std::runtime_error(strm.str());
	}
	if (*++endp != '\0') {
		std::ostringstream strm;
		strm << "Unable to parse trailing characters on floating point number '" << ptr << "'" << std::endl;
		throw std::runtime_error(strm.str());
	}
	return val;
}

// Converts an ASCII string in "HH:MM:SS" form into a INT64.  HH: or HH:MM:
// may be omitted.
// ptr - string to convert
// Returns the 64 bit integer representation
INT64 timestrtoint64(const char *ptr) {
	INT64 val;
	char *endp;
	errno = 0;
	val = strtoul(ptr, &endp, 0);
	if (*endp == ':') {
		val = val * 60 + strtoul(endp + 1, &endp, 0);
		if (*endp == ':') {
			val = val * 60 + strtoul(endp + 1, &endp, 0);
		}
	}
	if (errno != 0 || *endp != '\0') {
		std::ostringstream strm;
		strm << "Unable to parse time '" << ptr << "'" << std::endl;
		throw std::runtime_error(strm.str());
	}
	return val;
}

// Converts a 64 bit integer into an HH:MM:SS string
// val - integer to convert
// Returns a string containing the formatted value.
std::string int64totimestr(INT64 val) {
	std::ostringstream strm;
	int sec = static_cast<int>(val % 60);
	val /= 60;
	int min = static_cast<int>(val % 60);
	int hrs = static_cast<int>(val / 60);

	strm << std::setfill('0');
	if (hrs) {
		strm << std::setw(2) << hrs << ":";
	}
	strm << std::setw(2) << min << ":" << std::setw(2) << sec;

	return strm.str();
}

} // namespace TPCE
