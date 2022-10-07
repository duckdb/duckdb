#include "lib/ryu.hpp"
#include "liblwgeom/liblwgeom_internal.hpp"

namespace duckdb {

/*
 * Print an ordinate value using at most **maxdd** number of decimal digits
 * The actual number of printed decimal digits may be less than the
 * requested ones if out of significant digits.
 *
 * The function will write at most OUT_DOUBLE_BUFFER_SIZE bytes, including the
 * terminating NULL.
 * It returns the number of bytes written (exluding the final NULL)
 *
 */
int lwprint_double(double d, int maxdd, char *buf) {
	int length;
	double ad = fabs(d);
	int precision = FP_MAX(0, maxdd);

	if (ad <= OUT_MIN_DOUBLE || ad >= OUT_MAX_DOUBLE) {
		length = d2sexp_buffered_n(d, precision, buf);
	} else {
		length = d2sfixed_buffered_n(d, precision, buf);
	}
	buf[length] = '\0';

	return length;
}

} // namespace duckdb