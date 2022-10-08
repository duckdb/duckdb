#include "liblwgeom/liblwgeom.hpp"
#include "liblwgeom/liblwgeom_internal.hpp"
#include "liblwgeom/lwinline.hpp"

#include <cassert>
#include <cstring>

namespace duckdb {

float next_float_down(double d) {
	float result;
	if (d > (double)FLT_MAX)
		return FLT_MAX;
	if (d <= (double)-FLT_MAX)
		return -FLT_MAX;
	result = d;

	if (((double)result) <= d)
		return result;

	return nextafterf(result, -1 * FLT_MAX);
}

/*
 * Returns the float that's very close to the input, but >=.
 * handles the funny differences in float4 and float8 reps.
 */
float next_float_up(double d) {
	float result;
	if (d >= (double)FLT_MAX)
		return FLT_MAX;
	if (d < (double)-FLT_MAX)
		return -FLT_MAX;
	result = d;

	if (((double)result) >= d)
		return result;

	return nextafterf(result, FLT_MAX);
}

/*
 * set point N to the given value
 * NOTE that the pointarray can be of any
 * dimension, the appropriate ordinate values
 * will be extracted from it
 *
 */
void ptarray_set_point4d(POINTARRAY *pa, uint32_t n, const POINT4D *p4d) {
	uint8_t *ptr;
	assert(n < pa->npoints);
	ptr = getPoint_internal(pa, n);
	switch (FLAGS_GET_ZM(pa->flags)) {
	case 3:
		memcpy(ptr, p4d, sizeof(POINT4D));
		break;
	case 2:
		memcpy(ptr, p4d, sizeof(POINT3DZ));
		break;
	case 1:
		memcpy(ptr, p4d, sizeof(POINT2D));
		ptr += sizeof(POINT2D);
		memcpy(ptr, &(p4d->m), sizeof(double));
		break;
	case 0:
		memcpy(ptr, p4d, sizeof(POINT2D));
		break;
	}
}

/*
 * Copies a point from the point array into the parameter point
 * will set point's z=NO_Z_VALUE  if pa is 2d
 * will set point's m=NO_M_VALUE  if pa is 3d or 2d
 *
 * NOTE: this will modify the point4d pointed to by 'point'.
 *
 * @return 0 on error, 1 on success
 */
int getPoint4d_p(const POINTARRAY *pa, uint32_t n, POINT4D *op) {
	uint8_t *ptr;
	int zmflag;

	if (!pa) {
		// lwerror("%s [%d] NULL POINTARRAY input", __FILE__, __LINE__);
		return 0;
	}

	if (n >= pa->npoints) {
		// lwnotice("%s [%d] called with n=%d and npoints=%d", __FILE__, __LINE__, n, pa->npoints);
		return 0;
	}

	/* Get a pointer to nth point offset and zmflag */
	ptr = getPoint_internal(pa, n);
	zmflag = FLAGS_GET_ZM(pa->flags);

	switch (zmflag) {
	case 0: /* 2d  */
		memcpy(op, ptr, sizeof(POINT2D));
		op->m = NO_M_VALUE;
		op->z = NO_Z_VALUE;
		break;

	case 3: /* ZM */
		memcpy(op, ptr, sizeof(POINT4D));
		break;

	case 2: /* Z */
		memcpy(op, ptr, sizeof(POINT3DZ));
		op->m = NO_M_VALUE;
		break;

	case 1: /* M */
		memcpy(op, ptr, sizeof(POINT3DM));
		op->m = op->z; /* we use Z as temporary storage */
		op->z = NO_Z_VALUE;
		break;

	default:
		// lwerror("Unknown ZM flag ??");
		return 0;
	}
	return 1;
}

} // namespace duckdb
