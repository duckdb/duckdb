//---------------------------------------------------------------------------
//	@filename:
//		CDouble.h
//
//	@doc:
//		Safe implementation of floating point numbers and operations.
//---------------------------------------------------------------------------
#ifndef GPOS_CDouble_H
#define GPOS_CDouble_H

#include <math.h>

#include "duckdb/optimizer/cascade/base.h"

#define GPOS_FP_ABS_MIN (1e-250)  // min value: 4.94066e-324
#define GPOS_FP_ABS_MAX (1e+250)  // max value: 8.98847e+307

namespace gpos
{
//---------------------------------------------------------------------------
//	@class:
//		CDouble
//
//	@doc:
//		Class representing floating-point numbers
//
//---------------------------------------------------------------------------
class CDouble
{
private:
	// double-precision value
	DOUBLE m_d;

	// check validity in ctor
	inline void
	CheckValidity()
	{
		GPOS_ASSERT(0.0 < GPOS_FP_ABS_MIN);
		GPOS_ASSERT(GPOS_FP_ABS_MIN < GPOS_FP_ABS_MAX);

		double abs_value = fabs(m_d);

		if (GPOS_FP_ABS_MAX < abs_value)
		{
			SetSignedVal(GPOS_FP_ABS_MAX);
		}

		if (GPOS_FP_ABS_MIN > abs_value)
		{
			SetSignedVal(GPOS_FP_ABS_MIN);
		}
	}

	// assign value while maintaining current sign
	inline void
	SetSignedVal(DOUBLE val)
	{
		if (0.0 <= m_d)
		{
			m_d = val;
		}
		else
		{
			m_d = -val;
		}
	}


public:
	// ctors
	inline CDouble(DOUBLE d) : m_d(d)
	{
		CheckValidity();
	}

	inline CDouble(ULLONG ull) : m_d(DOUBLE(ull))
	{
		CheckValidity();
	}

	inline CDouble(ULONG ul) : m_d(DOUBLE(ul))
	{
		CheckValidity();
	}

	inline CDouble(LINT li) : m_d(DOUBLE(li))
	{
		CheckValidity();
	}

	inline CDouble(INT i) : m_d(DOUBLE(i))
	{
		CheckValidity();
	}

	// value accessor
	inline DOUBLE
	Get() const
	{
		return m_d;
	}

	// assignment
	inline CDouble &
	operator=(const CDouble &right)
	{
		this->m_d = right.m_d;

		return (*this);
	}

	// arithmetic operators
	friend CDouble
	operator+(const CDouble &left, const CDouble &right)
	{
		return CDouble(left.m_d + right.m_d);
	}

	friend CDouble
	operator-(const CDouble &left, const CDouble &right)
	{
		return CDouble(left.m_d - right.m_d);
	}

	friend CDouble
	operator*(const CDouble &left, const CDouble &right)
	{
		return CDouble(left.m_d * right.m_d);
	}

	friend CDouble
	operator/(const CDouble &left, const CDouble &right)
	{
		return CDouble(left.m_d / right.m_d);
	}

	// negation
	friend CDouble
	operator-(const CDouble &d)
	{
		return CDouble(-d.m_d);
	}

	// logical operators
	friend BOOL
	operator==(const CDouble &left, const CDouble &right)
	{
		CDouble fpCompare(left.m_d - right.m_d);

		return (fabs(fpCompare.m_d) == GPOS_FP_ABS_MIN);
	}

	friend BOOL operator!=(const CDouble &left, const CDouble &right);
	friend BOOL operator<(const CDouble &left, const CDouble &right);
	friend BOOL operator<=(const CDouble &left, const CDouble &right);
	friend BOOL
	operator>(const CDouble &left, const CDouble &right)
	{
		CDouble fpCompare(left.m_d - right.m_d);

		return (fpCompare.m_d > GPOS_FP_ABS_MIN);
	}

	friend BOOL operator>=(const CDouble &left, const CDouble &right);

	// absolute
	inline CDouble
	Absolute() const
	{
		return CDouble(fabs(m_d));
	}

	// floor
	inline CDouble
	Floor() const
	{
		return CDouble(floor(m_d));
	}

	// ceiling
	inline CDouble
	Ceil() const
	{
		return CDouble(ceil(m_d));
	}

	// power
	inline CDouble
	Pow(const CDouble &d) const
	{
		return CDouble(pow(m_d, d.m_d));
	}

	// log to the base 2
	inline CDouble
	Log2() const
	{
		return CDouble(log2(m_d));
	}

	// print to stream
	inline IOstream &
	OsPrint(IOstream &os) const
	{
		return os << m_d;
	}

	// compare two double values using given precision
	inline static BOOL
	Equals(DOUBLE dLeft, DOUBLE dRight, DOUBLE dPrecision = GPOS_FP_ABS_MIN)
	{
		return fabs(dRight - dLeft) <= dPrecision;
	}

};	// class CDouble

// arithmetic operators
inline CDouble operator+(const CDouble &left, const CDouble &right);
inline CDouble operator-(const CDouble &left, const CDouble &right);
inline CDouble operator*(const CDouble &left, const CDouble &right);
inline CDouble operator/(const CDouble &left, const CDouble &right);

// logical operators
inline BOOL operator==(const CDouble &left, const CDouble &right);
inline BOOL operator>(const CDouble &left, const CDouble &right);

// negation
inline CDouble operator-(const CDouble &d);

// '!=' operator
inline BOOL
operator!=(const CDouble &left, const CDouble &right)
{
	return (!(left == right));
}

// '<=' operator
inline BOOL
operator>=(const CDouble &left, const CDouble &right)
{
	return (left == right || left > right);
}

// '>' operator
inline BOOL
operator<(const CDouble &left, const CDouble &right)
{
	return (right > left);
}

// '>=' operator
inline BOOL
operator<=(const CDouble &left, const CDouble &right)
{
	return (right == left || right > left);
}

// print shorthand
inline IOstream &
operator<<(IOstream &os, CDouble d)
{
	return d.OsPrint(os);
}
}  // namespace gpos

#endif
