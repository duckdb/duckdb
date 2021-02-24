/******************************************************************************
 * src/algorithms/rotations.h
 *
 * Rotations for fixed number of elements.
 *
 ******************************************************************************
 * Copyright (C) 2014 Martin Aumüller <martin.aumueller@tu-ilmenau.de>
 *
 * This program is free software: you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the Free
 * Software Foundation, either version 3 of the License, or (at your option)
 * any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License for
 * more details.
 *
 * You should have received a copy of the GNU General Public License along with
 * this program.  If not, see <http://www.gnu.org/licenses/>.
 *****************************************************************************/

namespace rotations {

template <typename ValueType>
void swap(ValueType& a, ValueType& b)
{
    ValueType tmp = a;
    a = b;
    b = tmp;
    //g_assignments += 3;
}

template <typename ValueType>
void rotate2(ValueType& a0, ValueType& a1)
{
    ValueType tmp = a0;
    a0 = a1;
    a1 = tmp; 
}

template <typename ValueType>
void rotate3(ValueType& a0, ValueType& a1, ValueType& a2)
{
    ValueType tmp = a0;
    a0 = a1;
    a1 = a2;
    a2 = tmp; 
}

template <typename ValueType>
void rotate4(ValueType& a0, ValueType& a1, ValueType& a2, ValueType& a3)
{
    ValueType tmp = a0;
    a0 = a1;
    a1 = a2;
    a2 = a3;
    a3 = tmp; 
}

template <typename ValueType>
void rotate5(ValueType& a0, ValueType& a1, ValueType& a2, ValueType& a3, ValueType& a4)
{
    ValueType tmp = a0;
    a0 = a1;
    a1 = a2;
    a2 = a3;
    a3 = a4;
    a4 = tmp; 
}

template <typename ValueType>
void rotate6(ValueType& a0, ValueType& a1, ValueType& a2, ValueType& a3, ValueType& a4, ValueType& a5)
{
    ValueType tmp = a0;
    a0 = a1;
    a1 = a2;
    a2 = a3;
    a3 = a4;
    a4 = a5;
    a5 = tmp; 
}

template <typename ValueType>
void rotate7(ValueType& a0, ValueType& a1, ValueType& a2, ValueType& a3, ValueType& a4, ValueType& a5, ValueType& a6)
{
    ValueType tmp = a0;
    a0 = a1;
    a1 = a2;
    a2 = a3;
    a3 = a4;
    a4 = a5;
    a5 = a6;
    a6 = tmp; 
}

template <typename ValueType>
void rotate8(ValueType& a0, ValueType& a1, ValueType& a2, ValueType& a3, ValueType& a4, ValueType& a5, ValueType& a6, ValueType& a7)
{
    ValueType tmp = a0;
    a0 = a1;
    a1 = a2;
    a2 = a3;
    a3 = a4;
    a4 = a5;
    a5 = a6;
    a6 = a7;
    a7 = tmp; 
}

template <typename ValueType>
void rotate9(ValueType& a0, ValueType& a1, ValueType& a2, ValueType& a3, ValueType& a4, ValueType& a5, ValueType& a6, ValueType& a7, ValueType& a8)
{
    ValueType tmp = a0;
    a0 = a1;
    a1 = a2;
    a2 = a3;
    a3 = a4;
    a4 = a5;
    a5 = a6;
    a6 = a7;
    a7 = a8;
    a8 = tmp; 
}

template <typename ValueType>
void rotate10(ValueType& a0, ValueType& a1, ValueType& a2, ValueType& a3, ValueType& a4, ValueType& a5, ValueType& a6, ValueType& a7, ValueType& a8, ValueType& a9)
{
    ValueType tmp = a0;
    a0 = a1;
    a1 = a2;
    a2 = a3;
    a3 = a4;
    a4 = a5;
    a5 = a6;
    a6 = a7;
    a7 = a8;
    a8 = a9;
    a9 = tmp; 
}

template <typename ValueType>
void rotate11(ValueType& a0, ValueType& a1, ValueType& a2, ValueType& a3, ValueType& a4, ValueType& a5, ValueType& a6, ValueType& a7, ValueType& a8, ValueType& a9, ValueType& a10)
{
    ValueType tmp = a0;
    a0 = a1;
    a1 = a2;
    a2 = a3;
    a3 = a4;
    a4 = a5;
    a5 = a6;
    a6 = a7;
    a7 = a8;
    a8 = a9;
    a9 = a10;
    a10 = tmp; 
}

template <typename ValueType>
void rotate12(ValueType& a0, ValueType& a1, ValueType& a2, ValueType& a3, ValueType& a4, ValueType& a5, ValueType& a6, ValueType& a7, ValueType& a8, ValueType& a9, ValueType& a10, ValueType& a11)
{
    ValueType tmp = a0;
    a0 = a1;
    a1 = a2;
    a2 = a3;
    a3 = a4;
    a4 = a5;
    a5 = a6;
    a6 = a7;
    a7 = a8;
    a8 = a9;
    a9 = a10;
    a10 = a11;
    a11 = tmp; 
}

template <typename ValueType>
void rotate13(ValueType& a0, ValueType& a1, ValueType& a2, ValueType& a3, ValueType& a4, ValueType& a5, ValueType& a6, ValueType& a7, ValueType& a8, ValueType& a9, ValueType& a10, ValueType& a11, ValueType& a12)
{
    ValueType tmp = a0;
    a0 = a1;
    a1 = a2;
    a2 = a3;
    a3 = a4;
    a4 = a5;
    a5 = a6;
    a6 = a7;
    a7 = a8;
    a8 = a9;
    a9 = a10;
    a10 = a11;
    a11 = a12;
    a12 = tmp; 
}

template <typename ValueType>
void rotate14(ValueType& a0, ValueType& a1, ValueType& a2, ValueType& a3, ValueType& a4, ValueType& a5, ValueType& a6, ValueType& a7, ValueType& a8, ValueType& a9, ValueType& a10, ValueType& a11, ValueType& a12, ValueType& a13)
{
    ValueType tmp = a0;
    a0 = a1;
    a1 = a2;
    a2 = a3;
    a3 = a4;
    a4 = a5;
    a5 = a6;
    a6 = a7;
    a7 = a8;
    a8 = a9;
    a9 = a10;
    a10 = a11;
    a11 = a12;
    a12 = a13;
    a13 = tmp; 
}

template <typename ValueType>
void rotate15(ValueType& a0, ValueType& a1, ValueType& a2, ValueType& a3, ValueType& a4, ValueType& a5, ValueType& a6, ValueType& a7, ValueType& a8, ValueType& a9, ValueType& a10, ValueType& a11, ValueType& a12, ValueType& a13, ValueType& a14)
{
    ValueType tmp = a0;
    a0 = a1;
    a1 = a2;
    a2 = a3;
    a3 = a4;
    a4 = a5;
    a5 = a6;
    a6 = a7;
    a7 = a8;
    a8 = a9;
    a9 = a10;
    a10 = a11;
    a11 = a12;
    a12 = a13;
    a13 = a14;
    a14 = tmp; 
}

template <typename ValueType>
void rotate16(ValueType& a0, ValueType& a1, ValueType& a2, ValueType& a3, ValueType& a4, ValueType& a5, ValueType& a6, ValueType& a7, ValueType& a8, ValueType& a9, ValueType& a10, ValueType& a11, ValueType& a12, ValueType& a13, ValueType& a14, ValueType& a15)
{
    ValueType tmp = a0;
    a0 = a1;
    a1 = a2;
    a2 = a3;
    a3 = a4;
    a4 = a5;
    a5 = a6;
    a6 = a7;
    a7 = a8;
    a8 = a9;
    a9 = a10;
    a10 = a11;
    a11 = a12;
    a12 = a13;
    a13 = a14;
    a14 = a15;
    a15 = tmp; 
}

template <typename ValueType>
void rotate17(ValueType& a0, ValueType& a1, ValueType& a2, ValueType& a3, ValueType& a4, ValueType& a5, ValueType& a6, ValueType& a7, ValueType& a8, ValueType& a9, ValueType& a10, ValueType& a11, ValueType& a12, ValueType& a13, ValueType& a14, ValueType& a15, ValueType& a16)
{
    ValueType tmp = a0;
    a0 = a1;
    a1 = a2;
    a2 = a3;
    a3 = a4;
    a4 = a5;
    a5 = a6;
    a6 = a7;
    a7 = a8;
    a8 = a9;
    a9 = a10;
    a10 = a11;
    a11 = a12;
    a12 = a13;
    a13 = a14;
    a14 = a15;
    a15 = a16;
    a16 = tmp; 
}

template <typename ValueType>
void rotate18(ValueType& a0, ValueType& a1, ValueType& a2, ValueType& a3, ValueType& a4, ValueType& a5, ValueType& a6, ValueType& a7, ValueType& a8, ValueType& a9, ValueType& a10, ValueType& a11, ValueType& a12, ValueType& a13, ValueType& a14, ValueType& a15, ValueType& a16, ValueType& a17)
{
    ValueType tmp = a0;
    a0 = a1;
    a1 = a2;
    a2 = a3;
    a3 = a4;
    a4 = a5;
    a5 = a6;
    a6 = a7;
    a7 = a8;
    a8 = a9;
    a9 = a10;
    a10 = a11;
    a11 = a12;
    a12 = a13;
    a13 = a14;
    a14 = a15;
    a15 = a16;
    a16 = a17;
    a17 = tmp; 
}

template <typename ValueType>
void rotate19(ValueType& a0, ValueType& a1, ValueType& a2, ValueType& a3, ValueType& a4, ValueType& a5, ValueType& a6, ValueType& a7, ValueType& a8, ValueType& a9, ValueType& a10, ValueType& a11, ValueType& a12, ValueType& a13, ValueType& a14, ValueType& a15, ValueType& a16, ValueType& a17, ValueType& a18)
{
    ValueType tmp = a0;
    a0 = a1;
    a1 = a2;
    a2 = a3;
    a3 = a4;
    a4 = a5;
    a5 = a6;
    a6 = a7;
    a7 = a8;
    a8 = a9;
    a9 = a10;
    a10 = a11;
    a11 = a12;
    a12 = a13;
    a13 = a14;
    a14 = a15;
    a15 = a16;
    a16 = a17;
    a17 = a18;
    a18 = tmp; 
}

template <typename ValueType>
void rotate20(ValueType& a0, ValueType& a1, ValueType& a2, ValueType& a3, ValueType& a4, ValueType& a5, ValueType& a6, ValueType& a7, ValueType& a8, ValueType& a9, ValueType& a10, ValueType& a11, ValueType& a12, ValueType& a13, ValueType& a14, ValueType& a15, ValueType& a16, ValueType& a17, ValueType& a18, ValueType& a19)
{
    ValueType tmp = a0;
    a0 = a1;
    a1 = a2;
    a2 = a3;
    a3 = a4;
    a4 = a5;
    a5 = a6;
    a6 = a7;
    a7 = a8;
    a8 = a9;
    a9 = a10;
    a10 = a11;
    a11 = a12;
    a12 = a13;
    a13 = a14;
    a14 = a15;
    a15 = a16;
    a16 = a17;
    a17 = a18;
    a18 = a19;
    a19 = tmp; 
}

template <typename ValueType>
void rotate21(ValueType& a0, ValueType& a1, ValueType& a2, ValueType& a3, ValueType& a4, ValueType& a5, ValueType& a6, ValueType& a7, ValueType& a8, ValueType& a9, ValueType& a10, ValueType& a11, ValueType& a12, ValueType& a13, ValueType& a14, ValueType& a15, ValueType& a16, ValueType& a17, ValueType& a18, ValueType& a19, ValueType& a20)
{
    ValueType tmp = a0;
    a0 = a1;
    a1 = a2;
    a2 = a3;
    a3 = a4;
    a4 = a5;
    a5 = a6;
    a6 = a7;
    a7 = a8;
    a8 = a9;
    a9 = a10;
    a10 = a11;
    a11 = a12;
    a12 = a13;
    a13 = a14;
    a14 = a15;
    a15 = a16;
    a16 = a17;
    a17 = a18;
    a18 = a19;
    a19 = a20;
    a20 = tmp; 
}

template <typename ValueType>
void rotate22(ValueType& a0, ValueType& a1, ValueType& a2, ValueType& a3, ValueType& a4, ValueType& a5, ValueType& a6, ValueType& a7, ValueType& a8, ValueType& a9, ValueType& a10, ValueType& a11, ValueType& a12, ValueType& a13, ValueType& a14, ValueType& a15, ValueType& a16, ValueType& a17, ValueType& a18, ValueType& a19, ValueType& a20, ValueType& a21)
{
    ValueType tmp = a0;
    a0 = a1;
    a1 = a2;
    a2 = a3;
    a3 = a4;
    a4 = a5;
    a5 = a6;
    a6 = a7;
    a7 = a8;
    a8 = a9;
    a9 = a10;
    a10 = a11;
    a11 = a12;
    a12 = a13;
    a13 = a14;
    a14 = a15;
    a15 = a16;
    a16 = a17;
    a17 = a18;
    a18 = a19;
    a19 = a20;
    a20 = a21;
    a21 = tmp; 
}

template <typename ValueType>
void rotate23(ValueType& a0, ValueType& a1, ValueType& a2, ValueType& a3, ValueType& a4, ValueType& a5, ValueType& a6, ValueType& a7, ValueType& a8, ValueType& a9, ValueType& a10, ValueType& a11, ValueType& a12, ValueType& a13, ValueType& a14, ValueType& a15, ValueType& a16, ValueType& a17, ValueType& a18, ValueType& a19, ValueType& a20, ValueType& a21, ValueType& a22)
{
    ValueType tmp = a0;
    a0 = a1;
    a1 = a2;
    a2 = a3;
    a3 = a4;
    a4 = a5;
    a5 = a6;
    a6 = a7;
    a7 = a8;
    a8 = a9;
    a9 = a10;
    a10 = a11;
    a11 = a12;
    a12 = a13;
    a13 = a14;
    a14 = a15;
    a15 = a16;
    a16 = a17;
    a17 = a18;
    a18 = a19;
    a19 = a20;
    a20 = a21;
    a21 = a22;
    a22 = tmp; 
}

template <typename ValueType>
void rotate24(ValueType& a0, ValueType& a1, ValueType& a2, ValueType& a3, ValueType& a4, ValueType& a5, ValueType& a6, ValueType& a7, ValueType& a8, ValueType& a9, ValueType& a10, ValueType& a11, ValueType& a12, ValueType& a13, ValueType& a14, ValueType& a15, ValueType& a16, ValueType& a17, ValueType& a18, ValueType& a19, ValueType& a20, ValueType& a21, ValueType& a22, ValueType& a23)
{
    ValueType tmp = a0;
    a0 = a1;
    a1 = a2;
    a2 = a3;
    a3 = a4;
    a4 = a5;
    a5 = a6;
    a6 = a7;
    a7 = a8;
    a8 = a9;
    a9 = a10;
    a10 = a11;
    a11 = a12;
    a12 = a13;
    a13 = a14;
    a14 = a15;
    a15 = a16;
    a16 = a17;
    a17 = a18;
    a18 = a19;
    a19 = a20;
    a20 = a21;
    a21 = a22;
    a22 = a23;
    a23 = tmp; 
}

template <typename ValueType>
void rotate25(ValueType& a0, ValueType& a1, ValueType& a2, ValueType& a3, ValueType& a4, ValueType& a5, ValueType& a6, ValueType& a7, ValueType& a8, ValueType& a9, ValueType& a10, ValueType& a11, ValueType& a12, ValueType& a13, ValueType& a14, ValueType& a15, ValueType& a16, ValueType& a17, ValueType& a18, ValueType& a19, ValueType& a20, ValueType& a21, ValueType& a22, ValueType& a23, ValueType& a24)
{
    ValueType tmp = a0;
    a0 = a1;
    a1 = a2;
    a2 = a3;
    a3 = a4;
    a4 = a5;
    a5 = a6;
    a6 = a7;
    a7 = a8;
    a8 = a9;
    a9 = a10;
    a10 = a11;
    a11 = a12;
    a12 = a13;
    a13 = a14;
    a14 = a15;
    a15 = a16;
    a16 = a17;
    a17 = a18;
    a18 = a19;
    a19 = a20;
    a20 = a21;
    a21 = a22;
    a22 = a23;
    a23 = a24;
    a24 = tmp; 
}

template <typename ValueType>
void rotate26(ValueType& a0, ValueType& a1, ValueType& a2, ValueType& a3, ValueType& a4, ValueType& a5, ValueType& a6, ValueType& a7, ValueType& a8, ValueType& a9, ValueType& a10, ValueType& a11, ValueType& a12, ValueType& a13, ValueType& a14, ValueType& a15, ValueType& a16, ValueType& a17, ValueType& a18, ValueType& a19, ValueType& a20, ValueType& a21, ValueType& a22, ValueType& a23, ValueType& a24, ValueType& a25)
{
    ValueType tmp = a0;
    a0 = a1;
    a1 = a2;
    a2 = a3;
    a3 = a4;
    a4 = a5;
    a5 = a6;
    a6 = a7;
    a7 = a8;
    a8 = a9;
    a9 = a10;
    a10 = a11;
    a11 = a12;
    a12 = a13;
    a13 = a14;
    a14 = a15;
    a15 = a16;
    a16 = a17;
    a17 = a18;
    a18 = a19;
    a19 = a20;
    a20 = a21;
    a21 = a22;
    a22 = a23;
    a23 = a24;
    a24 = a25;
    a25 = tmp; 
}

template <typename ValueType>
void rotate27(ValueType& a0, ValueType& a1, ValueType& a2, ValueType& a3, ValueType& a4, ValueType& a5, ValueType& a6, ValueType& a7, ValueType& a8, ValueType& a9, ValueType& a10, ValueType& a11, ValueType& a12, ValueType& a13, ValueType& a14, ValueType& a15, ValueType& a16, ValueType& a17, ValueType& a18, ValueType& a19, ValueType& a20, ValueType& a21, ValueType& a22, ValueType& a23, ValueType& a24, ValueType& a25, ValueType& a26)
{
    ValueType tmp = a0;
    a0 = a1;
    a1 = a2;
    a2 = a3;
    a3 = a4;
    a4 = a5;
    a5 = a6;
    a6 = a7;
    a7 = a8;
    a8 = a9;
    a9 = a10;
    a10 = a11;
    a11 = a12;
    a12 = a13;
    a13 = a14;
    a14 = a15;
    a15 = a16;
    a16 = a17;
    a17 = a18;
    a18 = a19;
    a19 = a20;
    a20 = a21;
    a21 = a22;
    a22 = a23;
    a23 = a24;
    a24 = a25;
    a25 = a26;
    a26 = tmp; 
}

template <typename ValueType>
void rotate28(ValueType& a0, ValueType& a1, ValueType& a2, ValueType& a3, ValueType& a4, ValueType& a5, ValueType& a6, ValueType& a7, ValueType& a8, ValueType& a9, ValueType& a10, ValueType& a11, ValueType& a12, ValueType& a13, ValueType& a14, ValueType& a15, ValueType& a16, ValueType& a17, ValueType& a18, ValueType& a19, ValueType& a20, ValueType& a21, ValueType& a22, ValueType& a23, ValueType& a24, ValueType& a25, ValueType& a26, ValueType& a27)
{
    ValueType tmp = a0;
    a0 = a1;
    a1 = a2;
    a2 = a3;
    a3 = a4;
    a4 = a5;
    a5 = a6;
    a6 = a7;
    a7 = a8;
    a8 = a9;
    a9 = a10;
    a10 = a11;
    a11 = a12;
    a12 = a13;
    a13 = a14;
    a14 = a15;
    a15 = a16;
    a16 = a17;
    a17 = a18;
    a18 = a19;
    a19 = a20;
    a20 = a21;
    a21 = a22;
    a22 = a23;
    a23 = a24;
    a24 = a25;
    a25 = a26;
    a26 = a27;
    a27 = tmp; 
}

template <typename ValueType>
void rotate29(ValueType& a0, ValueType& a1, ValueType& a2, ValueType& a3, ValueType& a4, ValueType& a5, ValueType& a6, ValueType& a7, ValueType& a8, ValueType& a9, ValueType& a10, ValueType& a11, ValueType& a12, ValueType& a13, ValueType& a14, ValueType& a15, ValueType& a16, ValueType& a17, ValueType& a18, ValueType& a19, ValueType& a20, ValueType& a21, ValueType& a22, ValueType& a23, ValueType& a24, ValueType& a25, ValueType& a26, ValueType& a27, ValueType& a28)
{
    ValueType tmp = a0;
    a0 = a1;
    a1 = a2;
    a2 = a3;
    a3 = a4;
    a4 = a5;
    a5 = a6;
    a6 = a7;
    a7 = a8;
    a8 = a9;
    a9 = a10;
    a10 = a11;
    a11 = a12;
    a12 = a13;
    a13 = a14;
    a14 = a15;
    a15 = a16;
    a16 = a17;
    a17 = a18;
    a18 = a19;
    a19 = a20;
    a20 = a21;
    a21 = a22;
    a22 = a23;
    a23 = a24;
    a24 = a25;
    a25 = a26;
    a26 = a27;
    a27 = a28;
    a28 = tmp; 
}

template <typename ValueType>
void rotate30(ValueType& a0, ValueType& a1, ValueType& a2, ValueType& a3, ValueType& a4, ValueType& a5, ValueType& a6, ValueType& a7, ValueType& a8, ValueType& a9, ValueType& a10, ValueType& a11, ValueType& a12, ValueType& a13, ValueType& a14, ValueType& a15, ValueType& a16, ValueType& a17, ValueType& a18, ValueType& a19, ValueType& a20, ValueType& a21, ValueType& a22, ValueType& a23, ValueType& a24, ValueType& a25, ValueType& a26, ValueType& a27, ValueType& a28, ValueType& a29)
{
    ValueType tmp = a0;
    a0 = a1;
    a1 = a2;
    a2 = a3;
    a3 = a4;
    a4 = a5;
    a5 = a6;
    a6 = a7;
    a7 = a8;
    a8 = a9;
    a9 = a10;
    a10 = a11;
    a11 = a12;
    a12 = a13;
    a13 = a14;
    a14 = a15;
    a15 = a16;
    a16 = a17;
    a17 = a18;
    a18 = a19;
    a19 = a20;
    a20 = a21;
    a21 = a22;
    a22 = a23;
    a23 = a24;
    a24 = a25;
    a25 = a26;
    a26 = a27;
    a27 = a28;
    a28 = a29;
    a29 = tmp; 
}

template <typename ValueType>
void rotate31(ValueType& a0, ValueType& a1, ValueType& a2, ValueType& a3, ValueType& a4, ValueType& a5, ValueType& a6, ValueType& a7, ValueType& a8, ValueType& a9, ValueType& a10, ValueType& a11, ValueType& a12, ValueType& a13, ValueType& a14, ValueType& a15, ValueType& a16, ValueType& a17, ValueType& a18, ValueType& a19, ValueType& a20, ValueType& a21, ValueType& a22, ValueType& a23, ValueType& a24, ValueType& a25, ValueType& a26, ValueType& a27, ValueType& a28, ValueType& a29, ValueType& a30)
{
    ValueType tmp = a0;
    a0 = a1;
    a1 = a2;
    a2 = a3;
    a3 = a4;
    a4 = a5;
    a5 = a6;
    a6 = a7;
    a7 = a8;
    a8 = a9;
    a9 = a10;
    a10 = a11;
    a11 = a12;
    a12 = a13;
    a13 = a14;
    a14 = a15;
    a15 = a16;
    a16 = a17;
    a17 = a18;
    a18 = a19;
    a19 = a20;
    a20 = a21;
    a21 = a22;
    a22 = a23;
    a23 = a24;
    a24 = a25;
    a25 = a26;
    a26 = a27;
    a27 = a28;
    a28 = a29;
    a29 = a30;
    a30 = tmp; 
}

template <typename ValueType>
void rotate32(ValueType& a0, ValueType& a1, ValueType& a2, ValueType& a3, ValueType& a4, ValueType& a5, ValueType& a6, ValueType& a7, ValueType& a8, ValueType& a9, ValueType& a10, ValueType& a11, ValueType& a12, ValueType& a13, ValueType& a14, ValueType& a15, ValueType& a16, ValueType& a17, ValueType& a18, ValueType& a19, ValueType& a20, ValueType& a21, ValueType& a22, ValueType& a23, ValueType& a24, ValueType& a25, ValueType& a26, ValueType& a27, ValueType& a28, ValueType& a29, ValueType& a30, ValueType& a31)
{
    ValueType tmp = a0;
    a0 = a1;
    a1 = a2;
    a2 = a3;
    a3 = a4;
    a4 = a5;
    a5 = a6;
    a6 = a7;
    a7 = a8;
    a8 = a9;
    a9 = a10;
    a10 = a11;
    a11 = a12;
    a12 = a13;
    a13 = a14;
    a14 = a15;
    a15 = a16;
    a16 = a17;
    a17 = a18;
    a18 = a19;
    a19 = a20;
    a20 = a21;
    a21 = a22;
    a22 = a23;
    a23 = a24;
    a24 = a25;
    a25 = a26;
    a26 = a27;
    a27 = a28;
    a28 = a29;
    a29 = a30;
    a30 = a31;
    a31 = tmp; 
}




} // namespace rotations
