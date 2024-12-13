/* SPDX-License-Identifier: MIT */
/* Copyright (c) 2022 Max Bachmann */

#pragma once
#include <algorithm>
#include <cassert>
#include <cstddef>
#include <stdio.h>
#include <vector>

namespace duckdb_rapidfuzz::detail {

template <typename T, bool IsConst>
struct BitMatrixView {

    using value_type = T;
    using size_type = size_t;
    using pointer = std::conditional_t<IsConst, const value_type*, value_type*>;
    using reference = std::conditional_t<IsConst, const value_type&, value_type&>;

    BitMatrixView(pointer vector, size_type cols) noexcept : m_vector(vector), m_cols(cols)
    {}

    reference operator[](size_type col) noexcept
    {
        assert(col < m_cols);
        return m_vector[col];
    }

    size_type size() const noexcept
    {
        return m_cols;
    }

private:
    pointer m_vector;
    size_type m_cols;
};

template <typename T>
struct BitMatrix {

    using value_type = T;

    BitMatrix() : m_rows(0), m_cols(0), m_matrix(nullptr)
    {}

    BitMatrix(size_t rows, size_t cols, T val) : m_rows(rows), m_cols(cols), m_matrix(nullptr)
    {
        if (m_rows && m_cols) m_matrix = new T[m_rows * m_cols];
        std::fill_n(m_matrix, m_rows * m_cols, val);
    }

    BitMatrix(const BitMatrix& other) : m_rows(other.m_rows), m_cols(other.m_cols), m_matrix(nullptr)
    {
        if (m_rows && m_cols) m_matrix = new T[m_rows * m_cols];
        std::copy(other.m_matrix, other.m_matrix + m_rows * m_cols, m_matrix);
    }

    BitMatrix(BitMatrix&& other) noexcept : m_rows(0), m_cols(0), m_matrix(nullptr)
    {
        other.swap(*this);
    }

    BitMatrix& operator=(BitMatrix&& other) noexcept
    {
        other.swap(*this);
        return *this;
    }

    BitMatrix& operator=(const BitMatrix& other)
    {
        BitMatrix temp = other;
        temp.swap(*this);
        return *this;
    }

    void swap(BitMatrix& rhs) noexcept
    {
        using std::swap;
        swap(m_rows, rhs.m_rows);
        swap(m_cols, rhs.m_cols);
        swap(m_matrix, rhs.m_matrix);
    }

    ~BitMatrix()
    {
        delete[] m_matrix;
    }

    BitMatrixView<value_type, false> operator[](size_t row) noexcept
    {
        assert(row < m_rows);
        return {&m_matrix[row * m_cols], m_cols};
    }

    BitMatrixView<value_type, true> operator[](size_t row) const noexcept
    {
        assert(row < m_rows);
        return {&m_matrix[row * m_cols], m_cols};
    }

    size_t rows() const noexcept
    {
        return m_rows;
    }

    size_t cols() const noexcept
    {
        return m_cols;
    }

private:
    size_t m_rows;
    size_t m_cols;
    T* m_matrix;
};

template <typename T>
struct ShiftedBitMatrix {
    using value_type = T;

    ShiftedBitMatrix()
    {}

    ShiftedBitMatrix(size_t rows, size_t cols, T val) : m_matrix(rows, cols, val), m_offsets(rows)
    {}

    ShiftedBitMatrix(const ShiftedBitMatrix& other) : m_matrix(other.m_matrix), m_offsets(other.m_offsets)
    {}

    ShiftedBitMatrix(ShiftedBitMatrix&& other) noexcept
    {
        other.swap(*this);
    }

    ShiftedBitMatrix& operator=(ShiftedBitMatrix&& other) noexcept
    {
        other.swap(*this);
        return *this;
    }

    ShiftedBitMatrix& operator=(const ShiftedBitMatrix& other)
    {
        ShiftedBitMatrix temp = other;
        temp.swap(*this);
        return *this;
    }

    void swap(ShiftedBitMatrix& rhs) noexcept
    {
        using std::swap;
        swap(m_matrix, rhs.m_matrix);
        swap(m_offsets, rhs.m_offsets);
    }

    bool test_bit(size_t row, size_t col, bool default_ = false) const noexcept
    {
        ptrdiff_t offset = m_offsets[row];

        if (offset < 0) {
            col += static_cast<size_t>(-offset);
        }
        else if (col >= static_cast<size_t>(offset)) {
            col -= static_cast<size_t>(offset);
        }
        /* bit on the left of the band */
        else {
            return default_;
        }

        size_t word_size = sizeof(value_type) * 8;
        size_t col_word = col / word_size;
        value_type col_mask = value_type(1) << (col % word_size);

        return bool(m_matrix[row][col_word] & col_mask);
    }

    auto operator[](size_t row) noexcept
    {
        return m_matrix[row];
    }

    auto operator[](size_t row) const noexcept
    {
        return m_matrix[row];
    }

    void set_offset(size_t row, ptrdiff_t offset)
    {
        m_offsets[row] = offset;
    }

private:
    BitMatrix<value_type> m_matrix;
    std::vector<ptrdiff_t> m_offsets;
};

} // namespace duckdb_rapidfuzz::detail