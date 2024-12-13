/* SPDX-License-Identifier: MIT */
/* Copyright © 2022-present Max Bachmann */

#pragma once
#include <rapidfuzz/distance/DamerauLevenshtein.hpp>
#include <rapidfuzz/distance/Hamming.hpp>
#include <rapidfuzz/distance/Indel.hpp>
#include <rapidfuzz/distance/Jaro.hpp>
#include <rapidfuzz/distance/JaroWinkler.hpp>
#include <rapidfuzz/distance/LCSseq.hpp>
#include <rapidfuzz/distance/Levenshtein.hpp>
#include <rapidfuzz/distance/OSA.hpp>
#include <rapidfuzz/distance/Postfix.hpp>
#include <rapidfuzz/distance/Prefix.hpp>

namespace duckdb_rapidfuzz {

namespace detail {
template <typename ReturnType, typename InputIt1, typename InputIt2>
ReturnType editops_apply_impl(const Editops& ops, InputIt1 first1, InputIt1 last1, InputIt2 first2,
                              InputIt2 last2)
{
    auto len1 = static_cast<size_t>(std::distance(first1, last1));
    auto len2 = static_cast<size_t>(std::distance(first2, last2));

    ReturnType res_str;
    res_str.resize(len1 + len2);
    size_t src_pos = 0;
    size_t dest_pos = 0;

    for (const auto& op : ops) {
        /* matches between last and current editop */
        while (src_pos < op.src_pos) {
            res_str[dest_pos] =
                static_cast<typename ReturnType::value_type>(first1[static_cast<ptrdiff_t>(src_pos)]);
            src_pos++;
            dest_pos++;
        }

        switch (op.type) {
        case EditType::None:
        case EditType::Replace:
            res_str[dest_pos] =
                static_cast<typename ReturnType::value_type>(first2[static_cast<ptrdiff_t>(op.dest_pos)]);
            src_pos++;
            dest_pos++;
            break;
        case EditType::Insert:
            res_str[dest_pos] =
                static_cast<typename ReturnType::value_type>(first2[static_cast<ptrdiff_t>(op.dest_pos)]);
            dest_pos++;
            break;
        case EditType::Delete: src_pos++; break;
        }
    }

    /* matches after the last editop */
    while (src_pos < len1) {
        res_str[dest_pos] =
            static_cast<typename ReturnType::value_type>(first1[static_cast<ptrdiff_t>(src_pos)]);
        src_pos++;
        dest_pos++;
    }

    res_str.resize(dest_pos);
    return res_str;
}

template <typename ReturnType, typename InputIt1, typename InputIt2>
ReturnType opcodes_apply_impl(const Opcodes& ops, InputIt1 first1, InputIt1 last1, InputIt2 first2,
                              InputIt2 last2)
{
    auto len1 = static_cast<size_t>(std::distance(first1, last1));
    auto len2 = static_cast<size_t>(std::distance(first2, last2));

    ReturnType res_str;
    res_str.resize(len1 + len2);
    size_t dest_pos = 0;

    for (const auto& op : ops) {
        switch (op.type) {
        case EditType::None:
            for (auto i = op.src_begin; i < op.src_end; ++i) {
                res_str[dest_pos++] =
                    static_cast<typename ReturnType::value_type>(first1[static_cast<ptrdiff_t>(i)]);
            }
            break;
        case EditType::Replace:
        case EditType::Insert:
            for (auto i = op.dest_begin; i < op.dest_end; ++i) {
                res_str[dest_pos++] =
                    static_cast<typename ReturnType::value_type>(first2[static_cast<ptrdiff_t>(i)]);
            }
            break;
        case EditType::Delete: break;
        }
    }

    res_str.resize(dest_pos);
    return res_str;
}

} // namespace detail

template <typename CharT, typename InputIt1, typename InputIt2>
std::basic_string<CharT> editops_apply_str(const Editops& ops, InputIt1 first1, InputIt1 last1,
                                           InputIt2 first2, InputIt2 last2)
{
    return detail::editops_apply_impl<std::basic_string<CharT>>(ops, first1, last1, first2, last2);
}

template <typename CharT, typename Sentence1, typename Sentence2>
std::basic_string<CharT> editops_apply_str(const Editops& ops, const Sentence1& s1, const Sentence2& s2)
{
    return detail::editops_apply_impl<std::basic_string<CharT>>(ops, detail::to_begin(s1), detail::to_end(s1),
                                                                detail::to_begin(s2), detail::to_end(s2));
}

template <typename CharT, typename InputIt1, typename InputIt2>
std::basic_string<CharT> opcodes_apply_str(const Opcodes& ops, InputIt1 first1, InputIt1 last1,
                                           InputIt2 first2, InputIt2 last2)
{
    return detail::opcodes_apply_impl<std::basic_string<CharT>>(ops, first1, last1, first2, last2);
}

template <typename CharT, typename Sentence1, typename Sentence2>
std::basic_string<CharT> opcodes_apply_str(const Opcodes& ops, const Sentence1& s1, const Sentence2& s2)
{
    return detail::opcodes_apply_impl<std::basic_string<CharT>>(ops, detail::to_begin(s1), detail::to_end(s1),
                                                                detail::to_begin(s2), detail::to_end(s2));
}

template <typename CharT, typename InputIt1, typename InputIt2>
std::vector<CharT> editops_apply_vec(const Editops& ops, InputIt1 first1, InputIt1 last1, InputIt2 first2,
                                     InputIt2 last2)
{
    return detail::editops_apply_impl<std::vector<CharT>>(ops, first1, last1, first2, last2);
}

template <typename CharT, typename Sentence1, typename Sentence2>
std::vector<CharT> editops_apply_vec(const Editops& ops, const Sentence1& s1, const Sentence2& s2)
{
    return detail::editops_apply_impl<std::vector<CharT>>(ops, detail::to_begin(s1), detail::to_end(s1),
                                                          detail::to_begin(s2), detail::to_end(s2));
}

template <typename CharT, typename InputIt1, typename InputIt2>
std::vector<CharT> opcodes_apply_vec(const Opcodes& ops, InputIt1 first1, InputIt1 last1, InputIt2 first2,
                                     InputIt2 last2)
{
    return detail::opcodes_apply_impl<std::vector<CharT>>(ops, first1, last1, first2, last2);
}

template <typename CharT, typename Sentence1, typename Sentence2>
std::vector<CharT> opcodes_apply_vec(const Opcodes& ops, const Sentence1& s1, const Sentence2& s2)
{
    return detail::opcodes_apply_impl<std::vector<CharT>>(ops, detail::to_begin(s1), detail::to_end(s1),
                                                          detail::to_begin(s2), detail::to_end(s2));
}

} // namespace duckdb_rapidfuzz
