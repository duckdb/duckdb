/* SPDX-License-Identifier: MIT */
/* Copyright © 2022 Max Bachmann */

#pragma once

#include <cmath>
#include <rapidfuzz/details/Range.hpp>
#include <rapidfuzz/details/common.hpp>
#include <rapidfuzz/details/simd.hpp>
#include <type_traits>

namespace duckdb_rapidfuzz::detail {

template <typename T, typename... Args>
struct NormalizedMetricBase {
    template <typename InputIt1, typename InputIt2,
              typename = std::enable_if_t<!std::is_same_v<InputIt2, double>>>
    static double normalized_distance(InputIt1 first1, InputIt1 last1, InputIt2 first2, InputIt2 last2,
                                      Args... args, double score_cutoff, double score_hint)
    {
        return _normalized_distance(Range(first1, last1), Range(first2, last2), std::forward<Args>(args)...,
                                    score_cutoff, score_hint);
    }

    template <typename Sentence1, typename Sentence2>
    static double normalized_distance(const Sentence1& s1, const Sentence2& s2, Args... args,
                                      double score_cutoff, double score_hint)
    {
        return _normalized_distance(Range(s1), Range(s2), std::forward<Args>(args)..., score_cutoff,
                                    score_hint);
    }

    template <typename InputIt1, typename InputIt2,
              typename = std::enable_if_t<!std::is_same_v<InputIt2, double>>>
    static double normalized_similarity(InputIt1 first1, InputIt1 last1, InputIt2 first2, InputIt2 last2,
                                        Args... args, double score_cutoff, double score_hint)
    {
        return _normalized_similarity(Range(first1, last1), Range(first2, last2), std::forward<Args>(args)...,
                                      score_cutoff, score_hint);
    }

    template <typename Sentence1, typename Sentence2>
    static double normalized_similarity(const Sentence1& s1, const Sentence2& s2, Args... args,
                                        double score_cutoff, double score_hint)
    {
        return _normalized_similarity(Range(s1), Range(s2), std::forward<Args>(args)..., score_cutoff,
                                      score_hint);
    }

protected:
    template <typename InputIt1, typename InputIt2>
    static double _normalized_distance(const Range<InputIt1>& s1, const Range<InputIt2>& s2, Args... args,
                                       double score_cutoff, double score_hint)
    {
        auto maximum = T::maximum(s1, s2, args...);
        auto cutoff_distance =
            static_cast<decltype(maximum)>(std::ceil(static_cast<double>(maximum) * score_cutoff));
        auto hint_distance =
            static_cast<decltype(maximum)>(std::ceil(static_cast<double>(maximum) * score_hint));
        auto dist = T::_distance(s1, s2, std::forward<Args>(args)..., cutoff_distance, hint_distance);
        double norm_dist = (maximum != 0) ? static_cast<double>(dist) / static_cast<double>(maximum) : 0.0;
        return (norm_dist <= score_cutoff) ? norm_dist : 1.0;
    }

    template <typename InputIt1, typename InputIt2>
    static double _normalized_similarity(const Range<InputIt1>& s1, const Range<InputIt2>& s2, Args... args,
                                         double score_cutoff, double score_hint)
    {
        double cutoff_score = NormSim_to_NormDist(score_cutoff);
        double hint_score = NormSim_to_NormDist(score_hint);
        double norm_dist =
            _normalized_distance(s1, s2, std::forward<Args>(args)..., cutoff_score, hint_score);
        double norm_sim = 1.0 - norm_dist;
        return (norm_sim >= score_cutoff) ? norm_sim : 0.0;
    }

    NormalizedMetricBase()
    {}
    friend T;
};

template <typename T, typename ResType, int64_t WorstSimilarity, int64_t WorstDistance, typename... Args>
struct DistanceBase : public NormalizedMetricBase<T, Args...> {
    template <typename InputIt1, typename InputIt2,
              typename = std::enable_if_t<!std::is_same_v<InputIt2, double>>>
    static ResType distance(InputIt1 first1, InputIt1 last1, InputIt2 first2, InputIt2 last2, Args... args,
                            ResType score_cutoff, ResType score_hint)
    {
        return T::_distance(Range(first1, last1), Range(first2, last2), std::forward<Args>(args)...,
                            score_cutoff, score_hint);
    }

    template <typename Sentence1, typename Sentence2>
    static ResType distance(const Sentence1& s1, const Sentence2& s2, Args... args, ResType score_cutoff,
                            ResType score_hint)
    {
        return T::_distance(Range(s1), Range(s2), std::forward<Args>(args)..., score_cutoff, score_hint);
    }

    template <typename InputIt1, typename InputIt2,
              typename = std::enable_if_t<!std::is_same_v<InputIt2, double>>>
    static ResType similarity(InputIt1 first1, InputIt1 last1, InputIt2 first2, InputIt2 last2, Args... args,
                              ResType score_cutoff, ResType score_hint)
    {
        return _similarity(Range(first1, last1), Range(first2, last2), std::forward<Args>(args)...,
                           score_cutoff, score_hint);
    }

    template <typename Sentence1, typename Sentence2>
    static ResType similarity(const Sentence1& s1, const Sentence2& s2, Args... args, ResType score_cutoff,
                              ResType score_hint)
    {
        return _similarity(Range(s1), Range(s2), std::forward<Args>(args)..., score_cutoff, score_hint);
    }

protected:
    template <typename InputIt1, typename InputIt2>
    static ResType _similarity(Range<InputIt1> s1, Range<InputIt2> s2, Args... args, ResType score_cutoff,
                               ResType score_hint)
    {
        auto maximum = T::maximum(s1, s2, args...);
        if (score_cutoff > maximum) return 0;

        score_hint = std::min(score_cutoff, score_hint);
        ResType cutoff_distance = maximum - score_cutoff;
        ResType hint_distance = maximum - score_hint;
        ResType dist = T::_distance(s1, s2, std::forward<Args>(args)..., cutoff_distance, hint_distance);
        ResType sim = maximum - dist;
        return (sim >= score_cutoff) ? sim : 0;
    }

    DistanceBase()
    {}
    friend T;
};

template <typename T, typename ResType, int64_t WorstSimilarity, int64_t WorstDistance, typename... Args>
struct SimilarityBase : public NormalizedMetricBase<T, Args...> {
    template <typename InputIt1, typename InputIt2,
              typename = std::enable_if_t<!std::is_same_v<InputIt2, double>>>
    static ResType distance(InputIt1 first1, InputIt1 last1, InputIt2 first2, InputIt2 last2, Args... args,
                            ResType score_cutoff, ResType score_hint)
    {
        return _distance(Range(first1, last1), Range(first2, last2), std::forward<Args>(args)...,
                         score_cutoff, score_hint);
    }

    template <typename Sentence1, typename Sentence2>
    static ResType distance(const Sentence1& s1, const Sentence2& s2, Args... args, ResType score_cutoff,
                            ResType score_hint)
    {
        return _distance(Range(s1), Range(s2), std::forward<Args>(args)..., score_cutoff, score_hint);
    }

    template <typename InputIt1, typename InputIt2,
              typename = std::enable_if_t<!std::is_same_v<InputIt2, double>>>
    static ResType similarity(InputIt1 first1, InputIt1 last1, InputIt2 first2, InputIt2 last2, Args... args,
                              ResType score_cutoff, ResType score_hint)
    {
        return T::_similarity(Range(first1, last1), Range(first2, last2), std::forward<Args>(args)...,
                              score_cutoff, score_hint);
    }

    template <typename Sentence1, typename Sentence2>
    static ResType similarity(const Sentence1& s1, const Sentence2& s2, Args... args, ResType score_cutoff,
                              ResType score_hint)
    {
        return T::_similarity(Range(s1), Range(s2), std::forward<Args>(args)..., score_cutoff, score_hint);
    }

protected:
    template <typename InputIt1, typename InputIt2>
    static ResType _distance(const Range<InputIt1>& s1, const Range<InputIt2>& s2, Args... args,
                             ResType score_cutoff, ResType score_hint)
    {
        auto maximum = T::maximum(s1, s2, args...);
        ResType cutoff_similarity =
            (maximum >= score_cutoff) ? maximum - score_cutoff : static_cast<ResType>(WorstSimilarity);
        ResType hint_similarity =
            (maximum >= score_hint) ? maximum - score_hint : static_cast<ResType>(WorstSimilarity);
        ResType sim = T::_similarity(s1, s2, std::forward<Args>(args)..., cutoff_similarity, hint_similarity);
        ResType dist = maximum - sim;

        if constexpr (std::is_floating_point_v<ResType>)
            return (dist <= score_cutoff) ? dist : 1.0;
        else
            return (dist <= score_cutoff) ? dist : score_cutoff + 1;
    }

    SimilarityBase()
    {}
    friend T;
};

template <typename T>
struct CachedNormalizedMetricBase {
    template <typename InputIt2>
    double normalized_distance(InputIt2 first2, InputIt2 last2, double score_cutoff = 1.0,
                               double score_hint = 1.0) const
    {
        return _normalized_distance(Range(first2, last2), score_cutoff, score_hint);
    }

    template <typename Sentence2>
    double normalized_distance(const Sentence2& s2, double score_cutoff = 1.0, double score_hint = 1.0) const
    {
        return _normalized_distance(Range(s2), score_cutoff, score_hint);
    }

    template <typename InputIt2>
    double normalized_similarity(InputIt2 first2, InputIt2 last2, double score_cutoff = 0.0,
                                 double score_hint = 0.0) const
    {
        return _normalized_similarity(Range(first2, last2), score_cutoff, score_hint);
    }

    template <typename Sentence2>
    double normalized_similarity(const Sentence2& s2, double score_cutoff = 0.0,
                                 double score_hint = 0.0) const
    {
        return _normalized_similarity(Range(s2), score_cutoff, score_hint);
    }

protected:
    template <typename InputIt2>
    double _normalized_distance(const Range<InputIt2>& s2, double score_cutoff, double score_hint) const
    {
        const T& derived = static_cast<const T&>(*this);
        auto maximum = derived.maximum(s2);
        auto cutoff_distance =
            static_cast<decltype(maximum)>(std::ceil(static_cast<double>(maximum) * score_cutoff));
        auto hint_distance =
            static_cast<decltype(maximum)>(std::ceil(static_cast<double>(maximum) * score_hint));
        double dist = static_cast<double>(derived._distance(s2, cutoff_distance, hint_distance));
        double norm_dist = (maximum != 0) ? dist / static_cast<double>(maximum) : 0.0;
        return (norm_dist <= score_cutoff) ? norm_dist : 1.0;
    }

    template <typename InputIt2>
    double _normalized_similarity(const Range<InputIt2>& s2, double score_cutoff, double score_hint) const
    {
        double cutoff_score = NormSim_to_NormDist(score_cutoff);
        double hint_score = NormSim_to_NormDist(score_hint);
        double norm_dist = _normalized_distance(s2, cutoff_score, hint_score);
        double norm_sim = 1.0 - norm_dist;
        return (norm_sim >= score_cutoff) ? norm_sim : 0.0;
    }

    CachedNormalizedMetricBase()
    {}
    friend T;
};

template <typename T, typename ResType, int64_t WorstSimilarity, int64_t WorstDistance>
struct CachedDistanceBase : public CachedNormalizedMetricBase<T> {
    template <typename InputIt2>
    ResType distance(InputIt2 first2, InputIt2 last2,
                     ResType score_cutoff = static_cast<ResType>(WorstDistance),
                     ResType score_hint = static_cast<ResType>(WorstDistance)) const
    {
        const T& derived = static_cast<const T&>(*this);
        return derived._distance(Range(first2, last2), score_cutoff, score_hint);
    }

    template <typename Sentence2>
    ResType distance(const Sentence2& s2, ResType score_cutoff = static_cast<ResType>(WorstDistance),
                     ResType score_hint = static_cast<ResType>(WorstDistance)) const
    {
        const T& derived = static_cast<const T&>(*this);
        return derived._distance(Range(s2), score_cutoff, score_hint);
    }

    template <typename InputIt2>
    ResType similarity(InputIt2 first2, InputIt2 last2,
                       ResType score_cutoff = static_cast<ResType>(WorstSimilarity),
                       ResType score_hint = static_cast<ResType>(WorstSimilarity)) const
    {
        return _similarity(Range(first2, last2), score_cutoff, score_hint);
    }

    template <typename Sentence2>
    ResType similarity(const Sentence2& s2, ResType score_cutoff = static_cast<ResType>(WorstSimilarity),
                       ResType score_hint = static_cast<ResType>(WorstSimilarity)) const
    {
        return _similarity(Range(s2), score_cutoff, score_hint);
    }

protected:
    template <typename InputIt2>
    ResType _similarity(const Range<InputIt2>& s2, ResType score_cutoff, ResType score_hint) const
    {
        const T& derived = static_cast<const T&>(*this);
        ResType maximum = derived.maximum(s2);
        if (score_cutoff > maximum) return 0;

        score_hint = std::min(score_cutoff, score_hint);
        ResType cutoff_distance = maximum - score_cutoff;
        ResType hint_distance = maximum - score_hint;
        ResType dist = derived._distance(s2, cutoff_distance, hint_distance);
        ResType sim = maximum - dist;
        return (sim >= score_cutoff) ? sim : 0;
    }

    CachedDistanceBase()
    {}
    friend T;
};

template <typename T, typename ResType, int64_t WorstSimilarity, int64_t WorstDistance>
struct CachedSimilarityBase : public CachedNormalizedMetricBase<T> {
    template <typename InputIt2>
    ResType distance(InputIt2 first2, InputIt2 last2,
                     ResType score_cutoff = static_cast<ResType>(WorstDistance),
                     ResType score_hint = static_cast<ResType>(WorstDistance)) const
    {
        return _distance(Range(first2, last2), score_cutoff, score_hint);
    }

    template <typename Sentence2>
    ResType distance(const Sentence2& s2, ResType score_cutoff = static_cast<ResType>(WorstDistance),
                     ResType score_hint = static_cast<ResType>(WorstDistance)) const
    {
        return _distance(Range(s2), score_cutoff, score_hint);
    }

    template <typename InputIt2>
    ResType similarity(InputIt2 first2, InputIt2 last2,
                       ResType score_cutoff = static_cast<ResType>(WorstSimilarity),
                       ResType score_hint = static_cast<ResType>(WorstSimilarity)) const
    {
        const T& derived = static_cast<const T&>(*this);
        return derived._similarity(Range(first2, last2), score_cutoff, score_hint);
    }

    template <typename Sentence2>
    ResType similarity(const Sentence2& s2, ResType score_cutoff = static_cast<ResType>(WorstSimilarity),
                       ResType score_hint = static_cast<ResType>(WorstSimilarity)) const
    {
        const T& derived = static_cast<const T&>(*this);
        return derived._similarity(Range(s2), score_cutoff, score_hint);
    }

protected:
    template <typename InputIt2>
    ResType _distance(const Range<InputIt2>& s2, ResType score_cutoff, ResType score_hint) const
    {
        const T& derived = static_cast<const T&>(*this);
        ResType maximum = derived.maximum(s2);
        ResType cutoff_similarity = (maximum > score_cutoff) ? maximum - score_cutoff : 0;
        ResType hint_similarity = (maximum > score_hint) ? maximum - score_hint : 0;
        ResType sim = derived._similarity(s2, cutoff_similarity, hint_similarity);
        ResType dist = maximum - sim;

        if constexpr (std::is_floating_point_v<ResType>)
            return (dist <= score_cutoff) ? dist : 1.0;
        else
            return (dist <= score_cutoff) ? dist : score_cutoff + 1;
    }

    CachedSimilarityBase()
    {}
    friend T;
};

template <typename T, typename ResType>
struct MultiNormalizedMetricBase {
    template <typename InputIt2>
    void normalized_distance(double* scores, size_t score_count, InputIt2 first2, InputIt2 last2,
                             double score_cutoff = 1.0) const
    {
        _normalized_distance(scores, score_count, Range(first2, last2), score_cutoff);
    }

    template <typename Sentence2>
    void normalized_distance(double* scores, size_t score_count, const Sentence2& s2,
                             double score_cutoff = 1.0) const
    {
        _normalized_distance(scores, score_count, Range(s2), score_cutoff);
    }

    template <typename InputIt2>
    void normalized_similarity(double* scores, size_t score_count, InputIt2 first2, InputIt2 last2,
                               double score_cutoff = 0.0) const
    {
        _normalized_similarity(scores, score_count, Range(first2, last2), score_cutoff);
    }

    template <typename Sentence2>
    void normalized_similarity(double* scores, size_t score_count, const Sentence2& s2,
                               double score_cutoff = 0.0) const
    {
        _normalized_similarity(scores, score_count, Range(s2), score_cutoff);
    }

protected:
    template <typename InputIt2>
    void _normalized_distance(double* scores, size_t score_count, const Range<InputIt2>& s2,
                              double score_cutoff = 1.0) const
    {
        const T& derived = static_cast<const T&>(*this);
        if (score_count < derived.result_count())
            throw std::invalid_argument("scores has to have >= result_count() elements");

        // reinterpretation only works when the types have the same size
        ResType* scores_orig = nullptr;
        if constexpr (sizeof(double) == sizeof(ResType))
            scores_orig = reinterpret_cast<ResType*>(scores);
        else
            scores_orig = new ResType[derived.result_count()];

        derived.distance(scores_orig, derived.result_count(), s2);

        for (size_t i = 0; i < derived.get_input_count(); ++i) {
            auto maximum = derived.maximum(i, s2);
            double norm_dist =
                (maximum != 0) ? static_cast<double>(scores_orig[i]) / static_cast<double>(maximum) : 0.0;
            scores[i] = (norm_dist <= score_cutoff) ? norm_dist : 1.0;
        }

        if constexpr (sizeof(double) != sizeof(ResType)) delete[] scores_orig;
    }

    template <typename InputIt2>
    void _normalized_similarity(double* scores, size_t score_count, const Range<InputIt2>& s2,
                                double score_cutoff) const
    {
        const T& derived = static_cast<const T&>(*this);
        _normalized_distance(scores, score_count, s2);

        for (size_t i = 0; i < derived.get_input_count(); ++i) {
            double norm_sim = 1.0 - scores[i];
            scores[i] = (norm_sim >= score_cutoff) ? norm_sim : 0.0;
        }
    }

    MultiNormalizedMetricBase()
    {}
    friend T;
};

template <typename T, typename ResType, int64_t WorstSimilarity, int64_t WorstDistance>
struct MultiDistanceBase : public MultiNormalizedMetricBase<T, ResType> {
    template <typename InputIt2>
    void distance(ResType* scores, size_t score_count, InputIt2 first2, InputIt2 last2,
                  ResType score_cutoff = static_cast<ResType>(WorstDistance)) const
    {
        const T& derived = static_cast<const T&>(*this);
        derived._distance(scores, score_count, Range(first2, last2), score_cutoff);
    }

    template <typename Sentence2>
    void distance(ResType* scores, size_t score_count, const Sentence2& s2,
                  ResType score_cutoff = static_cast<ResType>(WorstDistance)) const
    {
        const T& derived = static_cast<const T&>(*this);
        derived._distance(scores, score_count, Range(s2), score_cutoff);
    }

    template <typename InputIt2>
    void similarity(ResType* scores, size_t score_count, InputIt2 first2, InputIt2 last2,
                    ResType score_cutoff = static_cast<ResType>(WorstSimilarity)) const
    {
        _similarity(scores, score_count, Range(first2, last2), score_cutoff);
    }

    template <typename Sentence2>
    void similarity(ResType* scores, size_t score_count, const Sentence2& s2,
                    ResType score_cutoff = static_cast<ResType>(WorstSimilarity)) const
    {
        _similarity(scores, score_count, Range(s2), score_cutoff);
    }

protected:
    template <typename InputIt2>
    void _similarity(ResType* scores, size_t score_count, const Range<InputIt2>& s2,
                     ResType score_cutoff) const
    {
        const T& derived = static_cast<const T&>(*this);
        derived._distance(scores, score_count, s2);

        for (size_t i = 0; i < derived.get_input_count(); ++i) {
            ResType maximum = derived.maximum(i, s2);
            ResType sim = maximum - scores[i];
            scores[i] = (sim >= score_cutoff) ? sim : 0;
        }
    }

    MultiDistanceBase()
    {}
    friend T;
};

template <typename T, typename ResType, int64_t WorstSimilarity, int64_t WorstDistance>
struct MultiSimilarityBase : public MultiNormalizedMetricBase<T, ResType> {
    template <typename InputIt2>
    void distance(ResType* scores, size_t score_count, InputIt2 first2, InputIt2 last2,
                  ResType score_cutoff = static_cast<ResType>(WorstDistance)) const
    {
        _distance(scores, score_count, Range(first2, last2), score_cutoff);
    }

    template <typename Sentence2>
    void distance(ResType* scores, size_t score_count, const Sentence2& s2,
                  ResType score_cutoff = static_cast<ResType>(WorstDistance)) const
    {
        _distance(scores, score_count, Range(s2), score_cutoff);
    }

    template <typename InputIt2>
    void similarity(ResType* scores, size_t score_count, InputIt2 first2, InputIt2 last2,
                    ResType score_cutoff = static_cast<ResType>(WorstSimilarity)) const
    {
        const T& derived = static_cast<const T&>(*this);
        derived._similarity(scores, score_count, Range(first2, last2), score_cutoff);
    }

    template <typename Sentence2>
    void similarity(ResType* scores, size_t score_count, const Sentence2& s2,
                    ResType score_cutoff = static_cast<ResType>(WorstSimilarity)) const
    {
        const T& derived = static_cast<const T&>(*this);
        derived._similarity(scores, score_count, Range(s2), score_cutoff);
    }

protected:
    template <typename InputIt2>
    void _distance(ResType* scores, size_t score_count, const Range<InputIt2>& s2, ResType score_cutoff) const
    {
        const T& derived = static_cast<const T&>(*this);
        derived._similarity(scores, score_count, s2);

        for (size_t i = 0; i < derived.get_input_count(); ++i) {
            ResType maximum = derived.maximum(i, s2);
            ResType dist = maximum - scores[i];

            if constexpr (std::is_floating_point_v<ResType>)
                scores[i] = (dist <= score_cutoff) ? dist : 1.0;
            else
                scores[i] = (dist <= score_cutoff) ? dist : score_cutoff + 1;
        }
    }

    MultiSimilarityBase()
    {}
    friend T;
};

} // namespace duckdb_rapidfuzz::detail
