"""_split_range 切片：升序、无缝、无重叠、跨度有界、整体覆盖恰好等于输入区间。"""

from datetime import date, timedelta

from api.services.readthrough import _split_range


def test_no_limit_returns_whole_range():
    assert list(_split_range(date(1990, 12, 19), date(2026, 6, 11), None)) == [
        (date(1990, 12, 19), date(2026, 6, 11))
    ]


def test_range_within_limit_is_single_slice():
    assert list(_split_range(date(2026, 1, 1), date(2026, 1, 10), 30)) == [
        (date(2026, 1, 1), date(2026, 1, 10))
    ]


def test_single_day_range():
    assert list(_split_range(date(2026, 6, 11), date(2026, 6, 11), 10)) == [
        (date(2026, 6, 11), date(2026, 6, 11))
    ]


def test_exact_multiple_of_limit():
    slices = list(_split_range(date(2026, 1, 1), date(2026, 1, 20), 10))
    assert slices == [
        (date(2026, 1, 1), date(2026, 1, 10)),
        (date(2026, 1, 11), date(2026, 1, 20)),
    ]


def test_full_history_slices_are_contiguous_and_bounded():
    fs, fe, max_days = date(1990, 12, 19), date(2026, 6, 11), 3650
    slices = list(_split_range(fs, fe, max_days))

    assert slices[0][0] == fs
    assert slices[-1][1] == fe
    for s, e in slices:
        assert s <= e
        assert (e - s).days + 1 <= max_days
    for (_, prev_end), (next_start, _) in zip(slices, slices[1:]):
        assert next_start == prev_end + timedelta(days=1)
