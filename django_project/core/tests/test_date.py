# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Unit test for date utils.
"""

from datetime import datetime, timezone
from django.test import TestCase

from core.utils.date import (
    find_max_min_epoch_dates,
    split_epochs_by_year,
    split_epochs_by_year_month,
    closest_leap_year
)


class TestDateUtilities(TestCase):
    """Test Date utilities."""

    def test_both_none(self):
        """Test that returns both None."""
        self.assertEqual(
            find_max_min_epoch_dates(None, None, 1000),
            (1000, 1000)
        )

    def test_min_none(self):
        """Test that returns min None."""
        self.assertEqual(
            find_max_min_epoch_dates(None, 2000, 1000),
            (1000, 2000)
        )

    def test_max_none(self):
        """Test that returns max None."""
        self.assertEqual(
            find_max_min_epoch_dates(500, None, 1000),
            (500, 1000)
        )

    def test_epoch_smaller_than_min(self):
        """Test if epoch smaller than min."""
        self.assertEqual(
            find_max_min_epoch_dates(1500, 2000, 1000),
            (1000, 2000)
        )

    def test_epoch_larger_than_max(self):
        """Test if epoch larger than max."""
        self.assertEqual(
            find_max_min_epoch_dates(1500, 2000, 2500),
            (1500, 2500)
        )

    def test_epoch_within_range(self):
        """Test if epoch within range."""
        self.assertEqual(
            find_max_min_epoch_dates(1000, 2000, 1500),
            (1000, 2000)
        )

    def test_same_year(self):
        """Test if start and end in the same year."""
        start_epoch = datetime(2023, 5, 1, tzinfo=timezone.utc).timestamp()
        end_epoch = datetime(
            2023, 12, 31, 23, 59, 59, tzinfo=timezone.utc).timestamp()
        expected = [(2023, int(start_epoch), int(end_epoch))]
        self.assertEqual(
            split_epochs_by_year(int(start_epoch), int(end_epoch)),
            expected
        )

    def test_crossing_two_years(self):
        """Test if start and end crossing two years."""
        start_epoch = datetime(2023, 11, 1, tzinfo=timezone.utc).timestamp()
        end_epoch = datetime(2024, 3, 1, tzinfo=timezone.utc).timestamp()
        expected = [
            (
                2023, int(start_epoch),
                int(
                    datetime(
                        2023, 12, 31, 23, 59, 59,
                        tzinfo=timezone.utc
                    ).timestamp()
                )
            ),
            (
                2024,
                int(datetime(2024, 1, 1, tzinfo=timezone.utc).timestamp()),
                int(end_epoch)
            )
        ]
        self.assertEqual(
            split_epochs_by_year(int(start_epoch), int(end_epoch)), expected
        )

    def test_full_multiple_years(self):
        """Test multiple years."""
        start_epoch = datetime(2021, 1, 1, tzinfo=timezone.utc).timestamp()
        end_epoch = datetime(
            2023, 12, 31, 23, 59, 59, tzinfo=timezone.utc
        ).timestamp()
        expected = [
            (
                2021,
                int(datetime(2021, 1, 1, tzinfo=timezone.utc).timestamp()),
                int(
                    datetime(
                        2021, 12, 31, 23, 59, 59,
                        tzinfo=timezone.utc
                    ).timestamp()
                )
            ),
            (
                2022,
                int(datetime(2022, 1, 1, tzinfo=timezone.utc).timestamp()),
                int(
                    datetime(
                        2022, 12, 31, 23, 59, 59,
                        tzinfo=timezone.utc
                    ).timestamp()
                )
            ),
            (
                2023,
                int(datetime(2023, 1, 1, tzinfo=timezone.utc).timestamp()),
                int(
                    datetime(
                        2023, 12, 31, 23, 59, 59,
                        tzinfo=timezone.utc
                    ).timestamp()
                )
            )
        ]
        self.assertEqual(
            split_epochs_by_year(int(start_epoch), int(end_epoch)), expected
        )

    def test_partial_years(self):
        """Test partial years."""
        start_epoch = datetime(2022, 6, 15, tzinfo=timezone.utc).timestamp()
        end_epoch = datetime(2024, 8, 20, tzinfo=timezone.utc).timestamp()
        expected = [
            (
                2022,
                int(start_epoch),
                int(
                    datetime(
                        2022, 12, 31, 23, 59, 59,
                        tzinfo=timezone.utc
                    ).timestamp()
                )
            ),
            (
                2023,
                int(datetime(2023, 1, 1, tzinfo=timezone.utc).timestamp()),
                int(
                    datetime(
                        2023, 12, 31, 23, 59, 59,
                        tzinfo=timezone.utc
                    ).timestamp()
                )
            ),
            (
                2024,
                int(datetime(2024, 1, 1, tzinfo=timezone.utc).timestamp()),
                int(end_epoch)
            )
        ]
        self.assertEqual(
            split_epochs_by_year(int(start_epoch), int(end_epoch)), expected
        )

    def test_same_start_and_end(self):
        """Test same year."""
        start_epoch = datetime(2023, 7, 15, tzinfo=timezone.utc).timestamp()
        expected = [(2023, int(start_epoch), int(start_epoch))]
        self.assertEqual(
            split_epochs_by_year(int(start_epoch), int(start_epoch)), expected
        )


class TestSplitEpochsByYearMonth(TestCase):
    """Test method split_epochs_by_year_month."""

    def test_same_month(self):
        """Test same month."""
        start_epoch = datetime(2023, 5, 10, tzinfo=timezone.utc).timestamp()
        end_epoch = datetime(2023, 5, 25, tzinfo=timezone.utc).timestamp()
        expected = [(2023, 5, int(start_epoch), int(end_epoch))]
        self.assertEqual(
            split_epochs_by_year_month(int(start_epoch), int(end_epoch)),
            expected
        )

    def test_crossing_two_months(self):
        """Test crossing two months."""
        start_epoch = datetime(2023, 11, 20, tzinfo=timezone.utc).timestamp()
        end_epoch = datetime(2023, 12, 10, tzinfo=timezone.utc).timestamp()
        expected = [
            (2023, 11, int(start_epoch),
             int(datetime(2023, 11, 30, 23, 59, 59,
                          tzinfo=timezone.utc).timestamp())),
            (2023, 12,
             int(datetime(2023, 12, 1, tzinfo=timezone.utc).timestamp()),
             int(end_epoch))
        ]
        self.assertEqual(
            split_epochs_by_year_month(int(start_epoch), int(end_epoch)),
            expected
        )

    def test_crossing_year_boundary(self):
        """Test crossing year."""
        start_epoch = datetime(2023, 12, 20, tzinfo=timezone.utc).timestamp()
        end_epoch = datetime(2024, 1, 10, tzinfo=timezone.utc).timestamp()
        expected = [
            (2023, 12, int(start_epoch),
             int(datetime(2023, 12, 31, 23, 59, 59,
                          tzinfo=timezone.utc).timestamp())),
            (2024, 1,
             int(datetime(2024, 1, 1, tzinfo=timezone.utc).timestamp()),
             int(end_epoch))
        ]
        self.assertEqual(
            split_epochs_by_year_month(int(start_epoch), int(end_epoch)),
            expected
        )

    def test_multiple_years_and_months(self):
        """Test multiple years and months."""
        start_epoch = datetime(2022, 10, 15, tzinfo=timezone.utc).timestamp()
        end_epoch = datetime(2023, 2, 20, tzinfo=timezone.utc).timestamp()
        expected = [
            (2022, 10, int(start_epoch),
             int(datetime(2022, 10, 31, 23, 59, 59,
                          tzinfo=timezone.utc).timestamp())),
            (2022, 11,
             int(datetime(2022, 11, 1, tzinfo=timezone.utc).timestamp()),
             int(datetime(2022, 11, 30, 23, 59, 59,
                          tzinfo=timezone.utc).timestamp())),
            (2022, 12,
             int(datetime(2022, 12, 1, tzinfo=timezone.utc).timestamp()),
             int(datetime(2022, 12, 31, 23, 59, 59,
                          tzinfo=timezone.utc).timestamp())),
            (2023, 1,
             int(datetime(2023, 1, 1, tzinfo=timezone.utc).timestamp()),
             int(datetime(2023, 1, 31, 23, 59, 59,
                          tzinfo=timezone.utc).timestamp())),
            (2023, 2,
             int(datetime(2023, 2, 1, tzinfo=timezone.utc).timestamp()),
             int(end_epoch))
        ]
        self.assertEqual(
            split_epochs_by_year_month(int(start_epoch), int(end_epoch)),
            expected
        )

    def test_same_start_and_end(self):
        """Test same input."""
        start_epoch = datetime(2023, 7, 15, tzinfo=timezone.utc).timestamp()
        expected = [(2023, 7, int(start_epoch), int(start_epoch))]
        self.assertEqual(
            split_epochs_by_year_month(int(start_epoch), int(start_epoch)),
            expected
        )

    def test_closest_leap_year(self):
        """Test closest_leap_year."""
        self.assertEqual(closest_leap_year(2023), 2020)
        self.assertEqual(closest_leap_year(2020), 2020)
        self.assertEqual(closest_leap_year(2019), 2016)
        self.assertEqual(closest_leap_year(2001), 2000)
        self.assertEqual(closest_leap_year(1999), 1996)
