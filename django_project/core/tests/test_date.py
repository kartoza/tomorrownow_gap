# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Unit test for date utils.
"""

from datetime import datetime, timezone, date
from unittest.mock import patch
from django.test import TestCase

from core.utils.date import (
    find_max_min_epoch_dates,
    split_epochs_by_year,
    split_epochs_by_year_month,
    closest_leap_year,
    get_previous_day
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


class TestGetPreviousDay(TestCase):
    """Test get_previous_day function."""

    def setUp(self):
        """Set up test fixtures with known reference dates."""
        # Wednesday, September 17, 2025
        self.test_date_wed = date(2025, 9, 17)
        # Monday, September 15, 2025
        self.test_date_mon = date(2025, 9, 15)
        # Sunday, September 21, 2025
        self.test_date_sun = date(2025, 9, 21)

    def test_basic_functionality_from_wednesday(self):
        """Test finding previous days from Wednesday reference."""
        # From Wednesday (Sep 17), find previous Thursday (Sep 11)
        result = get_previous_day(3, self.test_date_wed)  # Thursday
        expected = date(2025, 9, 11)
        self.assertEqual(result, expected)
        self.assertEqual(result.weekday(), 3)  # Verify it's Thursday

    def test_all_days_from_wednesday(self):
        """Test finding all previous days from Wednesday."""
        test_cases = [
            (0, date(2025, 9, 15)),  # Previous Monday
            (1, date(2025, 9, 16)),  # Previous Tuesday
            (2, date(2025, 9, 10)),  # Previous Wednesday (7 days back)
            (3, date(2025, 9, 11)),  # Previous Thursday
            (4, date(2025, 9, 12)),  # Previous Friday
            (5, date(2025, 9, 13)),  # Previous Saturday
            (6, date(2025, 9, 14)),  # Previous Sunday
        ]

        for day_num, expected_date in test_cases:
            with self.subTest(day_number=day_num):
                result = get_previous_day(day_num, self.test_date_wed)
                self.assertEqual(result, expected_date)
                self.assertEqual(result.weekday(), day_num)

    def test_same_day_goes_back_one_week(self):
        """Test that requesting the same day goes back one full week."""
        # From Wednesday, find previous Wednesday
        result = get_previous_day(2, self.test_date_wed)  # Wednesday
        expected = date(2025, 9, 10)  # One week earlier
        self.assertEqual(result, expected)
        self.assertEqual(result.weekday(), 2)  # Still Wednesday

        # Verify it's exactly 7 days earlier
        self.assertEqual((self.test_date_wed - result).days, 7)

    def test_different_reference_days(self):
        """Test with different reference days of the week."""
        # From Monday, find previous Friday
        result = get_previous_day(4, self.test_date_mon)  # Friday
        expected = date(2025, 9, 12)
        self.assertEqual(result, expected)

        # From Sunday, find previous Tuesday
        result = get_previous_day(1, self.test_date_sun)  # Tuesday
        expected = date(2025, 9, 16)
        self.assertEqual(result, expected)

    def test_datetime_input(self):
        """Test with datetime object as reference_date."""
        # Create datetime object
        test_datetime = datetime(2025, 9, 17, 14, 30, 0)  # Wednesday afternoon

        result = get_previous_day(3, test_datetime)  # Previous Thursday
        expected = date(2025, 9, 11)
        self.assertEqual(result, expected)

    def test_week_boundary_transitions(self):
        """Test transitions across week boundaries."""
        # From Monday (Sep 15), find previous Sunday (Sep 14)
        result = get_previous_day(6, self.test_date_mon)  # Sunday
        expected = date(2025, 9, 14)
        self.assertEqual(result, expected)

        # From Sunday (Sep 21), find previous Monday (Sep 16)
        result = get_previous_day(0, self.test_date_sun)  # Monday
        expected = date(2025, 9, 15)
        self.assertEqual(result, expected)

    def test_month_boundary_transitions(self):
        """Test transitions across month boundaries."""
        # From October 1st (Wednesday), find previous Thursday (Sep 26)
        oct_first = date(2025, 10, 1)
        result = get_previous_day(3, oct_first)  # Thursday
        expected = date(2025, 9, 25)
        self.assertEqual(result, expected)

    def test_year_boundary_transitions(self):
        """Test transitions across year boundaries."""
        # From January 1st, 2026 (Wednesday), find previous Thursday
        jan_first = date(2026, 1, 1)
        result = get_previous_day(3, jan_first)  # Thursday
        expected = date(2025, 12, 25)
        self.assertEqual(result, expected)

    @patch('datetime.datetime')
    def test_default_reference_date(self, mock_datetime):
        """Test default behavior when no reference_date is provided."""
        # Mock the current date to be our test date
        mock_datetime.now.return_value = datetime(2025, 9, 17)

        result = get_previous_day(3)  # Should use mocked "today"
        expected = date(2025, 9, 11)
        self.assertEqual(result, expected)

    def test_invalid_day_numbers(self):
        """Test behavior with invalid day numbers."""
        # These should work due to modular arithmetic, but verify behavior
        with self.subTest("day_number=7"):
            # day_number 7 should behave like 0 (Monday)
            result = get_previous_day(7, self.test_date_wed)
            expected_monday = get_previous_day(0, self.test_date_wed)
            self.assertEqual(result, expected_monday)

        with self.subTest("day_number=-1"):
            # day_number -1 should behave like 6 (Sunday)
            result = get_previous_day(-1, self.test_date_wed)
            expected_sunday = get_previous_day(6, self.test_date_wed)
            self.assertEqual(result, expected_sunday)

    def test_return_type(self):
        """Test that function returns date object."""
        result = get_previous_day(3, self.test_date_wed)
        self.assertIsInstance(result, date)
        self.assertNotIsInstance(result, datetime)

    def test_leap_year_february(self):
        """Test behavior around leap year February."""
        # March 1, 2024 (leap year, Friday)
        march_first = date(2024, 3, 1)

        # Find previous Thursday (Feb 29, 2024 - leap day)
        result = get_previous_day(3, march_first)  # Thursday
        expected = date(2024, 2, 29)
        self.assertEqual(result, expected)

        # Verify it's actually February 29th (leap day)
        self.assertEqual(result.month, 2)
        self.assertEqual(result.day, 29)
