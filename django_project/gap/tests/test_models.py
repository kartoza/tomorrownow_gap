# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Unit tests for GAP Models.
"""

from django.test import TestCase

from gap.factories import (
    ProviderFactory,
    AttributeFactory,
    CountryFactory,
    MeasurementFactory
)
from gap.models import (
    Provider,
    Attribute,
    Country,
    Measurement,
    CollectorSession,
    IngestorType,
    DatasetTimeStep,
    DatasetStore
)


class ProviderCRUDTest(TestCase):
    """Provider test case."""

    def test_create_provider(self):
        """Test create provider object."""
        provider = ProviderFactory()
        self.assertIsInstance(provider, Provider)
        self.assertTrue(Provider.objects.filter(id=provider.id).exists())

    def test_read_provider(self):
        """Test read provider object."""
        provider = ProviderFactory()
        fetched_provider = Provider.objects.get(id=provider.id)
        self.assertEqual(provider, fetched_provider)

    def test_update_provider(self):
        """Test update provider object."""
        provider = ProviderFactory()
        new_name = "Updated Provider Name"
        provider.name = new_name
        provider.save()
        updated_provider = Provider.objects.get(id=provider.id)
        self.assertEqual(updated_provider.name, new_name)

    def test_delete_provider(self):
        """Test delete provider object."""
        provider = ProviderFactory()
        provider_id = provider.id
        provider.delete()
        self.assertFalse(Provider.objects.filter(id=provider_id).exists())


class AttributeCRUDTest(TestCase):
    """Attribute test case."""

    def test_create_attribute(self):
        """Test create attribute object."""
        attribute = AttributeFactory()
        self.assertIsInstance(attribute, Attribute)
        self.assertTrue(Attribute.objects.filter(id=attribute.id).exists())

    def test_read_attribute(self):
        """Test read attribute object."""
        attribute = AttributeFactory()
        fetched_attribute = Attribute.objects.get(id=attribute.id)
        self.assertEqual(attribute, fetched_attribute)

    def test_update_attribute(self):
        """Test update attribute object."""
        attribute = AttributeFactory()
        new_name = "Updated Attribute Name"
        attribute.name = new_name
        attribute.save()
        updated_attribute = Attribute.objects.get(id=attribute.id)
        self.assertEqual(updated_attribute.name, new_name)

    def test_delete_attribute(self):
        """Test delete attribute object."""
        attribute = AttributeFactory()
        attribute_id = attribute.id
        attribute.delete()
        self.assertFalse(Attribute.objects.filter(id=attribute_id).exists())


class CountryCRUDTest(TestCase):
    """Country test case."""

    def test_create_country(self):
        """Test create country object."""
        country = CountryFactory()
        self.assertIsInstance(country, Country)
        self.assertTrue(Country.objects.filter(id=country.id).exists())

    def test_read_country(self):
        """Test read country object."""
        country = CountryFactory()
        fetched_country = Country.objects.get(id=country.id)
        self.assertEqual(country, fetched_country)

    def test_update_country(self):
        """Test update country object."""
        country = CountryFactory()
        new_name = "Updated Country Name"
        country.name = new_name
        country.save()
        updated_country = Country.objects.get(id=country.id)
        self.assertEqual(updated_country.name, new_name)

    def test_delete_country(self):
        """Test delete country object."""
        country = CountryFactory()
        country_id = country.id
        country.delete()
        self.assertFalse(Country.objects.filter(id=country_id).exists())


class MeasurementCRUDTest(TestCase):
    """Measurement test case."""

    def test_create_measurement(self):
        """Test create measurement object."""
        measurement = MeasurementFactory()
        self.assertIsInstance(measurement, Measurement)
        self.assertTrue(Measurement.objects.filter(id=measurement.id).exists())

    def test_read_measurement(self):
        """Test read measurement object."""
        measurement = MeasurementFactory()
        fetched_measurement = Measurement.objects.get(id=measurement.id)
        self.assertEqual(measurement, fetched_measurement)

    def test_update_measurement(self):
        """Test update measurement object."""
        measurement = MeasurementFactory()
        new_value = 42.0
        measurement.value = new_value
        measurement.save()
        updated_measurement = Measurement.objects.get(id=measurement.id)
        self.assertEqual(updated_measurement.value, new_value)

    def test_delete_measurement(self):
        """Test delete measurement object."""
        measurement = MeasurementFactory()
        measurement_id = measurement.id
        measurement.delete()
        self.assertFalse(
            Measurement.objects.filter(id=measurement_id).exists())


class CollectorSessionCRUDTest(TestCase):
    """CollectorSession test case."""

    def test_collector_session(self):
        """Test collector session."""
        session = CollectorSession.objects.create(
            ingestor_type=IngestorType.SALIENT
        )
        self.assertTrue(
            CollectorSession.objects.filter(id=session.id).exists())
        self.assertEqual(
            str(session),
            f'{session.id}-{session.ingestor_type}-{session.status}'
        )
        session.delete()
        self.assertFalse(
            CollectorSession.objects.filter(id=session.id).exists())


class DatasetTimeStepTest(TestCase):
    """DatasetTimeStep test case."""

    def test_convert_freq(self):
        """Test to_freq function."""
        freq = DatasetTimeStep.to_freq(DatasetTimeStep.DAILY)
        self.assertEqual(freq, 'D')
        freq = DatasetTimeStep.to_freq(DatasetTimeStep.HOURLY)
        self.assertEqual(freq, 'h')
        freq = DatasetTimeStep.to_freq(DatasetTimeStep.QUARTER_HOURLY)
        self.assertEqual(freq, '15min')
        with self.assertRaises(ValueError) as ctx:
            DatasetTimeStep.to_freq(DatasetTimeStep.OTHER)
        self.assertIn('Unsupported time_step', str(ctx.exception))


class DatasetStoreTest(TestCase):
    """DatasetStore test case."""

    def test_convert_extension(self):
        """Test to_ext function."""
        self.assertEqual(
            DatasetStore.to_ext(DatasetStore.NETCDF),
            '.nc'
        )
        self.assertEqual(
            DatasetStore.to_ext(DatasetStore.ZARR),
            '.zarr'
        )
        self.assertEqual(
            DatasetStore.to_ext(DatasetStore.PARQUET),
            ''
        )
        self.assertEqual(
            DatasetStore.to_ext(DatasetStore.ZIP_FILE),
            '.zip'
        )
        with self.assertRaises(NotImplementedError):
            DatasetStore.to_ext(DatasetStore.EXT_API)
