# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Unit tests for GAP Models.
"""

from django.test import TestCase
from gap.models import (
    Provider,
    Attribute,
    Country,
    Station,
    Measurement,
    ObservationType
)
from gap.factories import (
    ProviderFactory,
    AttributeFactory,
    CountryFactory,
    StationFactory,
    MeasurementFactory,
    ObservationTypeFactory
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


class ObservationTypeCRUDTest(TestCase):
    """Attribute test case."""

    def test_create_observation_type(self):
        """Test create observation type object."""
        observation_type = ObservationTypeFactory()
        self.assertIsInstance(observation_type, ObservationType)
        self.assertTrue(
            ObservationType.objects.filter(id=observation_type.id).exists())

    def test_read_observation_type(self):
        """Test read observation_type object."""
        observation_type = ObservationTypeFactory()
        fetched_type = ObservationType.objects.get(id=observation_type.id)
        self.assertEqual(observation_type, fetched_type)

    def test_update_observation_type(self):
        """Test update observation_type object."""
        observation_type = ObservationTypeFactory()
        new_name = "Updated Type Name"
        observation_type.name = new_name
        observation_type.save()
        updated_type = ObservationType.objects.get(id=observation_type.id)
        self.assertEqual(updated_type.name, new_name)

    def test_delete_observation_type(self):
        """Test delete observation_type object."""
        observation_type = ObservationTypeFactory()
        observation_type_id = observation_type.id
        observation_type.delete()
        self.assertFalse(
            ObservationType.objects.filter(id=observation_type_id).exists())


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


class StationCRUDTest(TestCase):
    """Station test case."""

    def test_create_station(self):
        """Test create station object."""
        station = StationFactory()
        self.assertIsInstance(station, Station)
        self.assertTrue(Station.objects.filter(id=station.id).exists())

    def test_read_station(self):
        """Test read station object."""
        station = StationFactory()
        fetched_station = Station.objects.get(id=station.id)
        self.assertEqual(station, fetched_station)

    def test_update_station(self):
        """Test update station object."""
        station = StationFactory()
        new_name = "Updated Station Name"
        station.name = new_name
        station.save()
        updated_station = Station.objects.get(id=station.id)
        self.assertEqual(updated_station.name, new_name)

    def test_delete_station(self):
        """Test delete station object."""
        station = StationFactory()
        station_id = station.id
        station.delete()
        self.assertFalse(Station.objects.filter(id=station_id).exists())


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
