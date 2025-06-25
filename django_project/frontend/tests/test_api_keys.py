"""Tests for API Key management endpoints."""

from django.contrib.auth import get_user_model
from django.urls import reverse
from rest_framework import status
from rest_framework.test import APITestCase, APIClient

from knox.models import AuthToken

User = get_user_model()


class APIKeyTests(APITestCase):
    """
    Test the API Key management endpoints.

      - GET    /api/api-keys/
      - POST   /api/api-keys/
      - DELETE /api/api-keys/<key_id>/
    """

    def setUp(self):
        """Set up the test client and a user."""
        self.client = APIClient()
        self.user = User.objects.create_user(
            username='test', password='secret123'
        )
        self.client.force_authenticate(user=self.user)
        self.list_url = reverse('api_key_list_create')

    def test_list_keys_empty(self):
        """GET initially returns an empty list."""
        response = self.client.get(self.list_url)
        assert response.status_code == status.HTTP_200_OK
        assert response.json() == []

    def test_create_key_returns_metadata_and_token(self):
        """
        POST creates a new key.

        Returns id, token, created, expiry, name, description.
        """
        payload = {
            'name': 'my-token',
            'description': 'testing API key creation'
        }
        response = self.client.post(self.list_url, payload, format='json')
        assert response.status_code == status.HTTP_201_CREATED
        data = response.json()
        # required fields
        for field in (
            "id", "token", "created", "expiry", "name", "description"
        ):
            assert field in data
        assert data['name'] == 'my-token'
        assert data['description'] == 'testing API key creation'
        # DB record exists
        assert AuthToken.objects.filter(
            pk=data['id'], user=self.user
        ).exists()

    def test_list_keys_shows_created_key(self):
        """
        After POST, GET.

        Returns the created key with metadata.
        """
        payload = {
            'name': 'another-token',
            'description': 'list view test'
        }
        self.client.post(self.list_url, payload, format='json')
        items = self.client.get(self.list_url).json()
        assert len(items) == 1
        item = items[0]

        expected_keys = {
            'id',
            'token',
            'created',
            'name',
            'description',
            'expiry'
        }
        assert set(item.keys()) == expected_keys
        assert item['name'] == 'another-token'
        assert item['description'] == 'list view test'

    def test_delete_key_revokes_and_returns_detail(self):
        """DELETE revokes the key and returns a confirmation."""
        payload = {'name': 'to-be-deleted', 'description': ''}
        key_id = self.client.post(
            self.list_url, payload, format='json'
        ).json()['id']
        destroy_url = reverse('api_key_destroy', kwargs={'key_id': key_id})
        del_resp = self.client.delete(destroy_url)
        assert del_resp.status_code == status.HTTP_200_OK
        assert del_resp.json().get('detail') == 'API key revoked'
        assert self.client.get(self.list_url).json() == []

    def test_cannot_delete_other_user_key(self):
        """Users cannot delete tokens belonging to someone else."""
        other = User.objects.create_user(username='bob', password='pw')
        other_instance, _ = AuthToken.objects.create(user=other)
        destroy_url = reverse(
            'api_key_destroy', kwargs={'key_id': other_instance.pk}
        )
        resp = self.client.delete(destroy_url)
        assert resp.status_code == status.HTTP_404_NOT_FOUND
