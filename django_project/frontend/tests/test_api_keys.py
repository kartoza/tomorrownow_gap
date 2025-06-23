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
        """POST creates a new key and returns id, token, created, expiry."""
        response = self.client.post(self.list_url, {}, format='json')
        assert response.status_code == status.HTTP_201_CREATED
        data = response.json()
        # required fields
        assert 'id' in data
        assert 'token' in data
        assert 'created' in data
        assert 'expiry' in data
        # DB record exists
        assert AuthToken.objects.filter(pk=data['id'], user=self.user).exists()

    def test_list_keys_shows_created_key(self):
        """After POST, GET returns the key without plaintext token."""
        self.client.post(self.list_url, {}, format='json')
        items = self.client.get(self.list_url).json()
        assert len(items) == 1
        item = items[0]
        assert set(item.keys()) == {'id', 'created', 'expiry'}

    def test_delete_key_revokes_and_returns_detail(self):
        """DELETE revokes the key and returns a confirmation."""
        key_id = self.client.post(
            self.list_url, {}, format='json'
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
