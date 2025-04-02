# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Unit test for email utilities.
"""

from django.test import TestCase, override_settings
from django.core import mail
from django.contrib.auth import get_user_model
from core.utils.emails import send_verification_email

User = get_user_model()


@override_settings(DEFAULT_FROM_EMAIL="no-reply@example.com")
class SendVerificationEmailTests(TestCase):
    def setUp(self):
        self.user = User.objects.create_user(
            first_name="Jane",
            last_name="Doe",
            username="janedoe",
            email="jane@example.com",
            password="securepassword",
            is_active=False,
        )
        self.uid = "testuid"
        self.token = "testtoken"

    def test_send_verification_email_success(self):
        send_verification_email(self.user, self.uid, self.token)

        self.assertEqual(len(mail.outbox), 1)
        email = mail.outbox[0]

        self.assertEqual(email.subject, "Verify Your Email")
        self.assertIn("jane@example.com", email.to)
        self.assertIn("verify your account", email.body)
        self.assertIn("uid=testuid", email.body)
        self.assertIn("token=testtoken", email.body)
        self.assertEqual(email.from_email, "no-reply@example.com")
