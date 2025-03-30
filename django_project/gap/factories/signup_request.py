# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Factory classes for SignUpRequest model.
"""
import factory
from gap.models import SignUpRequest
from factory.django import DjangoModelFactory


class SignUpRequestFactory(DjangoModelFactory):
    class Meta:
        model = SignUpRequest

    first_name = "Test"
    last_name = "User"
    email = factory.Sequence(lambda n: f"user{n}@example.com")
    description = "Test sign-up request"
