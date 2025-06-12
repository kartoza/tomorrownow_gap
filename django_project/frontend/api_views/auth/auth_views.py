"""
Tomorrow Now GAP.

.. note:: authentication views.
"""
from dj_rest_auth.registration.views import (
    SocialLoginView as BaseSocialLoginView
)
from rest_framework import status
from rest_framework.response import Response


class DeferredSocialLoginView(BaseSocialLoginView):
    """
    If the user is inactive (new social signup), incomplete_signup flag.
    """

    def get_response(self):
        user = self.user
        # new social users have been marked inactive by our adapter
        if not user.is_active:
            return Response(
                {
                    "incomplete_signup": True,
                    "email": user.email,
                    "first_name": user.first_name,
                    "last_name": user.last_name,
                },
                status=status.HTTP_202_ACCEPTED,
            )
        # otherwise, do the normal login response
        return super().get_response()
