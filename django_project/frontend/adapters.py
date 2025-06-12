# frontend/adapters.py
from allauth.socialaccount.adapter import DefaultSocialAccountAdapter
from django.contrib.auth import get_user_model
from gap.models import SignUpRequest, RequestStatus

User = get_user_model()


class SocialSignupAdapter(DefaultSocialAccountAdapter):
    """
    After a successful social login/signup:
    - mark the user inactive (they’ll set a password later or fill in details)
    - create or update a SignUpRequest in PENDING state
    """
    def save_user(self, request, sociallogin, form=None):
        user = super().save_user(request, sociallogin, form)
        if sociallogin.is_new:
            user.is_active = False
            user.save(update_fields=["is_active"])
            SignUpRequest.objects.update_or_create(
                email=user.email,
                defaults={
                    "first_name": user.first_name or "",
                    "last_name": user.last_name or "",
                    "status": RequestStatus.PENDING,
                },
            )
        return user
