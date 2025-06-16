# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Admin Sign Up Request

"""

from django.contrib import admin
from django.contrib.auth import get_user_model
from django.contrib.auth.models import Group
from django.utils.translation import gettext_lazy as _
from django.utils import timezone
from django.conf import settings
from django.core.mail import send_mail
from django.utils.http import urlsafe_base64_encode
from django.utils.encoding import force_bytes
from django.contrib.auth.tokens import default_token_generator
from django.contrib import messages
from allauth.socialaccount.models import SocialAccount
from gap.models.signup_request import RequestStatus
from gap.models import SignUpRequest, UserProfile


User = get_user_model()


class ApprovedByManagerFilter(admin.SimpleListFilter):
    """Filter approved_by by users in 'Manager' group."""

    title = _('Approved by (Managers only)')
    parameter_name = 'approved_by'

    def lookups(self, request, model_admin):
        """Return a list of tuples."""
        try:
            managers = User.objects.filter(groups__name='User Manager')
            return [(
                user.id, user.get_full_name() or user.username
            ) for user in managers]
        except Group.DoesNotExist:
            return []

    def queryset(self, request, queryset):
        """Return the filtered queryset."""
        if self.value():
            return queryset.filter(approved_by__id=self.value())
        return queryset


@admin.register(SignUpRequest)
class SignUpRequestAdmin(admin.ModelAdmin):
    """Sign Up Request Admin."""

    list_display = (
        'first_name', 'last_name',
        'email', 'status',
        'get_email_verified', 'organization',
        'submitted_at', 'approved_at'
    )
    search_fields = ('first_name', 'last_name', 'email')
    list_filter = ('status', 'submitted_at', ApprovedByManagerFilter)
    ordering = ('-submitted_at',)
    readonly_fields = ('submitted_at', 'approved_at')

    actions = ['approve_requests', 'reject_requests']


    def approve_requests(self, request, queryset):
        """Admin action to approve selected sign-up requests."""
        for req in queryset.filter(status=RequestStatus.PENDING):
            # Update request status
            req.status = RequestStatus.APPROVED
            req.approved_by = request.user
            req.approved_at = timezone.now()
            req.save()

            # Activate the user if they exist and email is verified
            try:
                user = User.objects.get(email=req.email)
            except User.DoesNotExist:
                self.message_user(
                    request,
                    f"No user account found for {req.email}.",
                    level=messages.WARNING
                )
                continue

            # Skip if email not verified in profile
            if not (
                hasattr(
                    user, 'userprofile'
                ) and user.userprofile.email_verified
            ):
                self.message_user(
                    request,
                    f"Email for {req.email} is not verified; skipping.",
                    level=messages.INFO
                )
                continue

            user.is_active = True
            user.save()

            if SocialAccount.objects.filter(user=user).exists():
                subject = "Your Global Access Platform account is now active"
                message = (
                    f"Hello {req.first_name},\n\n"
                    "Your account is approved and active!\n"
                    "Just sign in with your social provider (Google/GitHub) "
                    "as usual.\n\n"
                    "If you didn’t expect this, "
                    "you can ignore this message.\n\n"
                    "Welcome aboard,\n"
                    "Global Access Platform Team"
                )
            else:
                # Build UID and token
                uidb64 = urlsafe_base64_encode(force_bytes(user.pk))
                token = default_token_generator.make_token(user)

                # Build absolute link to your reset page
                relative = f"/signin?uid={uidb64}&token={token}"
                setup_url = request.build_absolute_uri(relative)

                # Send the email
                subject = "Set up your Global Access Platform password"
                message = (
                    f"Hello {req.first_name},\n\n"
                    "Your sign-up request has been approved!\n\n"
                    "To set your password and access the platform, "
                    "click here:\n"
                    f"{setup_url}\n\n"
                    "If you did not request this, "
                    "please ignore this email.\n\n"
                    "Welcome aboard,\n"
                    "Global Access Platform Team"
                )
            send_mail(
                subject=subject,
                message=message,
                from_email=settings.DEFAULT_FROM_EMAIL,
                recipient_list=[req.email],
                fail_silently=False,
            )

            self.message_user(
                request,
                f"Approval and e-mail sent to {req.email}",
                level=messages.SUCCESS
            )

    approve_requests.short_description = "Approve selected requests"

    def reject_requests(self, request, queryset):
        """Admin action to reject selected sign-up requests."""
        for req in queryset.filter(status=RequestStatus.PENDING):
            req.status = RequestStatus.REJECTED
            req.approved_by = request.user
            req.approved_at = timezone.now()
            req.save()

            # Deactivate user if exists
            try:
                user = User.objects.get(email=req.email)
                user.is_active = False
                user.save()
            except User.DoesNotExist:
                self.message_user(
                    request,
                    f"No user account for {req.email}",
                    level=messages.WARNING
                )
                continue

            # Send rejection email
            subject = "Your Account Signup Has Been Rejected"
            message = (
                f"Hello {req.first_name},\n\n"
                "We’re sorry to inform you that your sign-up request "
                "for Global Access Platform has been rejected.\n\n"
                "If you believe this is a mistake, please contact support.\n\n"
                "Regards,\n"
                "Global Access Platform Team"
            )
            send_mail(subject, message,
                      settings.DEFAULT_FROM_EMAIL,
                      [req.email],
                      fail_silently=False)

            self.message_user(
                request,
                f"Rejected and notified {req.email}",
                level=messages.INFO
            )
    reject_requests.short_description = "Reject selected requests"

    def formfield_for_foreignkey(self, db_field, request, **kwargs):
        """Set approved_by field to staff users only."""
        if db_field.name == 'approved_by':
            kwargs["queryset"] = User.objects.filter(is_staff=True)
        return super().formfield_for_foreignkey(db_field, request, **kwargs)

    def get_email_verified(self, obj):
        """Return email verified status."""
        # find the user by email
        try:
            user = User.objects.get(email=obj.email)
            if UserProfile.objects.filter(user=user).exists():
                return user.userprofile.email_verified
            return "-"
        except User.DoesNotExist:
            return "-"

    get_email_verified.short_description = _('Email Verified')
