"""
Tomorrow Now GAP.

.. note:: Definition admin
"""
import os

from django import forms
from django.contrib import admin
from django.contrib.auth import get_user_model
from django.contrib.auth.admin import GroupAdmin, UserAdmin
from django.contrib.auth.models import Group

from core.celery import cancel_task, app
from core.forms import CreateKnoxTokenForm, CreateAuthToken
from core.group_email_receiver import crop_plan_receiver
from core.models.background_task import BackgroundTask
from core.models.table_usage import TableUsage
from core.models.object_storage_manager import ObjectStorageManager
from core.settings.utils import absolute_path

User = get_user_model()

version = ''
try:
    folder = absolute_path('')
    version_file = os.path.join(folder, '_version.txt')
    if os.path.exists(version_file):
        version_from_file = (open(version_file, 'rb').read()).decode("utf-8")
        version = f'({version_from_file})'
except Exception:
    pass

admin.site.site_header = f'Django administration {version}'


class AbstractDefinitionAdmin(admin.ModelAdmin):
    """Abstract admin for definition."""

    list_display = (
        'name', 'description'
    )
    search_fields = ('name',)


class GroupAdminForm(forms.ModelForm):
    """ModelForm that contains users of the group."""

    users = forms.ModelMultipleChoiceField(
        User.objects.all(),
        widget=admin.widgets.FilteredSelectMultiple('Users', False),
        required=False,
    )

    def __init__(self, *args, **kwargs):
        """Initialise the form."""
        super(GroupAdminForm, self).__init__(*args, **kwargs)
        if self.instance.pk:
            initial_users = self.instance.user_set.values_list('pk', flat=True)
            self.initial['users'] = initial_users

    def save(self, *args, **kwargs):
        """Save the group."""
        kwargs['commit'] = True
        return super(GroupAdminForm, self).save(*args, **kwargs)

    def save_m2m(self):
        """Save the users in the group."""
        self.instance.user_set.clear()
        self.instance.user_set.add(*self.cleaned_data['users'])


admin.site.unregister(Group)


@admin.register(Group)
class CustomGroupAdmin(GroupAdmin):
    """Custom group admin that using GroupAdminForm."""

    form = GroupAdminForm


admin.site.unregister(User)


@admin.register(User)
class CustomUserAdmin(UserAdmin):
    """Custom user admin that using GroupAdminForm."""

    list_display = (
        "username", "email", "first_name", "last_name", "is_staff",
        "receive_email_for_crop_plan"
    )

    def receive_email_for_crop_plan(self, obj):
        """Return if user receive email for crop plan."""
        return obj.pk in crop_plan_receiver().values_list(
            'pk', flat=True
        )

    receive_email_for_crop_plan.boolean = True


@admin.action(description='Cancel Task')
def cancel_background_task(modeladmin, request, queryset):
    """Cancel a background task."""
    for background_task in queryset:
        if background_task.task_id:
            cancel_task(background_task.task_id)


@admin.register(BackgroundTask)
class BackgroundTaskAdmin(admin.ModelAdmin):
    """Admin class for BackgroundTask model."""

    list_display = (
        'task_name', 'task_id', 'status', 'started_at',
        'finished_at', 'last_update', 'context_id'
    )
    search_fields = ['task_name', 'status', 'task_id']
    actions = [cancel_background_task]
    list_filter = ["status", "task_name"]
    list_per_page = 30


@admin.register(CreateAuthToken)
class CreateAuthTokenAdmin(admin.ModelAdmin):
    """Create auth token."""

    add_form = CreateKnoxTokenForm

    def get_form(self, request, obj=None, **kwargs):
        """Get form of admin."""
        if not obj:
            self.form = self.add_form
        form = super(
            CreateAuthTokenAdmin, self
        ).get_form(request, obj, **kwargs)
        form.request = request
        return form

    def has_change_permission(self, request, obj=None):
        """Return change permission."""
        return False

    def has_delete_permission(self, request, obj=None):
        """Return delete permission."""
        return False

    def has_view_permission(self, request, obj=None):
        """Return view permission."""
        return False


@app.task(name='fetch_table_stats_task')
def fetch_table_stats_task(_id: int):
    """Fetch table stats."""
    TableUsage.get_table_stats_for_schema(_id)


@app.task(name='clear_temp_table_task')
def clear_temp_table_task(_id: int):
    """Clear temp table."""
    TableUsage.clear_temp_table(_id)


@admin.action(description='Run fetch table stats task')
def run_fetch_table_stats(modeladmin, request, queryset):
    """Run fetch table stats in background task."""
    for table_usage in queryset:
        fetch_table_stats_task.delay(table_usage.id)
        break
    modeladmin.message_user(
        request,
        'Fetch table stats task has been started.',
        level='success'
    )


@admin.action(description='Clear temp table')
def run_clear_temp_table(modeladmin, request, queryset):
    """Clear temp table in background task."""
    for table_usage in queryset:
        if table_usage.schema_name != 'temp':
            modeladmin.message_user(
                request,
                'Only temp table can be cleared.',
                level='error'
            )
            return
        if not table_usage.data:
            modeladmin.message_user(
                request,
                'No data to clear.',
                level='error'
            )
            return
        clear_temp_table_task.delay(table_usage.id)
        break
    modeladmin.message_user(
        request,
        'Clear temp table task has been started.',
        level='success'
    )


@admin.register(TableUsage)
class TableUsageAdmin(AbstractDefinitionAdmin):
    """Admin class for TableUsage model."""

    list_display = (
        'schema_name', 'created_on'
    )
    search_fields = ('schema_name',)
    list_filter = ('schema_name',)
    ordering = ['-created_on']
    readonly_fields = ('created_on',)
    actions = [run_fetch_table_stats, run_clear_temp_table]


@admin.register(ObjectStorageManager)
class ObjectStorageManagerAdmin(admin.ModelAdmin):
    """Admin class for ObjectStorageManager model."""

    list_display = (
        'connection_name', 'protocol', 'bucket_name',
        'endpoint_url', 'region_name', 'use_env_vars',
        'created_on'
    )
    search_fields = ('connection_name', 'bucket_name')
    list_filter = ('protocol',)
    ordering = ['-created_on']
