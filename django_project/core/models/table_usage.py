# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Table Usage

"""

from django.db import models, connection
from django.utils import timezone


class TableUsage(models.Model):
    """Class that represents table usage summary."""

    schema_name = models.CharField(
        max_length=255,
        default='public',
        verbose_name='Schema name',
    )
    created_on = models.DateTimeField(
        auto_now_add=True,
        verbose_name='Created on',
    )
    data = models.JSONField(
        default=dict,
        blank=True,
        null=True,
        verbose_name='Data',
    )

    @staticmethod
    def get_table_stats_for_schema(id):
        """Fetch table stats for a given schema."""
        table_usage = TableUsage.objects.get(id=id)
        schema_name = table_usage.schema_name
        results = {
            'datetime': timezone.now().isoformat()
        }

        with connection.cursor() as cursor:
            # Step 1: Get all tables in the schema
            cursor.execute("""
                SELECT tablename
                FROM pg_catalog.pg_tables
                WHERE schemaname = %s;
            """, [schema_name])
            tables = cursor.fetchall()

            for (table_name,) in tables:
                # Step 2a: Get row count
                cursor.execute(
                    f'SELECT COUNT(*) FROM "{schema_name}"."{table_name}";'
                )
                row_count = cursor.fetchone()[0]

                # Step 2b: Get total size (pretty)
                cursor.execute(f"""
                    SELECT pg_size_pretty(
                        pg_total_relation_size(
                            '"{schema_name}"."{table_name}"'
                        )
                    );
                """)
                table_size = cursor.fetchone()[0]

                results[table_name] = {
                    'table_name': table_name,
                    'row_count': row_count,
                    'table_size': table_size,
                }

        # create a new TableUsage object
        table_usage.created_on = timezone.now()
        table_usage.data = results
        table_usage.save()

        return table_usage

    class Meta:
        """Meta class for TableUsage."""

        verbose_name = 'Table Usage'
        verbose_name_plural = 'Table Usage'
        ordering = ['-created_on']
