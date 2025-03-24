import uuid as uuid_module

from django.db import models
from django.utils import timezone
from django.utils.translation import gettext_lazy as _, gettext

from app import task_asset_type, task_asset_status


class TaskAsset(models.Model):
    TYPES = (
        (task_asset_type.FOTO, "FOTO"),
        (task_asset_type.FOTO_360, "FOTO_360"),
        (task_asset_type.FOTO_GIGA, "FOTO_GIGA"),
        (task_asset_type.ORTHOPHOTO, "ORTHOPHOTO"),
        (task_asset_type.VIDEO, "VIDEO"),
    )
    STATUS_CODES = (
        (task_asset_status.PROCESSING, "PROCESSING"),
        (task_asset_status.ERROR, "ERROR"),
        (task_asset_status.SUCCESS, "SUCCESS"),
    )

    id = models.UUIDField(
        primary_key=True,
        default=uuid_module.uuid4,
        unique=True,
        serialize=False,
        editable=False,
        verbose_name=_("Id"),
    )
    type = models.IntegerField(
        choices=TYPES,
        db_index=True,
        null=False,
        blank=False,
        help_text=_("The type of asset."),
        verbose_name=_("Type of asset"),
    )
    name = models.CharField(
        max_length=255,
        null=True,
        blank=True,
        help_text=_("The task filename"),
        verbose_name=_("Name"),
    )
    task = models.ForeignKey(
        "app.Task",
        on_delete=models.CASCADE,
        null=False,
        blank=False,
        help_text=_("The task of this asset"),
        verbose_name=_("Task"),
        related_name="assets",
    )
    created_at = models.DateTimeField(
        default=timezone.now, help_text=_("Creation date"), verbose_name=_("Created at")
    )
    status = models.IntegerField(
        choices=TYPES,
        db_index=True,
        null=False,
        blank=False,
        help_text=_("Current status of asset."),
        verbose_name=_("Status of asset"),
    )
    latitude = models.FloatField(
        default=None,
        help_text=_("Latitude"),
        verbose_name=_("Latitude"),
        blank=True,
        null=True,
    )
    longitude = models.FloatField(
        default=None,
        help_text=_("Longitude"),
        verbose_name=_("Longitude"),
        blank=True,
        null=True,
    )
    altitude = models.FloatField(
        default=None,
        help_text=_("Altitude"),
        verbose_name=_("Altitude"),
        blank=True,
        null=True,
    )

    class Meta:
        verbose_name = _("Asset Task")
        verbose_name_plural = _("Asset Tasks")

    def __str__(self):
        name = self.name if self.name is not None else gettext("unnamed")

        return "Asset [{}] ({})".format(name, self.id)

    @staticmethod
    def get_query_with_numero(name_prefix: str, name_suffix: str):
        return TaskAsset.objects.annotate(
            numero=models.Cast(
                models.Func(
                    models.Func(
                        models.F("name"),
                        models.Value(name_prefix),
                        models.Value(""),
                        function="REGEXP_REPLACE",
                    ),
                    models.Value(name_suffix),
                    models.Value(""),
                    function="REGEXP_REPLACE",
                ),
                models.IntegerField(),
            )
        )
