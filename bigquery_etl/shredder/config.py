#!/usr/bin/env python3

"""Meta data about tables and ids for self serve deletion."""

from dataclasses import dataclass
from functools import partial
from itertools import chain
from typing import Optional, Tuple


SHARED_PROD = "moz-fx-data-shared-prod"


@dataclass(frozen=True)
class DeleteSource:
    """Data class for deletion request source."""

    table: str
    field: str
    project: str = SHARED_PROD

    @property
    def sql_table_id(self):
        """Fully qualified table id in standard sql format."""
        return f"{self.project}.{self.table}"


@dataclass(frozen=True)
class DeleteTarget:
    """Data class for deletion request target."""

    table: str
    field: str
    cluster_conditions: Optional[Tuple[Tuple[str, bool]]] = None
    project: str = SHARED_PROD

    @property
    def sql_table_id(self):
        """Fully qualified table id in standard sql format."""
        return f"{self.project}.{self.table}"


CLIENT_ID = "client_id"
GLEAN_CLIENT_ID = "client_info.client_id"
IMPRESSION_ID = "impression_id"
USER_ID = "user_id"
POCKET_ID = "pocket_id"
SHIELD_ID = "shield_id"
ECOSYSTEM_CLIENT_ID = "payload.ecosystem_client_id"
PIONEER_ID = "payload.pioneer_id"
ID = "id"

CLIENT_ID_SRC = DeleteSource(
    table="telemetry_stable.deletion_request_v4", field=CLIENT_ID
)
IMPRESSION_ID_SRC = DeleteSource(
    table="telemetry_stable.deletion_request_v4",
    field="payload.scalars.parent.deletion_request_impression_id",
)
SOURCES = [CLIENT_ID_SRC, IMPRESSION_ID_SRC]

client_id_target = partial(DeleteTarget, field=CLIENT_ID)
glean_target = partial(DeleteTarget, field=GLEAN_CLIENT_ID)
id_target = partial(DeleteTarget, field=ID)
impression_id_target = partial(DeleteTarget, field=IMPRESSION_ID)

DELETE_TARGETS = {
    client_id_target(
        table="search_derived.mobile_search_clients_daily_v1"
    ): CLIENT_ID_SRC,
    client_id_target(table="search_derived.search_clients_daily_v8"): CLIENT_ID_SRC,
    client_id_target(table="search_derived.search_clients_last_seen_v1"): CLIENT_ID_SRC,
    client_id_target(table="telemetry_derived.attitudes_daily_v1"): CLIENT_ID_SRC,
    client_id_target(
        table="telemetry_derived.clients_daily_histogram_aggregates_v1"
    ): CLIENT_ID_SRC,
    client_id_target(
        table="telemetry_derived.clients_daily_scalar_aggregates_v1"
    ): CLIENT_ID_SRC,
    client_id_target(table="telemetry_derived.clients_daily_v6"): CLIENT_ID_SRC,
    client_id_target(table="telemetry_derived.clients_last_seen_v1"): CLIENT_ID_SRC,
    client_id_target(
        table="telemetry_derived.clients_profile_per_install_affected_v1"
    ): CLIENT_ID_SRC,
    client_id_target(table="telemetry_derived.core_clients_daily_v1"): CLIENT_ID_SRC,
    client_id_target(
        table="telemetry_derived.core_clients_last_seen_v1"
    ): CLIENT_ID_SRC,
    client_id_target(table="telemetry_derived.event_events_v1"): CLIENT_ID_SRC,
    client_id_target(table="telemetry_derived.experiments_v1"): CLIENT_ID_SRC,
    client_id_target(table="telemetry_derived.main_events_v1"): CLIENT_ID_SRC,
    client_id_target(table="telemetry_derived.main_summary_v4"): CLIENT_ID_SRC,
    client_id_target(table="telemetry_stable.block_autoplay_v1"): CLIENT_ID_SRC,
    client_id_target(table="telemetry_stable.crash_v4"): CLIENT_ID_SRC,
    client_id_target(table="telemetry_stable.downgrade_v4"): CLIENT_ID_SRC,
    client_id_target(table="telemetry_stable.event_v4"): CLIENT_ID_SRC,
    client_id_target(table="telemetry_stable.first_shutdown_v4"): CLIENT_ID_SRC,
    client_id_target(table="telemetry_stable.focus_event_v1"): CLIENT_ID_SRC,
    client_id_target(table="telemetry_stable.frecency_update_v4"): CLIENT_ID_SRC,
    client_id_target(table="telemetry_stable.health_v4"): CLIENT_ID_SRC,
    client_id_target(table="telemetry_stable.heartbeat_v4"): CLIENT_ID_SRC,
    client_id_target(
        table="telemetry_stable.main_v4",
        cluster_conditions=tuple(
            (condition, needs_clustering)
            for condition, needs_clustering in chain(
                {
                    f"sample_id = {sample_id} AND normalized_channel = 'release'": False
                    for sample_id in range(100)
                }.items(),
                [("normalized_channel != 'release'", True)],
            )
        ),
    ): CLIENT_ID_SRC,
    client_id_target(table="telemetry_stable.modules_v4"): CLIENT_ID_SRC,
    client_id_target(table="telemetry_stable.new_profile_v4"): CLIENT_ID_SRC,
    client_id_target(table="telemetry_stable.saved_session_v4"): CLIENT_ID_SRC,
    client_id_target(table="telemetry_stable.shield_icq_v1_v4"): CLIENT_ID_SRC,
    client_id_target(table="telemetry_stable.shield_study_addon_v3"): CLIENT_ID_SRC,
    client_id_target(table="telemetry_stable.shield_study_error_v3"): CLIENT_ID_SRC,
    client_id_target(table="telemetry_stable.shield_study_v3"): CLIENT_ID_SRC,
    client_id_target(table="telemetry_stable.testpilot_v4"): CLIENT_ID_SRC,
    client_id_target(table="telemetry_stable.third_party_modules_v4"): CLIENT_ID_SRC,
    client_id_target(table="telemetry_stable.untrusted_modules_v4"): CLIENT_ID_SRC,
    client_id_target(table="telemetry_stable.update_v4"): CLIENT_ID_SRC,
    client_id_target(table="telemetry_stable.voice_v4"): CLIENT_ID_SRC,
}

UNSUPPORTED = [
    # activity stream
    impression_id_target(table="activity_stream_stable.impression_stats_v1"),
    impression_id_target(table="activity_stream_stable.spoc_fills_v1"),
    impression_id_target(table="messaging_system_stable.undesired_events_v1"),
    # TODO check that these do not contain data from clients past 30 days
    client_id_target(table="telemetry_derived.clients_histogram_aggregates_v1"),
    client_id_target(table="telemetry_derived.clients_scalar_aggregates_v1"),
    # TODO these should probably be deleted
    client_id_target(table="telemetry_derived.test3"),
    client_id_target(table="telemetry_derived.test4"),
    client_id_target(table="telemetry_derived.test_daily_original"),
    client_id_target(table="telemetry_derived.test_histogram_aggregates"),
    client_id_target(table="telemetry_derived.test_histogram_daily"),
    client_id_target(table="telemetry_derived.test_scalar_aggregates"),
    client_id_target(table="telemetry_derived.test_scalars_daily"),
    client_id_target(table="search_derived.search_clients_last_seen_v101*"),
    # pocket
    DeleteTarget(table="pocket_stable.fire_tv_events_v1", field=POCKET_ID),
    # fxa
    DeleteTarget(table="fxa_users_services_daily_v1", field=USER_ID),
    DeleteTarget(table="fxa_users_services_first_seen_v1", field=USER_ID),
    DeleteTarget(table="fxa_users_services_last_seen_v1", field=USER_ID),
    DeleteTarget(table="telemetry_derived.devtools_events_amplitude_v1", field=USER_ID),
    # mobile
    client_id_target(table="mobile_stable.activation_v1"),
    client_id_target(table="telemetry_stable.core_v1"),
    client_id_target(table="telemetry_stable.core_v2"),
    client_id_target(table="telemetry_stable.core_v3"),
    client_id_target(table="telemetry_stable.core_v4"),
    client_id_target(table="telemetry_stable.core_v5"),
    client_id_target(table="telemetry_stable.core_v6"),
    client_id_target(table="telemetry_stable.core_v7"),
    client_id_target(table="telemetry_stable.core_v8"),
    client_id_target(table="telemetry_stable.core_v9"),
    client_id_target(table="telemetry_stable.core_v10"),
    client_id_target(table="telemetry_stable.mobile_event_v1"),
    client_id_target(table="telemetry_stable.mobile_metrics_v1"),
    # internal
    client_id_target(table="activity_stream_stable.events_v1"),
    client_id_target(table="eng_workflow_stable.build_v1"),
    client_id_target(table="messaging_system_stable.cfr_v1"),
    client_id_target(table="messaging_system_stable.onboarding_v1"),
    client_id_target(table="messaging_system_stable.snippets_v1"),
    # glean
    glean_target(table="org_mozilla_fenix_stable.activation_v1"),
    glean_target(table="org_mozilla_fenix_stable.baseline_v1"),
    glean_target(table="org_mozilla_fenix_stable.bookmarks_sync_v1"),
    glean_target(table="org_mozilla_fenix_stable.events_v1"),
    glean_target(table="org_mozilla_fenix_stable.history_sync_v1"),
    glean_target(table="org_mozilla_fenix_stable.logins_sync_v1"),
    glean_target(table="org_mozilla_fenix_stable.metrics_v1"),
    client_id_target(table="org_mozilla_fenix_derived.clients_daily_v1"),
    client_id_target(table="org_mozilla_fenix_derived.clients_last_seen_v1"),
    glean_target(table="org_mozilla_fenix_nightly_stable.activation_v1"),
    glean_target(table="org_mozilla_fenix_nightly_stable.baseline_v1"),
    glean_target(table="org_mozilla_fenix_nightly_stable.bookmarks_sync_v1"),
    glean_target(table="org_mozilla_fenix_nightly_stable.events_v1"),
    glean_target(table="org_mozilla_fenix_nightly_stable.history_sync_v1"),
    glean_target(table="org_mozilla_fenix_nightly_stable.logins_sync_v1"),
    glean_target(table="org_mozilla_fenix_nightly_stable.metrics_v1"),
    glean_target(table="org_mozilla_reference_browser_stable.baseline_v1"),
    glean_target(table="org_mozilla_reference_browser_stable.events_v1"),
    glean_target(table="org_mozilla_reference_browser_stable.metrics_v1"),
    glean_target(table="org_mozilla_tv_firefox_stable.baseline_v1"),
    glean_target(table="org_mozilla_tv_firefox_stable.events_v1"),
    glean_target(table="org_mozilla_tv_firefox_stable.metrics_v1"),
    glean_target(table="org_mozilla_vrbrowser_stable.baseline_v1"),
    glean_target(table="org_mozilla_vrbrowser_stable.bookmarks_sync_v1"),
    glean_target(table="org_mozilla_vrbrowser_stable.events_v1"),
    glean_target(table="org_mozilla_vrbrowser_stable.history_sync_v1"),
    glean_target(table="org_mozilla_vrbrowser_stable.logins_sync_v1"),
    glean_target(table="org_mozilla_vrbrowser_stable.metrics_v1"),
    glean_target(table="org_mozilla_vrbrowser_stable.session_end_v1"),
    # other
    DeleteTarget(table="telemetry_stable.pioneer_study_v4", field=PIONEER_ID),
    DeleteTarget(table="telemetry_stable.pre_account_v4", field=ECOSYSTEM_CLIENT_ID),
    # TODO evaluate whether these are actually user ids
    DeleteTarget(
        table="telemetry_derived.survey_gizmo_daily_attitudes", field=SHIELD_ID
    ),
    id_target(table="firefox_launcher_process_stable.launcher_process_failure_v1"),
    id_target(table="telemetry_derived.origin_content_blocking"),
    id_target(table="telemetry_stable.anonymous_v4"),
    id_target(table="telemetry_stable.optout_v4"),
    id_target(table="telemetry_stable.pre_account_v4"),
    id_target(table="telemetry_stable.prio_v4"),
    id_target(table="telemetry_stable.sync_v4"),
    id_target(table="telemetry_stable.sync_v5"),
]
