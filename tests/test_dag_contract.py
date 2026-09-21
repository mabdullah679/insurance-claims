"""The DAG module's own logic, tested against the real Airflow 3 types.

WHY THIS FILE EXISTS. Every other test in this suite deliberately avoids
importing Airflow, which is what makes them fast and runnable anywhere. That
left the DAG module itself -- date arithmetic, the future-date guard, failure
introspection -- with no coverage at all, and a shipped defect lived in
exactly that gap: `_failed_tasks` called `dag_run.get_task_instances()`, a
method the Airflow 3 Task SDK's DagRun does not have. It raised AttributeError
on every run, and because `send_notification` is the DAG's only leaf task,
every run ended FAILED while the whole suite stayed green.

So these tests import Airflow and build the REAL
`airflow.sdk.api.datamodels._generated.DagRun` that a task receives at
runtime. They are skipped when Airflow is not installed, so the core suite
still runs without it.
"""
from __future__ import annotations

import datetime
import importlib.util
from datetime import date, timedelta
from pathlib import Path

import pytest

pytest.importorskip("airflow", reason="DAG contract tests require Airflow")

ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture(scope="module")
def dag_module():
    """The DAG module, loaded by path the way the scheduler loads it."""
    spec = importlib.util.spec_from_file_location(
        "insurance_claims_dag", ROOT / "dags" / "insurance_claims_dag.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def sdk_dag_run(run_id="scheduled__2026-09-12T02:00:00-04:00",
                logical=datetime.datetime(2026, 9, 12, 6,
                                          tzinfo=datetime.timezone.utc),
                conf=None):
    """A DagRun of the exact type a running task is handed in Airflow 3."""
    from airflow.sdk.api.datamodels._generated import DagRun, DagRunType
    return DagRun(
        dag_id="daily_insurance_claims_processing", run_id=run_id,
        logical_date=logical, data_interval_start=None, data_interval_end=None,
        run_after=logical, start_date=logical, end_date=None, clear_number=0,
        run_type=DagRunType.SCHEDULED, conf=conf or {}, consumed_asset_events=[])


class TestFailedTaskIntrospection:
    """The regression guard for the notification-task defect.

    `_failed_tasks` must never touch a method the SDK DagRun lacks. These
    tests fail loudly against the old implementation.
    """

    def test_does_not_call_get_task_instances_on_the_dag_run(self, dag_module):
        """The defect, stated as a test.

        The real SDK DagRun has no `get_task_instances`, so if the
        implementation reaches for it this raises AttributeError instead of
        returning a list.
        """
        dr = sdk_dag_run()
        assert not hasattr(dr, "get_task_instances"), (
            "the SDK DagRun grew this method; revisit _failed_tasks")

        class FakeTi:
            @staticmethod
            def get_task_states(dag_id, run_ids=None, **kw):
                return {run_ids[0]: {"publish_to_kafka": "failed",
                                     "validate_claims": "success"}}

        result = dag_module._failed_tasks({"dag_run": dr, "ti": FakeTi()})
        assert result == ["publish_to_kafka"]

    def test_reads_failures_from_the_supported_sdk_call(self, dag_module):
        dr = sdk_dag_run()

        class FakeTi:
            calls: list = []

            @classmethod
            def get_task_states(cls, dag_id, run_ids=None, **kw):
                cls.calls.append((dag_id, tuple(run_ids or ())))
                return {dr.run_id: {"enrich_claims": "failed",
                                    "check_input_file": "success",
                                    "validate_claims": "failed"}}

        failed = dag_module._failed_tasks({"dag_run": dr, "ti": FakeTi()})
        assert failed == ["enrich_claims", "validate_claims"]   # sorted
        assert FakeTi.calls == [("daily_insurance_claims_processing",
                                 (dr.run_id,))]

    def test_the_notifier_itself_is_never_reported_as_failed(self, dag_module):
        """Otherwise every failure alert would name send_notification."""
        dr = sdk_dag_run()

        class FakeTi:
            @staticmethod
            def get_task_states(dag_id, run_ids=None, **kw):
                return {dr.run_id: {"send_notification": "failed",
                                    "archive_input_file": "failed"}}

        assert dag_module._failed_tasks({"dag_run": dr, "ti": FakeTi()}) == [
            "archive_input_file"]

    def test_a_clean_run_reports_nothing_failed(self, dag_module):
        dr = sdk_dag_run()

        class FakeTi:
            @staticmethod
            def get_task_states(dag_id, run_ids=None, **kw):
                return {dr.run_id: {"check_input_file": "success",
                                    "publish_to_kafka": "success"}}

        assert dag_module._failed_tasks({"dag_run": dr, "ti": FakeTi()}) == []

    def test_introspection_failure_does_not_kill_the_notification(self, dag_module):
        """A failed run whose alert also fails is the worst available outcome."""
        dr = sdk_dag_run()

        class BrokenTi:
            @staticmethod
            def get_task_states(dag_id, run_ids=None, **kw):
                raise RuntimeError("supervisor comms unavailable")

        assert dag_module._failed_tasks({"dag_run": dr, "ti": BrokenTi()}) == []

    def test_missing_task_instance_is_handled(self, dag_module):
        assert dag_module._failed_tasks({"dag_run": sdk_dag_run()}) == []

    def test_enum_like_states_are_compared_by_value(self, dag_module):
        """Airflow may hand back a TaskInstanceState rather than a plain str."""
        dr = sdk_dag_run()

        class State:
            def __init__(self, value): self.value = value

        class FakeTi:
            @staticmethod
            def get_task_states(dag_id, run_ids=None, **kw):
                return {dr.run_id: {"publish_to_kafka": State("failed"),
                                    "validate_claims": State("success")}}

        assert dag_module._failed_tasks({"dag_run": dr, "ti": FakeTi()}) == [
            "publish_to_kafka"]


class TestProcessingDate:
    """"Process data from the previous calendar day" and the future guard."""

    def test_the_two_am_run_processes_yesterday(self, dag_module):
        dr = sdk_dag_run()
        assert dag_module.logical_day(
            {"dag_run": dr, "logical_date": dr.logical_date}) == date(2026, 9, 11)

    def test_a_manual_conf_date_wins_over_the_schedule(self, dag_module):
        """Requirement: allow manual reruns for a selected date."""
        dr = sdk_dag_run(conf={"processing_date": "2026-08-01"})
        assert dag_module.logical_day({"dag_run": dr}) == date(2026, 8, 1)

    def test_a_future_date_is_refused(self, dag_module):
        """Requirement: do not process future dates."""
        from claims.common.outcomes import ClaimsPipelineError
        import pendulum

        future = pendulum.now(dag_module.TZ).add(days=3).date()
        dr = sdk_dag_run(conf={"processing_date": future.isoformat()})
        with pytest.raises(ClaimsPipelineError, match="future date"):
            dag_module.logical_day({"dag_run": dr})

    def test_today_is_not_refused(self, dag_module):
        """The guard rejects the future, not the boundary."""
        import pendulum

        today = pendulum.now(dag_module.TZ).date()
        dr = sdk_dag_run(conf={"processing_date": today.isoformat()})
        assert dag_module.logical_day({"dag_run": dr}) == today

    def test_a_run_with_no_logical_date_still_resolves(self, dag_module):
        """The UI trigger path supplies no logical_date; it must not crash."""
        assert isinstance(dag_module.logical_day({"dag_run": sdk_dag_run()}), date)


class TestGraphShape:
    """The requirements' ordering promises, asserted on the parsed DAG."""

    def test_archive_is_gated_behind_reconciliation(self, dag_module):
        """"The input file is archived only after required work succeeds.\""""
        archive = dag_module.dag.get_task("archive_input_file")
        assert "reconcile_run" in archive.upstream_task_ids

    def test_archive_never_depends_on_publish_alone(self, dag_module):
        archive = dag_module.dag.get_task("archive_input_file")
        assert "publish_to_kafka" not in archive.upstream_task_ids

    def test_rejections_are_filed_independently_of_publishing(self, dag_module):
        """A broker outage must not also lose the data-quality record."""
        store = dag_module.dag.get_task("store_rejected_claims")
        assert "publish_to_kafka" not in store.upstream_task_ids
        assert store.trigger_rule == "all_done"

    def test_the_notifier_runs_on_failure_too(self, dag_module):
        notify = dag_module.dag.get_task("send_notification")
        assert notify.trigger_rule == "all_done"

    def test_publish_owns_its_retry_budget(self, dag_module):
        """Airflow retries would re-send the whole batch and double-count."""
        assert dag_module.dag.get_task("publish_to_kafka").retries == 0

    def test_only_one_run_at_a_time(self, dag_module):
        assert dag_module.dag.max_active_runs == 1

    def test_schedule_is_two_am_eastern(self, dag_module):
        assert dag_module.dag.schedule == "0 2 * * *"
        assert str(dag_module.dag.timezone) == "America/New_York"


class TestOperationalWindow:
    """Section 5: "the DAG exceeds its operational completion window.\""""

    def test_the_dag_has_a_run_timeout(self, dag_module):
        """Configured AND enforced -- sla_minutes used to be read by nothing."""
        assert dag_module.dag.dagrun_timeout is not None

    def test_the_timeout_matches_the_configured_sla(self, dag_module):
        from claims.common.config import AlertConfig
        assert dag_module.dag.dagrun_timeout == timedelta(
            minutes=AlertConfig().sla_minutes)


class TestFailureCapture:
    """Section 5 wants an error summary and retry status in the alert."""

    def test_the_callback_is_attached_to_every_task(self, dag_module):
        for task in dag_module.dag.tasks:
            assert task.on_failure_callback, (
                f"{task.task_id} would fail without recording its error")

    def test_the_callback_records_the_error_and_retry_status(self, dag_module,
                                                             tmp_path, monkeypatch):
        from claims.common.staging import Staging

        monkeypatch.setenv("CLAIMS_DATA_ROOT", str(tmp_path))

        class Ti:
            task_id = "publish_to_kafka"
            try_number = 3
            max_tries = 2

        dag_module.record_failure({
            "dag_run": sdk_dag_run(conf={"processing_date": "2026-09-11"}),
            "task_instance": Ti(),
            "exception": RuntimeError("broker unreachable")})

        recorded = Staging(tmp_path, date(2026, 9, 11)).read_metrics("error")
        assert recorded["message"] == "broker unreachable"
        assert recorded["task_id"] == "publish_to_kafka"
        assert recorded["try_number"] == 3
        assert recorded["max_tries"] == 2

    def test_the_callback_never_raises(self, dag_module):
        """It runs while a task is already failing; raising would mask the cause."""
        dag_module.record_failure({})           # must not raise
