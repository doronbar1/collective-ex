from dateutil.relativedelta import relativedelta
from django import db
from django.db.models import Q
from django.utils import timezone
from hyke.api.models import (
    ProgressStatus,
    StatusEngine,
)
from hyke.automation.jobs import (
    nps_calculator_onboarding,
    nps_calculator_running,
)
from hyke.email.jobs import send_transactional_email
from hyke.fms.jobs import create_dropbox_folders
from hyke.scheduled.base import next_annualreport_reminder
from hyke.scheduled.service.nps_surveys import (
    schedule_next_running_survey_sequence,
    schedule_onboarding_survey_sequence,
    send_client_onboarding_survey,
)
from structlog import get_logger

logger = get_logger(__name__)

FUNC_MAPPER = {
    "Client Onboarding Survey": _clinet_onboarding_survery,
    "Payment error email": _payment_email_error,
    "Running flow": _running_flow,
    "Kickoff Questionnaire Completed": _create_kick_off,
    "Kickoff Call Scheduled": _create_kick_off,
    "Kickoff Call Cancelled": _create_kick_off,
    "Transition Plan Submitted": _transition_plan_submitted,
    "BK Training Call Scheduled": _bk_training_call_scheduled,
    "BK Training Call Cancelled": _bk_training_call_cancelled,
    "Annual Report Uploaded": _annual_report_uploaded,
    "Calculate NPS Running": nps_calculator_running,
    "Calculate NPS Onboarding": nps_calculator_onboarding,
}



def scheduled_system():
    print("Scheduled task is started for Hyke System...")

    items = StatusEngine.objects.filter(Q(outcome=StatusEngine.SCHEDULED) &
                                        Q(formationtype__startswith="Hyke System") &
                                        Q(processstate=1))

    print("processstate=1 Active items in the job: " + str(len(items)))

    self.runner(items)
    
    items = StatusEngine.objects.filter(Q(outcome=StatusEngine.SCHEDULED) &
                                        Q(formationtype__startswith="Hyke System") &
                                        ~Q(processstate=1))

    print("processstate!=1 Active items in the job: " + str(len(items)))

    self.runner(items)
    
    print("Scheduled task is completed for Hyke System...\n")

    def runner(item):
        for item in items:
            met = FUNC_MAPPER.get(item.process, None)
            if met is None or met is not callable(met):
                print(f'Unknown proces: {item.process}, skipping')
                continue
            met(item)

    def _clinet_onboarding_survery(item):
        try:
            send_client_onboarding_survey(email=item.email)
        except Exception as e:
            logger.exception(f"Can't process Onboarding NPS Survey for status engine id={item.id}")

    def _payment_email_error(item):
        send_transactional_email(email=item.email, template="[Action required] - Please update your payment information")
        print("[Action required] - Please update your payment information email is sent to " + item.email)

    def _running_flow(item):
        updates = {
            "bookkeepingsetupstatus": "completed",
            "taxsetupstatus": "completed2"
        }
        ProgressStatus.objects.filter(email__iexact=item.email).update(**updates)
        
        # in case we want to insert multiple records, bulk_insert will perform better than 2 seperate inserts
        StatusEngine.objects.bulk_create([
            StatusEngine(
                email=item.email,
                process="Schedule Email",
                formationtype=StatusEngine.FORMATION_DAILY,
                data="What's upcoming with Collective?",
                executed=timezone.now() + relativedelta(days=1)
            ),
            StatusEngine(
                email=item.email,
                process="Running flow",
                formationtype=StatusEngine.FORMATION_SYSTEM,
                processstate=2
            )
        ])

        schedule_onboarding_survey_sequence(email=item.email)
        schedule_next_running_survey_sequence(email=item.email)

        create_dropbox_folders(email=item.email)

        print("Dropbox folders are created for " + item.email)

        has_run_before = StatusEngine.objects.filter(
            email=item.email, process=item.process, processstate=item.processstate, outcome=1,
        ).exists()
        if has_run_before:
            print(f"Not creating form w9 or emailing pops because dropbox folders job has already run for {item.email}")
    
    def _annual_report_uploaded(item):
        reportdetails = item.data.split("---")
        if len(reportdetails) < 2:
            print(f'Report details is missing for email: {item.email}, with reportdetails:{reportdetails}')
            return
        reportyear = reportdetails[0].strip()
        reportname = reportdetails[1].strip()
        reportstate = reportdetails[2].strip() if len(reportdetails) == 3 else None

        data_filter = Q(data=f"{reportyear} --- {reportname}")
        if reportstate:
            data_filter |= Q(data=f"{reportyear} --- {reportname} --- {reportstate}")

        updates = {
            "outcome": 1,
            "executed": timezone.now()
        }

        StatusEngine.objects.filter(
            email=item.email,
            process="Annual Report Reminder",
            outcome=-1).filter(data_filter).update(**updates)

        # complete this before we schedule the next reminder
        item.outcome = StatusEngine.COMPLETED
        item.executed = timezone.now()
        item.save()

        next_annualreport_reminder(item.email, reportname, reportstate)

    def _transition_plan_submitted(item):
        ProgressStatus.objects.get(email__iexact=item.email).update(questionnairestatus=ProgressStatus.SUBMITTED)
        StatusEngine.objects.bulk_create([
            StatusEngine(
                email=item.email,
                process=item.process,
                formationtype=StatusEngine.FORMATION_SALESFORCE,
            ),
            StatusEngine(
                email=item.email,
                process="Schedule Email",
                formationtype=StatusEngine.FORMATION_DAILY,
                data="Welcome to the Collective community!",
                executed=timezone.now() + relativedelta(days=1)
            )
        ])

    def _bk_training_call_scheduled(item):
        StatusEngine.objects.create(
            email=item.email,
            formationtype=StatusEngine.FORMATION_SALESFORCE,
            process="BK Training Call Scheduled",
            data=item.data,
        )
    
    def _bk_training_call_cancelled(item):
        ProgressStatus.objects.get(email__iexact=item.email).update(questionnairestatus=ProgressStatus.RESHCEDULE)
        StatusEngine.objects.bulk_insert([
            StatusEngine(
                email=item.email,
                process="Followup - BK Training",
                formationtype=StatusEngine.FORMATION_DAILY,
                executed=timezone.now() + relativedelta(days=2)
            ),
            StatusEngine(
                email=item.email,
                process="BK Training Call Cancelled",
                formationtype=StatusEngine.FORMATION_SALESFORCE
            )
        ])

    def _create_kick_off_completed(item):
        ProgressStatus.objects.get(email__iexact=item.email).update(questionnairestatus=ProgressStatus.SCHEDULED)
        self._create_kick_off(item)

    def _create_kick_off_canceled(item):
        ProgressStatus.objects.get(email__iexact=item.email).update(questionnairestatus=ProgressStatus.RESHCEDULE)
        self._create_kick_off(item)


    def _create_kick_off_scheduled(item):
        ProgressStatus.objects.get(email__iexact=item.email).update(questionnairestatus=ProgressStatus.SCHEDULED)
        self._create_kick_off(item)
    
    def _create_kick_off_completed(item):
        ProgressStatus.objects.get(email__iexact=item.email).update(questionnairestatus=ProgressStatus.SCHEDULED)
        self._create_kick_off(item)

    def _create_kick_off(item):
        _create_se({"email": item.email,
                    "process": item.process,
                    "formationtype": StatusEngine.FORMATION_SALESFORCE,
                    "data": item.data})

    def _create_se(fields):
        StatusEngine.objects.create(**fields)
        

if __name__ == "__main__":
    scheduled_system()
    db.close_old_connections()

