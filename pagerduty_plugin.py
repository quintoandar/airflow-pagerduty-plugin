from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.exceptions import AirflowException
from airflow.plugins_manager import AirflowPlugin

import logging
import pypd


class PagerDutyIncidentOperator(BaseOperator):
    """
    Creates PagerDuty Incidents.
    """

    @apply_defaults
    def __init__(self,
                 api_key=None,
                 from_email=None,
                 title='unset',
                 service_id='No service id has been set',
                 details='No details have been set',
                 *args,
                 **kwargs):
        super(PagerDutyIncidentOperator, self).__init__(*args, **kwargs)
        self.api_key = api_key
        self.title = title
        self.service_id = service_id
        self.details = details
        self.from_email = from_email

    def execute(self, **kwargs):
        pypd.api_key = self.api_key

        headers = None
        if self.from_email:
            headers = {
                'From': self.from_email
            }

        try:
            r = pypd.Incident.create(data={
                'type': 'incident',
                'title': self.title,
                'service': {
                    'type': 'service_reference',
                    'id': self.service_id,
                },
                'body': {
                    'type': 'incident_body',
                    'details': self.details,
                }
            }, add_headers=headers)
            logging.info(r)
        except Exception as ex:
            msg = "PagerDuty API call failed ({})".format(ex)
            logging.error(msg)
            raise AirflowException(msg)


# Defining the plugin class
class PagerDutyPlugin(AirflowPlugin):
    name = "pagerduty"
    operators = [PagerDutyIncidentOperator]
