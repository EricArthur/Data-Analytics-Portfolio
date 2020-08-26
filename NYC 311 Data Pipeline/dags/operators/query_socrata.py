from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from sodapy import Socrata
import json
import copy


class QuerySocrataOperator(BaseOperator):
        
    @apply_defaults
    def __init__(self,
                 socrata_domain,
                 socrata_dataset_identifier,
                 socrata_token,
                 json_output_filepath,
                 socrata_query_filters=None, # type: Optional[Dict]
                 *args, **kwargs):
        super(QuerySocrataOperator, self).__init__(*args, **kwargs)

        self.socrata_domain = socrata_domain
        self.socrata_dataset_identifier = socrata_dataset_identifier
        self.socrata_token = socrata_token
        self.json_output_filepath = json_output_filepath
        self.socrata_query_filters = socrata_query_filters

    def execute(self, context):
        # Authenticate Socrata client
        self.log.info('Authenticate Socrata client')
        client = Socrata(self.socrata_domain,
                         self.socrata_token)

        rendered_socrata_query_filters = copy.deepcopy(self.socrata_query_filters)
        if rendered_socrata_query_filters is not None:
            for filter, filter_value in self.socrata_query_filters.items():
                if isinstance(filter_value, str):
                    rendered_socrata_query_filters[filter] = filter_value.format(**context)

        # Get JSON results from API endpoint
        self.log.info('Query API')
        results = client.get(self.socrata_dataset_identifier,
                             **rendered_socrata_query_filters)
        self.log.info('Got {} results'.format(len(results)))

        self.log.info('Write JSON to file')
        rendered_json_output_filepath = self.json_output_filepath.format(**context)
        with open(rendered_json_output_filepath, 'w') as outfile:
            json.dump(results, outfile)
        self.log.info('Write JSON to {}'.format(rendered_json_output_filepath))