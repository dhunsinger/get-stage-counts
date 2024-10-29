from streamsets.sdk import ControlHub
import os,json

# Get user_id from environment
USER_ID = os.getenv('USER_ID')

# Get password from the environment
PASS = os.getenv('PASS')

# Control Hub URL, e.g. https://cloud.streamsets.com
#SCH_URL = 'https://cloud.streamsets.com'
SCH_URL = 'https://trailer.streamsetscloud.com'

# LABEL for jobs to target
#LABEL = 'natwest'

#Connect to Control Hub
sch = ControlHub(
    SCH_URL,
    username=USER_ID,
    password=PASS
    )

# print header method
def print_header(header):
    divider = 60 * '-'
    print('\n' + divider)
    print(header)
    print(divider)

print_header(f'Rertieving jobs from \"{SCH_URL}\"')

#Get list of jobs with correct label
# jobs = [job for job in sch.jobs if job.data_collector_labels == LABEL]
jobs = [job for job in sch.jobs]

# Get Pipeline IDs for matched jobs
pipelines = []
for job in jobs:
    if LABEL in job.data_collector_labels:
        pipelines.append(job.pipeline_id)

for i in pipelines:
    j = sch.pipelines.get_all(commit_id=job.commit_id)[0]
    pipeline_def = j.pipeline_definition
    pipeline_definition_json = json.loads(pipeline_def)
    for stage in pipeline_definition_json['stages']:
        print(f'{stage["instanceName"]}')
