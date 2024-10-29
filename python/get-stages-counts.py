#!/usr/bin/env python

'''
This script generates a list of all stages included in pipelines used in jobs on Legacy Control
Hub (SCH 3.x). Script looks only at the last job run for pipeline definitions/stage names.

Prerequisites:
 - Python 3.6+; Python 3.9+ preferred

 - StreamSets SDK for Python v3.x
   See: https://docs.streamsets.com/sdk/latest/index.html

 - Legacy Control Hub username/password for a user with Organization Administrator role

 - To avoid including User Credentials in the script, export these two environment variables
   prior to running the script:

        export USER_ID=<your user id>>
        export PASS=<your password>

 - Set DataCollector LABEL to evaluate jobs on matching DataCollector labels.

'''

from streamsets.sdk import ControlHub
import os,json

# Get user_id from environment
USER_ID = os.getenv('USER_ID')

# Get password from the environment
PASS = os.getenv('PASS')

# Control Hub URL, e.g. https://cloud.streamsets.com
SCH_URL = 'https://cloud.streamsets.com'

# LABEL for jobs to target
LABEL = 'daveh'

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
#jobs = [job for job in sch.jobs if job.data_collector_labels == LABEL]
jobs = [job for job in sch.jobs]

# Get Pipeline IDs for matched jobs
pipelines = []
for job in jobs:
   if LABEL in job.data_collector_labels:
       pipelines.append(job.pipeline_id)
print(f'Found {len(pipelines)} pipelines with Data Collector labels: {LABEL}')

for i in pipelines:
    j = sch.pipelines.get_all(commit_id=job.commit_id)[0]
    pipeline_def = j.pipeline_definition
    pipeline_definition_json = json.loads(pipeline_def)
    for stage in pipeline_definition_json['stages']:
        print(f'{stage["instanceName"]}')
