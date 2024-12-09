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
import os, json
from collections import Counter
import csv
from datetime import datetime
import sys

# Get user_id from environment
USER_ID = os.getenv('USER_ID')

# Get password from the environment
PASS = os.getenv('PASS')

# Control Hub URL, e.g. https://cloud.streamsets.com
#SCH_URL = 'https://cloud.streamsets.com'
SCH_URL = 'https://trailer.streamsetscloud.com'

# LABEL for jobs to target
LABEL = 'daveh'

# Generate a timestamp for file output tagging
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
# print header method
def print_header(header):
    divider = 60 * '-'
    print('\n' + divider)
    print(header)
    print(divider)

print_header('Connecting...')
#Connect to Control Hub
sch = None
try:
    sch = ControlHub(
        SCH_URL,
        username=USER_ID,
        password=PASS)
except Exception as e:
    print('Error connecting to Control Hub')
    print(str(e))
    sys.exit(1)

print_header(f'Connected to Control Hub at \"{SCH_URL}\"')

print_header(f'Rertieving jobs...')

#Get list of jobs with correct label
# jobs = [job for job in sch.jobs if job.data_collector_labels == LABEL]
jobs = [job for job in sch.jobs]

pipelines = []
for job in jobs:
    if LABEL in job.data_collector_labels:
        pipelines.append(job.pipeline_id)
print_header(f'Found {len(pipelines)} pipelines with Data Collector labels: {LABEL}')

stages = []
stage_frequency = Counter()
for i in pipelines:
    p = sch.pipelines.get(pipeline_id=i)
    pipeline_def = p.pipeline_definition
    pipeline_definition_json = json.loads(pipeline_def)
    # print(f'{pipeline_definition_json}')

    for stage in pipeline_definition_json['stages']:
        # print(f'{stage["instanceName"]}')
        stages.append(stage["stageName"])

    # Count occurrences of instanceNames
    stage_frequency.update(([stage for stage in stages]))

# Output the stage frequency as a dictionary
stage_frequency_dict = dict(stage_frequency)

# Deliver the result
sorted_stages = sorted(stage_frequency.items(), key=lambda item: item[1], reverse=True)

# Ask user for output choice
print("Choose an option for the output:")
print("1. Print to stdout")
print("2. Save to JSON file")
print("3. Save to CSV file")
print("4. Exit")
choice = input("Enter your choice (1/2/3/4): ").strip()

if choice == "1":
    # Print sorted stages to stdout
    print("Sorted Stages:")
    for stage, count in sorted_stages:
        print(f"{stage}: {count}")

elif choice == "2":
    # Save sorted stages to JSON
    output_file = f"stages_{label}_{timestamp}.json"
    with open(output_file, "w") as json_file:
        json.dump(dict(sorted_stages), json_file, indent=4)
    print(f"Sorted stages saved to {output_file}")

elif choice == "3":
    # Save sorted stages to CSV
    output_file = f"stages_{label}_{timestamp}.csv"
    with open(output_file, "w", newline="") as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(["Stage", "Count"])  # Write header
        writer.writerows(sorted_stages)  # Write data
    print(f"Sorted stages saved to {output_file}")
elif choice == "4":
    exit(0)

else:
    print("Invalid choice. Please choose 1, 2, 3, 4 or ctrl+c to exit.")
