This script generates a list of all stages used in pipelines used in jobs on Legacy Control
Hub (SCH 3.x). Script looks at the last job run for pipeline definitions.

Prerequisites:
 - Python 3.6+; Python 3.9+ preferred

 - StreamSets SDK for Python v3.x
   See: https://docs.streamsets.com/sdk/latest/index.html

 - Legacy Control Hub username/password for a user with Organization Administrator role

 - To avoid including User Credentials in the script, export these two environment variables
   prior to running the script:

        export USER_ID=<your user id>>
        export PASS=<your password>
 - Optional: Filter jobs based on data collector labels. Uncomment lines 37, 38, 57, comment out
   line 58.
