**Welcome to the Data Pipelines with Airflow project!**

Project template was already provided so , as a Data engineer I have made necessary changes to schedule the DAG every hour and retry 3 times.

**Redshift Serverless creation**

Created Redshift serverless workgroup and related IAM user and role using AWS UI

<img width="1280" alt="redshift" src="https://github.com/user-attachments/assets/282a5ac4-9901-45b5-964b-71e40b1aa840" />


**Initiating the Airflow Web Server**

Started airflow services using start-services.sh and start.sh
Created admin user and started scheduler
Created awsuser and redshift connection using Airflow UI.

<img width="1280" alt="user_conn" src="https://github.com/user-attachments/assets/015de6b9-03f2-498b-b50b-56c816012938" />
<img width="1280" alt="redshift_conn" src="https://github.com/user-attachments/assets/39967928-d7ce-41c2-8965-fa9a5c6c9e14" />

**Executing the DAG**
<img width="1280" alt="graph_final_project" src="https://github.com/user-attachments/assets/4925922d-4c0a-4cf7-8375-c694dab369be" />

I have written code to create table and create task dependencies in DAG templated
updated code to upload s3 data to redshift for staging_events, staging_songs, songplays, songs, artists, times, users
Updated code for data quality check.


DAG was executed successfully and loaded all data and quality check was also done.

I have used only the following data for faster execution

* log_data/2018/11
* song_data/A/A
