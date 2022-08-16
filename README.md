# API Server

Hi, this is my attempt to create contenarized Rest API service with Flask. Service host data of users which is stored in 02-src-data and then proccesed to be saved into minio bucket and processed_data directory. Service includes directory 02-src-data which is being monitored by main script of app and update processed data every 30 minutes. All changes are overwritten. Service hosts 3 simple endpoints:
- GET /data  
- POST /data  
- GET /stats  
host for web service is http://localhost:8080/xyz?a=0 where xyz is endpoint and ?a=0 is example query  
parameter.   

Program is scanning source directory every 30 minutes to keep track of changes using BackgroundScheduler from apscheduler library  
If any changes happens between periods, old version is overwritten and saved to minio bucket.  
If user enters POST /data endpoint, it forces to scan scource immediately  
  
My biggest problem was to use Docker, everything else was funny to learn and develop myself.   

To run the app, download github repository and run docker compose up in 01-docker-compose directory
