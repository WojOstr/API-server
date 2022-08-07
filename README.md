# ProvectusInternship_WojciechOstrowski

Hi, this is my approach on this task. I'm sorry about uploading docker container on github,  
but I'm not familiar with it and I coulnd't figure it out. It is fulyl written in Python,  
App is using Flask to host simple service with given endpoints:  
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
