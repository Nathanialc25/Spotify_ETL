# Data Engineering Project-1 | 03.03.2024 | Spotify ELT pipeline

## Brief Description
 The idea of this project is to combine Data Engineering practices with one of my passions, Music.
 In this project I used Spotipy, which is a lightweight python library built to access Spotify's API. I used Pandas to create some dataframes containing information about the top 10 songs of the a given day (Found here -> https://open.spotify.com/playlist/37i9dQZF1DXcBWIGoYBM5M), as well as information about the artists who made each song. I then loaded the data into a Postrgess database. This was all automated using Airflow, that was stood up on Docker. I automated this to kick off every sunday at 9 AM, and the data will stand in for top charting songs of the week, and top artist of the week. This project establishes the data flow necessary for a downstream upcoming project where I can use this data and create fun analysis to answer questions like the following:

 - How many weeks has a song/artist stayed in the top 10?
 - what genre of artist is usually in the top 10 on a week by week basis?
 - Who has the most songs that reached the top 10?

And many others! All will be addressed when I began the follow up project to this, after enough data is collected!
 
## Skills exemplified in this project
 1. Python (Pandas, Error Logging, os)
 2. Docker (Use of Docker Desktop, Docker Hub, adding layers to base images, building multicontainer environment with Docker-compose)
 3. Airflow (Creating dags, exposure to the Airflow UI, Python Operators, and Potgress Operators)
 4. Postrgess (Using DDL languagee to create tables, establishing a database and configuring it with Airflow's Connections)
 5. APIs (Creating secure credentials to allow my work to have access to Spotify's information)



## Steps involved (Will flesh out at later date)
1. Setting up Docker

2. Setting up Airflow scripts, Postgress and DAG

3. Running Docker from CLI

4. Working in Airflow UI on port 8080

## Final view of DAG running well

<img width="1502" alt="Screenshot 2024-03-03 at 7 30 20 PM" src="https://github.com/Nathanialc25/Spotify_ETL/assets/78894588/9fea4b6f-8db2-43a6-96a2-8f7f361951b4">
