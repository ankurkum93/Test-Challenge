### Introduction
- This repo instructions to build docker image with the Dockerfile and docker-compose which is included.
- This contains headers & items folder in the db folder which has the data that was sent
- This data is being used to create database which is further being copied and 2 new tables are being created which solves the part of the challenge
- The python scripts along with the tests for challemge are in the dags/Scripts folder
- The DAG is directly present in the dags folder
- The Database_creation task creates a database named `test_database`
- Testing_database_creation task checks if the database has been created successfully
- Copying_Tables task creates 2 new tables as required to hold historical data
- Tests checks if the new tables have been created succesfully as required by the challenge
- Queries task is final SQL queries which are required by the challenge, results of the query is pasted in the folder `dags/db/Results/{number_of_the_query}.csv`


## Pre-requisites
- [Docker](https://docs.docker.com/engine/install/)

## Usage
### Clone Repo
- Clone the repo. For example: `git clone https://github.com/ankurkum93/Test-Challenge.git`

### Initial Setup
- Build the Docker image using `docker compose build`

### Deploy Airflow without Custom Image
- Start docker containers: `docker compose up`
- if you are receiving an error spinning up the container, please check the port `8080` and rebuild the image

### Login to Airflow UI
- Go to `http://localhost:8080`
- Login with the username and password. The default username and password are `airflow` and `airflow`, respectively.

### DAG
There is a DAG present called `Creating_database` with 5 diff tasks which represent the challenge that was given to solve

### Tear down deployment
- Destroy Docker container: `docker compose down --volumes --remove-orphans`
- Delete the `./db/airflow.db` & `./db/test_database.db` file.

## Commands
- Build custom Airflow Image: `docker compose build`
- Spin Up Docker Containers: `docker compose up`
- Stop Docker Containers: `docker compose stop`
- Start stopped Docker Containers: `docker compose start`
- Destroy Docker Containers: `docker compose down --volumes --remove-orphans`

## References
- [Running Airflow in Docker](https://airflow.apache.org/docs/apache-airflow/2.8.0/howto/docker-compose/index.html)

