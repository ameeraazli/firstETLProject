# firstETLProject

## How to run?
` $ docker-compose up --build ` <br>
The command above will run the project in a container (docker). The container will be exposing port 5432 and will map into the exposed port 5432 of the host to connect to the postgreSQL database.

In the case that localhost port 5432 is being used for other purpose, you may change this in the file `csv_to_postgres.py` line 62, and `docker-compose.yml` line 16 (`host_port_num:container_port_num`).

## What is in the database?
The database consists of a crime_rate data in London, based on the year and borough. The data is extracted from an open source data hosted on [Github](https://github.com/datasets/london-crime).

The python script included had also made an aggregate findings of the lowest, highest, average and median value of the crime rates from the dataset.