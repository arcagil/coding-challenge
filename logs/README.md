# Log Processing and API Service

This project processes API request logs and provides statistics through an API.

## How to run
```
docker compose up
```

and then to interact with the API:
```
curl localhost:8000/customers/cust_1/stats
```

To inspect the api_requests.log file: it will be generated in the root of this directory.

## Run tests
```
cd app && python3 -m tests.test_log_processor
```

## Project Structure

app/ contains the main application code:

- api.py is the REST API using FastAPI
- log_processor.py is the Bytewax dataflow used to process logs and save to database
- models.py contains the database models (SQLAlchemy ORM)
- prepare_db.py is used to prepare the database (wait for it to start and run migrations)
- tests/ contains the tests for the log processor


docker-compose.yml is used to run the application in a container.

alembic/ contains the database migrations.

generator.py is used to generate logs (as given in the problem statement).

## Libraries used

Bytewax, FastAPI, SQLAlchemy, Alembic, Numpy.

Check pyproject.toml for more details.
