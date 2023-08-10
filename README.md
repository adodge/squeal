# `squeal`: SQL-Backed Message Queue

A library implementing a message queue using a relational database as the storage backend.

**Note**: This is an alpha version.  The interface is unstable.  Feel free to try it out, but be sure to pin the version, and keep an eye out for changes here.

## Why `squeal`?

`squeal` is targeting a scenario where you already have a database set up, and you just need a relatively light message queue.  It hasn't been benchmarked, but relational databases are pretty good actually, and you might be able to achieve a surprising amount of volume using this approach.  If you're already paying to run a database, this might be cheaper than paying to run an additional message queue service, assuming you don't have to upgrade your database to meet the additional load.

## Why not `squeal`?

You might not need a message queue at all.  _Don't queue it, just do it._  In a world where you can spin up as many compute resources as you want, on demand, and pay by the second, executing a bunch of work in parallel could be almost exactly the same price as queuing it up and execuring it in serial.

If you are doing some heavy or complex message passing, this is unlikely to be a better option than a dedicated message queue, like Kafka, RabbitMQ, AWS SQS, etc.

## What database backends are supported?

Currently, the only backend that has been tested is:

* [`pymysql`](https://github.com/PyMySQL/PyMySQL) with `mysql 8.1.0`

But theoretically other database libraries can be used, as long as they implement [PEP 249 (Python Database API Specification)](https://peps.python.org/pep-0249/).  Other database engines can probably be supported with minimal effort by changing the dialect of SQL that's generated.  (That is, creating a new subclass of `Backend`)

# Examples
(Coming soon)

# API
(Coming soon)

# TODO
* [ ] keep track of failure count for each message and implement exponential backoff
* [ ] ttl for messages
* [ ] a dead letter queue for messages that can't be delivered
* [ ] message priority (maybe just implemented as a wrapper around multiple topics)
* [ ] `.getmany` method to get a batch of messages at a time, possibly from multiple topics
* [ ] raise some better exceptions if we get an expected error from the SQL library (table doesn't exist, etc)
* [ ] Do some benchmarking and add indices
* [ ] Refactor tests so the same set of tests are run against all backends

# Contributing

Please feel free to submit an issue to the github for bugs, comments, or feature requests.  Also feel free to fork and make a PR.

## Formatting

Please use `black` to format your code.

## Running tests

The tests assume you have a mysql instance running locally.  The connection can be adjusted with envvars, but the defaults are:

```python3
SQUEAL_TEST_HOSTNAME = os.environ.get("SQUEAL_TEST_HOSTNAME", "localhost")
SQUEAL_TEST_PORT     = os.environ.get("SQUEAL_TEST_PORT", "3306")
SQUEAL_TEST_USERNAME = os.environ.get("SQUEAL_TEST_USERNAME", "root")
SQUEAL_TEST_PASSWORD = os.environ.get("SQUEAL_TEST_PASSWORD", "password")
SQUEAL_TEST_DATABASE = os.environ.get("SQUEAL_TEST_DATABASE", "test")
```

The easiest way to get this running is to just use docker:

```bash
docker run --name mysql -e MYSQL_ROOT_PASSWORD=password -d -p 3306:3306 mysql:8.1.0
```

Then the tests can be run with `pytest`:

```bash
python3 -m venv venv
. venv/bin/activate
pip install -r requirements.txt
pytest tests
```
