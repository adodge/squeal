# squeal: SQL-Backed Message Queue

A python library implementing a message queue using a relational database as the storage backend.

**Note**: This is an alpha version.  The interface is unstable.  Feel free to try it out, though, and let me know what you think.

## Why?

`squeal` offers a lightweight implementation of a message queue, using a backend that you probably already have as part of your infrastructure.  The basic functionality is exposed by the `squeal.Queue` object:

* `create` and `destroy` the required database tables
* `put` and `get` messages from a queue
* a message payload is just a binary blob
* messages have a priority, where higher-priority messages are retrieved first
* consumers can `ack` or `nack` messages to indicate success or failure
* if a consumer acquires a message but doesn't `ack` it, it will eventually be redelivered to another consumer
* a message that is `nack`ed will be put back in the queue with an exponential backoff delay
* a `Queue` object represents multiple logical queues, indicated by a message `topic`
* topics are dynamic: they only exist as long as there's a message with that topic
* a `Queue` can query for existing topics or the number of messages waiting in any particular topic

`Queue` objects delegate to a `Backend` object that implements database-specific methods.  The only backend is currently the `MySQLBackend`, which wraps a `Connection` from a mysql library, like `pymysql`.

## What database backends are supported?

Currently, the only backend that has been tested is:

* [`pymysql`](https://github.com/PyMySQL/PyMySQL) with `mysql 8.1.0`

But theoretically other database libraries can be used, as long as they implement [PEP 249 (Python Database API Specification)](https://peps.python.org/pep-0249/).  Other database engines can probably be supported with minimal effort by changing the dialect of SQL that's generated.  (That is, creating a new subclass of `Backend`)

# Examples
Check the `examples/` directory.

# API
(Coming soon)

# Contributing

## To-Do
* dead letter queue for messages that fail repeatedly
* raise some better exceptions if we get an expected error from the SQL library (table doesn't exist, etc)
* do some benchmarking and add indices
* refactor tests so all backends are compared against the same expectations

Please feel free to submit an issue to the github for bugs, comments, or feature requests.  Also feel free to fork and make a PR.

## Formatting
Please use `black` to format your code.

## Running tests
Install the dev requirements in a virtual env:
```python3
python3 -m venv venv
. venv/bin/activate
pip install -r requirements.txt
```

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
pytest tests
```
