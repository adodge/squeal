# `squeal`: SQL-Backed Message Queue

Create and use a message queue using an RDB as the storage backend.  

## Why `squeal`?

If you are doing a high-volume of message passing, this is unlikely to be a better option than a dedicated message queue, like Kafka, RabbitMQ, AWS SQS, etc.  `squeal` is targeting a scenario where you already have an RDB set up, and you just need a relatively light message queue.  It hasn't been benchmarked, but relational databases are pretty good actually, and you might be able to achieve a surprising amount of volume using this approach.

If you're already paying to run a database, this will probably be cheaper than paying to run an additional message queue service, assuming you don't have to upgrade your database to meet the additional load.

## Why not `squeal`?

Maybe you don't need a message queue at all.  _Don't queue it, just do it._  In a world where you can spin up as many compute resources as you want, on demand, and pay by the second, executing a bunch of work in parallel could be almost exactly the same price as queuing it up and execuring it in serial.

Or maybe it's not a big deal to set up something like Kafka or use SQS.  In which case, go for it.

## What database backends are supported?

Currently, the only backend that has been tested is:

* [`pymysql`](https://github.com/PyMySQL/PyMySQL) with `mysql 8.1.0`

But theoretically other database libraries can be used, as long as they implement [PEP 249 (Python Database API Specification)](https://peps.python.org/pep-0249/).  Other database engines can probably be supported with minimal effort by changing the dialect of SQL that's generated.

# Examples

## Producer
```python3
import datetime
import pymysql
import squeal
import time

conn = pymysql.connect(
    host='localhost',
    port=3306,
    user='root',
    password='password',
    database='test',
)

queue = squeal.Queue(
    squeal.MySQLBackend,
    connection=conn,
    prefix="squeal",
    acquire_timeout=60,
)

print("Creating queue")
queue.create()

try:
    while True:
        msg = datetime.datetime.now().isoformat().encode('utf-8')
        queue.put(msg, topic=1)
        print("Putting:", msg)
        time.sleep(0.1)

except KeyboardInterrupt:
    print("Destroying queue")
    queue.destroy()
```

## Consumer
```python3
import pymysql
import squeal

conn = pymysql.connect(
    host='localhost',
    port=3306,
    user='root',
    password='password',
    database='test',
)

queue = squeal.Queue(
    squeal.MySQLBackend,
    connection=conn,
    prefix="squeal",
    acquire_timeout=60,
)

while True:
    msg = queue.get(topic=1)
    print("Processed:", msg.payload)
    msg.ack()
```

# API Overview
## `squeal.Queue`
Note that there's no difference between a producer and consumer `squeal.Queue` object.  The `Queue` is constructed with a backend object (currently only `MySQLBacked` exists) and a connection argument.  `prefix` is used to name the table that will be used.  `acquire_timeout` is the number of seconds before a message will get set to another consumer.  Note that this is part of the consumer definition and it's up to the consumer to decide whether to release an existing message owned by someone else, so weird things might happen if different consumers put different values here.

### `q.create()`
Initialize the queue in the database, if it doens't already exist (this is safe for all clients to call before putting or getting)

### `q.destroy()`
Destroy the queue in the database, if it exists (probably only want to call this once all producers and consumers are exiting and all tasks are done)

### `q.put(payload: bytes, topic: int)`
Put a message into the queue

### `q.get(topic: int) -> Message`
Get a message from the queue.  By default, polls every second and never times out.  This can be changed by passing `default_timeout` and `default_poll_interval` values to the Queue constructor, or by passing `timeout` and `poll_interval` to the `get()` call.  Raises a `QueueEmpty` exception if the timeout is reached.

### `q.get_nowait() -> Message`
Equivalent to calling `.get()` with `timeout=0`

### `q.size(topic: int)`
Returns the number of available messages in the given topic

### `q.topics()`
Returns a list of tuples like `(topic, # of messages)`

## `squeal.MonoQueue`
This is a subclass of `Queue` where the topic is picked at construction time (or defaulting to zero.)  This is for your convenience if you don't intend to use multiple topics.  For example:

```python3
q = MonoQueue(
    squeal.MySQLBackend,
    connection=conn,
    prefix="squeal",
    acquire_timeout=60,
    topic=4,
)
q.put(b'notice no topic')
ret = q.get_nowait()
```

## `squeal.Message`
The `Message` object is returned from `.get()`.  You never have to construct this yourself.

`Message` can be used as a context manager like so:

```python3
with queue.get(topic=1) as msg:
    # [...try to do something with the message...]
    msg.ack()
```

The message will be automatically `nack`ed if it makes it to the end of the block without being acked, including if there's an exception thrown.  Explicitly `nack`ing the message is better than just crashing and waiting for the `acquire_timeout` to pass because it lets something else try to handle it sooner.

### `msg.payload`
the payload, as given to `q.put()`

### `msg.ack()`
mark this message as completed and remove it from the queue

### `msg.nack()`
mark this message failed and release it for another consumer to pick up

# TODO
* allow a message to be `put` with a delay before it's available (add a `not_before` column)
* keep track of failure count for each message and implement exponential backoff
* dead letter queue

# Contributing

Please feel free to submit an issue to the github for bugs, comments, or feature requests.  Also feel free to fork and make a PR.

## Formatting

Please use `black` to format your code.

## Running tests

The tests assume you have a mysql instance running locally.  The authentication can be adjusted with envvars, but the defaults are:

```python3
SQUEAL_TEST_HOSTNAME = os.environ.get("SQUEAL_TEST_HOSTNAME", "localhost")
SQUEAL_TEST_PORT = os.environ.get("SQUEAL_TEST_PORT", "3306")
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
