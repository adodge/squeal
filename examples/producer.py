import datetime
import pymysql
import squeal
import time

conn = pymysql.connect(
    host="localhost",
    port=3306,
    user="root",
    password="password",
    database="test",
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
        msg = datetime.datetime.now().isoformat().encode("utf-8")
        queue.put(msg, topic=1)
        print("Putting:", msg)
        time.sleep(0.1)

except KeyboardInterrupt:
    print("Destroying queue")
    queue.destroy()
