import pymysql
from squeal import MySQLBackend, Queue
import time

conn = pymysql.connect(
    host="localhost",
    user="root",
    password="password",
    database="test",
)

queue = Queue(backend=MySQLBackend(connection=conn, prefix="squeal"))

i = 0
while True:
    msg = str(i)
    queue.put(msg.encode("utf-8"), topic=1)
    print("Enqueue:", msg)
    i += 1
    time.sleep(0.1)
