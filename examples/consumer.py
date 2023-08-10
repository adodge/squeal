import pymysql
from squeal import MySQLBackend, Queue

conn = pymysql.connect(
    host="localhost",
    user="root",
    password="password",
    database="test",
)

queue = Queue(backend=MySQLBackend(connection=conn, prefix="squeal"))

while True:
    with queue.get(topic=1) as msg:
        print("Dequeue:", msg.payload)
        msg.ack()
