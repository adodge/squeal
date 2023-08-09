import pymysql
import squeal

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

while True:
    msg = queue.get(topic=1)
    print("Processed:", msg.payload)
    msg.ack()
