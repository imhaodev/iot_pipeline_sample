# Nội dung file này không thay đổi so với ví dụ trước.
# Cài đặt: pip install kafka-python
import json, time, uuid, random
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)
SENSOR_ID = str(uuid.uuid4())
print(f"Bắt đầu gửi dữ liệu cho cảm biến: {SENSOR_ID}")
while True:
    data = {
        'sensor_id': SENSOR_ID,
        'temperature': round(random.uniform(15.0, 40.0), 2),
        'timestamp': time.time()
    }
    producer.send('iot-telemetry', data)
    print(f"Sent: {data}")
    # time.sleep(5)