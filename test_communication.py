import requests
import pika
import time

def test_producer_health():
    try:
        response = requests.get("http://localhost:5000/health")
        response.raise_for_status()
        print("Producer health check successful!")
    except requests.exceptions.RequestException as e:
        print("Producer health check failed:", e)

def test_rabbitmq_communication():
    rabbitmq_host = 'rabbitmq'
    queue_name = 'test_queue'

    # Connect to RabbitMQ
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=rabbitmq_host))
    channel = connection.channel()

    # Declare a test queue
    channel.queue_declare(queue=queue_name)

    # Publish a test message
    message = "Test message"
    channel.basic_publish(exchange='', routing_key=queue_name, body=message)
    print(f"Published message: {message}")

    # Consume the test message
    def callback(ch, method, properties, body):
        print(f"Received message: {body.decode()}")
        channel.stop_consuming()

    channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=True)
    print("Waiting for message...")
    channel.start_consuming()

    # Clean up
    channel.queue_delete(queue=queue_name)
    connection.close()

if __name__ == "__main__":
    test_producer_health()
    test_rabbitmq_communication()