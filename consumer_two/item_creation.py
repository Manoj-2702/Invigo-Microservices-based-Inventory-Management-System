import pika

# RabbitMQ connection
rabbitmq_host = 'rabbitmq'
connection = pika.BlockingConnection(pika.ConnectionParameters(host=rabbitmq_host))
channel = connection.channel()

# Declare RabbitMQ queue
queue_name = 'inventory_queue'
channel.queue_declare(queue=queue_name)

def callback(ch, method, properties, body):
    print(f"Item creation: {body}")

channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=True)

print('Item creation consumer started. Waiting for messages...')
channel.start_consuming()