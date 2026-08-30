import pika
import os
from dotenv import load_dotenv

load_dotenv()

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "localhost")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT", 5672))
RABBITMQ_USER = os.getenv("RABBITMQ_USER", "admin")
RABBITMQ_PASS = os.getenv("RABBITMQ_PASS", "admin123")

def callback(ch, method, properties, body):
    print(f"\n[x] Evento recibido (Routing Key: {method.routing_key})")
    print(f"    Payload raw: {body.decode()}")
    print("    - Listo para ser deserializado y procesado -")

def consume_events():
    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
    parameters = pika.ConnectionParameters(host=RABBITMQ_HOST, port=RABBITMQ_PORT, credentials=credentials)
    
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()

    queue_name = 'test_queue'
    print(f"[*] Esperando eventos en '{queue_name}'. Presiona CTRL+C para salir.")
    
    channel.basic_consume(
        queue=queue_name,
        on_message_callback=callback,
        auto_ack=True
    )
    
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        print("\nSaliendo...")
        channel.stop_consuming()
    
    connection.close()

if __name__ == "__main__":
    consume_events()
