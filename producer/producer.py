import pika
from flask import Flask, request, jsonify
from pymongo import MongoClient

app = Flask(__name__)

# MongoDB connection
mongo_client = MongoClient('mongodb://mongodb:27017/')
db = mongo_client['inventory']
items_collection = db['items']

# RabbitMQ connection
rabbitmq_host = 'rabbitmq'
connection = pika.BlockingConnection(pika.ConnectionParameters(host=rabbitmq_host))
channel = connection.channel()

# Declare RabbitMQ queue
queue_name = 'inventory_queue'
channel.queue_declare(queue=queue_name)

# Health check endpoint
@app.route('/health', methods=['GET'])
def health_check():
    return jsonify({'status': 'ok'})

# CRUD endpoints
@app.route('/items', methods=['POST'])
def create_item():
    data = request.get_json()
    item = {
        'name': data['name'],
        'description': data['description'],
        'stock': data['stock']
    }
    result = items_collection.insert_one(item)
    message = {
        'operation': 'create',
        'item_id': str(result.inserted_id)
    }
    channel.basic_publish(exchange='', routing_key=queue_name, body=str(message))
    return jsonify({'message': 'Item created successfully'}), 201

@app.route('/items/<item_id>', methods=['GET'])
def get_item(item_id):
    item = items_collection.find_one({'_id': ObjectId(item_id)})
    if item:
        return jsonify(item)
    return jsonify({'error': 'Item not found'}), 404

@app.route('/items/<item_id>', methods=['PUT'])
def update_item(item_id):
    data = request.get_json()
    update_data = {
        '$set': {
            'name': data['name'],
            'description': data['description'],
            'stock': data['stock']
        }
    }
    result = items_collection.update_one({'_id': ObjectId(item_id)}, update_data)
    if result.modified_count > 0:
        message = {
            'operation': 'update',
            'item_id': item_id
        }
        channel.basic_publish(exchange='', routing_key=queue_name, body=str(message))
        return jsonify({'message': 'Item updated successfully'})
    return jsonify({'error': 'Item not found'}), 404

@app.route('/items/<item_id>', methods=['DELETE'])
def delete_item(item_id):
    result = items_collection.delete_one({'_id': ObjectId(item_id)})
    if result.deleted_count > 0:
        message = {
            'operation': 'delete',
            'item_id': item_id
        }
        channel.basic_publish(exchange='', routing_key=queue_name, body=str(message))
        return jsonify({'message': 'Item deleted successfully'})
    return jsonify({'error': 'Item not found'}), 404

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)