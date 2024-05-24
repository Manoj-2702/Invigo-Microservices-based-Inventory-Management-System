<h1 align="center">Invigo: Simplifying Inventory, Amplifying Efficiency</h1>

<p align="center">
 <img src="https://github.com/Manoj-2702/329_331_338_911_5/assets/103581128/3aae8858-8272-4093-be8f-e49e554201d3" alt="logo" />
</p>



## PES1UG21CS329_PES1UG21CS331_PES1UG21CS338_PES1UG21CS911-Microservices-communication-using-RabbitMQ

- PES1UG21CS329 - Manoj Kumar HS
- PES1UG21CS331 - Shriya Marella
- PES1UG21CS338 - Mutasim M
- PES1UG21CS911 - Sana Suman

This repository contains a containerized microservices-based Inventory Management System that utilizes RabbitMQ for interservice communication. The system is designed to handle various operations such as creating items, managing stock, processing orders, and performing health checks.


## Features
- <b>Microservices Architecture: </b>The system is built using a microservices architecture, with each service responsible for a specific task.
- <b>RabbitMQ Integration:</b> RabbitMQ is used as the message broker for communication between the microservices.
- <b>Docker Containerization:</b> The entire system is containerized using Docker and Docker Compose for easy deployment and scalability.
- <b>MongoDB Integration: </b>MongoDB is used as the database for storing inventory data.
- <b>Flask Web Application: </b>A Flask web application serves as the producer, providing a user interface and API endpoints for interacting with the system.

## Services
The repository contains the following services:

- <b>Producer</b> (```app.py```): A Flask web application that exposes endpoints for various operations such as health checks, creating items, managing stock, and processing orders. This service publishes messages to the appropriate RabbitMQ queues.
- <b>Consumer One</b> (```healthcheck.py```): A consumer service that listens for health check messages from the RabbitMQ queue and performs necessary actions.
- <b>Consumer Two </b>(```insert_record.py```): A consumer service that listens for messages to create new inventory items and inserts them into the MongoDB database.
- <b>Consumer Three</b> (```stock_management.py```): A consumer service that listens for messages to update stock levels and manages the deletion of items from the MongoDB database.
- <b>Consumer Four</b> (```order_processing.py```): A consumer service that listens for messages to retrieve all records from the MongoDB database and sends them back to the producer through RabbitMQ.

## Getting Started
To run the Inventory Management System locally, follow these steps:

1. Clone the repository: 
```bash
git clone https://github.com/Manoj-2702/dockerproject.git
```
2. Navigate to the project directory:
```bash
 cd dockerproject
```
3. Build and start the containers:
```bash
 docker-compose up
```
4. Once the containers are up and running, you can access the Flask web application at http://localhost:5000.

## License
This project is licensed under the MIT License.
