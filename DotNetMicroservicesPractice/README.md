# DotNet Microservices Practice

This project is a local event-driven microservice system built with .NET, Kafka, Redis, YARP API Gateway, Prometheus, Grafana, and Docker Compose.

The goal of this project is to understand how a real microservice-style system is structured, how services communicate, why Kafka is useful, why Redis is useful, how an API Gateway fits in, and how monitoring works with Prometheus and Grafana.

This project currently runs locally on a development machine. Later, it can be moved to Azure using Azure App Service, Azure SQL, Azure Cache for Redis, Azure Event Hubs, Azure Functions, or Azure Container Apps.

---

# 1. Project Overview

This system has three .NET projects/services:

1. ApiGateway
2. OrderService.Api
3. InventoryService.Api

It also has one shared class library:

1. Shared.Events

It uses the following infrastructure components:

1. Kafka
2. Redis
3. Prometheus
4. Grafana

High-level business flow:

```text
Client creates an order
        |
        v
ApiGateway receives the HTTP request
        |
        v
OrderService.Api handles the order request
        |
        v
OrderService.Api saves order data in Redis
        |
        v
OrderService.Api publishes OrderCreated event to Kafka
        |
        v
InventoryService.Api consumes OrderCreated event from Kafka
        |
        v
InventoryService.Api reduces stock in Redis



                              +----------------------+
                              |        Client        |
                              | Browser / Postman    |
                              +----------+-----------+
                                         |
                                         |
                                         v
                              +----------------------+
                              |      ApiGateway      |
                              |       YARP           |
                              | http://localhost:5202|
                              +----------+-----------+
                                         |
                         +---------------+---------------+
                         |                               |
                         v                               v
              +----------------------+       +-------------------------+
              |   OrderService.Api   |       |  InventoryService.Api   |
              | http://localhost:5067|       | http://localhost:5026   |
              +----------+-----------+       +-----------+-------------+
                         |                               ^
                         |                               |
                         | publishes                     | consumes
                         | OrderCreated event            | OrderCreated event
                         v                               |
                    +------------+                       |
                    |   Kafka    |-----------------------+
                    |   :9092    |
                    +------------+

              +----------------------+       +-------------------------+
              | Redis order cache    |       | Redis stock cache       |
              | order:{orderId}      |       | stock:{productId}       |
              +----------------------+       +-------------------------+


Monitoring:

              +----------------------+
              |     Prometheus       |
              | http://localhost:9090|
              +----------+-----------+
                         |
                         | scrapes /metrics
                         |
       +-----------------+------------------+------------------+
       |                                    |                  |
       v                                    v                  v
ApiGateway /metrics              OrderService /metrics   InventoryService /metrics


              +----------------------+
              |       Grafana        |
              | http://localhost:3000|
              +----------+-----------+
                         |
                         | reads metrics from
                         v
                    Prometheus






 3. Services Explained
3.1 ApiGateway

The API Gateway is the single entry point for clients.

Without an API Gateway, a client would call services directly:

Client ---> OrderService.Api
Client ---> InventoryService.Api

With an API Gateway, the client calls one place:

Client ---> ApiGateway

Then the gateway forwards the request to the correct service.

In this project, the gateway uses YARP.

YARP stands for:

Yet Another Reverse Proxy

It is a Microsoft reverse proxy library for .NET.

Current gateway routes:

/orders/*      ---> OrderService.Api
/inventory/*   ---> InventoryService.Api

Example:

Client calls:

GET http://localhost:5202/inventory/P1001

ApiGateway forwards the request to:

GET http://localhost:5026/api/inventory/P1001

Another example:

Client calls:

POST http://localhost:5202/orders

ApiGateway forwards the request to:

POST http://localhost:5067/api/orders

In real production systems, an API Gateway can also handle:

1. Authentication
2. Authorization
3. Rate limiting
4. Request logging
5. Request routing
6. SSL termination
7. API versioning

In this project, it is currently used for routing.

3.2 OrderService.Api

OrderService.Api is responsible for order-related operations.

Current responsibilities:

1. Receive order creation requests.
2. Validate order request data.
3. Save order information into Redis.
4. Publish an OrderCreated event to Kafka.

Main endpoints:

POST /api/orders
GET  /api/orders/{orderId}

Through the API Gateway, these become:

POST http://localhost:5202/orders
GET  http://localhost:5202/orders/{orderId}

Example order request:

{
  "customerId": "C1001",
  "productId": "P1001",
  "quantity": 3
}

When OrderService receives this request, it creates an event like this:

{
  "eventId": "generated-guid",
  "eventType": "OrderCreated",
  "orderId": "generated-guid",
  "customerId": "C1001",
  "productId": "P1001",
  "quantity": 3,
  "createdAt": "2026-07-05T12:00:00Z"
}

Then it publishes this event to Kafka topic:

order-created

Important idea:

OrderService does not know how inventory stock is reduced.
OrderService only announces that an order was created.

That is event-driven design.

3.3 InventoryService.Api

InventoryService.Api is responsible for product stock.

Current responsibilities:

1. Set product stock manually for testing.
2. Get product stock.
3. Listen to OrderCreated events from Kafka.
4. Reduce stock when an order is created.
5. Save updated stock in Redis.

Main endpoints:

GET  /api/inventory/{productId}
POST /api/inventory/{productId}/stock/{quantity}

Through the API Gateway, these become:

GET  http://localhost:5202/inventory/{productId}
POST http://localhost:5202/inventory/{productId}/stock/{quantity}

Example:

POST http://localhost:5202/inventory/P1001/stock/50

This stores stock in Redis:

Key:   stock:P1001
Value: 50

Then if an order is created for product P1001 with quantity 3, InventoryService consumes the OrderCreated event from Kafka and updates Redis:

Old stock:      50
Order quantity: 3
New stock:      47

Then this request:

GET http://localhost:5202/inventory/P1001

returns:

{
  "productId": "P1001",
  "stock": 47
}

InventoryService has a background worker called:

OrderCreatedConsumer

This worker keeps listening to Kafka.

3.4 Shared.Events

Shared.Events is a class library.

It contains shared event contracts.

Currently it contains:

OrderCreatedEvent

Both services use this class:

OrderService.Api uses it to publish the event.
InventoryService.Api uses it to deserialize and consume the event.

The reason this project uses a shared library is to avoid duplicating the same event class in multiple services.

For this practice project, this is simple and clean.

In a larger production system, event contracts may be managed more strictly, for example using versioned NuGet packages, schema registry, or protobuf/Avro contracts.

4. Why Kafka Is Used

Kafka is used for asynchronous communication between services.

Without Kafka, OrderService might directly call InventoryService:

OrderService.Api ---> HTTP call ---> InventoryService.Api

That works for small systems, but it creates tight coupling.

If InventoryService is down, OrderService may fail.

With Kafka, the design becomes:

OrderService.Api ---> Kafka ---> InventoryService.Api

OrderService only publishes an event.

InventoryService consumes the event when it is available.

This gives several benefits.

4.1 Loose Coupling

OrderService does not need to know where InventoryService is hosted.

OrderService only knows Kafka.

OrderService says:
"An order was created."

InventoryService says:
"I care about OrderCreated events, so I will reduce stock."

This makes services more independent.

4.2 Fault Tolerance

If InventoryService is temporarily down, OrderService can still publish the event to Kafka.

When InventoryService comes back online, it can continue consuming events.

That is better than losing the request completely.

4.3 Scalability

If there are many orders, we can scale InventoryService separately.

For example:

OrderService.Api       1 instance
InventoryService.Api   3 instances

Kafka consumer groups can distribute messages across multiple consumers.

4.4 Event History

Kafka stores messages for a period of time.

This means consumers can process events independently.

In real systems, this is useful for:

1. Replaying events
2. Debugging
3. Adding new consumers later
4. Auditing event flow
5. Why Redis Is Used

Redis is used as a fast in-memory data store.

In this project, Redis stores:

1. Order cache
2. Product stock
3. Processed event markers

Examples:

order:{orderId}
stock:{productId}
processed-event:{eventId}

Redis is very fast because it stores data in memory.

In real systems, Redis is commonly used for:

1. Caching
2. Session storage
3. Temporary data
4. Fast lookup data
5. Distributed locks
6. Rate limiting

In this project, Redis is currently used as a simple data store for practice.

Later, a real database can be added:

OrderService.Api       ---> Orders database
InventoryService.Api   ---> Inventory database
Redis                  ---> Cache layer

That would be closer to a production-style architecture.

6. Why Prometheus Is Used

Prometheus is used to collect metrics.

Each .NET service exposes a /metrics endpoint.

Current metrics endpoints:

ApiGateway:
http://localhost:5202/metrics

OrderService.Api:
http://localhost:5067/metrics

InventoryService.Api:
http://localhost:5026/metrics

Prometheus scrapes these endpoints.

Scraping means Prometheus calls each /metrics endpoint repeatedly and stores metric values.

Prometheus runs at:

http://localhost:9090

Prometheus target page:

http://localhost:9090/targets

In this project, Prometheus scrapes:

1. ApiGateway
2. OrderService.Api
3. InventoryService.Api

This is the correct microservice monitoring style because every service should be monitored separately.

The gateway metrics show traffic going through the gateway.

The service metrics show what is happening inside each service.

7. Why Grafana Is Used

Grafana is used to visualize metrics.

Prometheus collects and stores metrics.

Grafana displays those metrics in dashboards.

The relationship is:

Services expose /metrics
        |
        v
Prometheus scrapes metrics
        |
        v
Grafana displays dashboards

Grafana runs at:

http://localhost:3000

Default login:

Username: admin
Password: admin

When adding Prometheus as a Grafana data source, use:

http://prometheus:9090

Why?

Because Grafana and Prometheus are both running inside Docker Compose.

Inside Docker Compose, containers can communicate using service names.

So Grafana can reach Prometheus using:

prometheus
8. Request Flow in Detail
8.1 Set Initial Stock

Client sends this request:

POST http://localhost:5202/inventory/P1001/stock/50

The request first reaches:

ApiGateway

The gateway sees this path:

/inventory/P1001/stock/50

YARP route config maps:

/inventory/{**catch-all}

to:

http://localhost:5026/api/inventory/{**catch-all}

So the actual forwarded request becomes:

POST http://localhost:5026/api/inventory/P1001/stock/50

InventoryService receives the request and stores stock in Redis:

Key:   stock:P1001
Value: 50
8.2 Create Order

Client sends:

POST http://localhost:5202/orders

Request body:

{
  "customerId": "C1001",
  "productId": "P1001",
  "quantity": 3
}

The request reaches ApiGateway.

YARP route config maps:

/orders/{**catch-all}

to:

http://localhost:5067/api/orders/{**catch-all}

So the actual forwarded request becomes:

POST http://localhost:5067/api/orders

OrderService receives the request.

It validates:

CustomerId is not empty.
ProductId is not empty.
Quantity is greater than 0.

Then it creates:

OrderId
EventId
CreatedAt

It stores order data in Redis:

Key:   order:{orderId}
Value: serialized OrderCreatedEvent JSON

Then it publishes an event to Kafka.

Kafka topic:

order-created

Event type:

OrderCreated
8.3 Kafka Event Is Published

OrderService serializes the event to JSON.

Then it sends it to Kafka.

The event means:

An order has been created.

The event does not command InventoryService directly.

It does not say:

Reduce stock now.

It simply says:

OrderCreated

InventoryService chooses to react to that event.

This is important because event-driven systems are built around facts that happened.

Good event names are usually past tense:

OrderCreated
PaymentCompleted
InventoryReserved
EmailSent
8.4 InventoryService Consumes Event

InventoryService has a background service:

OrderCreatedConsumer

This consumer subscribes to Kafka topic:

order-created

When it receives an event, it deserializes the JSON back into:

OrderCreatedEvent

Then it reads stock from Redis:

Key: stock:P1001
Value: 50

Then it subtracts the order quantity:

50 - 3 = 47

Then it updates Redis:

Key: stock:P1001
Value: 47

It also stores a processed-event marker:

Key: processed-event:{eventId}
Value: processed date/time

This is the beginning of an idempotency pattern.

A complete idempotency implementation would check this key before processing the event.

8.5 Check Updated Stock

Client sends:

GET http://localhost:5202/inventory/P1001

ApiGateway forwards it to:

GET http://localhost:5026/api/inventory/P1001

InventoryService reads Redis:

Key: stock:P1001
Value: 47

Response:

{
  "productId": "P1001",
  "stock": 47
}
9. Folder Structure
DotNetMicroservicesPractice/
│
├── README.md
├── docker-compose.yml
│
├── monitoring/
│   └── prometheus.yml
│
└── src/
    │
    ├── ApiGateway/
    │   ├── appsettings.json
    │   ├── Program.cs
    │   └── ApiGateway.csproj
    │
    ├── OrderService.Api/
    │   ├── Controllers/
    │   │   └── OrdersController.cs
    │   ├── Messaging/
    │   │   └── KafkaProducer.cs
    │   ├── Models/
    │   │   └── CreateOrderRequest.cs
    │   ├── appsettings.json
    │   ├── Program.cs
    │   └── OrderService.Api.csproj
    │
    ├── InventoryService.Api/
    │   ├── Controllers/
    │   │   └── InventoryController.cs
    │   ├── Messaging/
    │   │   └── OrderCreatedConsumer.cs
    │   ├── appsettings.json
    │   ├── Program.cs
    │   └── InventoryService.Api.csproj
    │
    └── Shared.Events/
        ├── OrderCreatedEvent.cs
        └── Shared.Events.csproj
10. Project Creation Notes

This section explains the important project creation steps and why they were done.

10.1 Main Folder

The main folder is:

DotNetMicroservicesPractice

It contains the whole system.

This includes:

1. Source code
2. Docker Compose file
3. Monitoring config
4. README
5. Git repository

Keeping everything in one root folder makes the project easy to run and understand.

10.2 Solution File

The solution file groups all .NET projects together.

dotnet new sln -n DotNetMicroservicesPractice

A .sln file is useful because this system has multiple projects:

ApiGateway
OrderService.Api
InventoryService.Api
Shared.Events

Once all projects are added to the solution, this command builds everything:

dotnet build

Without a solution file, you would need to build each project separately.

For a microservice practice project, a solution file keeps the developer experience simple.

10.3 Source Folder

The src folder contains application source code.

src/

This keeps code separate from root-level files like:

README.md
docker-compose.yml
monitoring/

This structure is common in professional projects.

10.4 Service Projects

The service projects are:

OrderService.Api
InventoryService.Api
ApiGateway
Shared.Events

Why these project types?

OrderService.Api:
ASP.NET Core Web API because it exposes HTTP endpoints for order operations.

InventoryService.Api:
ASP.NET Core Web API because it exposes HTTP endpoints and also runs a Kafka background consumer.

ApiGateway:
Lightweight ASP.NET Core web project because it only forwards requests using YARP.

Shared.Events:
Class library because it only contains shared C# event classes.
10.5 Adding Projects to the Solution

Each project was added to the solution.

dotnet sln add src/OrderService.Api/OrderService.Api.csproj
dotnet sln add src/InventoryService.Api/InventoryService.Api.csproj
dotnet sln add src/Shared.Events/Shared.Events.csproj
dotnet sln add src/ApiGateway/ApiGateway.csproj

This tells the solution:

These projects are part of this system.

After that, running this from the root builds all projects:

dotnet build

This is important because a microservice system usually has multiple projects.

10.6 Project References

OrderService and InventoryService both reference Shared.Events.

dotnet add src/OrderService.Api/OrderService.Api.csproj reference src/Shared.Events/Shared.Events.csproj
dotnet add src/InventoryService.Api/InventoryService.Api.csproj reference src/Shared.Events/Shared.Events.csproj

Why?

Both services need to understand this class:

OrderCreatedEvent

OrderService uses it to create and publish the event.

InventoryService uses it to deserialize and consume the event.

Using a shared library avoids duplicating the same event class in two services.

For learning, this is a clean approach.

In a larger production system, event contracts may be managed more strictly, for example using schema registries or versioned packages.

10.7 NuGet Packages

Kafka package:

Confluent.Kafka

Used by:

OrderService.Api       -> Kafka producer
InventoryService.Api   -> Kafka consumer

Redis package:

StackExchange.Redis

Used by:

OrderService.Api
InventoryService.Api

This package allows the services to connect to Redis.

YARP package:

Yarp.ReverseProxy

Used by:

ApiGateway

This package allows the gateway to route requests to downstream services.

Prometheus package:

prometheus-net.AspNetCore

Used by:

ApiGateway
OrderService.Api
InventoryService.Api

This package exposes the /metrics endpoint from each service.

Swagger package:

Swashbuckle.AspNetCore

Used by:

OrderService.Api
InventoryService.Api

This package provides Swagger UI for testing APIs in the browser.

11. Important Files
11.1 docker-compose.yml

This file starts local infrastructure:

Kafka
Redis
Prometheus
Grafana

Current docker-compose.yml:

services:
  kafka:
    image: apache/kafka:4.1.2
    container_name: practice-kafka
    ports:
      - "9092:9092"

  redis:
    image: redis:7
    container_name: practice-redis
    ports:
      - "6379:6379"

  prometheus:
    image: prom/prometheus:latest
    container_name: practice-prometheus
    ports:
      - "9090:9090"
    volumes:
      - ./monitoring/prometheus.yml:/etc/prometheus/prometheus.yml

  grafana:
    image: grafana/grafana:latest
    container_name: practice-grafana
    ports:
      - "3000:3000"

Start infrastructure:

docker compose up -d

Check containers:

docker ps

Expected containers:

practice-kafka
practice-redis
practice-prometheus
practice-grafana
11.2 monitoring/prometheus.yml

This file tells Prometheus which services to scrape.

global:
  scrape_interval: 5s

scrape_configs:
  - job_name: "api-gateway"
    metrics_path: "/metrics"
    static_configs:
      - targets: ["host.docker.internal:5202"]

  - job_name: "order-service"
    metrics_path: "/metrics"
    static_configs:
      - targets: ["host.docker.internal:5067"]

  - job_name: "inventory-service"
    metrics_path: "/metrics"
    static_configs:
      - targets: ["host.docker.internal:5026"]

Why host.docker.internal?

Prometheus runs inside Docker.

The .NET services run directly on the Mac.

From inside a Docker container, localhost means the container itself, not the Mac.

So Prometheus uses:

host.docker.internal

to access services running on the host machine.

11.3 ApiGateway/appsettings.json

This file configures YARP routes.

{
  "ReverseProxy": {
    "Routes": {
      "orders-route": {
        "ClusterId": "orders-cluster",
        "Match": {
          "Path": "/orders/{**catch-all}"
        },
        "Transforms": [
          {
            "PathPattern": "/api/orders/{**catch-all}"
          }
        ]
      },
      "inventory-route": {
        "ClusterId": "inventory-cluster",
        "Match": {
          "Path": "/inventory/{**catch-all}"
        },
        "Transforms": [
          {
            "PathPattern": "/api/inventory/{**catch-all}"
          }
        ]
      }
    },
    "Clusters": {
      "orders-cluster": {
        "Destinations": {
          "orders-api": {
            "Address": "http://localhost:5067"
          }
        }
      },
      "inventory-cluster": {
        "Destinations": {
          "inventory-api": {
            "Address": "http://localhost:5026"
          }
        }
      }
    }
  },
  "AllowedHosts": "*"
}

This means:

/orders/* goes to OrderService.Api.
/inventory/* goes to InventoryService.Api.
12. How to Run the Project Locally
12.1 Start Docker Infrastructure

From the root folder:

docker compose up -d

Check:

docker ps

Expected:

practice-kafka
practice-redis
practice-prometheus
practice-grafana
12.2 Run OrderService.Api
dotnet run --project src/OrderService.Api/OrderService.Api.csproj

Expected URL:

http://localhost:5067

Swagger:

http://localhost:5067/swagger

Metrics:

http://localhost:5067/metrics
12.3 Run InventoryService.Api
dotnet run --project src/InventoryService.Api/InventoryService.Api.csproj

Expected URL:

http://localhost:5026

Swagger:

http://localhost:5026/swagger

Metrics:

http://localhost:5026/metrics

The terminal should show:

InventoryService is listening to Kafka topic: order-created
12.4 Run ApiGateway
dotnet run --project src/ApiGateway/ApiGateway.csproj

Expected URL:

http://localhost:5202

Metrics:

http://localhost:5202/metrics
13. How to Test the Full Flow
13.1 Set Stock

Use the API Gateway:

curl -X POST http://localhost:5202/inventory/P1001/stock/50

Expected response:

{
  "productId": "P1001",
  "stock": 50,
  "message": "Stock updated."
}
13.2 Check Stock
curl http://localhost:5202/inventory/P1001

Expected:

{
  "productId": "P1001",
  "stock": 50
}
13.3 Create Order
curl -X POST http://localhost:5202/orders \
  -H "Content-Type: application/json" \
  -d '{"customerId":"C1001","productId":"P1001","quantity":3}'

Expected response:

{
  "message": "Order created and OrderCreated event published.",
  "orderId": "generated-guid",
  "eventId": "generated-guid"
}

InventoryService terminal should show something like:

OrderCreated event processed.
OrderId: generated-guid
ProductId: P1001
Quantity: 3
Remaining stock: 47
13.4 Check Stock Again
curl http://localhost:5202/inventory/P1001

Expected:

{
  "productId": "P1001",
  "stock": 47
}

This proves the full event flow works:

ApiGateway
    |
    v
OrderService.Api
    |
    v
Kafka
    |
    v
InventoryService.Api
    |
    v
Redis
14. Monitoring
14.1 Prometheus

Open:

http://localhost:9090

Targets page:

http://localhost:9090/targets

Expected targets:

api-gateway
order-service
inventory-service

All should show:

UP

If a target is down, check:

1. Is the related .NET service running?
2. Is the port correct in prometheus.yml?
3. Is the /metrics endpoint working in browser?
14.2 Grafana

Open:

http://localhost:3000

Default login:

Username: admin
Password: admin

Add Prometheus data source:

http://prometheus:9090

Then create dashboards using Prometheus metrics.

Useful metrics to explore:

http_requests_received_total
http_request_duration_seconds
process_cpu_seconds_total
process_working_set_bytes
dotnet_collection_count_total

Metric names may vary slightly depending on package version.

15. Current Ports
ApiGateway:           http://localhost:5202
OrderService.Api:     http://localhost:5067
InventoryService.Api: http://localhost:5026
Kafka:                localhost:9092
Redis:                localhost:6379
Prometheus:           http://localhost:9090
Grafana:              http://localhost:3000
16. Current Kafka Topic
order-created

OrderService publishes to this topic.

InventoryService consumes from this topic.

17. Redis Keys Used

OrderService stores orders:

order:{orderId}

InventoryService stores stock:

stock:{productId}

InventoryService also stores processed event markers:

processed-event:{eventId}
18. Important Microservice Concepts Practiced

This project demonstrates:

1. Service separation
2. API Gateway routing
3. Event-driven communication
4. Kafka producer
5. Kafka consumer
6. Redis caching
7. Background worker service
8. Shared event contracts
9. Docker Compose infrastructure
10. Prometheus metrics
11. Grafana monitoring
12. Basic idempotency marker pattern                   