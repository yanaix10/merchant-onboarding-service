Gin - http framework
Uber-Fx - dependency injection 
pgx - postgres driver 
google/uuid

1. Root Files
go.mod & go.sum: These files handle dependency management (similar to package.json and package-lock.json in Node.js or pom.xml in Java). They track the exact versions of the external libraries (Gin, Fx, pgx) used in the project.

docker-compose.yml: Contains the configuration to quickly spin up local infrastructure. In our case, it defines and runs the PostgreSQL database container.

2. The cmd/ Directory
This directory contains the entry points (the main applications) for the project.

cmd/app/main.go: The starting point of the application. This file is responsible for wiring everything together. It uses Uber-Fx for Dependency Injection to initialize the database, register the Gin router, connect the handlers, and start the HTTP server.

3. The internal/ Directory
In Go, code placed inside an internal/ directory is private. It cannot be imported by other external Go projects. This is where the core business logic and architecture live.

internal/config/ (Infrastructure Setup)

database.go: Handles establishing the connection pool to PostgreSQL using the pgx driver. It also contains the lifecycle hooks to automatically create the necessary database tables when the application starts.

internal/handlers/ (The HTTP Layer)

merchant.go: Acts as the controller. It defines the API endpoints (e.g., the Onboard function). Its responsibilities include:

Parsing incoming HTTP requests and validating JSON payloads.

Generating the TrackingID.

Calling the repository layer to save the data.

Formatting and returning the HTTP response (e.g., sending the 202 Accepted status).

internal/repository/ (The Data Access Layer)

merchant.go: Manages all direct database interactions. It defines the MerchantEntity struct (the representation of our database table in Go) and contains the SQL queries (like the INSERT statement in the Save method) to persist the data to PostgreSQL.


Structs and Custom Types  Pointers (*) Maps and Slices
https://go.dev/blog/error-handling-and-go

https://pkg.go.dev/context

net/http    encoding/json    crypto/sha256 & crypto/hmac  time  io  

