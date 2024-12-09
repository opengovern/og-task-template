# Tasks Template

Define a Task Image for Run by Opencomply Platform. User Can Define a Task Image and Run it on Opencomply Platform.
You can fork this repository and create your own task image.

## Task Definition

Task is a set of instructions that can be executed by Opencomply Platform.
It should be run once every time it is executed. And have an output that can be written to a file.

Defining a task has 2 main parts:

### 1. Code

First part is the code that will be executed. It can be a shell script, python script, or any other executable file.
We Use [task.sh](./sender/task.sh) as an example.

```shell
#!/bin/bash
echo "Hello World"
echo "This is an example task"
```

### 2. Write Dockerfile

Second part is the Dockerfile that will be used to build the task image. It should have the following structure:

```dockerfile
# Use a base image
# Do not change the code below
# Run The Sender Go Application
FROM golang:1.23-alpine AS Final
RUN cd /sender
# Copy and download dependencies
COPY go.mod go.sum ./
RUN go mod download

# Copy the source code
COPY . .
# Build the Go application
RUN  go build -o sender .
ENTRYPOINT ["/sender"]
```

We use [Dockerfile](./Dockerfile) as an example.

