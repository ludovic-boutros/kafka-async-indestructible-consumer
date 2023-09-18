# Async processing Kafka Consumer Pattern

This project is a simple example of how we can implement external system call like long SQL queries or HTTP API
using async processing and pause/resume on consumers.

TODO: complete this README

# Testing

Testcontainer is used for testing.
On macos, I need to use these two properties to make it work with Colima:

```properties
DOCKER_HOST=unix://<your_user_account>/.colima/default/docker.sock
TESTCONTAINERS_RYUK_DISABLED=true
```