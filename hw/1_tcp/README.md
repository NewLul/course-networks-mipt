# TCP Protocol

Implementation of a consistent TCP protocol over UDP.

The protocol is implemented in class `MyTCPProtocol` in `protocol.py`.

To test the protocol, run the `test.sh` script. All Python dependencies can be found in `requirements.txt` file.

## About

We have client and server that are sending messages to each other. But we have network problems such as: packet losses, packet duplications, and reordering. This is simulated using `netem` utility.  

## Running with Docker
```
docker compose up --build
```

## Local Running
Run `test.sh` under root privileges.
