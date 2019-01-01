# Proximo
Proximo is a proxy for multiple different publish-subscribe queuing systems.

It is based on a GRPC interface definition, making it easy to create new client libraries.
It already supports a number of popular queueing systems, and adding new ones is intended to be simple.

This is a fork of https://github.com/uw-labs/proximo containing uw specific logic.  General development should be done upstream first and then merged here, unless it is uw specific, in which case it should be done in here directly.

## Goals
* Expose multiple consumer (fan out) semantics where needed

* Minimise overhead over direct use of a given queuing system

* Allow configuration of the underlying queue system via runtime configuration of Proximo

* Allow replacement of a queueing system with no change to the Proximo client applications

* Enabling easy creation of client libraries for new languages (anything that has GRPC support)

## Non goals
* Exposing specific details of the underlying queue system via the client API

## Server

This is the Proximo server implementation, written in Go

[proximo server](proximo-server/README.md)

## Proximo client libraries

[Go client library](proximoc-go)

[Java client library](proximoc-java)

## API definition (protobuf)

[protobuf definitions](proto/)

