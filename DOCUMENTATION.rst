======
Haigha
======

:Version: 0.3.3
:Download: http://pypi.python.org/pypi/haigha
:Source: https://github.com/agoragames/haigha

.. contents::
    :local:

.. _haigha-overview:

Overview
========

Goals of this Document
^^^^^^^^^^^^^^^^^^^^^^

This document describes Haigha, a client for `AMQP`_ servers. AMQP is a messaging protocol which can be used to route large volumes of data across a wide network of application servers. The document covers the design, implementation and usage of Haigha to support fast, event-driven Python applications using AMQP. It should provide sufficient specifications for an engineer to integrate haigha into their applications.

Organization of this Document
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

This document is divided into chapters according to the layout of the AMQP 0.9.1 `specification [PDF]  <http://www.amqp.org/confluence/download/attachments/720900/amqp0-9-1.pdf>`_. 

1. **Overview** Read this for a general introduction

2. **Architecture** The architecture of haigha code base

3. **Functional Specifications** How applications work with haigha

4. **Technical Specifications** How haigha transport layer is implemented

Conventions
^^^^^^^^^^^

**TODO** Write conventions on common terms and usage

.. _haigha-architecture:

Architecture
============

Model Architecture
^^^^^^^^^^^^^^^^^^

This section describes the semantics of haigha to integrate with AMQP servers

Main Entities
-------------

The AMQP protocol divides the tasks of message routing and delivery between two distinct objects:

* **Exchange**, to which messages are written
* **Queue**, to which messages are routed and stored for consumption by clients

To connect an exchange and a queue, a binding is defined. When a message is published to an exchange, a route is supplied which is compared against the binding to determine delivery.

To manage the stateful connection to a broker for both the publishing and consuming of messages, the following entities are defined:

* **Connection** The authenticated socket connection between a client and broker on a specific vhost
* **Channel** **TODO** how to even describe this


Queue
-----

The message queue is the final destination of any published message, and it is the location from which a client will consume messages. Each queue with a binding to an exchange for which a message was published with a matching routing key will receive a copy of a message [#]_.


Haigha implements queue declaration and deletion, in the `QueueClass`_. 

Exchange
--------

The exchange accepts messages from applications. There are several different exchange types, the standard ones defined in the `specification <AMQPSpec>`_ and possibly some additional ones supplied by your broker. The common types of exchanges are:

* **direct** The routing key and binding key must exactly match
* **topic** The routing key must match the pattern defined by the binding keu
* **fanout** All queues will receive a copy of the message.

Haigha implements exchange declaration and deletion in the `ExchangeClass`_.

Bindings
--------

After an exchange and a queue have been declared, one or more bindings can be defined between them. It is possible for a single queue to be bound to multiple exchanges, or a shared queue can be used to distribute messages among a pool of consumers.

Haigha implements bindings in the `QueueClass`_ and consumers in the `BasicClass`_.

Constructing a Shared Queue
***************************

Shared queues are the standard point-to-point queue, useful for distributing messages among consumers. It assumes a `Connection`_ is initiated to ``connection`` and that the user has the method ``application_consumer`` defined to receive messages. ::

  ch = connection.channel()
  ch.exchange.declare('an_exchange', 'direct')
  ch.queue.declare('a_queue')
  ch.queue.bind('a_queue', 'an_exchange', routing_key='route')
  ch.basic.consume('a_queue', application_consumer)

Constructing a Reply Queue
**************************

Handling replies, or receiving consumer-targetted messages, is a common use case for creating exclusive queues for a process. In this example, we'll let the broker assign the queue name and use callbacks to set up a consumer after the server has replied. ::

  ch = connection.channel()
  ch.exchange.declare('reply', 'direct')
  ch.queue.declare(exclusive=True,cb=lambda queue,messages,consumers: \
    ch.queue.bind(queue, 'reply', route=queue)

By convention, we'll now use a ``reply-to`` header in our messages when this consumer requests data from another consumer, so that the reply can be routed using the appropriate binding key.
  
Constructing a Pub-Sub Queue
****************************

Topic routing forms the basis of pub-sub models. When combined with a shared queue semantics, it allows for AMQP to be used as a powerful routing engine across a large pool of varied applications. ::

  ch = connection.channel()
  ch.exchange.declare('pub', 'topic')
  ch.queue.declare('stock.usd')
  ch.queue.bind('stock.usd', 'pub', routing_key='stock.usd.*')
 
Command Architecture
^^^^^^^^^^^^^^^^^^^^

This section describes how haigha talks to the broker.

Protocol Commands
-----------------

The AMQP protocol divides its commands among classes of functionality. The `ProtocolClass`_ defines the base class for each of these, with each class of functionality defined in a subclass such as `QueueClass`_, `ExchangeClass`_, etc, for each of the AMQP protocol classes ``[basic, channel, exchange, queue, transaction]``. These are exposed in the `Channel`_ as properties as shown in the examples above.

The protocol also separates commands between synchronous and asynchronous. In all cases[#]_, if an operation is (optionally) synchronous it will support a ``cb=`` keyword argument. Many methods support both synchronous and asynchronous behavior; haigha always defaults to asynchronous operation when available through the ``nowait=True`` keyword argument, and automatically switches to synchronous mode if an application callback is supplied.

Commands are further identified as originating from the client, server or either. As haigha is a client library, it only supports those commands which can be initiated by the client. With the exception of publishing, these commands are available soley in the respective `ProtocolClass`_ to which the command belongs. For convenience, the `Channel`_ exposes two publishing methods, ``publish`` and ``publish_synchronous``, as well as ``open`` and ``close``. All methods of a `ProtocolClass`_ which handle server-originated messages are named beginning with the string ``_recv_``.

Mapping AMQP to the API
-----------------------

The mapping of classes and commands has already been described via the `ProtocolClass`_ and its implementations. Each method is responsible for constructing the frame(s) necessary to implement the command, and the user should never have to worry about constructing frames by hand.

Connection
----------

The `Connection`_ class manages the state of the AMQP connection. The life-cycle is:

* User creates a new `Connection`_ object, setting the configuration through keyword params (**TODO** document).
* A `ConnectionStrategy`_ is created and a blocking TCP connection is initiated to the broker.
* After a socket connection is created, it is set to non-blocking mode.
* The `Connection`_ sends a protocol header defining specification 0.9.1.
* The `ConnectionChannel`_, id ``0``, receives the ``start`` command and replies with ``start-ok`` login credentials.
* If authorized, the server responds with the ``secure`` command, to which `ConnectionChannel`_ responds with ``open``. If not authorized, the socket is immediately closed.
* The server responds with ``open-ok`` and any pending frames are flushed.
* At any time, the client or server may send or reply with ``tune`` or ``tune-ok`` respectively to negotiate frame size or channel limits.
* The connection is available for the application.
* The server sends a ``close`` command, or client sends it by calling ``connection.close``.
* Peer acknowledges with ``close-ok`` and sock is disconnected.

The `Connection`_ class manages the state of the socket connection and the negotiation with the broker. It is also responsible for maintaining a buffer of both input and output frames. The output buffer is used during the initialization of the connection, so that it can be used immediately by the application. ::

  connection = Connection()
  channel = connection.channel()

In this example, the channel will be negotiated immediately following the receipt of the ``open-ok`` command in the `ConnectionChannel`_.

Channel
-------

AMQP multiplexes frames across channels. The `Channel`_ class implements the stateful behavior of channels, and writes frames back to the `Connection`_ on which it was created. The life-cycle is:

* User creates a `Channel`_ by calling ``connection.channel``. The channel is enumerated, and references to existing channels can be fetched by id.
* The `Channel`_ initializes all supported protocol classes and internal buffers.
* The channel immediate sends the ``open`` command.
* The server responds with ``open-ok``.
* The channel is available for the application.
* The server sends a ``close`` command, or the client sends it by calling ``channel.close``.
* Peer acknowledges with ``close-ok`` and the channel is closed. All future use will raise a ``ChannelClosed`` exception.

The AMQP protocol isolates all synchronous and asynchronous transactions per channel. The `Channel`_ class implements this behavior by maintaining a buffer of pending outbound frames. If the buffer is empty, a frame is immediately forwarded to the `Connection`_, else it's appended to the end. When a synchronous method is called by the user, after all frames have been sent or queued, a callback is appended to the buffer.

When a command is received from the broker, the dispatch will find the appropriate haigha method and if that method is at the front of the buffer, will pop it off. All remaining frames are then flushed until the buffer is empty, or the first item is another pending synchronous callback. This solution implements a very lightweight system for reliably managing multiple outstanding synchronous calls in an asynchronous dispatch loop. The user is free to interact with AMQP without worrying about whether a method is synchronous or not [#]_.

When receiving frames, the `Connection`_ first queues frames to each channel via ``channel.buffer_frame()``. It then iterates over all channels for which a frame was queued and calls ``channel.process_frames()``. In most cases, an AMQP command is isolated to one frame, but in the case of messages, the content may be split across multiple frames. In the situation where not all content frames have been received yet, the `BasicClass`_ will raise a ``ProtocolClass.FrameUnderflow`` exception and re-buffer any message frames on the channel. When the next frame arrives for the channel, the process will repeat, until all frames have arrived and the message is complete.

Exchange
--------

The `ExchangeClass`_ is used to declare and delete exchanges.

All methods of `ExchangeClass`_ are optionally synchronous and can callback to user code.

**TODO** say something more

Queue
-----

The `QueueClass`_ is used to declare, delete, bind and purge queues.

All methods of `QueueClass`_ are optionally or permanently synchronous and can callback to user code.

**TODO** say something more

Basic
-----

The `BasicClass`_ is used to publish messages, manage consumers, handle message delivery, acknolwedge receipts, and synchronously fetch messages.

**TODO** say something more

Transaction
-----------

The `TransactionClass`_ is used to setup and use server-side transaction isolation. The life-cycle is:

* User calls ``channel.transaction.select()`` to send ``select`` command to the server.
* Server replies with ``select-ok`` and the channel is permanently in transaction mode.
* The application publishes or acknowledges messages.
* The application commits or rolls-back the publish or acknowledge commands through ``channel.transaction.commit()`` or ``channel.transaction.rollback()``.

All methos of the `TransactionClass`_ are synchronous and can callback to application code.

Transport Architecture
^^^^^^^^^^^^^^^^^^^^^^

This section describes how haigha implements the wire-level protocol.

General
-------

AMQP is a frame-oriented protocol and haigha is designed around this in every respect. 

The `Connection`_ class implements an `EventSocket`_ callback which will call ``connection._read_frames()``. It will take the current buffer on the socket, place it in a `Reader`_ object, and pass that to the ``read_frames()`` method of the `Frame`_ class. The reader acts as both a stream object, with methods such as ``seek()`` and ``tell()``, as well as an implementation of the basic data types in AMQP. 

For each frame read, the connection will queue the frame on to the channel specified in the frame, for later processing. If the input buffer contains a partial frame, a ``Reader.BufferUnderflow`` exception will be raised and ``Frame.read_frames()`` will exit, leaving the reader positioned at the end of the last full frame (or beginning of the buffer). The connection will re-buffer any pending data on the socket and wait for the next callback to attempt to read frames from the byte stream.

To send frames, each command implemented by a `ProtocolClass`_ will construct a `Writer`_ object which is used to format the arguments for that command. It then constructs a subclass of `Frame`_, usually a `MethodFrame`_, and writes that to the channel to which the protocol class is bound.

Data Types
----------

AMQP defines several data types which form the basis of all frames. One of these data types, tables (i.e. dicts), supports the basic types in addition to a few others.  There is disagreement on official versus supported types in tables, as well as subtle differences in the encoding of some types. Haigha is written to conform to the `errata <http://dev.rabbitmq.com/wiki/Amqp091Errata#section_3>`_ implemented in RabbitMQ.

The implementation of the data types is in both the `Reader`_ and `Writer_` classes. When converting from Python to AMQP data types when serializing tables, the `Writer`_ assumes that all floats are double-precision, converts unicode to utf8 strings, and intelligently packs integers according to their required byte-width.

Error Handling
--------------

AMQP defines two classes of exceptions for error handling. Operational errors, such as invalid queue names, will close a channel. Structural errors, such as invalid or out-of-order frames, will result in a connection closure.

Because haigha is asynchronous, handlers must be defined to receive notification when a connection or channel are closed [#]_. The closed state will be saved on the respective connection or channel, and accessible via the ``close_info`` property. This will always return a dictionary with the following fields defined:

* **reply_code** The 3 digit error code
* **reply_text** The text of the error message
* **class_id** The class id of the offending command
* **method_id** The method id of the offending command

When closing due to an error on the client side, these same parameters can be supplied to ``connection.close()`` and ``channel.close()``.

Client Architecture
^^^^^^^^^^^^^^^^^^^

Haigha's client architecture closely matches AMQP's recommended abstraction layers.

Framing
-------

The framing layer is shared across a number of different classes.

* **Connection** Manages input byte buffer, calls into frame reader, and writes frames to the socket
* **Frame** Implements frame reading, calls into frame implementations for further decoding, subclasses implement ``write_frame()`` method
* **Channel** Implements input frame buffer, dispatch to protocol classes, and interfaces for sending frames

Connection Manager
------------------

The connection management is handled primarily by the `Connection`_ class. The AMQP specification suggests that this layer may also be responsible for sending content, but that is handled in the frame buffering implementation of `Channel`_ and the specific implementation of `BasicClass`_.

API Layer
---------

The primary API of haigha are the methods exposed through the subclasses of `ProtocolClass`_ and which are made available in the afore-mentioned per-channel properties that map to the classes of AMQP protocol messages, ``[basic, channel, exchange, queue, transaction]``. Additional APIs of which the user should be aware:

* `Connection`_ Exposes ``channel()`` and ``close()``
* `Channel`_ Exposes ``close()``, ``publish()`` and ``publish_synchronous()``
* `ChannelPool`_ Transaction-based publishing for guaranteed delivery and high-throughput

.. _haigha-functional-specifications:

Functional Specifications
=========================

Client Functional Specification
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

**TODO** Document other features that the client implements.

Messages and Content
--------------------

Messages are created with the `Message`_ class and sent via one of several publishing methods.

* ``channel.basic.publish`` The "standard" publish which is the publish command exposed by the `BasicClass`_.
* ``channel.publish`` A convenience method that aliases ``basic.publish``.
* ``channel.publish_synchronous`` A wrapper around ``transaction.select``, ``basic.publish``, ``transaction.commit``. A callback argument will be called when the server acknowledges ``commit``.
* ``channelpool.publish`` Publish using a pool of transaction-isolated channels. Will create a new channel if none are free. A callback argument will be called when the server acknowledges transaction commit.

Consumers
---------

The preferred mechanism for reading messages from an AMQP queue is to register a consumer via ``basic.consume`` call. This will register a Python function to be called each time the client receives a message from a queue.


Command Specification
^^^^^^^^^^^^^^^^^^^^^

.. _haigha-technical-specifications:

Technical Specifications
========================

Write-Level Format
^^^^^^^^^^^^^^^^^^

Channel Multiplexing
^^^^^^^^^^^^^^^^^^^^

Channel Closure
^^^^^^^^^^^^^^^

Content Synchronization
^^^^^^^^^^^^^^^^^^^^^^^

Content Ordering Guarantees
^^^^^^^^^^^^^^^^^^^^^^^^^^^

Error Handling
^^^^^^^^^^^^^^

Limitations
^^^^^^^^^^^


.. _AMQP: http://www.amqp.org/
.. _AMQPSpec: http://www.amqp.org/confluence/download/attachments/720900/amqp0-9-1.pdf
.. _EventSocket: https://github.com/agoragames/py-eventsocket
.. _Connection: https://github.com/agoragames/haigha/blob/master/haigha/connection.py
.. _ConnectionChannel: https://github.com/agoragames/haigha/blob/master/haigha/connection.py
.. _Channel: https://github.com/agoragames/haigha/blob/master/haigha/channel.py
.. _ChannelPool: https://github.com/agoragames/haigha/blob/master/haigha/channel_pool.py
.. _ConnectionStrategy: https://github.com/agoragames/haigha/blob/master/haigha/connection_strategy.py
.. _Message: https://github.com/agoragames/haigha/blob/master/haigha/message.py
.. _Reader: https://github.com/agoragames/haigha/blob/master/haigha/reader.py
.. _Writer: https://github.com/agoragames/haigha/blob/master/haigha/writer.py
.. _BasicClass: https://github.com/agoragames/haigha/blob/master/haigha/classes/basic_class.py
.. _ChannelClass: https://github.com/agoragames/haigha/blob/master/haigha/classes/channel_class.py
.. _ExchangeClass: https://github.com/agoragames/haigha/blob/master/haigha/classes/exchange_class.py
.. _ProtocolClass: https://github.com/agoragames/haigha/blob/master/haigha/classes/protocol_class.py
.. _QueueClass: https://github.com/agoragames/haigha/blob/master/haigha/classes/queue_class.py
.. _TransactionClass: https://github.com/agoragames/haigha/blob/master/haigha/classes/transaction_class.py
.. _ContentFrame: https://github.com/agoragames/haigha/blob/master/haigha/frames/content_frame.py
.. _Frame: https://github.com/agoragames/haigha/blob/master/haigha/frames/frame.py
.. _HeaderFrame: https://github.com/agoragames/haigha/blob/master/haigha/frames/header_frame.py
.. _HeartbeatFrame: https://github.com/agoragames/haigha/blob/master/haigha/frames/heartbeat_frame.py
.. _MethodFrame: https://github.com/agoragames/haigha/blob/master/haigha/frames/method_frame.py



.. rubric:: Footnotes

.. [#] Your broker may support other types of exchanges, such as a deliver-once exchange.
.. [#] All synchronous methods will support callbacks by 0.4.0.
.. [#] Synchronous methods have more overhead, so some awareness and caution is recommended.
.. [#] Channel close callbacks will be supported by 0.4.0.
