======
Haigha
======

:Version: 0.3.2
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

* The exchange, to which messages are written
* The queue, to which messages are routed and stored for consumption by clients

To connect an exchange and a queue, a binding is defined. When a message is published to an exchange, a route is supplied which is compared against the binding to determine delivery.

To manage the stateful connection to a broker for both the publishing and consuming of messages, the following entities are defined:

* **Connection** The authenticated socket connection between a client and broker on a specific vhost
* **Channel** **TODO** how to even describe this


Message Queue
-------------

The message queue is the final destination of any published message, and it is the location from which a client will consume messages. Each queue with a binding to an exchange for which a message was published with a matching routing key will receive a copy of a message [#]_.

Haigha implements queue managment, including declaration and binding, in the `QueueClass`_. The consumer implementation is available in the `BasicClass`_.

Command Architecture
^^^^^^^^^^^^^^^^^^^^

Transport Architecture
^^^^^^^^^^^^^^^^^^^^^^

Client Architecture
^^^^^^^^^^^^^^^^^^^

.. _haigha-functional-specifications:

Functional Specifications
=========================

Client Functional Specification
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

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
.. _Connection: https://github.com/agoragames/haigha/blob/master/haigha/connection.py
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

.. [#] Your broker may support other types of exchanges, such as a deliver-once exchange
