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

Organization of this Document
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Conventions
^^^^^^^^^^^

.. _haigha-architecture:

Architecture
============

Model Architecture
^^^^^^^^^^^^^^^^^^

Command Architecture
^^^^^^^^^^^^^^^^^^^^

Transport Architecture
^^^^^^^^^^^^^^^^^^^^^^

Client Architecture
^^^^^^^^^^^^^^^^^^^

.. _haigha-functional-specification:

Functional Specification
========================

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
