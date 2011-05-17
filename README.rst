=====================================
 Haigha - AMQP libevent Python client
=====================================

:Version: 0.2.2
:Download: http://pypi.python.org/pypi/haigha
:Source: https://github.com/agoragames/haigha
:Keywords: python, amqp, rabbitmq, event, libevent

.. contents::
    :local:

.. _haigha-overview:

Overview
========

Haigha provides a simple to use client library for interacting with AMQP brokers. It currently supports the 0.9.1 protocol and is integration tested against the latest RabbitMQ 2.4.1. Haigha is a descendant of ``py-amqplib`` and owes much to its developers.

The goals of haigha are performance, simplicity, and adherence to the form and function of the AMQP protocol. It adds a few useful features, such as the ``ChannelPool`` class and ``Channel.publish_synchronous``, to ease use of powerful features in real-world applications.

By default, Haigha operates in a completely asynchronous mode, relying on callbacks to notify application code of responses from the broker. Where applicable, ``nowait`` defaults to ``True``. The application code is welcome to call a series of methods, and Haigha will manage the stack and synchronous handshakes in the event loop.

This is a preview release, lacking full unit test coverage and documentation, and possibly including some errata. It is in production use however, and processes dozen of GBs per day of traffic.

Example
=======

See the ``scripts`` directory for several examples, in particular the ``stress_test`` script which you can use to test the performance of haigha against your broker. Below is a simple example of a client that connects, processes one message and quits. ::

  from haigha.connection import Connection
  from haigha.message import Message
  import event


  connection = Connection( 
    user='guest', password='guest', 
    vhost='/', host='localhost', 
    heartbeat=None, debug=True)

  def consumer(msg):
    print msg
    connection.close()
    event.timeout( 2, event.abort )

  ch = connection.channel()
  ch.exchange.declare('test_exchange', 'direct', auto_delete=True)
  ch.queue.declare('test_queue', auto_delete=True)
  ch.queue.bind('test_queue', 'test_exchange', 'test_key')
  ch.basic.consume('test_queue', consumer)
  ch.basic.publish( Message('body', application_headers={'hello':'world'}),
    'test_exchange', 'test_key' )

  event.dispatch()

Future
======

Planned updates include substantially more documentation, and full unit test coverage. Please report bugs to https://github.com/agoragames/haigha.

Installation
============

To install using ``pip``,::

  pip install -r requirements.txt

Bug tracker
===========

If you have any suggestions, bug reports or annoyances please report them
to our issue tracker at https://github.com/agoragames/haigha/issues

License
=======

This software is licensed under the `New BSD License`. See the ``LICENSE.txt``
file in the top distribution directory for the full license text.

.. # vim: syntax=rst expandtab tabstop=4 shiftwidth=4 shiftround
