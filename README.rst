==========================================================
 Haigha - Synchronous and asynchronous AMQP client library
==========================================================

.. image:: https://travis-ci.org/agoragames/haigha.svg?branch=master
    :target: https://travis-ci.org/agoragames/haigha


:Version: 0.9.0
:Download: http://pypi.python.org/pypi/haigha
:Source: https://github.com/agoragames/haigha
:Keywords: python, amqp, rabbitmq, event, libevent, gevent

.. contents::
    :local:

.. _haigha-overview:

Overview
========

Haigha provides a simple to use client library for interacting with AMQP brokers. It currently supports the 0.9.1 protocol and is integration tested against the latest RabbitMQ 2.8.1 (see `errata <http://dev.rabbitmq.com/wiki/Amqp091Errata>`_). Haigha is a descendant of ``py-amqplib`` and owes much to its developers.

The goals of haigha are performance, simplicity, and adherence to the form and function of the AMQP protocol. It adds a few useful features, such as the ``ChannelPool`` class and ``Channel.publish_synchronous``, to ease use of powerful features in real-world applications.

By default, Haigha operates in a completely asynchronous mode, relying on callbacks to notify application code of responses from the broker. Where applicable, ``nowait`` defaults to ``True``. The application code is welcome to call a series of methods, and Haigha will manage the stack and synchronous handshakes in the event loop.

Starting with the 0.5.0 series, haigha natively supports 3 transport types; libevent, gevent and standard sockets. The socket implementation defaults to synchronous mode and is useful for an interactive console or scripting, and the gevent transport is the preferred asynchronous backend though it can also be used synchronously as well.

Documentation
=============

This file and the various files in the ``scripts`` directory serve as a simple introduction to haigha. For more complete documentation, see `DOCUMENTATION.rst <https://github.com/agoragames/haigha/blob/master/DOCUMENTATION.rst>`_.


Example
=======

See the ``scripts`` and ``examples`` directories for several examples, in particular the ``stress_test`` script which you can use to test the performance of haigha against your broker. Below is a simple example of a client that connects, processes one message and quits. ::

  from haigha.connection import Connection
  from haigha.message import Message

  connection = Connection( 
    user='guest', password='guest', 
    vhost='/', host='localhost', 
    heartbeat=None, debug=True)

  ch = connection.channel()
  ch.exchange.declare('test_exchange', 'direct')
  ch.queue.declare('test_queue', auto_delete=True)
  ch.queue.bind('test_queue', 'test_exchange', 'test_key')
  ch.basic.publish( Message('body', application_headers={'hello':'world'}),
    'test_exchange', 'test_key' )
  print ch.basic.get('test_queue')
  connection.close()

To use protocol extensions for RabbitMQ, initialize the connection with the ``haigha.connections.rabbit_connection.RabbitConnection`` class.

Roadmap
=======

* Documentation (there's always more)
* Improved error handling
* Implementation of error codes in the spec
* Testing and integration with brokers other than RabbitMQ
* Identify and improve inefficient code
* Edge cases in frame management
* Improvements to usabililty
* SSL
* Allow nowait when asynchronous transport but Connection put into synchronous mode.

Haigha has been tested exclusively with Python 2.6 and 2.7, but we intend for it to work with the 3.x series as well. Please `report <https://github.com/agoragames/haigha/issues>`_ any issues you may have.

Installation
============

Haigha is available on `pypi <http://pypi.python.org/pypi/haigha>`_ and can be installed using ``pip`` ::

  pip install haigha

If installing from source:

* with development requirements (e.g. testing frameworks) ::

    pip install -r development.txt

* without development requirements ::

    pip install -r requirements.txt

Note that haigha does not install either gevent or libevent support automatically. For libevent, haigha has been tested and deployed with the ``event-agora==0.4.1`` library.


Testing
=======

Unit tests can be run with either the included script, or with `nose <http://pypi.python.org/pypi/nose>`_ ::

  ./haigha$ scripts/test 
  ./haigha$ nosetests

There are two other testing scripts of note. ``rabbit_table_test`` is a simple integration test that confirms compliance with RabbitMQ `errata <http://dev.rabbitmq.com/wiki/Amqp091Errata>`_. The ``stress_test`` script is a valuable tool that offers load-testing capability similar to `Apache Bench <http://httpd.apache.org/docs/2.0/programs/ab.html>`_ or `Siege <http://www.joedog.org/index/siege-home>`_. It is used both to confirm the robustness of haigha, as well as benchmark hardware or a broker configuration.

Bug tracker
===========

If you have any suggestions, bug reports or annoyances please report them
to our issue tracker at https://github.com/agoragames/haigha/issues

License
=======

This software is licensed under the `New BSD License`. See the ``LICENSE.txt``
file in the top distribution directory for the full license text.

.. # vim: syntax=rst expandtab tabstop=4 shiftwidth=4 shiftround
