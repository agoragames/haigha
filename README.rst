=====================================
 Haigha - AMQP libevent Python client
=====================================

:Version: 0.4.1
:Download: http://pypi.python.org/pypi/haigha
:Source: https://github.com/agoragames/haigha
:Keywords: python, amqp, rabbitmq, event, libevent

.. contents::
    :local:

.. _haigha-overview:

Overview
========

Haigha provides a simple to use client library for interacting with AMQP brokers. It currently supports the 0.9.1 protocol and is integration tested against the latest RabbitMQ 2.4.1 (see `errata <http://dev.rabbitmq.com/wiki/Amqp091Errata>`_). Haigha is a descendant of ``py-amqplib`` and owes much to its developers.

The goals of haigha are performance, simplicity, and adherence to the form and function of the AMQP protocol. It adds a few useful features, such as the ``ChannelPool`` class and ``Channel.publish_synchronous``, to ease use of powerful features in real-world applications.

By default, Haigha operates in a completely asynchronous mode, relying on callbacks to notify application code of responses from the broker. Where applicable, ``nowait`` defaults to ``True``. The application code is welcome to call a series of methods, and Haigha will manage the stack and synchronous handshakes in the event loop.

Documentation
=============

This file and the various files in the ``scripts`` directory serve as a simple introduction to haigha. For more complete documentation, see `DOCUMENTATION.rst <https://github.com/agoragames/haigha/blob/master/DOCUMENTATION.rst>`_.


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

The 0.3.0 series will focus on the following areas:

* Full unit test coverage
* Implementation of error response codes according to spec
* Add callback chains where they're missing
* Documentation, including doctstrings, API docs, and tutorials
* Bug fixes

By the 0.4.0 series the library should be feature-complete and well documented. We'll then switch our focus to libevent itself, with the goal of supporting `gevent <http://www.gevent.org/>`_, `libev <http://software.schmorp.de/pkg/libev.html>`_ and `pypy <http://pypy.org/>`_.

Haigha has been tested exclusively with Python 2.6 and 2.7, but we intend for it to work with the 3.x series as well. Please `report <http://pypi.python.org/pypi/haigha>`_ any issues you may have.

Installation
============

Haigha is available on `pypi <http://pypi.python.org/pypi/haigha>`_ and can be installed using ``pip`` ::

  pip install haigha

If installing from source:

* with development requirements (e.g. testing frameworks) ::

    pip install -r development.txt

* without development requirements ::

    pip install -r requirements.txt


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
