- Feature: Optional MQTT topic mapping in the structure report
- Status: Open
- Submit Date: 2026-11-05
- Authors: Niklas Ekström
- Type: Protocol
- PR:
- Version: 2.0

Summary
=======

This document proposes an optional ``MQTT`` object in the structure report.
The object maps SECoP accessibles onto MQTT topics.  It names the topics for
the structure report and for the live values of its accessibles.  A client
finds the SEC nodes that are present by subscribing to those structure
topics, and removes a node when its structure topic is withdrawn.

SECoP addresses a value by a module and an accessible.  MQTT addresses a
value by a topic.  The ``MQTT`` object records that correspondence.  SECoP
messages and MQTT publications are both carried over TCP or WebSocket.  A
JSON payload that is not a SECoP data report is the subject of RFC-109.


Goal
====

An ECS that speaks SECoP over MQTT needs a place to record which topic is
which accessible.  That place is the ``MQTT`` object in the structure report.

The payload on ``value_topic`` and on ``set_topic`` is a SECoP data report.
A device that already publishes that report can be connected by the topic
names alone.


Technical explanation
=====================

Provenance
----------

Octopy stores topic names under ``octopy.links`` with the keys
``value topic``, ``get topic``, and ``set topic``.

``MQTT`` is proposed as a predefined optional property of the SEC node and of
an accessible.  Until the specification lists it, a strict reading of the
custom-property rule rejects that name as a custom property.

Addressing
----------

SECoP addresses each value by an accessible.  On a SECoP connection that
address is written in the message, as ``<module>:<accessible>``.  In the
structure report the same address is the accessible's place in its module.
MQTT addresses each value by a topic.  The topic is part of the MQTT
protocol, in the same way the accessible name is part of a SECoP message.
The ``MQTT`` object maps an accessible onto the topics for its value, for a
fresh read, and for a write or a command.

SECoP messages and MQTT publications are both carried over TCP or over
WebSocket.  A SECoP connection can also use a serial link, which the
specification already allows.  This proposal adds the address mapping to
also allow for topics based communication.

On the SECoP connection a request (``describe``, ``read``, ``change``,
``do``, and the other actions) is followed by a reply, and in async mode an
``update`` may arrive without a new request.  The mapped topics carry the
same values.  A publication on the value topic corresponds to ``reply``,
``update``, ``changed``, or ``done``.  The get topic corresponds to
``read``.  The set topic corresponds to ``change`` or ``do``.

The ``MQTT`` property
---------------------

The property is a JSON object.  It may appear on the SEC node description and
on each accessible.  Unknown keys in the object are reserved for later
revisions and are ignored.

On the SEC node the members are:

``value_topic``
    Topic of this structure report, including its ``MQTT`` objects.
    The default is ``SECoP/structures/<equipment_id>``.

``get_topic``
    A publication on this topic asks for ``describe`` again for this SEC
    node.  The payload is ignored.
    The default is ``SECoP/structures/<equipment_id>/get``.
    When ``value_topic`` is present and this key is absent, the device does
    not support a get topic.

Note that opens up for any MQTT client to be the one who defines the structure
report, not just the SEC node. It can for example be the ECS or anyone in between

The node object has no ``set_topic``.

On an accessible the members are:

``value_topic``
    Topic of the live value for that accessible.
    The payload is the SECoP data report, ``[value, qualifiers]``.
    The default topic is ``<equipment_id>/<module>/<accessible>``.

``get_topic``
    A publication on this topic asks for a SECoP ``read`` of this accessible.
    Commands have no ``get_topic``.
    JSON ``null`` means there is no get topic.
    When ``value_topic`` is present and this key is absent, the device does
    not support a get topic. This device should publish data in a frequency
    defined by the implementer that is suitable for the application.

``set_topic``
    A publication on this topic is a SECoP ``change`` for a parameter and a
    SECoP ``do`` for a command.
    The payload is the SECoP data report, ``[argument, qualifiers]``.
    A parameter with ``readonly`` true has no ``set_topic``.
    JSON ``null`` means there is no set topic.

``error_topic``
    Topic of a SECoP error report for this accessible,
    ``[<error class>, <text>, <qualifiers>]``.
    JSON ``null`` or an absent key means there is no error topic.
    Then an error report is published on ``value_topic``.

Several accessibles may name the same ``value_topic``.  One publication on
that topic updates each of them.

Example, for one readable value::

    {
      "equipment_id": "test_node",
      "description": "CPU load.",
      "modules": {
        "cpu": {
          "interface_classes": ["Readable"],
          "accessibles": {
            "value": {
              "description": "current value",
              "readonly": true,
              "datainfo": {"type": "double"},
              "MQTT": {
                "value_topic": "testnode/cpuload",
                "get_topic": null,
                "set_topic": null
              }
            }
          }
        }
      },
      "MQTT": {
        "value_topic": "SECoP/structures/test_node/",
        "get_topic": "SECoP/structures/test_node/get"
      }
    }

What is published on a topic
-----------------------------

``value_topic`` of an accessible carries a SECoP data report, ``[value,
qualifiers]``.  Qualifiers may be an empty object, but a timestamp is
strongly recommended.  The publisher sends a data report at the rate it
chooses and may vary depending on the current mode of the device.
If the accessible has a ``get_topic``, a publication there is
answered by a data report on ``value_topic``.  A SECoP ``reply``,
``update``, ``changed``, or ``done`` for this accessible is published on
``value_topic`` as that data report.

A payload that is not a JSON array is read as ``[payload, {}]``.
A JSON array is a data report when its length is 1 or 2 and element 1 is
a JSON object or not present.
A JSON array of length 3 whose first element is a SECoP error class is an
error report, ``[<error class>, <text>, <qualifiers>]``.
If the accessible has an ``error_topic``, the error report is published
there and the value topic is not checked for an error report.
Any other array is handled as ``[payload, {}]``.

``get_topic`` of an accessible triggers ``read <module>:<accessible>``.
When the SEC node has no cached value and the accessible has a ``get_topic``,
it publishes ``{}`` on that topic and waits for a later value publication.

``set_topic`` of a parameter triggers ``change``.  ``set_topic`` of a command
triggers ``do``.  The publication is a data report
``[argument, {"t": <unix seconds>}]``.  The argument of the SECoP message is
the value in that report.

``value_topic`` of the SEC node carries the structure report.
``get_topic`` of the SEC node triggers ``describe`` for that
``equipment_id``.

Presence of a SEC node
----------------------

The default structure topic is one level,
``SECoP/structures/<equipment_id>``.  The equipment id contains no ``/``.
An ECS discovers which nodes are present by subscribing to

::

    SECoP/structures/+

The ``+`` is a single MQTT topic level.  It matches
``SECoP/structures/frappy_demo_node`` and does not match
``SECoP/structures/frappy_demo_node/get``.  On subscribe, the broker delivers
each retained structure report.  Later publications on those topics arrive
the same way.

A JSON object that contains a ``modules`` object is a structure report.  The
node is present.  The publisher sends it with the retain flag set.  The
broker stores that one message and delivers it to a subscriber that connects
later, so the subscriber learns the nodes that are already up without
waiting for the next publication.

The node is no longer present when that same topic carries a zero-length
payload, also with the retain flag set.  The payload has no bytes.  It is
not JSON, and it is not the JSON ``null`` used for an absent ``get_topic``,
``set_topic``, or ``error_topic``.  MQTT 3.1.1 treats a retained message
with a zero-length payload as a deletion of the retained message.  A
subscriber that is already connected receives that empty publication and
removes the node.  The broker then holds nothing for the topic, so a
subscriber that connects afterwards does not revive the node.  A SEC node
sends this on its structure ``value_topic`` before it disconnects.

This only works when the publisher shuts down cleanly.  If the process
stops without sending the zero-length payload, the broker keeps the last
structure report until some other client deletes it.  Subscribers that are
connected, and subscribers that connect later, still see a node that is
gone.

A node whose structure ``value_topic`` is not
``SECoP/structures/<equipment_id>`` is not found by the ``+`` subscription.
A client that already knows that topic still treats a zero-length payload
on it as withdrawal.

Specification changes
---------------------

If this RFC is accepted, the specification changes are:

- ``protocol/specification/descriptive.rst``: ``MQTT`` as an optional SEC-node
  property and as an optional accessible property, with ``value_topic``,
  ``get_topic``, ``set_topic``, and ``error_topic``.  State that a client
  discovers nodes on ``SECoP/structures/+``, that the structure report is
  retained, and that a zero-length retained payload on that topic withdraws
  the node.
- ``protocol/specification/transport.rst``: a short pointer that the ``MQTT``
  property maps accessibles onto MQTT topics, and that SECoP messages and
  MQTT publications are both carried over TCP or WebSocket.
  The SECoP messages themselves are unchanged.  State that withdrawal is a
  zero-length retained payload, and that a structure left behind by a
  publisher that did not shut down cleanly stays until it is deleted.
- ``protocol/specification/changes.rst``: one backwards-compatible bullet
  under Version 2.0.

No change to the V1.0 or V1.1 text.


Disadvantages, Alternatives
===========================


Alternatives
------------

The keys ``MQTT``, ``value_topic``, ``get_topic``, and ``set_topic`` could
have names better aligned with the rest of the SECoP structure report.
For example, ``MQTT`` could be renamed to ``mqtt``.  ``value_topic`` could
be renamed to ``data`` to match the data report.  ``get_topic`` could be
renamed to ``get`` or ``read`` to match the read message.  ``set_topic``
could be renamed to ``set`` or ``change`` to match the change message.

Discovery sees only the default structure topics.  A node that sets a
different ``value_topic`` does not appear on ``SECoP/structures/+``.


Open Questions
==============

Should a node that sets its own structure ``value_topic`` also publish the
same report on ``SECoP/structures/<equipment_id>``, so discovery still finds
it?

A retained structure stays on the broker when the publisher does not shut
down cleanly.  One remedy is to publish the structure report at a fixed
interval without the retain flag.  A subscriber that has not seen a new
report within that interval removes the node.  Nothing is left on the broker
for a later subscriber to revive, and a crash stops the publications by
itself.  A subscriber that connects between publications has to wait for the
next one before it learns that the node exists.

The text above specifies the retained report and the zero-length deletion.
The periodic non-retained report is not specified.  Supporting both at once
is possible: retain the report for a late subscriber, and also repeat it
without the retain flag so silence still means the node is gone.  A client
then has two signals for one fact, a deletion and a timeout, and a publisher
has to send both.  That is more machinery than either scheme alone.  Whether
the silence rule should replace the retained deletion, or stand beside it,
is open.

