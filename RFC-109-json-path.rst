- Feature: JSON path for values published on MQTT topics
- Status: Open
- Submit Date: 2026-11-11
- Authors: Niklas Ekström
- Type: Protocol
- PR:
- Version: 2.0

Summary
=======

This document proposes an optional ``json_path`` member of the accessible
``MQTT`` object from RFC-108.  The member tells an ECS how to read the value
and the qualifiers from a JSON document that is not a SECoP data report.

The path only addresses children of that document.  That is the subset the
common JSONPath implementations share.


Goal
====

An off-the-shelf device often publishes a JSON document of its own design on
an MQTT topic.  An ECS that speaks SECoP over MQTT needs to read the value
and the qualifiers from that document, without a translator in front of the
topic.

The qualifiers defined today are the timestamp ``t`` and the uncertainty
``e``.  A qualifier added to SECoP later is a further member of
``json_path``, with the same name as the qualifier.


Technical explanation
=====================

``json_path`` is an optional member of the accessible ``MQTT`` object defined
in RFC-108.  It is a JSON object.  ``value`` selects the accessible value.
Each other member name is a qualifier name, and its value is the path to that
qualifier.  The members for the qualifiers defined today are::

    {"value": "<path>", "t": "<path>", "e": "<path>"}

``value`` is required when the object is present.  ``t`` and ``e`` are
optional.  The path ``$`` means the whole JSON document.  The selected value
is then coerced with the accessible's ``datainfo`` (number, string, bool,
enum member, or tuple).  A timestamp selected by ``t`` becomes the qualifier
``t``, in UNIX seconds.  A numeric timestamp whose absolute value is greater
than ``1e11`` is treated as milliseconds and divided by 1000.  A number
selected by ``e`` becomes the qualifier ``e``, in the same units as the value.

When ``json_path`` is omitted, the payload is the SECoP data report required
by RFC-108.  That report is the same as these paths:

- ``value``: ``$[0]``
- ``t``: ``$[1].t``
- ``e``: ``$[1].e``

When ``json_path`` is present, the payload on ``value_topic`` is the device's
JSON document, and each path is applied to that document.  A publication on
``set_topic`` is the bare argument.  The SECoP ``change`` or ``do`` uses that
argument as its value.

Child steps
-----------

A path addresses children of the JSON value, one step at a time, and selects
that one node.  These steps are the ones the common JSONPath implementations
share, including IETF RFC 9535 and the libraries that follow Stefan
Goessner's notation:

- ``$`` for the whole document
- ``.name`` for a child whose name is an identifier (a letter or underscore,
  then letters, digits, or underscores)
- ``['name']`` or ``["name"]`` for a child with any name
- ``[n]`` for the child at array index ``n``, where ``n`` is a non-negative
  integer

Steps combine, as in ``$.loadavg[0]`` and ``$[1].t``.  An array element is
written ``[0]``.  A child whose name is ``"0"`` is written ``['0']``.

A receiver can implement this by walking the JSON value, or by calling a
JSONPath library and taking the single selected node.  Filters, wildcards,
recursive descent, slices, and unions are left out.  A structure report must
not rely on them.

Example
-------

::

    "MQTT": {
      "value_topic": "testnode/cpuload",
      "get_topic": null,
      "set_topic": null,
      "json_path": {
        "value": "$.loadavg[0]",
        "t": "$.timestamp"
      }
    }

The publication on ``testnode/cpuload`` is the device's JSON document.  The
accessible value is the number at ``loadavg[0]``.  The qualifier ``t`` is the
number at ``timestamp``.

Specification changes
---------------------

If this RFC is accepted, together with RFC-108, the specification changes are:

- ``protocol/specification/descriptive.rst``: ``json_path`` as an optional
  member of the accessible ``MQTT`` property.
- ``protocol/specification/changes.rst``: one backwards-compatible bullet
  under Version 2.0.

No change to the V1.0 or V1.1 text.


Disadvantages, Alternatives
===========================

Disadvantages
-------------

``json_path`` is a second syntax beside the SECoP data report.  It is limited
to child steps so that the common JSONPath implementations select the same
node.  A payload that is only reachable by a filter or a search across the
document has no path in this specification.

Alternatives
------------

The key ``json_path`` could be renamed to ``jsonpath``.  The member ``value``
could be renamed to ``data``, to match the data report and to keep it
distinct from the SECoP parameter ``value``.

JSON path levels
~~~~~~~~~~~~~~~~

Child steps, as specified above, are the subset the common implementations
already share.  Other levels were considered.

Full JSONPath, as in IETF RFC 9535.
    One written standard, including filters, slices, and functions.  Many
    small implementations only walk children.  A structure report that used a
    filter would be read by some clients and missed by others.

One library's JSONPath.
    jsonpath-plus and Jayway JsonPath each accept filters and other
    extensions of their own.  Choosing one library covers that library and
    leaves the others out.

No path language.
    This is RFC-108.  The MQTT payload is a SECoP data report, and the
    accessible is addressed only by its topic.  A device that publishes its
    own JSON needs a translator in front of the topic.


Open Questions
==============

- The proposed ``json_path`` object uses ``value`` for the accessible value.
  Should that member be named ``data``, to match the SECoP data report?
