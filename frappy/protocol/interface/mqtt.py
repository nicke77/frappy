# *****************************************************************************
#
# This program is free software; you can redistribute it and/or modify it under
# the terms of the GNU General Public License as published by the Free Software
# Foundation; either version 2 of the License, or (at your option) any later
# version.
#
# This program is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
# FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more
# details.
#
# You should have received a copy of the GNU General Public License along with
# this program; if not, write to the Free Software Foundation, Inc.,
# 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
#
# Module authors:
#   Niklas Ekström
#
# *****************************************************************************
"""MQTT interface for a SEC node (RFC-108, RFC-109).

The node publishes its structure and live values, and answers ``get_topic``
and ``set_topic``.  An accessible with ``json_path`` is fed by the device
document on ``value_topic`` instead of a SECoP data report.
"""

import json
import time
from collections import defaultdict
from threading import Event

from frappy.client.mqttclient import PahoTransport
from frappy.errors import ConfigError, SECoPError
from frappy.mqtt import decode_value_payload, parse_mqtt_uri
from frappy.protocol.messages import COMMANDREPLY, COMMANDREQUEST, \
    EVENTREPLY, READREPLY, READREQUEST, WRITEREPLY, WRITEREQUEST


_VALUE_ACTIONS = {EVENTREPLY, READREPLY, WRITEREPLY, COMMANDREPLY}


def _parse_payload(payload):
    if isinstance(payload, (bytes, bytearray)):
        payload = payload.decode('utf-8')
    if isinstance(payload, str):
        return json.loads(payload)
    return payload


class MQTTServer:
    """Publish a SEC node on an MQTT broker and accept get and set."""

    def __init__(self, name, logger, options, srv):
        self.name = name
        self.log = logger
        self.dispatcher = srv.dispatcher
        self.secnode = srv.secnode
        try:
            self.info = parse_mqtt_uri(options.pop('uri'), require_target=False)
        except ValueError as exc:
            raise ConfigError(str(exc)) from exc
        self.transport = options.pop('transport', None)
        if self.transport is None:
            self.transport = PahoTransport(self.info)
        self.node_get = None
        self.by_ident = {}
        self.listeners = defaultdict(list)
        self._echo = None
        self._orig_change = None
        self._orig_do = None
        self._patched = False
        self._stopped = Event()
        self._closed = False
        try:
            self.open()
        except Exception:
            self.close()
            raise

    def open(self):
        self._patch_dispatcher()
        self.transport.start(self._on_message)
        self._index()
        self._publish_structure()
        if self.node_get:
            self.transport.subscribe(self.node_get)
        for topic in self.listeners:
            if topic != self.node_get:
                self.transport.subscribe(topic)
        self.dispatcher.add_connection(self)
        # publish the current parameter values and receive later updates
        self.dispatcher.handle_activate(self, None, None)
        self.log.info('MQTT interface connected to %s:%s', self.info['host'], self.info['port'])

    def serve_forever(self):
        self._stopped.wait()

    def shutdown(self):
        self._stopped.set()
        self.close()

    def close(self):
        if self._closed:
            return
        self._closed = True
        self._stopped.set()
        if self._patched:
            self.dispatcher.handle_change = self._orig_change
            self.dispatcher.handle_do = self._orig_do
            self._patched = False
        try:
            self.dispatcher.remove_connection(self)
        except Exception:
            pass
        try:
            self.transport.stop()
        except Exception:
            pass

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.shutdown()

    def _patch_dispatcher(self):
        """Publish a bare set when a SECoP change or do targets a json_path accessible."""
        dispatcher = self.dispatcher
        self._orig_change = dispatcher.handle_change
        self._orig_do = dispatcher.handle_do

        def handle_change(conn, specifier, data):
            result = self._orig_change(conn, specifier, data)
            if conn is not self:
                self._publish_bare_set(specifier, data)
            return result

        def handle_do(conn, specifier, data):
            result = self._orig_do(conn, specifier, data)
            if conn is not self:
                self._publish_bare_set(specifier, data)
                self.send_reply(result)
            return result

        dispatcher.handle_change = handle_change
        dispatcher.handle_do = handle_do
        self._patched = True

    def _index(self):
        description = self.secnode.descriptive_data
        node_mqtt = description.get('MQTT') or {}
        self.node_get = node_mqtt.get('get_topic')
        for modname, moddesc in description.get('modules', {}).items():
            module = self.secnode.modules.get(modname)
            if module is None:
                continue
            for export, entry in moddesc.get('accessibles', {}).items():
                mqtt = entry.get('MQTT')
                if not isinstance(mqtt, dict) or not mqtt.get('value_topic'):
                    continue
                internal = module.accessiblename2attr.get(export, export)
                binding = {
                    'module': modname,
                    'export': export,
                    'internal': internal,
                    'specifier': f'{modname}:{export}',
                    'is_command': internal in module.commands,
                    'json_path': mqtt.get('json_path'),
                    'value_topic': mqtt.get('value_topic'),
                    'get_topic': mqtt.get('get_topic'),
                    'set_topic': mqtt.get('set_topic'),
                    'error_topic': mqtt.get('error_topic'),
                }
                self.by_ident[binding['specifier']] = binding
                if binding['json_path']:
                    self.listeners[binding['value_topic']].append(('value', binding))
                if binding['get_topic']:
                    self.listeners[binding['get_topic']].append(('get', binding))
                if binding['set_topic']:
                    self.listeners[binding['set_topic']].append(('set', binding))

    def _publish_structure(self):
        description = self.secnode.get_descriptive_data('')
        topic = (description.get('MQTT') or {}).get('value_topic')
        if topic:
            self._publish(topic, description, retain=True)

    def _publish_bare_set(self, specifier, argument):
        binding = self.by_ident.get(specifier)
        if not binding or not binding['json_path'] or not binding['set_topic']:
            return
        self._publish(binding['set_topic'], argument, retain=False)

    def _publish(self, topic, payload, retain):
        # A broker can deliver our own publication back.  Ignore that echo.
        self._echo = (topic, payload)
        try:
            self.transport.publish(topic, payload, retain=retain)
        finally:
            self._echo = None

    def send_reply(self, msg):
        """Publish a SECoP value message as a retained data report."""
        if not msg:
            return
        action, specifier, data = msg
        if action.startswith('error_'):
            self._publish_error(specifier, data)
            return
        if action not in _VALUE_ACTIONS or not specifier or ':' not in specifier:
            return
        binding = self.by_ident.get(specifier)
        # json_path value topics carry the device document, not a data report
        if binding is None or binding['json_path'] or not binding['value_topic']:
            return
        if not isinstance(data, (list, tuple)) or not data:
            return
        qualifiers = data[1] if len(data) > 1 and isinstance(data[1], dict) else {}
        self._publish(binding['value_topic'], [data[0], qualifiers], retain=True)

    def _publish_error(self, specifier, data):
        """Publish a SECoP error report.

        With ``error_topic`` the report goes there and the value topic stays a
        value.  Without it, the report is published on ``value_topic``.
        """
        binding = self.by_ident.get(specifier)
        if binding is None or not isinstance(data, (list, tuple)) or len(data) < 2:
            self.log.error('mqtt error %s %r', specifier, data)
            return
        qualifiers = data[2] if len(data) > 2 and isinstance(data[2], dict) else {}
        report = [data[0], data[1] if isinstance(data[1], str) else str(data[1]), qualifiers]
        if binding['error_topic']:
            self._publish(binding['error_topic'], report, retain=False)
            return
        if binding['json_path'] or not binding['value_topic']:
            self.log.error('mqtt error %s %r', specifier, report)
            return
        self._publish(binding['value_topic'], report, retain=False)

    def _on_message(self, topic, payload):
        try:
            data = _parse_payload(payload)
        except (UnicodeError, json.JSONDecodeError, TypeError) as exc:
            self.log.warning('ignored non-JSON MQTT payload on %s (%s)', topic, exc)
            return
        if self._echo is not None and self._echo[0] == topic and self._echo[1] == data:
            return
        if topic == self.node_get:
            self._publish_structure()
        for role, binding in self.listeners.get(topic, ()):
            try:
                if role == 'get':
                    self._dispatch(READREQUEST, binding['specifier'], None)
                elif role == 'set':
                    self._handle_set(binding, data)
                elif role == 'value':
                    self._handle_value(binding, data)
            except Exception as exc:
                self.log.error('mqtt %s on %s failed: %s', role, topic, exc)

    def _handle_set(self, binding, payload):
        if binding['json_path']:
            argument = payload
        else:
            decoded = decode_value_payload(payload, None)
            if decoded is None:
                self.log.error('invalid set payload on %s', binding['set_topic'])
                return
            argument = decoded[0]
        action = COMMANDREQUEST if binding['is_command'] else WRITEREQUEST
        self._dispatch(action, binding['specifier'], argument)

    def _handle_value(self, binding, payload):
        if binding['is_command'] or not binding['json_path']:
            return
        decoded = decode_value_payload(payload, binding['json_path'])
        if decoded is None:
            self.log.warning('json path value missed for %s', binding['specifier'])
            return
        value, qualifiers = decoded
        module = self.secnode.modules.get(binding['module'])
        if module is None:
            return
        parameter = module.parameters.get(binding['internal'])
        if parameter is None:
            return
        try:
            value = parameter.datatype.import_value(value)
        except Exception as exc:
            self.log.warning('mqtt value rejected for %s (%s)', binding['specifier'], exc)
            return
        timestamp = qualifiers.get('t')
        if isinstance(timestamp, bool) or not isinstance(timestamp, (int, float)):
            timestamp = None
        module.announceUpdate(binding['internal'], value, timestamp=timestamp, validate=False)

    def _dispatch(self, action, specifier, data):
        try:
            result = self.dispatcher.handle_request(self, (action, specifier, data))
        except SECoPError as exc:
            self.log.error('mqtt %s %s: %s', action, specifier, exc)
            self._publish_error(specifier, [exc.name, str(exc), {'t': time.time()}])
            return
        self.send_reply(result)
