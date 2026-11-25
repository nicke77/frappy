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
"""SECoP client over MQTT (RFC-108, RFC-109).

``SecopClient`` uses this when its URI starts with ``mqtt://``.  The broker
connection speaks MQTT topics from the structure report.  It does not speak
the TCP message framing.
"""

import json
import time
from collections import defaultdict
from threading import Event, RLock

from frappy.errors import HardwareError, NoSuchCommandError, NoSuchModuleError, \
    NoSuchParameterError, ProtocolError, ReadOnlyError, make_secop_error
from frappy.mqtt import decode_value_payload, default_node_get_topic, \
    encode_set_payload, error_report, normalize_accessible_mqtt, \
    normalize_node_mqtt, parse_mqtt_uri
from frappy.protocol.messages import COMMANDREPLY, COMMANDREQUEST, \
    DESCRIPTIONREPLY, DESCRIPTIONREQUEST, DISABLEEVENTSREPLY, \
    DISABLEEVENTSREQUEST, ENABLEEVENTSREPLY, ENABLEEVENTSREQUEST, \
    HEARTBEATREPLY, HEARTBEATREQUEST, READREPLY, READREQUEST, WRITEREPLY, \
    WRITEREQUEST


def connect_mqtt(client, try_period):
    """Connect ``client`` to the broker described by its ``mqtt://`` URI."""
    session = getattr(client, '_mqtt', None)
    if session is not None and not session.closed and session.transport.connected():
        return
    info = parse_mqtt_uri(client.uri)
    transport = getattr(client, '_mqtt_transport', None)
    if transport is None:
        transport = PahoTransport(info)
    session = MqttSession(client, transport, info)
    client._mqtt = session
    try:
        session.open(try_period)
    except Exception:
        session.close()
        client._mqtt = None
        raise


def mqtt_request(client, action, ident=None, data=None):
    """Perform one SECoP request over the client's MQTT session."""
    client.connect()
    session = client._mqtt
    if session is None or session.closed:
        raise ConnectionError('connection shut down')
    return session.request(action, ident, data)


class MqttSession:
    """Subscriptions and request/reply matching for one MQTT SEC node."""

    def __init__(self, client, transport, info):
        self.client = client
        self.transport = transport
        self.info = info
        self.lock = RLock()
        self.structure_event = Event()
        self.structure = None
        self.structure_topic = info['structure_topic']
        self.structure_topics = {self.structure_topic}
        self.node_get_topic = None
        self.by_topic = {}
        self.error_by_topic = {}
        self.by_ident = {}
        self.waiters = defaultdict(list)
        self.apply_lock = RLock()
        self.started = False
        self.closed = False

    def open(self, try_period):
        client = self.client
        with client._lock:
            client._shutdown.clear()
            if client.online:
                client._set_state(True, 'reconnecting')
            else:
                client._set_state(False, 'connecting')
        self.closed = False
        self.started = False
        self.transport.start(self._on_message)
        self.structure_event.clear()
        self.transport.subscribe(self.structure_topic)
        if not self.structure_event.is_set():
            get_topic = None
            if self.info['equipment_id']:
                get_topic = default_node_get_topic(self.info['equipment_id'])
            if get_topic:
                self.transport.publish(get_topic, {})
            timeout = try_period if try_period else 10
            if not self.structure_event.wait(timeout):
                raise TimeoutError('no structure report')
        if self.closed:
            raise ConnectionError('connection shut down')
        if not isinstance(self.structure, dict):
            raise TimeoutError('no structure report')
        with self.apply_lock:
            self.started = True
            self._apply_structure(self.structure)
        with client._lock:
            if self.closed or client._shutdown.is_set():
                raise ConnectionError('connection shut down')
            if client.activate:
                client._set_state(True, 'activating')
            client._set_state(True, 'connected')
            client._running = True
        client.log.info('%s ready', client.nodename)

    def close(self):
        self.closed = True
        self.started = False
        self.structure_event.set()
        with self.lock:
            pending = [event for events in self.waiters.values() for event in events]
            topics = list(self.by_topic) + list(self.structure_topics)
        for event in pending:
            event.set()
        for topic in topics:
            try:
                self.transport.unsubscribe(topic)
            except Exception:
                pass
        try:
            self.transport.stop()
        except Exception:
            pass

    def request(self, action, ident=None, data=None):
        if action == DESCRIPTIONREQUEST:
            return self._describe(ident)
        if action == ENABLEEVENTSREQUEST:
            return ENABLEEVENTSREPLY, ident, None
        if action == DISABLEEVENTSREQUEST:
            return DISABLEEVENTSREPLY, ident, None
        if action == HEARTBEATREQUEST:
            if not self.transport.connected():
                raise ConnectionError('broker connection is down')
            return HEARTBEATREPLY, ident, None
        if action == READREQUEST:
            return self._transfer(ident, action, None, READREPLY)
        if action == WRITEREQUEST:
            return self._transfer(ident, action, data, WRITEREPLY)
        if action == COMMANDREQUEST:
            return self._transfer(ident, action, data, COMMANDREPLY)
        raise ProtocolError(f'unhandled message: {action!r}')

    def _describe(self, ident):
        if self.node_get_topic:
            self.structure_event.clear()
            self.transport.publish(self.node_get_topic, {})
            if not self.structure_event.wait(10):
                raise TimeoutError('no structure report')
            if self.closed:
                raise ConnectionError('connection shut down')
        return DESCRIPTIONREPLY, ident or '.', self.client.descriptive_data

    def _transfer(self, ident, action, argument, reply_action):
        binding = self._require_binding(ident, action)
        if action == READREQUEST:
            if not binding['has_get']:
                if binding['last'] is None:
                    raise HardwareError(f'{ident} has no get topic and no cached value')
                return self._reply(reply_action, ident, binding)
            topic = binding['get_topic']
            payload = {}
        else:
            if binding['is_command'] and action != COMMANDREQUEST:
                raise NoSuchParameterError(f'{ident} is a command')
            if not binding['is_command'] and action == COMMANDREQUEST:
                raise NoSuchCommandError(f'{ident} is not a command')
            if not binding['has_set']:
                if binding['readonly']:
                    raise ReadOnlyError(f'{ident} is readonly')
                raise HardwareError(f'{ident} has no set topic')
            topic = binding['set_topic']
            payload = encode_set_payload(argument, binding['json_path'], time.time())
        return self._publish_and_wait(binding, topic, payload, reply_action, ident)

    def _publish_and_wait(self, binding, topic, payload, reply_action, ident):
        event = Event()
        with self.lock:
            self.waiters[binding['key']].append(event)
        try:
            self.transport.publish(topic, payload)
            if not event.wait(10):
                raise TimeoutError('no response within 10s')
            if self.closed:
                raise ConnectionError('connection shut down')
            if binding.get('error') is not None:
                raise binding['error']
            if binding['last'] is None:
                raise TimeoutError('no response within 10s')
            return self._reply(reply_action, ident, binding)
        finally:
            with self.lock:
                waiters = self.waiters.get(binding['key'], [])
                if event in waiters:
                    waiters.remove(event)

    def _reply(self, action, ident, binding):
        value, qualifiers = binding['last']
        return action, ident, [value, qualifiers]

    def _require_binding(self, ident, action):
        with self.lock:
            return self._find(ident, action)

    def _find(self, ident, action):
        if not ident:
            raise ProtocolError(f'{action} requests need a specifier')
        binding = self.by_ident.get(ident)
        if binding is None and ':' not in ident:
            name = 'target' if action == WRITEREQUEST else 'value'
            binding = self.by_ident.get(f'{ident}:{name}')
        if binding is not None:
            return binding
        module = ident.split(':', 1)[0]
        if module not in self.client.modules:
            raise NoSuchModuleError(f'module {module!r} does not exist')
        if action == COMMANDREQUEST:
            raise NoSuchCommandError(f'{ident} has no MQTT mapping')
        raise NoSuchParameterError(f'{ident} has no MQTT mapping')

    def _on_message(self, topic, payload):
        try:
            data = _parse_payload(payload)
        except (UnicodeError, json.JSONDecodeError, TypeError) as exc:
            self.client.log.warning('ignored non-JSON MQTT payload on %s (%s)', topic, exc)
            return
        if topic in self.structure_topics:
            self._on_structure(data)
            return
        updates, events = self._take_errors(topic, data)
        value_updates, value_events = self._take_updates(topic, data)
        updates.extend(value_updates)
        events.extend(value_events)
        for module, param, value, timestamp, readerror in updates:
            try:
                self.client.updateValue(module, param, value, timestamp, readerror)
            except Exception as exc:
                self.client.log.warning(
                    'mqtt value rejected for %s:%s (%s)', module, param, exc)
        for event in events:
            event.set()

    def _on_structure(self, data):
        if not isinstance(data, dict) or not isinstance(data.get('modules'), dict):
            self.client.log.warning('ignored MQTT structure payload')
            return
        with self.lock:
            self.structure = data
            apply_now = self.started
        # Apply before waking a describe request, so the reply sees the new
        # structure.  The lock keeps two updates from interleaving.
        if apply_now:
            with self.apply_lock:
                try:
                    self._apply_structure(data)
                except Exception as exc:
                    self.client.log.warning('ignored MQTT structure update (%s)', exc)
        self.structure_event.set()

    def _apply_structure(self, data):
        self.client._init_descriptive_data(data)
        equipment_id = data.get('equipment_id')
        if equipment_id:
            self.client.nodename = equipment_id
        value_topic, get_topic = self._node_topics(data)
        self.node_get_topic = get_topic
        if value_topic and value_topic not in self.structure_topics:
            self.structure_topics.add(value_topic)
            self.structure_topic = value_topic
            self.transport.subscribe(value_topic)
        self._rebind(data)

    def _node_topics(self, data):
        equipment_id = data.get('equipment_id') or self.info['equipment_id']
        if isinstance(data.get('MQTT'), dict):
            mqtt = normalize_node_mqtt(data['MQTT'], equipment_id)
            return mqtt.get('value_topic'), mqtt.get('get_topic')
        # The structure names no node topics.  Keep the subscription used to
        # obtain it, and refresh through the default get topic when the
        # equipment id is known.
        get_topic = default_node_get_topic(equipment_id) if equipment_id else None
        return self.structure_topic, get_topic

    def _rebind(self, data):
        equipment_id = data.get('equipment_id') or self.info['equipment_id']
        by_topic = defaultdict(list)
        error_by_topic = defaultdict(list)
        by_ident = {}
        for modname, moddesc in data.get('modules', {}).items():
            module = self.client.modules.get(modname)
            if module is None:
                continue
            for aname, aentry in moddesc.get('accessibles', {}).items():
                raw = aentry.get('MQTT')
                if not isinstance(raw, dict):
                    continue
                internal = self.client.internalize_name(aname)
                if internal in module['commands']:
                    entry = module['commands'][internal]
                    is_command = True
                elif internal in module['parameters']:
                    entry = module['parameters'][internal]
                    is_command = False
                else:
                    continue
                readonly = bool(entry.get('readonly', False))
                mqtt = normalize_accessible_mqtt(
                    raw, equipment_id, modname, aname,
                    readonly=readonly, is_command=is_command)
                ident = f'{modname}:{aname}'
                binding = {
                    'module': modname,
                    'internal': internal,
                    'key': (modname, internal),
                    'is_command': is_command,
                    'readonly': readonly,
                    'datatype': entry.get('datatype'),
                    'json_path': mqtt.get('json_path'),
                    'has_get': mqtt.get('get_topic') is not None,
                    'has_set': mqtt.get('set_topic') is not None,
                    'get_topic': mqtt.get('get_topic'),
                    'set_topic': mqtt.get('set_topic'),
                    'error_topic': mqtt.get('error_topic'),
                    'value_topic': mqtt['value_topic'],
                    'last': None,
                    'error': None,
                }
                previous = self.by_ident.get(ident)
                if previous is not None:
                    binding['last'] = previous['last']
                    binding['error'] = previous['error']
                by_topic[mqtt['value_topic']].append(binding)
                if binding['error_topic']:
                    error_by_topic[binding['error_topic']].append(binding)
                by_ident[ident] = binding
        with self.lock:
            old_topics = set(self.by_topic) | set(self.error_by_topic)
            self.by_topic = by_topic
            self.error_by_topic = error_by_topic
            self.by_ident = by_ident
        new_topics = set(by_topic) | set(error_by_topic)
        for topic in new_topics - old_topics:
            self.transport.subscribe(topic)
        for topic in old_topics - new_topics:
            self.transport.unsubscribe(topic)

    def _take_updates(self, topic, data):
        updates = []
        events = []
        with self.lock:
            bindings = list(self.by_topic.get(topic, ()))
            for binding in bindings:
                # An error_topic carries the error report, so the value topic
                # does not have to be checked for one.
                if not binding['json_path'] and not binding['error_topic']:
                    report = error_report(data)
                    if report is not None:
                        self._apply_error(binding, report, updates, events)
                        continue
                decoded = decode_value_payload(data, binding['json_path'])
                if decoded is None:
                    if error_report(data) is None:
                        self.client.log.warning(
                            'json path value missed for %s:%s',
                            binding['module'], binding['internal'])
                    continue
                value, qualifiers = decoded
                try:
                    _check_datatype(binding, value)
                except Exception as exc:
                    self.client.log.warning(
                        'mqtt payload rejected for %s:%s (%s)',
                        binding['module'], binding['internal'], exc)
                    continue
                binding['error'] = None
                binding['last'] = (value, qualifiers)
                if not binding['is_command']:
                    updates.append((
                        binding['module'], binding['internal'], value,
                        _cache_timestamp(qualifiers), None))
                events.extend(list(self.waiters.get(binding['key'], ())))
        return updates, events

    def _take_errors(self, topic, data):
        updates = []
        events = []
        report = error_report(data)
        with self.lock:
            for binding in list(self.error_by_topic.get(topic, ())):
                if report is None:
                    self.client.log.warning(
                        'ignored non-error payload on %s', binding['error_topic'])
                    continue
                self._apply_error(binding, report, updates, events)
        return updates, events

    def _apply_error(self, binding, report, updates, events):
        name, text, qualifiers = report
        error = make_secop_error(name, text)
        binding['error'] = error
        if not binding['is_command']:
            updates.append((
                binding['module'], binding['internal'], None,
                _cache_timestamp(qualifiers), error))
        events.extend(list(self.waiters.get(binding['key'], ())))


def _check_datatype(binding, value):
    datatype = binding['datatype']
    if datatype is None:
        return
    if binding['is_command']:
        if datatype.result is not None:
            datatype.result.import_value(value)
        return
    datatype.import_value(value)


def _cache_timestamp(qualifiers):
    now = time.time()
    timestamp = qualifiers.get('t', now)
    if isinstance(timestamp, bool) or not isinstance(timestamp, (int, float)):
        return now
    return min(now, timestamp)


def _parse_payload(payload):
    if isinstance(payload, (bytes, bytearray)):
        payload = payload.decode('utf-8')
    if isinstance(payload, str):
        return json.loads(payload)
    return payload


class PahoTransport:
    """Broker connection used when no transport is injected."""

    def __init__(self, info):
        self.info = info
        self._client = None
        self._connected = Event()
        self._connect_error = None

    def start(self, on_message):
        try:
            import paho.mqtt.client as mqtt
        except ImportError as exc:
            raise HardwareError('paho-mqtt is required for mqtt:// connections') from exc

        def handle_connect(_client, _userdata, _flags, reason_code, _properties=None):
            if _reason_ok(reason_code):
                self._connected.set()
            else:
                self._connect_error = reason_code

        def handle_message(_client, _userdata, message):
            on_message(message.topic, message.payload)

        self._connected.clear()
        self._connect_error = None
        try:
            client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1)
        except AttributeError:
            client = mqtt.Client()
        if self.info['username'] is not None:
            client.username_pw_set(self.info['username'], self.info['password'])
        client.on_connect = handle_connect
        client.on_message = handle_message
        self._client = client
        try:
            client.connect(self.info['host'], self.info['port'], keepalive=60)
        except Exception:
            self._client = None
            raise
        client.loop_start()
        if not self._connected.wait(10):
            error = self._connect_error
            self.stop()
            if error is not None:
                raise ConnectionError(f'mqtt connect failed: {error}')
            raise TimeoutError('mqtt connect timed out')

    def subscribe(self, topic):
        result, _mid = self._client.subscribe(topic)
        if result != 0:
            raise ConnectionError(f'mqtt subscribe {topic} failed ({result})')

    def unsubscribe(self, topic):
        if self._client is not None:
            self._client.unsubscribe(topic)

    def publish(self, topic, payload, retain=False):
        info = self._client.publish(topic, json.dumps(payload), retain=retain)
        if info.rc != 0:
            raise ConnectionError(f'mqtt publish {topic} failed ({info.rc})')

    def connected(self):
        return self._client is not None and self._client.is_connected()

    def stop(self):
        client = self._client
        self._client = None
        if client is None:
            return
        try:
            client.loop_stop()
            client.disconnect()
        except Exception:
            pass


def _reason_ok(reason_code):
    try:
        return int(reason_code) == 0
    except (TypeError, ValueError):
        return not getattr(reason_code, 'is_failure', True)
