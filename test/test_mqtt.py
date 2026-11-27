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
"""MQTT topic mapping, JSON path, and the MQTT client."""

import json
from collections import defaultdict

import pytest

from frappy.client import SecopClient
from frappy.datatypes import FloatRange
from frappy.errors import BadValueError, ConfigError, HardwareError, NoSuchParameterError, \
    ProgrammingError, ReadOnlyError
from frappy.lib import generalConfig
from frappy.modulebase import HasAccessibles, Module
from frappy.mqtt import MQTTMapping, PathMiss, classify_structure_payload, \
    compile_json_path, decode_value_payload, encode_set_payload, \
    equipment_id_from_structure_topic, error_report, evaluate_json_path, \
    normalize_accessible_mqtt, normalize_node_mqtt, parse_mqtt_uri, \
    validate_mqtt_mapping
from frappy.params import Command, Parameter
from frappy.protocol.interface.mqtt import MQTTServer
from frappy.protocol.messages import DESCRIPTIONREPLY, DESCRIPTIONREQUEST, \
    ENABLEEVENTSREPLY, ENABLEEVENTSREQUEST, READREQUEST
from frappy.server import Server


class LoggerStub:
    def __init__(self):
        self.parent = self
        self.handlers = []

    def debug(self, fmt, *args):
        pass

    def getChild(self, *args, **kwds):
        return self

    def addHandler(self, *args):
        pass

    info = warning = exception = error = debug


class WithCommand(Module):
    @Command(FloatRange(), result=FloatRange())
    def go(self, arg):
        """run"""
        return arg


class FakeBroker:
    """In-memory pub/sub.  Subscribers receive UTF-8 JSON bytes."""

    def __init__(self):
        self.retained = {}
        self.subscribers = defaultdict(list)
        self.published = []
        self.on_publish = None

    def publish(self, topic, payload, retain=False):
        self._emit(topic, json.dumps(payload).encode('utf-8'), payload, retain)

    def publish_raw(self, topic, payload, retain=False):
        encoded = payload if isinstance(payload, (bytes, bytearray)) else payload.encode('utf-8')
        self._emit(topic, encoded, payload, retain)

    def _emit(self, topic, encoded, logged, retain):
        if retain:
            if encoded == b'':
                self.retained.pop(topic, None)
            else:
                self.retained[topic] = encoded
        self.published.append((topic, logged, retain))
        for callback in list(self.subscribers.get(topic, ())):
            callback(topic, encoded)
        if self.on_publish:
            self.on_publish(topic, logged, retain)


class FakeTransport:
    def __init__(self, broker):
        self.broker = broker
        self.on_message = None
        self.subs = set()
        self._connected = False

    def start(self, on_message):
        self.on_message = on_message
        self._connected = True

    def subscribe(self, topic):
        if topic in self.subs:
            return
        self.subs.add(topic)
        self.broker.subscribers[topic].append(self._deliver)
        if topic in self.broker.retained:
            self._deliver(topic, self.broker.retained[topic])

    def unsubscribe(self, topic):
        self.subs.discard(topic)
        callbacks = self.broker.subscribers.get(topic, [])
        if self._deliver in callbacks:
            callbacks.remove(self._deliver)

    def _deliver(self, topic, payload):
        if topic in self.subs and self.on_message is not None:
            self.on_message(topic, payload)

    def publish(self, topic, payload, retain=False):
        self.broker.publish(topic, payload, retain)

    def publish_raw(self, topic, payload, retain=False):
        self.broker.publish_raw(topic, payload, retain)

    def connected(self):
        return self._connected

    def stop(self):
        for topic in list(self.subs):
            self.unsubscribe(topic)
        self._connected = False


def test_json_path_steps():
    document = {'loadavg': [0.25, 0.5], 'a': {'b': 1}, 'b-c': 7, '0': 'name'}
    assert evaluate_json_path(document, '$') == document
    assert evaluate_json_path(document, '$.loadavg[0]') == 0.25
    assert evaluate_json_path(document, "$['b-c']") == 7
    assert evaluate_json_path(document, '$.a["b"]') == 1
    assert evaluate_json_path(document, "$['0']") == 'name'
    with pytest.raises(PathMiss):
        evaluate_json_path(document['loadavg'], "$['0']")
    assert compile_json_path('$[1].t') == (('index', 1), ('key', 't'))
    for path in ('$.*', '$..a', '$[?(@.a)]', '$[0:1]', '$[0,1]', 'loadavg', '$.',
                 "$['unterminated", '$.a[01]', '$[-1]', '$.a[ 0]'):
        with pytest.raises(BadValueError):
            compile_json_path(path)


def test_decode_data_report_and_json_path():
    assert decode_value_payload(1.5) == (1.5, {})
    assert decode_value_payload([1.5]) == (1.5, {})
    assert decode_value_payload([1.5, {}]) == (1.5, {})
    assert decode_value_payload([1.5, {'t': 2, 'e': 0.1}]) == (1.5, {'t': 2, 'e': 0.1})
    assert decode_value_payload([100, 'idle']) == ([100, 'idle'], {})
    assert decode_value_payload([[100, 'idle'], {'t': 4}]) == ([100, 'idle'], {'t': 4})
    assert decode_value_payload([1, 2, 3]) == ([1, 2, 3], {})
    assert error_report(['HardwareError', 'boom', {'t': 2}]) == (
        'HardwareError', 'boom', {'t': 2})
    assert error_report([1, 2, 3]) is None
    assert decode_value_payload(['HardwareError', 'boom', {'t': 2}]) is None
    assert decode_value_payload({'temp': 1}) == ({'temp': 1}, {})
    assert decode_value_payload([1, {'t': True, 'e': True}]) == (1, {})
    assert decode_value_payload([1, {'t': 1e11}])[1]['t'] == 1e11
    assert decode_value_payload([1, {'t': -2e11}])[1]['t'] == -2e8

    document = {'loadavg': [0.5, 0.2], 'timestamp': 1710000000000, 'note': 'ms'}
    assert decode_value_payload(document, {'value': '$.loadavg[0]', 't': '$.timestamp'}) == (
        0.5, {'t': 1710000000.0})
    assert decode_value_payload({'a': 1}, {'value': '$.missing'}) is None
    assert decode_value_payload({'a': 1}, {'value': '$.a', 't': '$.missing', 'e': '$.missing'}) == (1, {})
    assert decode_value_payload({'a': 1, 't': 'soon'}, {'value': '$.a', 't': '$.t'}) == (1, {})
    assert decode_value_payload({'a': 1, 'u': 'm'}, {'value': '$.a', 'u': '$.u'}) == (1, {'u': 'm'})
    assert decode_value_payload(document, {'value': '$'})[0] == document


def test_encode_set_payload():
    assert encode_set_payload(1.5, None, 10) == [1.5, {'t': 10}]
    assert encode_set_payload(1.5, {'value': '$.v'}, 10) == 1.5


def test_topic_defaults_and_validation():
    assert normalize_node_mqtt({}, 'eq') == {
        'value_topic': 'SECoP/structures/eq',
        'get_topic': 'SECoP/structures/eq/get',
    }
    assert normalize_node_mqtt({'value_topic': 'custom'}, 'eq') == {'value_topic': 'custom'}
    assert normalize_node_mqtt({'get_topic': None, 'extra': 1}, 'eq') == {
        'value_topic': 'SECoP/structures/eq',
        'get_topic': None,
        'extra': 1,
    }
    assert normalize_accessible_mqtt(
        {}, 'eq', 'mod', 'value', readonly=True, is_command=False) == {
            'value_topic': 'eq/mod/value'}
    assert normalize_accessible_mqtt(
        {'get_topic': None, 'set_topic': None}, 'eq', 'mod', 'go',
        readonly=False, is_command=True)['get_topic'] is None
    with pytest.raises(ConfigError):
        normalize_accessible_mqtt(
            {'set_topic': 'x'}, 'eq', 'mod', 'value', readonly=True, is_command=False)
    with pytest.raises(ConfigError):
        normalize_accessible_mqtt(
            {'get_topic': 'x'}, 'eq', 'mod', 'go', readonly=False, is_command=True)
    with pytest.raises(BadValueError):
        validate_mqtt_mapping({'value_topic': 'a/+'})
    with pytest.raises(BadValueError):
        validate_mqtt_mapping({'json_path': {'t': '$.t'}})
    with pytest.raises(BadValueError):
        validate_mqtt_mapping({'json_path': {'value': '$.*'}})
    assert MQTTMapping().validate(None) is None

    class Mod(HasAccessibles):
        value = Parameter('current', FloatRange(), MQTT={'get_topic': None})

    assert Mod.value.for_export()['MQTT'] == {'get_topic': None}

    with pytest.raises(ProgrammingError):
        class Bad(HasAccessibles):
            value = Parameter('current', FloatRange(), MQTT={'json_path': {'value': '$..a'}})


def test_parse_mqtt_uri():
    assert parse_mqtt_uri('mqtt://localhost/eq') == {
        'host': 'localhost',
        'port': 1883,
        'username': None,
        'password': None,
        'equipment_id': 'eq',
        'structure_topic': 'SECoP/structures/eq',
    }
    parsed = parse_mqtt_uri('mqtt://user:s%40cret@broker:1884/eq?topic=custom/struct')
    assert parsed['host'] == 'broker'
    assert parsed['port'] == 1884
    assert parsed['username'] == 'user'
    assert parsed['password'] == 's@cret'
    assert parsed['structure_topic'] == 'custom/struct'
    with pytest.raises(ValueError):
        parse_mqtt_uri('mqtt://localhost')
    broker_only = parse_mqtt_uri('mqtt://localhost:1883', require_target=False)
    assert broker_only['host'] == 'localhost'
    assert broker_only['port'] == 1883
    assert broker_only['equipment_id'] is None


def _server(tmp_path, source):
    generalConfig.testinit(confdir=[tmp_path], piddir=tmp_path)
    cfg = tmp_path / 'node_cfg.py'
    cfg.write_text(source)
    server = Server('node', LoggerStub(), cfgfiles=[str(cfg)], testonly=True)
    server._processCfg()
    return server


def test_structure_exports_mqtt_when_configured(tmp_path):
    server = _server(tmp_path, """
Node('test_node', 'CPU load.', 'tcp://10769', MQTT={})
Mod('cpu', 'frappy.modules.Readable', 'cpu module',
    value=Param(MQTT={'value_topic': 'testnode/cpuload', 'get_topic': None,
                      'set_topic': None,
                      'json_path': {'value': '$.loadavg[0]', 't': '$.timestamp'}}))
Mod('drive', 'frappy.modules.Writable', 'drive',
    target=Param(MQTT={'set_topic': 'test_node/drive/target/set'}))
Mod('cmd', 'test.test_mqtt.WithCommand', 'command module',
    go=Param(MQTT={'set_topic': 'test_node/cmd/go/set'}))
""")
    desc = server.secnode.get_descriptive_data('')
    assert desc['MQTT'] == {
        'value_topic': 'SECoP/structures/test_node',
        'get_topic': 'SECoP/structures/test_node/get',
    }
    value = desc['modules']['cpu']['accessibles']['value']['MQTT']
    assert value['value_topic'] == 'testnode/cpuload'
    assert value['get_topic'] is None
    assert value['set_topic'] is None
    assert value['json_path']['value'] == '$.loadavg[0]'
    assert 'MQTT' not in desc['modules']['cpu']['accessibles']['status']
    target = desc['modules']['drive']['accessibles']['target']['MQTT']
    assert target['value_topic'] == 'test_node/drive/target'
    assert target['set_topic'] == 'test_node/drive/target/set'
    assert 'get_topic' not in target
    command = desc['modules']['cmd']['accessibles']['go']['MQTT']
    assert command['value_topic'] == 'test_node/cmd/go'
    assert command['set_topic'] == 'test_node/cmd/go/set'
    assert 'get_topic' not in command


def test_structure_omits_mqtt_unless_configured(tmp_path):
    server = _server(tmp_path, """
Node('plain', 'no mqtt', 'tcp://10769')
Mod('cpu', 'frappy.modules.Readable', 'cpu module')
""")
    desc = server.secnode.get_descriptive_data('')
    assert 'MQTT' not in desc
    assert 'MQTT' not in desc['modules']['cpu']['accessibles']['value']


def test_structure_rejects_forbidden_topics(tmp_path):
    with pytest.raises(ConfigError):
        _server(tmp_path, """
Node('test_node', 'CPU load.', 'tcp://10769')
Mod('cpu', 'frappy.modules.Readable', 'cpu module',
    value=Param(MQTT={'set_topic': 'secret'}))
""")
    with pytest.raises(ConfigError):
        _server(tmp_path, """
Node('test_node', 'CPU load.', 'tcp://10769')
Mod('cmd', 'test.test_mqtt.WithCommand', 'command module',
    go=Param(MQTT={'get_topic': 'secret'}))
""")


def _client_structure():
    return {
        'equipment_id': 'test_node',
        'description': 'CPU load.',
        'MQTT': {
            'value_topic': 'SECoP/structures/test_node',
            'get_topic': 'SECoP/structures/test_node/get',
        },
        'modules': {
            'cpu': {
                'description': 'cpu',
                'accessibles': {
                    'value': {
                        'description': 'current value',
                        'readonly': True,
                        'datainfo': {'type': 'double'},
                        'MQTT': {
                            'value_topic': 'testnode/cpuload',
                            'get_topic': 'testnode/cpuload/get',
                            'set_topic': None,
                        },
                    },
                    'target': {
                        'description': 'target',
                        'readonly': False,
                        'datainfo': {'type': 'double'},
                        'MQTT': {
                            'value_topic': 'testnode/target',
                            'set_topic': 'testnode/target/set',
                        },
                    },
                    'setpoint': {
                        'description': 'setpoint from a device document',
                        'readonly': False,
                        'datainfo': {'type': 'double'},
                        'MQTT': {
                            'value_topic': 'testnode/setpoint',
                            'set_topic': 'testnode/setpoint/set',
                            'json_path': {'value': '$.v', 't': '$.t'},
                        },
                    },
                    'go': {
                        'description': 'run',
                        'datainfo': {
                            'type': 'command',
                            'argument': {'type': 'double'},
                            'result': {'type': 'double'},
                        },
                        'MQTT': {
                            'value_topic': 'testnode/go',
                            'set_topic': 'testnode/go/set',
                        },
                    },
                    'load': {
                        'description': 'load',
                        'readonly': True,
                        'datainfo': {'type': 'double'},
                        'MQTT': {
                            'value_topic': 'testnode/status',
                            'get_topic': None,
                            'set_topic': None,
                            'json_path': {'value': '$.loadavg[0]', 't': '$.timestamp'},
                        },
                    },
                    'idle': {
                        'description': 'second load',
                        'readonly': True,
                        'datainfo': {'type': 'double'},
                        'MQTT': {
                            'value_topic': 'testnode/status',
                            'json_path': {'value': '$.loadavg[1]'},
                        },
                    },
                    'plain': {
                        'description': 'not on MQTT',
                        'readonly': True,
                        'datainfo': {'type': 'double'},
                    },
                },
            },
        },
    }


def _connect(structure=None, retained_values=None):
    broker = FakeBroker()
    transport = FakeTransport(broker)
    broker.retained['SECoP/structures/test_node'] = json.dumps(
        structure if structure is not None else _client_structure()).encode('utf-8')
    for topic, payload in (retained_values or {}).items():
        broker.retained[topic] = json.dumps(payload).encode('utf-8')
    client = SecopClient('mqtt://localhost/test_node', LoggerStub())
    client._mqtt_transport = transport
    client.connect()
    return client, broker


def test_client_retained_values_and_shared_topic():
    client, broker = _connect(retained_values={
        'testnode/cpuload': [21.5, {'t': 1710000000}],
        'testnode/status': {'loadavg': [0.5, 0.2], 'timestamp': 1710000000000},
    })
    try:
        assert client.nodename == 'test_node'
        assert client.cache['cpu', 'value'].value == 21.5
        assert client.cache['cpu', 'value'].timestamp == 1710000000
        assert client.cache['cpu', 'load'].value == 0.5
        assert client.cache['cpu', 'load'].timestamp == 1710000000.0
        assert client.cache['cpu', 'idle'].value == 0.2
        assert client.request(ENABLEEVENTSREQUEST)[0] == ENABLEEVENTSREPLY
        assert [topic for topic, _payload, _retain in broker.published] == []
        # a path miss leaves the previous values in place
        broker.publish('testnode/status', {'loadavg': []})
        assert client.cache['cpu', 'load'].value == 0.5
        assert client.cache['cpu', 'idle'].value == 0.2
    finally:
        client.disconnect()


def test_client_read_change_and_do():
    client, broker = _connect()
    try:
        def on_publish(topic, payload, _retain):
            if topic == 'testnode/cpuload/get':
                broker.publish('testnode/cpuload', [22.0, {'t': 1710000001, 'e': 0.2}])
            elif topic == 'testnode/target/set':
                assert payload[0] == 4.5
                assert 't' in payload[1]
                broker.publish('testnode/target', [4.5, {'t': 1710000002}])
            elif topic == 'testnode/setpoint/set':
                assert payload == 3.5
                broker.publish('testnode/setpoint', {'v': 3.5, 't': 1710000003})
            elif topic == 'testnode/go/set':
                assert payload[0] == 1.25
                broker.publish('testnode/go', [9.0, {'t': 1710000004}])

        broker.on_publish = on_publish
        assert client.readParameter('cpu', 'value').value == 22.0
        assert client.setParameter('cpu', 'target', 4.5).value == 4.5
        assert client.setParameter('cpu', 'setpoint', 3.5).value == 3.5
        result, qualifiers = client.execCommand('cpu', 'go', 1.25)
        assert result == 9.0
        assert qualifiers['t'] == 1710000004
        with pytest.raises(ReadOnlyError):
            client.setParameter('cpu', 'value', 1)
        with pytest.raises(NoSuchParameterError):
            client.request(READREQUEST, 'cpu:plain')
        with pytest.raises(HardwareError):
            client.request(READREQUEST, 'cpu:load')
    finally:
        client.disconnect()


def test_client_requests_structure_when_none_is_retained():
    broker = FakeBroker()
    transport = FakeTransport(broker)
    structure = _client_structure()

    def on_publish(topic, _payload, _retain):
        if topic == 'SECoP/structures/test_node/get':
            broker.publish('SECoP/structures/test_node', structure, retain=True)

    broker.on_publish = on_publish
    client = SecopClient('mqtt://localhost/test_node', LoggerStub())
    client._mqtt_transport = transport
    client.connect(try_period=2)
    try:
        assert client.nodename == 'test_node'
        reply = client.request(DESCRIPTIONREQUEST)
        assert reply[0] == DESCRIPTIONREPLY
        assert reply[2]['equipment_id'] == 'test_node'
    finally:
        client.disconnect()


class _Conn:
    def send_reply(self, msg):
        pass


def _published(broker, start, topic):
    return [payload for item_topic, payload, _retain in broker.published[start:]
            if item_topic == topic]


def test_mqtt_interface_fills_topic_defaults(tmp_path):
    server = _server(tmp_path, """
Node('eq', 'mqtt node', 'tcp://10769', secondary=['mqtt://localhost:1883'])
Mod('cpu', 'frappy.modules.Readable', 'cpu')
""")
    desc = server.secnode.get_descriptive_data('')
    assert desc['MQTT'] == {
        'value_topic': 'SECoP/structures/eq',
        'get_topic': 'SECoP/structures/eq/get',
    }
    value = desc['modules']['cpu']['accessibles']['value']['MQTT']
    assert value == {'value_topic': 'eq/cpu/value'}
    assert 'MQTT' in desc['modules']['cpu']['accessibles']['status']


def test_mqtt_server_publishes_and_accepts_get_and_set(tmp_path):
    server = _server(tmp_path, """
Node('eq', 'mqtt node', 'tcp://10769', secondary=['mqtt://localhost:1883'])
Mod('drv', 'frappy.modules.Writable', 'drive',
    value=Param(MQTT={'get_topic': 'eq/drv/value/get'}),
    target=Param(min=0, MQTT={'set_topic': 'eq/drv/target/set',
                             'error_topic': 'eq/drv/target/error'}))
Mod('cmd', 'test.test_mqtt.WithCommand', 'command',
    go=Param(MQTT={'set_topic': 'eq/cmd/go/set'}))
Mod('ext', 'frappy.modules.Writable', 'external',
    target=Param(MQTT={'value_topic': 'device/target',
                       'set_topic': 'device/target/set',
                       'json_path': {'value': '$.v'}}))
""")
    broker = FakeBroker()
    transport = FakeTransport(broker)
    mqtt = MQTTServer('mqtt', LoggerStub(), {
        'uri': 'mqtt://localhost:1883',
        'transport': transport,
    }, server)
    try:
        structure = json.loads(broker.retained['SECoP/structures/eq'])
        assert structure['equipment_id'] == 'eq'
        broker.retained['SECoP/structures/eq'] = b'{}'
        broker.publish('SECoP/structures/eq/get', {})
        assert json.loads(broker.retained['SECoP/structures/eq'])['equipment_id'] == 'eq'

        server.secnode.modules['drv'].announceUpdate('value', 21.5)
        reports = [payload for topic, payload, retain in broker.published
                   if topic == 'eq/drv/value' and retain]
        assert reports[-1][0] == 21.5
        assert isinstance(reports[-1][1], dict)

        start = len(broker.published)
        broker.publish('eq/drv/value/get', {'ignored': True})
        assert _published(broker, start, 'eq/drv/value')[-1][0] == 21.5

        start = len(broker.published)
        broker.publish('eq/drv/target/set', [5.5, {'t': 10}])
        assert server.secnode.modules['drv'].target == 5.5
        assert _published(broker, start, 'eq/drv/target')[-1][0] == 5.5

        start = len(broker.published)
        broker.publish('eq/drv/target/set', [-2])
        assert server.secnode.modules['drv'].target == 5.5
        rejected = _published(broker, start, 'eq/drv/target/error')
        assert rejected[-1][0] == 'RangeError'
        assert _published(broker, start, 'eq/drv/target') == []

        start = len(broker.published)
        broker.publish('eq/cmd/go/set', [1.25, {'t': 10}])
        assert _published(broker, start, 'eq/cmd/go')[-1][0] == 1.25

        start = len(broker.published)
        broker.publish('device/target', {'v': 3.25})
        assert server.secnode.modules['ext'].target == 3.25
        assert _published(broker, start, 'device/target') == [{'v': 3.25}]

        start = len(broker.published)
        mqtt.dispatcher.handle_change(_Conn(), 'ext:target', 4.5)
        assert _published(broker, start, 'device/target/set') == [4.5]
        assert _published(broker, start, 'device/target') == []
    finally:
        mqtt.shutdown()


def test_client_error_report_on_value_or_error_topic():
    structure = {
        'equipment_id': 'test_node',
        'description': 'errors',
        'modules': {
            'cpu': {
                'description': 'cpu',
                'accessibles': {
                    'value': {
                        'description': 'plain',
                        'readonly': True,
                        'datainfo': {'type': 'double'},
                        'MQTT': {
                            'value_topic': 'testnode/plain',
                            'get_topic': None,
                            'set_topic': None,
                        },
                    },
                    'guarded': {
                        'description': 'with error topic',
                        'readonly': True,
                        'datainfo': {'type': 'double'},
                        'MQTT': {
                            'value_topic': 'testnode/guarded',
                            'error_topic': 'testnode/guarded/error',
                        },
                    },
                },
            },
        },
    }
    client, broker = _connect(structure, {
        'testnode/plain': [1.5, {}],
        'testnode/guarded': [2.5, {}],
    })
    try:
        broker.publish('testnode/plain', ['HardwareError', 'boom', {'t': 1710000000}])
        assert client.cache['cpu', 'value'].readerror.name == 'HardwareError'
        assert str(client.cache['cpu', 'value'].readerror) == 'boom'
        assert client.cache['cpu', 'value'].timestamp == 1710000000

        broker.publish('testnode/guarded', ['HardwareError', 'ignored', {'t': 1}])
        assert client.cache['cpu', 'guarded'].value == 2.5
        assert client.cache['cpu', 'guarded'].readerror is None
        broker.publish('testnode/guarded/error', ['ReadOnly', 'locked', {'t': 1710000001}])
        assert client.cache['cpu', 'guarded'].readerror.name == 'ReadOnly'
        assert client.cache['cpu', 'guarded'].value is None
    finally:
        client.disconnect()


def test_server_publishes_error_report(tmp_path):
    server = _server(tmp_path, """
Node('eq', 'mqtt node', 'tcp://10769', secondary=['mqtt://localhost:1883'])
Mod('plain', 'frappy.modules.Readable', 'plain')
Mod('guarded', 'frappy.modules.Readable', 'guarded',
    value=Param(MQTT={'error_topic': 'eq/guarded/value/error'}))
""")
    broker = FakeBroker()
    transport = FakeTransport(broker)
    mqtt = MQTTServer('mqtt', LoggerStub(), {
        'uri': 'mqtt://localhost:1883',
        'transport': transport,
    }, server)
    try:
        server.secnode.modules['plain'].announceUpdate('value', err=HardwareError('boom'))
        plain = [payload for topic, payload, retain in broker.published
                 if topic == 'eq/plain/value' and not retain]
        assert plain[-1][0] == 'HardwareError'
        assert plain[-1][1] == 'boom'

        start = len(broker.published)
        server.secnode.modules['guarded'].announceUpdate('value', err=HardwareError('hidden'))
        assert _published(broker, start, 'eq/guarded/value/error')[-1][0] == 'HardwareError'
        assert _published(broker, start, 'eq/guarded/value') == []
    finally:
        mqtt.shutdown()


def test_structure_payload_empty_withdraws_and_null_does_not():
    assert classify_structure_payload(b'') == 'withdraw'
    assert classify_structure_payload('') == 'withdraw'
    assert classify_structure_payload(b'null') is None
    assert classify_structure_payload('null') is None
    assert classify_structure_payload(None) is None
    assert classify_structure_payload(b'"null"') is None
    assert classify_structure_payload(b'[]') is None
    assert equipment_id_from_structure_topic('SECoP/structures/frappy_demo_node') == (
        'frappy_demo_node')
    assert equipment_id_from_structure_topic('SECoP/structures/frappy_demo_node/get') is None


def test_client_structure_withdrawn_by_empty_payload():
    client, broker = _connect()
    try:
        broker.publish('SECoP/structures/test_node', None, retain=True)
        assert client.state == 'connected'
        assert client.online is True
        broker.publish_raw('SECoP/structures/test_node', b'', retain=True)
        assert client.state == 'withdrawn'
        assert client.online is False
        assert 'SECoP/structures/test_node' not in broker.retained
    finally:
        client.disconnect()


def test_client_retained_null_is_not_a_structure():
    broker = FakeBroker()
    transport = FakeTransport(broker)
    broker.retained['SECoP/structures/test_node'] = b'null'
    client = SecopClient('mqtt://localhost/test_node', LoggerStub())
    client._mqtt_transport = transport
    with pytest.raises(TimeoutError):
        client.connect(try_period=0.05)
    client.disconnect()

    broker = FakeBroker()
    transport = FakeTransport(broker)
    broker.retained['SECoP/structures/test_node'] = b''
    client = SecopClient('mqtt://localhost/test_node', LoggerStub())
    client._mqtt_transport = transport
    with pytest.raises(TimeoutError):
        client.connect(try_period=0.05)
    client.disconnect()


def test_server_withdraws_structure_on_shutdown(tmp_path):
    server = _server(tmp_path, """
Node('eq', 'mqtt node', 'tcp://10769', secondary=['mqtt://localhost:1883'])
Mod('cryo', 'frappy.modules.Readable', 'cryo')
""")
    broker = FakeBroker()
    transport = FakeTransport(broker)
    mqtt = MQTTServer('mqtt', LoggerStub(), {
        'uri': 'mqtt://localhost:1883',
        'transport': transport,
    }, server)
    assert 'SECoP/structures/eq' in broker.retained
    mqtt.shutdown()
    withdrawal = [item for item in broker.published if item[0] == 'SECoP/structures/eq']
    assert withdrawal[-1] == ('SECoP/structures/eq', b'', True)
    assert all(item[1] != b'' for item in withdrawal[:-1])
    assert 'SECoP/structures/eq' not in broker.retained


def test_client_bad_structure_times_out():
    broker = FakeBroker()
    transport = FakeTransport(broker)
    broker.retained['custom'] = json.dumps([]).encode('utf-8')
    client = SecopClient('mqtt://localhost?topic=custom', LoggerStub())
    client._mqtt_transport = transport
    with pytest.raises(TimeoutError):
        client.connect(try_period=0.05)
    client.disconnect()
