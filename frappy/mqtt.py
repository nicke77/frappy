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
"""MQTT topic mapping and JSON path (RFC-108, RFC-109).

This module validates the ``MQTT`` object stored in the structure report and
decodes publications for the client.
"""

import json
import re
from urllib.parse import parse_qs, unquote, urlparse

from frappy.errors import BadValueError, ConfigError, SECoPError


_IDENT = re.compile(r'[A-Za-z_][A-Za-z0-9_]*')
_INDEX = re.compile(r'(0|[1-9][0-9]*)\]')
_ESCAPES = {
    '"': '"',
    '\\': '\\',
    '/': '/',
    'b': '\b',
    'f': '\f',
    'n': '\n',
    'r': '\r',
    't': '\t',
}


class PathMiss(Exception):
    """A JSON path did not select a node."""


# One topic level under this prefix is a SEC node's default structure topic.
DISCOVERY_STRUCTURE_TOPIC = 'SECoP/structures/+'
_STRUCTURE_TOPIC_PREFIX = 'SECoP/structures/'


def default_node_value_topic(equipment_id):
    return f'SECoP/structures/{equipment_id}'


def default_node_get_topic(equipment_id):
    return f'SECoP/structures/{equipment_id}/get'


def default_accessible_value_topic(equipment_id, module, accessible):
    return f'{equipment_id}/{module}/{accessible}'


def equipment_id_from_structure_topic(topic):
    """Return the equipment id for a default structure topic.

    ``SECoP/structures/<equipment_id>`` matches.  A get topic, or any topic
    with a further slash, does not.
    """
    if not isinstance(topic, str) or not topic.startswith(_STRUCTURE_TOPIC_PREFIX):
        return None
    equipment_id = topic[len(_STRUCTURE_TOPIC_PREFIX):]
    if not equipment_id or '/' in equipment_id:
        return None
    return equipment_id


def classify_structure_payload(payload):
    """Classify a publication on a structure topic.

    A zero-length payload withdraws the node.  That is the MQTT deletion of a
    retained message, not a JSON value.  JSON ``null`` is ignored here.  A
    JSON object with a ``modules`` object is a structure report.
    """
    if isinstance(payload, (bytes, bytearray)):
        if len(payload) == 0:
            return 'withdraw'
        try:
            payload = payload.decode('utf-8')
        except UnicodeError:
            return None
    if isinstance(payload, str):
        if payload == '':
            return 'withdraw'
        try:
            payload = json.loads(payload)
        except json.JSONDecodeError:
            return None
    if isinstance(payload, dict) and isinstance(payload.get('modules'), dict):
        return payload
    return None


def is_mqtt_uri(uri):
    return isinstance(uri, str) and uri.startswith('mqtt://')


def parse_mqtt_uri(uri, require_target=True):
    """Parse ``mqtt://[user[:password]@]host[:port][/equipment_id][?topic=...]``.

    The structure subscription is ``topic`` when the query gives one, otherwise
    ``SECoP/structures/<equipment_id>``.  The port defaults to 1883.

    A SEC node broker URI has no equipment id.  Pass ``require_target=False``
    for that form.  The client URI still requires an equipment id or a topic.
    """
    parsed = urlparse(uri)
    if parsed.scheme != 'mqtt' or not parsed.hostname:
        raise ValueError(f'invalid mqtt uri {uri!r}')
    equipment_id = unquote(parsed.path[1:]) if parsed.path.startswith('/') else unquote(parsed.path)
    query = parse_qs(parsed.query)
    topic = query.get('topic', [None])[0]
    if topic:
        topic = unquote(topic)
    if not topic:
        if equipment_id:
            topic = default_node_value_topic(equipment_id)
        elif require_target:
            raise ValueError('mqtt uri needs an equipment id or a topic query')
    return {
        'host': parsed.hostname,
        'port': parsed.port or 1883,
        'username': unquote(parsed.username) if parsed.username else None,
        'password': unquote(parsed.password) if parsed.password else None,
        'equipment_id': equipment_id or None,
        'structure_topic': topic,
    }


def compile_json_path(path):
    """Compile a child-step JSON path into ``('key', name)`` and ``('index', n)`` steps.

    The accepted steps are ``$``, ``.name``, ``['name']``, ``["name"]`` and ``[n]``.
    """
    if not isinstance(path, str) or not path.startswith('$'):
        raise BadValueError(f'json path must start with $: {path!r}')
    steps = []
    i = 1
    length = len(path)
    while i < length:
        char = path[i]
        if char == '.':
            match = _IDENT.match(path, i + 1)
            if not match:
                raise BadValueError(f'unsupported json path step in {path!r}')
            steps.append(('key', match.group(0)))
            i = match.end()
        elif char == '[':
            step, i = _compile_bracket(path, i + 1)
            steps.append(step)
        else:
            raise BadValueError(f'unsupported json path step in {path!r}')
    return tuple(steps)


def _compile_bracket(path, i):
    if i >= len(path):
        raise BadValueError(f'unsupported json path step in {path!r}')
    if path[i] in ('"', "'"):
        name, i = _decode_quoted(path, i)
        if i >= len(path) or path[i] != ']':
            raise BadValueError(f'unsupported json path step in {path!r}')
        return ('key', name), i + 1
    match = _INDEX.match(path, i)
    if not match:
        raise BadValueError(f'unsupported json path step in {path!r}')
    return ('index', int(match.group(1))), match.end()


def _decode_quoted(path, i):
    quote = path[i]
    i += 1
    chars = []
    while i < len(path):
        char = path[i]
        if char == '\\':
            if i + 1 >= len(path):
                raise BadValueError(f'bad escape in json path {path!r}')
            escaped = path[i + 1]
            if escaped == 'u':
                hexdigits = path[i + 2:i + 6]
                if len(hexdigits) != 4 or any(c not in '0123456789abcdefABCDEF' for c in hexdigits):
                    raise BadValueError(f'bad escape in json path {path!r}')
                chars.append(chr(int(hexdigits, 16)))
                i += 6
                continue
            if escaped not in _ESCAPES:
                raise BadValueError(f'bad escape in json path {path!r}')
            chars.append(_ESCAPES[escaped])
            i += 2
            continue
        if char == quote:
            return ''.join(chars), i + 1
        chars.append(char)
        i += 1
    raise BadValueError(f'unterminated string in json path {path!r}')


def evaluate_json_path(document, path):
    """Return the one node selected by ``path``.

    ``path`` is a string or steps from :func:`compile_json_path`.
    Raises :class:`PathMiss` when a step has no such child.
    """
    steps = compile_json_path(path) if isinstance(path, str) else path
    current = document
    for kind, arg in steps:
        if kind == 'key':
            if not isinstance(current, dict) or arg not in current:
                raise PathMiss(path)
            current = current[arg]
        else:
            if not isinstance(current, list) or arg >= len(current):
                raise PathMiss(path)
            current = current[arg]
    return current


def error_report(payload):
    """Return ``(name, text, qualifiers)`` when ``payload`` is a SECoP error report.

    An error report is a JSON array of length 3 whose first element is a SECoP
    error class.  Any other array is not an error report.
    """
    if not isinstance(payload, list) or len(payload) != 3:
        return None
    name = payload[0]
    if not isinstance(name, str) or name not in SECoPError.name2class:
        return None
    text = payload[1] if isinstance(payload[1], str) else str(payload[1])
    qualifiers = payload[2] if isinstance(payload[2], dict) else {}
    return name, text, qualifiers


def as_data_report(payload):
    """Apply the RFC-108 array rules and return a ``[value, qualifiers]`` document.

    A non-array is ``[payload, {}]``.  An array of length 1, or length 2 whose
    second element is an object, is already a data report.  An error report is
    not a data report.  Any other array is ``[payload, {}]``.
    """
    if isinstance(payload, list):
        if error_report(payload):
            return None
        if len(payload) == 1:
            return [payload[0], {}]
        if len(payload) == 2 and isinstance(payload[1], dict):
            return payload
        return [payload, {}]
    return [payload, {}]


def decode_value_payload(payload, json_path=None):
    """Decode one value-topic payload.

    :param payload: parsed JSON value
    :param json_path: ``None`` for a SECoP data report, or a mapping whose
        ``value`` member is required.  Other members are qualifier paths.
    :return: ``(value, qualifiers)``, or ``None`` when the value path misses
        or the payload is an error report
    """
    if json_path is None:
        document = as_data_report(payload)
        if document is None:
            return None
        paths = {'value': '$[0]', 't': '$[1].t', 'e': '$[1].e'}
    else:
        document = payload
        paths = json_path
    try:
        value = evaluate_json_path(document, paths['value'])
    except PathMiss:
        return None
    qualifiers = {}
    for name, path in paths.items():
        if name == 'value':
            continue
        try:
            selected = evaluate_json_path(document, path)
        except PathMiss:
            continue
        qualifier = _qualifier(name, selected)
        if qualifier is not None:
            qualifiers[name] = qualifier
    return value, qualifiers


def _qualifier(name, selected):
    if name == 't':
        if isinstance(selected, bool) or not isinstance(selected, (int, float)):
            return None
        if abs(selected) > 1e11:
            return selected / 1000.0
        return selected
    if name == 'e':
        if isinstance(selected, bool) or not isinstance(selected, (int, float)):
            return None
        return selected
    return selected


def encode_set_payload(argument, json_path, timestamp):
    """Payload for ``set_topic``.

    With ``json_path`` the publication is the bare argument.  Otherwise it is
    the data report ``[argument, {"t": timestamp}]``.
    """
    if json_path:
        return argument
    return [argument, {'t': timestamp}]


def _validate_topic(value, key, allow_null):
    if value is None and allow_null:
        return None
    if not isinstance(value, str) or not value or any(char in value for char in '+#\x00'):
        raise BadValueError(f'{key} must be a concrete MQTT topic')
    return value


def _validate_json_path(value):
    if not isinstance(value, dict):
        raise BadValueError('json_path must be a JSON object')
    if 'value' not in value:
        raise BadValueError('json_path.value is required')
    result = {}
    for key, path in value.items():
        if not isinstance(key, str) or not key:
            raise BadValueError('json_path members must be named by strings')
        if not isinstance(path, str):
            raise BadValueError(f'json_path.{key} must be a string')
        compile_json_path(path)
        result[key] = path
    return result


def validate_mqtt_mapping(value, node=False):
    """Validate an ``MQTT`` object.  ``None`` means the property is unset.

    Unknown keys are kept.  On a node, only ``value_topic`` and ``get_topic``
    are interpreted; ``json_path`` and ``set_topic`` are unknown there.
    """
    if value is None:
        return None
    if not isinstance(value, dict):
        raise BadValueError('MQTT must be a JSON object')
    result = {}
    for key, item in value.items():
        if not isinstance(key, str):
            raise BadValueError('MQTT keys must be strings')
        if key in ('value_topic', 'get_topic') or (key in ('set_topic', 'error_topic') and not node):
            result[key] = _validate_topic(item, key, allow_null=key != 'value_topic')
        elif key == 'json_path' and not node:
            result[key] = _validate_json_path(item)
        else:
            result[key] = item
    return result


class MQTTMapping:
    """Property datatype for an accessible ``MQTT`` object."""

    default = None

    def validate(self, value):
        return validate_mqtt_mapping(value, node=False)

    def __call__(self, value):
        return self.validate(value)

    def export_value(self, value):
        return value

    def __repr__(self):
        return 'MQTTMapping()'


def normalize_node_mqtt(mqtt, equipment_id):
    """Fill node topic defaults.  ``None`` stays unset.

    ``get_topic`` is filled only when ``value_topic`` was not given on its own.
    A present ``value_topic`` with no ``get_topic`` key means there is no get topic.
    """
    if mqtt is None:
        return None
    try:
        mqtt = validate_mqtt_mapping(mqtt, node=True)
    except BadValueError as exc:
        raise ConfigError(f'node MQTT: {exc}') from exc
    value_explicit = 'value_topic' in mqtt
    result = dict(mqtt)
    if not value_explicit:
        result['value_topic'] = default_node_value_topic(equipment_id)
    if 'get_topic' not in mqtt and not value_explicit:
        result['get_topic'] = default_node_get_topic(equipment_id)
    return result


def normalize_accessible_mqtt(mqtt, equipment_id, module, accessible, *, readonly, is_command):
    """Fill an accessible ``value_topic`` and reject topics the accessible cannot have.

    ``get_topic`` and ``set_topic`` are not invented.  A readonly parameter cannot
    name a ``set_topic``.  A command cannot name a ``get_topic``.  JSON ``null``
    means the topic is absent and is allowed.
    """
    if mqtt is None:
        return None
    try:
        mqtt = validate_mqtt_mapping(mqtt, node=False)
    except BadValueError as exc:
        raise ConfigError(f'{module}:{accessible} MQTT: {exc}') from exc
    if is_command and mqtt.get('get_topic') is not None and 'get_topic' in mqtt:
        raise ConfigError(f'{module}:{accessible} is a command and cannot have a get_topic')
    if readonly and mqtt.get('set_topic') is not None and 'set_topic' in mqtt:
        raise ConfigError(f'{module}:{accessible} is readonly and cannot have a set_topic')
    result = dict(mqtt)
    if 'value_topic' not in result:
        result['value_topic'] = default_accessible_value_topic(equipment_id, module, accessible)
    return result
