Frappy fork with MQTT (RFC-108 and RFC-109)
==========================================

This is a fork of [Frappy](https://github.com/SampleEnvironment/frappy), a Python framework for SECoP nodes and clients. It adds the optional `MQTT` object from the drafts in this repository:

- [RFC-108](RFC-108-mqtt-structure.rst) maps a SEC node and its accessibles onto MQTT topics.
- [RFC-109](RFC-109-json-path.rst) reads a value out of a device JSON document with `json_path`.

TCP and WebSocket are unchanged. A node publishes MQTT only when its configuration lists an `mqtt://` interface.

Requirements
------------

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
```

`requirements.txt` includes `paho-mqtt`, the library Frappy uses to open a connection to a broker.

A broker for testing
--------------------

In SECoP over TCP the GUI connects to the node. With MQTT, both the node and the GUI connect to a broker. The broker is a small program that forwards publications. A topic is the name of one such channel, for example `frappy_demo_node/cryo/value`. Nothing in Frappy listens on port 1883. That port belongs to the broker.

`./start` expects a broker on `localhost:1883`. Pick one of the two setups below before running it.

**On this machine.** Install [Mosquitto](https://mosquitto.org/) and leave it running:

```bash
# Debian, Ubuntu, or WSL
sudo apt install mosquitto
sudo service mosquitto start

# macOS
brew install mosquitto
brew services start mosquitto

# Docker, any OS
docker run --rm -p 1883:1883 eclipse-mosquitto
```

`mosquitto -p 1883` in a second terminal is enough when you only need it for one test session.

To see the topics without learning the command line, use [MQTT Explorer](https://mqtt-explorer.com/) or another MQTT desktop client. Connect it to `localhost` port 1883, or to `test.mosquitto.org` port 1883. It shows a tree of topics, the JSON payload, and retained messages such as the structure report. Subscribe to `frappy_demo_node/#` after `./start` to watch the cryostat. Publishing `[20]` on `frappy_demo_node/cryo/target/set` from that client is the same as a SECoP `change`.

Check that a publication arrives:

```bash
mosquitto_sub -t 'test/#'
mosquitto_pub -t test/hello -m hi
```

**A public broker.** [test.mosquitto.org](https://test.mosquitto.org/) port 1883 needs no install and no account. Anyone on the internet can read and write every topic, so use it only for a throwaway test and pick an equipment id that is not `frappy_demo_node`. Point the node and the client at that host:

```python
secondary=['mqtt://test.mosquitto.org:1883']
```

```text
mqtt://test.mosquitto.org:1883/my_test_node
```

The same `mosquitto_sub` and `mosquitto_pub` commands take `-h test.mosquitto.org`. `./start` itself stays on localhost. For the public broker, change `mqtt://localhost:1883` in [cfg/frappy_demo_node_cfg.py](cfg/frappy_demo_node_cfg.py) and in [start](start), and change the equipment id in both places to your own id.

Try the cryo demo
-----------------

`./start` launches the simulated cryostat as equipment id `frappy_demo_node` and opens the Frappy GUI on `mqtt://localhost:1883/frappy_demo_node`. Closing the GUI stops the node. The broker has to be running first.

The node also listens for SECoP on TCP port 10769. Its configuration is [cfg/frappy_demo_node_cfg.py](cfg/frappy_demo_node_cfg.py).

With the node running, these topics carry the regulation temperature:

| Topic | Payload |
| --- | --- |
| `frappy_demo_node/cryo/value` | live value, `[21.5, {"t": 1710000000.0}]` |
| `frappy_demo_node/cryo/target/set` | new target, `[20]` or `[20, {"t": 1710000000.0}]` |
| `frappy_demo_node/cryo/target/error` | rejected change, `["RangeError", "-2 must be between 0 and …", {"t": 1710000000.0}]` |
| `SECoP/structures/frappy_demo_node` | structure report, retained |

```bash
mosquitto_sub -t 'frappy_demo_node/cryo/#' -t 'SECoP/structures/frappy_demo_node'
mosquitto_pub -t frappy_demo_node/cryo/target/set -m '[20]'
mosquitto_pub -t frappy_demo_node/cryo/target/set -m '[-2]'
```

Quote the payload. An unquoted `[-2]` is a shell glob, not JSON.

A length-1 array is a data report: `[20]` is the value 20. A length-3 array whose first element is a SECoP error class, such as `RangeError` or `HardwareError`, is an error report.

Publish a SEC node
------------------

Add the broker as the main interface or as a secondary one. The equipment id is the node id, not part of the URI.

```python
Node('frappy_demo_node', 'simulated cryostat', 'tcp://10769',
     secondary=['mqtt://localhost:1883'])
```

`mqtt://[user[:password]@]host[:port]` is enough. The port defaults to 1883.

When that interface is present, the structure report gains an `MQTT` object. Omitted topics are filled in as follows:

- Node structure: `SECoP/structures/<equipment_id>`
- Node refresh: `SECoP/structures/<equipment_id>/get` (any payload asks for `describe` again)
- Accessible value: `<equipment_id>/<module>/<accessible>`

`get_topic`, `set_topic`, and `error_topic` are not invented. Set them when a parameter must be readable on request, writable, or must report errors on their own topic. JSON `null` means that topic is absent. A readonly parameter cannot have a `set_topic`. A command cannot have a `get_topic`.

```python
target = Param(10.0, MQTT={
    'set_topic': 'frappy_demo_node/cryo/target/set',
    'error_topic': 'frappy_demo_node/cryo/target/error',
})
```

With `error_topic`, a rejected `change` or `do` is published only there. Without it, the same error report is published on `value_topic`. The structure publication and the latest good value are retained. Error reports are not.

`get_topic` asks for a fresh `read`. Its payload is ignored. `set_topic` is a `change` for a parameter and a `do` for a command. The publication is a data report, `[argument, {"t": <unix seconds>}]`.

Connect a client
----------------

The GUI, `SecopClient`, and the proxy accept the same URI. The path is the equipment id. The client subscribes to `SECoP/structures/<equipment_id>` unless `?topic=` names another structure topic.

```text
mqtt://localhost:1883/frappy_demo_node
mqtt://user:secret@broker:1883/frappy_demo_node?topic=SECoP/structures/custom
```

```python
from frappy.client import SecopClient

client = SecopClient('mqtt://localhost:1883/frappy_demo_node')
client.connect()
client.getParameter('cryo', 'value')
client.setParameter('cryo', 'target', 20)
```

`read` publishes `{}` on `get_topic` when that topic exists. Otherwise it uses the latest value. `change` and `do` publish on `set_topic` and wait for the next value, or for the error report.

Read a device JSON document
---------------------------

`json_path` is optional. When it is present, `value_topic` carries the device's own JSON document instead of a SECoP data report. `value` is required. `t` and `e` are optional qualifier paths. A numeric timestamp whose absolute value is greater than `1e11` is treated as milliseconds.

The path language is the child steps shared by common JSONPath implementations: `$`, `.name`, `['name']`, `["name"]`, and `[n]`.

```python
value = Param(readonly=True, MQTT={
    'value_topic': 'device/cpuload',
    'json_path': {'value': '$.loadavg[0]', 't': '$.timestamp'},
})
```

A `change` or `do` for that accessible publishes the bare argument on `set_topic`, for example `20` rather than `[20, {"t": ...}]`.

Run the node without the GUI
----------------------------

```bash
.venv/bin/python bin/frappy-server frappy_demo_node
```

The name `frappy_demo_node` loads [cfg/frappy_demo_node_cfg.py](cfg/frappy_demo_node_cfg.py) from `cfg/`. Log files go to `log/` and the pid file to `pid/`, as set in [cfg/generalConfig.cfg](cfg/generalConfig.cfg).

Upstream Frappy
---------------

The framework without this MQTT work is documented at <https://forge.frm2.tum.de/public/doc/frappy/html/>. Development of upstream Frappy is on [Gerrit](https://forge.frm2.tum.de/review/q/project:secop%252Ffrappy), with a read-only mirror on [GitHub](https://github.com/SampleEnvironment/frappy).
