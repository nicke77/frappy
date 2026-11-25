# Simulated cryostat from frappy_demo.cryo, published on MQTT.
# The GUI in ./start connects to mqtt://localhost:1883/frappy_demo_node.

EQ = 'frappy_demo_node'
MOD = 'cryo'


def mqtt_writable(name):
    """Change topic and the topic where an illegal change is reported."""
    return {
        'set_topic': '%s/%s/%s/set' % (EQ, MOD, name),
        'error_topic': '%s/%s/%s/error' % (EQ, MOD, name),
    }


Node(EQ,
     'simulated cryostat\n\n'
     'A cc cryostat with heat-load, specific heat for the sample and a '
     'temperature dependent heat-link between sample and regulation.',
     'tcp://10769',
     secondary=['mqtt://localhost:1883'],
)

Mod(MOD,
    'frappy_demo.cryo.Cryostat',
    'A simulated cc cryostat with heat-load, specific heat for the sample and a '
    'temperature dependent heat-link between sample and regulation.',
    group='very important/stuff',
    jitter=0.1,
    T_start=10.0,
    target=Param(10.0, MQTT=mqtt_writable('target')),
    looptime=1,
    ramp=Param(6, MQTT=mqtt_writable('ramp')),
    maxpower=Param(20.0, MQTT=mqtt_writable('maxpower')),
    heater=4.1,
    mode=Param('pid', MQTT=mqtt_writable('mode')),
    tolerance=Param(0.1, MQTT=mqtt_writable('tolerance')),
    window=Param(30, MQTT=mqtt_writable('window')),
    timeout=Param(900, MQTT=mqtt_writable('timeout')),
    p=Param(40, unit='%/K', group='pid', MQTT=mqtt_writable('p')),
    i=Param(10, group='pid', MQTT=mqtt_writable('i')),
    d=Param(2, group='pid', MQTT=mqtt_writable('d')),
    pid=Param(MQTT=mqtt_writable('pid')),
    stop=Param(MQTT=mqtt_writable('stop')),
    pollinterval=Param(export=False),
    value=Param(unit='K', test='customized value',
                MQTT={'error_topic': '%s/%s/value/error' % (EQ, MOD)}),
)
