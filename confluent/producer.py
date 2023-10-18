import time
import random
import numpy as np
import threading
from confluent_kafka import SerializingProducer
from configparser import ConfigParser
import json
import logging
from datetime import datetime


# Configuration du logging
logging.basicConfig(level=logging.DEBUG)

def get_current_timestamp():
    return datetime.now().strftime('%Y-%m-%dT%H:%M:%S')

# Constantes
STATIONS = ['Station A', 'Station B', 'Station C', 'Station D']
INCIDENT_TYPES = ['suspect package', 'Technique']
EQUIPMENT_LIST = ['validator', 'escalator', 'notice board']
PEAK_HOURS = [(7, 9), (17, 19)]
DIRECTIONS = ['North', 'South', 'East', 'West']
LOCATIONS = ['Corridor', 'Platform']
SEVERITIES = ['Minor', 'Major']
INCIDENT_RATE = 0.005
EQUIPMENT_FAILURE_RATE = 0.01
simulated_hour = 6
simulated_minute = 0
train_arrival_count = 0
train_departure_count = 0
incident_count = 0
equipment_failure_count = 0
ticket_validation_count = 0
passenger_flow_count = 0


global_state = {
    'incidents': [],
    'equipment_failures': [],
    'train_arrivals': [],
    'train_departures': [],
    'ticket_validations': [],
    'passenger_flow': [],
}


# Configuration de Kafka
config = ConfigParser()
config.read('client.properties')
producer_conf = {
    'bootstrap.servers': config.get('DEFAULT', 'bootstrap.servers'),
    'security.protocol': config.get('DEFAULT', 'security.protocol'),
    'sasl.mechanisms': config.get('DEFAULT', 'sasl.mechanisms'),
    'sasl.username': config.get('DEFAULT', 'sasl.username'),
    'sasl.password': config.get('DEFAULT', 'sasl.password'),
}

try:
    producer = SerializingProducer(producer_conf)
except Exception as e:
    logging.error(f"Erreur lors de la configuration du producteur: {e}")
    raise

def send_to_kafka(topic, data):
    for record in data:
        try:
            serialized_data = json.dumps(record).encode('utf-8')
            producer.produce(topic=topic, key=str(record['id']), value=serialized_data)
        except Exception as e:
            logging.error(f"Erreur lors de la sérialisation des données pour Kafka: {e}")
    producer.flush()



class Train:
    def __init__(self, train_id, station, time, passengers, dock_number, wagon_count, direction):
        self.train_id = train_id
        self.station = station
        self.time = time
        self.passengers = passengers
        self.dock_number = dock_number
        self.wagon_count = wagon_count
        self.direction = direction

class Incident:
    def __init__(self, incident_id, incident_type, station, location, time, severity, duration):
        self.incident_id = incident_id
        self.incident_type = incident_type
        self.station = station
        self.location = location
        self.time = time
        self.severity = severity
        self.duration = duration

class EquipmentFailure:
    def __init__(self, failure_id, equipment, station, location, time, duration):
        self.failure_id = failure_id
        self.equipment = equipment
        self.station = station
        self.location = location
        self.time = time
        self.duration = duration


def update_simulation_time():
    global simulated_minute, simulated_hour
    simulated_minute += 5
    if simulated_minute >= 60:
        simulated_minute = 0
        simulated_hour += 1
        if simulated_hour > 22:
            simulated_hour = 6

def reset_global_state():
    global global_state
    global_state = {
        'incidents': [],
        'equipment_failures': [],
        'train_arrivals': [],
        'train_departures': [],
        'ticket_validations': [],
        'passenger_flow': [],
    }

def is_peak_hour(hour):
    return any(start <= hour <= end for start, end in PEAK_HOURS)

def generate_train_event(station, event_type):
    global train_arrival_count, train_departure_count
    count = train_arrival_count if event_type == "arrival" else train_departure_count
    count += 1
    train_id = f'{event_type[:3]}{count:02d}'
    passengers = np.random.poisson(50 if is_peak_hour(simulated_hour) else 20)
    dock_number = random.randint(1, 5)
    wagon_count = random.randint(4, 10)
    direction = random.choice(DIRECTIONS)
    timestamp = get_current_timestamp()  # Obtenez l'horodatage actuel
    return (train_id, station, timestamp, passengers, dock_number, wagon_count, direction)

def generate_train_arrivals():
    global train_arrival_count
    for station in STATIONS:
        if any(incident['station'] == station for incident in global_state['incidents']):
            continue
        if random.random() < (0.05 if is_peak_hour(simulated_hour) else 1.0):
            train_arrival_count += 1
            train_arrival = {
                'id': f'arr{train_arrival_count:02d}',
                'station': station,
                'timestamp': get_current_timestamp(),
                'passengers': np.random.poisson(50 if is_peak_hour(simulated_hour) else 20),
                'dock_number': random.randint(1, 5),
                'wagon_count': random.randint(4, 10),
                'direction': random.choice(DIRECTIONS)
            }
            global_state['train_arrivals'].append(train_arrival)


def generate_train_departures():
    global train_departure_count
    for station in STATIONS:
        if any(incident['station'] == station for incident in global_state['incidents']):
            continue
        if random.random() < (1.0 if is_peak_hour(simulated_hour) else 0.02):
            train_departure_count += 1
            train_departure = {
                'id': f'dep{train_departure_count:02d}',
                'station': station,
                'timestamp': get_current_timestamp(),
                'passengers': np.random.poisson(50 if is_peak_hour(simulated_hour) else 20),
                'dock_number': random.randint(1, 5),
                'wagon_count': random.randint(4, 10),
                'direction': random.choice(DIRECTIONS)
            }
            global_state['train_departures'].append(train_departure)

def generate_incidents():
    global incident_count
    for station in STATIONS:
        if random.random() < INCIDENT_RATE:
            incident_count += 1
            incident = {
                'id': f'inc{incident_count:03d}',
                'type': random.choice(INCIDENT_TYPES),
                'station': station,
                'location': f"Dock {random.randint(1,5)}, {random.choice(LOCATIONS)}",
                'timestamp': get_current_timestamp(),
                'severity': random.choice(SEVERITIES),
                'duration': np.random.exponential(60)
            }
            global_state['incidents'].append(incident)


def generate_equipment_failures():
    global equipment_failure_count
    for station in STATIONS:
        if random.random() < EQUIPMENT_FAILURE_RATE:
            equipment_failure_count += 1
            equipment_failure = {
                'id': f'eqf{equipment_failure_count:03d}',
                'equipment': random.choice(EQUIPMENT_LIST),
                'station': station,
                'location': f"Dock {random.randint(1,5)}, {random.choice(LOCATIONS)}",
                'timestamp': get_current_timestamp(),
                'duration': np.random.exponential(30)
            }
            global_state['equipment_failures'].append(equipment_failure)


def generate_passenger_flow():
    global passenger_flow_count
    for train in global_state['train_arrivals']:
        if any(incident['station'] == train['station'] for incident in global_state['incidents']):
            reduced_flow_factor = 0.5  # Reduction factor for flow due to incident
        else:
            reduced_flow_factor = 1.0
        
        passenger_flow_count += 1
        passenger_flow = {
            'id': f'psf{passenger_flow_count:03d}',
            'train_id': train['id'],
            'station': train['station'],
            'timestamp': get_current_timestamp(),
            'waiting_time': np.random.exponential(5),
            'ticket_check_time': np.random.uniform(1, 3),
            'reduced_flow_factor': reduced_flow_factor
        }
        global_state['passenger_flow'].append(passenger_flow)


def generate_ticket_validations():
    global ticket_validation_count
    for train in global_state['train_arrivals']:
        ticket_validation_count += 1
        ticket_validation = {
            'id': f'tkv{ticket_validation_count:03d}',
            'train_id': train['id'],
            'station': train['station'],
            'timestamp': get_current_timestamp(),
            'validations': np.random.poisson(train['passengers'] * 0.9)
        }
        global_state['ticket_validations'].append(ticket_validation)




def append_to_json(data, filename):
    try:
        with open(filename, 'r') as f:
            existing_data = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        existing_data = []

    updated_data = existing_data + data
    with open(filename, 'w') as f:
        json.dump(updated_data, f, indent=4)


def generate_data():
    generate_train_arrivals()
    generate_train_departures()
    generate_ticket_validations()
    generate_equipment_failures()
    generate_passenger_flow()
    generate_incidents()

def send_data_to_kafka():
    send_to_kafka('incidents', global_state['incidents'])
    send_to_kafka('equipment_failures', global_state['equipment_failures'])
    send_to_kafka('train_arrivals', global_state['train_arrivals'])
    send_to_kafka('train_departures', global_state['train_departures'])
    send_to_kafka('ticket_validations', global_state['ticket_validations'])
    send_to_kafka('passenger_flow', global_state['passenger_flow'])

def output_data():
    for key, value in global_state.items():
        print(f'{key}: {value}')
    append_to_json(global_state['incidents'], 'incidents.json')
    append_to_json(global_state['equipment_failures'], 'equipment_failures.json')
    append_to_json(global_state['train_arrivals'], 'train_arrivals.json')
    append_to_json(global_state['train_departures'], 'train_departures.json')
    append_to_json(global_state['ticket_validations'], 'ticket_validations.json')
    append_to_json(global_state['passenger_flow'], 'passenger_flow.json')

def schedule_simulation():
    threading.Timer(180, schedule_simulation).start()
    simulate()

def simulate():
    update_simulation_time()
    reset_global_state()
    generate_data()
    send_data_to_kafka()
    output_data()

if __name__ == "__main__":
    schedule_simulation()

