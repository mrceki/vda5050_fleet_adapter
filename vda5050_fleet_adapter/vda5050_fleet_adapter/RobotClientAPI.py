import paho.mqtt.client as mqtt
import json
import time
import uuid
from datetime import datetime
import yaml
import os
import enum
import time
from rmf_dispenser_msgs.msg import DispenserResult
from rmf_ingestor_msgs.msg import IngestorResult


class RobotAPIResult(enum.IntEnum):
    WAITING = 0
    """Waiting for the trigger (passing the mode, entering the edge)"""

    INITIALIZING = 1

    RUNNING = 2
    """The client connected but something about the request is impossible"""

    PAUSED = 3
    """Paused by instantAction or external trigger"""

    FINISHED = 4

    FAILED = 5
    """Action could not be performed."""


class RobotAPI:
    def __init__(self, config_yaml, node):
        self.prefix = config_yaml['prefix']
        self.user = config_yaml['user']
        self.password = self.timeout = 5.0
        self.debug = False
        self.robot_state_data = {}
        self.robot_visualization_data = {}
        self.previous_destination_node = {}
        self.task_order_data = {}
        self.sequence_id_map = {}
        self.client = mqtt.Client()
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message
        self.node = node
        print("Connecting to MQTT broker")
        self.client.connect("localhost", 1883, 60)
        self.client.loop_start()
        self.current_robot_positions = {}
        self.previous_node_theta = {}
        self.factsheet_directory = "factsheets"
        self.previous_command = {}
        self.previous_action_id = {}

    def on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            self.is_broker_connected = True
            self.client.subscribe(f"{self.prefix}/#")
        else:
            self.is_broker_connected = False
            print(f"Connection failed with result code {rc}")

    def on_message(self, client, userdata, msg):
        if msg.topic.endswith("/connection"):
            self.handle_connection_message(msg.payload.decode())
        elif msg.topic.endswith("/state"):
            self.handle_state_message(msg.payload.decode())
        elif msg.topic.endswith("/visualization"):
            self.handle_visualization_message(msg.payload.decode())
        elif msg.topic.endswith("/factsheet"):
            self.handle_factsheet_message(msg.payload.decode())

    def check_connection(self):
        return True

    def handle_connection_message(self, payload):
        try:
            message = json.loads(payload)
            if message.get("status") == "connected":
                self.is_robot_connected = True
            else:
                self.is_robot_connected = False
        except json.JSONDecodeError:
            print("Error decoding JSON message")

    def handle_state_message(self, payload):
        try:
            message = json.loads(payload)
            robot_name = message.get("serialNumber")
            if robot_name:
                self.robot_state_data[robot_name] = message
                self.current_robot_positions[robot_name] = [message.get("agvPosition").get(
                    "x"), message.get("agvPosition").get("y"), message.get("agvPosition").get("theta")]
        except json.JSONDecodeError:
            print("Error decoding JSON message")

    def handle_visualization_message(self, payload):
        try:
            message = json.loads(payload)
            robot_name = message.get("serialNumber")
            self.robot_visualization_data[robot_name] = message
            if robot_name:
                self.current_robot_positions[robot_name] = [message.get("agvPosition").get(
                    "x"), message.get("agvPosition").get("y"), message.get("agvPosition").get("theta")]

        except json.JSONDecodeError:
            print("Error decoding JSON message")

    def handle_factsheet_message(self, payload):
        print(f"Received factsheet message : {payload}")
        try:
            factsheet = json.loads(payload)
            serial_number = factsheet.get('serialNumber')
            if not serial_number:
                print("Factsheet does not contain a serial number.")
                return

            json_filename = os.path.join(
                self.factsheet_directory, f"{serial_number}.json")
            yaml_filename = os.path.join(
                self.factsheet_directory, f"{serial_number}.yaml")

            if not os.path.exists(json_filename) or not os.path.exists(yaml_filename):
                with open(json_filename, 'w') as json_file:
                    json.dump(factsheet, json_file, indent=2)

                yaml_data = self.convert_factsheet_to_yaml(factsheet)
                with open(yaml_filename, 'w') as yaml_file:
                    yaml_file.write(yaml_data)

                print(
                    f"New factsheet saved as {json_filename} and {yaml_filename}")
            else:
                print(f"Factsheet for {serial_number} already exists.")
        except json.JSONDecodeError:
            print("Error decoding JSON message")

    def convert_factsheet_to_yaml(self, factsheet):
        rmf_fleet_config = {
            'rmf_fleet': {
                'name': factsheet.get('manufacturer', 'UNKNOWN'),
                'limits': {
                    'linear': [
                        factsheet['physicalParameters'].get('speedMax', 0.0),
                        factsheet['physicalParameters'].get(
                            'accelerationMax', 0.0)
                    ],
                    'angular': [0.6, 2.0]
                },
                'profile': {
                    'footprint': 0.3,
                    'vicinity': 0.5
                },
                'reversible': True,
                'battery_system': {
                    'voltage': 12.0,
                    'capacity': 24.0,
                    'charging_current': 5.0
                },
                'mechanical_system': {
                    'mass': factsheet['typeSpecification'].get('maxLoadMass', 0.0),
                    'moment_of_inertia': 10.0,
                    'friction_coefficient': 0.22
                },
                'ambient_system': {
                    'power': 20.0
                },
                'tool_system': {
                    'power': 0.0
                },
                'recharge_threshold': 0.10,
                'recharge_soc': 1.0,
                'publish_fleet_state': 20.0,
                'account_for_battery_drain': True,
                'task_capabilities': {
                    'loop': True,
                    'delivery': True,
                    'clean': False
                },
                'actions': ["teleop"],
                'robots': {
                    factsheet.get('serialNumber', 'UNKNOWN'): {
                        'charger': "start"
                    }
                }
            }
        }
        return yaml.dump(rmf_fleet_config, default_flow_style=False)

    def check_robot_connection(self, robot_name: str):
        return True

    def check_broker_connection(self):
        return True

    def send_factsheet_request(self, robot_name: str):
        factsheet_request_action = {
            "actionId": str(uuid.uuid4()),
            "actionType": "factsheetRequest",
            # "blockingType": "SOFT",
            "actionParameters": []
        }

        instant_action = {
            "headerId": int(time.time()),
            "timestamp": datetime.utcnow().isoformat() + 'Z',
            "version": "2.0.0",
            "manufacturer": "OSRF",
            "serialNumber": robot_name,
            "actions": [factsheet_request_action]
        }
        print(f"Requesting factsheet from robot {robot_name}")
        print(f"Instant action: {instant_action}")
        self.client.publish(
            f"{self.prefix}/{robot_name}/instantActions", json.dumps(instant_action))
        return True

    def navigate(
        self,
        robot_name: str,
        pose,
        map_name: str,
        speed_limit=0.0,
        task_id=None,
        nodes=None, edges=None
    ):
        if task_id not in self.task_order_data:
            orderId = str(uuid.uuid4())
            orderUpdateId = 0
            self.task_order_data[task_id] = {
                "orderId": orderId, "orderUpdateId": orderUpdateId}
            # Initialize hashmap for the task
            self.sequence_id_map[task_id] = {"nodes": {}, "edges": {}}
        else:
            self.task_order_data[task_id]["orderUpdateId"] += 1
            orderId = self.task_order_data[task_id]["orderId"]
            orderUpdateId = self.task_order_data[task_id]["orderUpdateId"]

        print(f"Task ID: {task_id}")
        print(f"Order ID: {orderId}")
        print(f"Order Update ID: {orderUpdateId}")
        print(f"Navigate to {pose[0]}, {pose[1]} on map {map_name}")
        print(f"Nodes: {nodes}")
        print(f"Edges: {edges}")
        if edges is None:
            return

        order_nodes = []
        for i, node in enumerate(nodes):
            if i == 0 and node[0] == self.previous_destination_node.get(robot_name):
                node_theta = self.previous_node_theta.get(
                    robot_name) if self.previous_node_theta.get(robot_name) is not None else pose[2]
            else:
                node_theta = pose[2]

            node_position = {
                "x": node[1][0],
                "y": node[1][1],
                "theta": node_theta,
                "mapId": "map",
                "allowedDeviationXY": 0.6,
                "allowed_deviation_theta": 3.141592653589793
            }
            sequence_id = self.generate_sequence_id(
                task_id, "nodes", node[0], base_node=(i == 0))
            order_nodes.append({
                "nodeId": node[0],
                "sequenceId": sequence_id,
                "released": True,
                "nodePosition": node_position,
                "actions": []
            })

        order_edges = []
        for i, edge in enumerate(edges):
            sequence_id = self.generate_sequence_id(task_id, "edges", edge)
            start_node_id = nodes[i][0] if i < len(nodes) else ""
            end_node_id = nodes[i + 1][0] if i + 1 < len(nodes) else ""
            order_edges.append({
                "edgeId": str(edge),
                "released": True,
                "sequenceId": sequence_id + 1,
                "startNodeId": start_node_id,
                "endNodeId": end_node_id,
                "actions": []
            })

        order = {
            "headerId": int(time.time()),
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
            "version": "2.0.0",
            "manufacturer": "OSRF",
            "serialNumber": robot_name,
            "orderId": orderId,
            "orderUpdateId": orderUpdateId,
            "nodes": order_nodes,
            "edges": order_edges
        }

        self.client.publish(
            f"{self.prefix}/{robot_name}/order", json.dumps(order))
        print(f"Order published: {order}")
        self.previous_command[robot_name] = "navigate"
        self.last_task = task_id
        self.previous_destination_node[robot_name] = nodes[-1][0] if nodes else None
        # if nodes else None#and not nodes[-1] == nodes[0] else None
        self.previous_node_theta[robot_name] = node_theta
        return True

    def generate_sequence_id(self, task_id, entity_type, entity_id, base_node=False):
        """
        Get sequence ID for a given node or edge in a task.

        Args:
            task_id (str): The task ID.
            entity_type (str): Type of the entity ('nodes' or 'edges').
            entity_id (str): ID of the entity (node ID or edge ID).
            base_node (bool): Whether the node is the base node (first node in the order).

        Returns:
            int: The sequence ID.
        """
        if task_id not in self.sequence_id_map:
            self.sequence_id_map[task_id] = {"nodes": {}, "edges": {}}
        if entity_id in self.sequence_id_map[task_id][entity_type]:
            if base_node:
                return self.sequence_id_map[task_id][entity_type][entity_id]
            else:
                existing_ids = list(
                    self.sequence_id_map[task_id][entity_type].values())
                existing_id = self.sequence_id_map[task_id][entity_type][entity_id]
                new_sequence_id = max(existing_ids) + 2
                self.sequence_id_map[task_id][entity_type][entity_id] = new_sequence_id
                return new_sequence_id
        else:
            existing_ids = list(
                self.sequence_id_map[task_id][entity_type].values())
            new_sequence_id = max(existing_ids) + 2 if existing_ids else 0
            self.sequence_id_map[task_id][entity_type][entity_id] = new_sequence_id
            return new_sequence_id

    def fetch_action_states(self, robot_name, task_id):
        start_time = time.time()
        while True:
            if robot_name in self.robot_state_data:
                action_status = self.robot_state_data[robot_name].get(
                    "actionStates", [])
                for action_state in action_status:
                    print(
                        f"Action state id from robot: {action_state.get('actionId')}")
                    print(f"Task id: {task_id}")
                    if action_state.get("actionId") == task_id:
                        return action_state.get("actionStatus")
            print(f"Waiting for action states for robot {robot_name}...")
            time.sleep(1)
            if time.time() - start_time >= 20:
                return False

    def start_activity(self, robot_name: str, task_id, activity: str, label: str):
        self.node.get_logger().warn(
            f"Starting activity {activity} for robot {robot_name}!!!!!!1")

        def initialize_task_order():
            orderId = str(uuid.uuid4())
            actionId = str(uuid.uuid4())
            self.task_order_data[task_id] = {
                "orderId": orderId,
                "orderUpdateId": 0,
                "actionId": actionId,
                "activity": activity
            }
            self.previous_command[robot_name] = activity
            print(f"Starting new activityXXXXXXXXXXXXXX: {activity}")
            self.publish_instant_action(robot_name, activity, actionId)

        def initialize_action_order():
            actionId = str(uuid.uuid4())
            self.task_order_data[task_id].update({
                "actionId": actionId,
                "activity": activity
            })
            self.previous_command[robot_name] = activity
            print(f"Starting new activityXXXXXXXXXXXXXX: {activity}")
            self.publish_instant_action(robot_name, activity, actionId)

        def update_task_order_with_new_action():
            actionId = str(uuid.uuid4())
            self.task_order_data[task_id].update({
                "actionId": actionId,
                "activity": activity
            })
            self.previous_command[robot_name] = activity
            print(f"Updating new activityXXXXXXXXXXXXXX: {activity}")
            self.publish_instant_action(robot_name, activity, actionId)

        if task_id not in self.task_order_data:
            print("Starting new activity")
            initialize_task_order()

        elif self.task_order_data[task_id].get("activity") is None:
            print(f"Starting new activity: {activity}")
            initialize_action_order()
        elif self.task_order_data[task_id]["activity"] != activity:
            print(f"Starting new activity: {activity}")
            update_task_order_with_new_action()
        else:
            print(f"Continuing task: {self.task_order_data[task_id]}")

            # Get the current actionId from the task order
            actionId = self.task_order_data[task_id].get("actionId")

            # if not actionId or self.previous_command[robot_name] != activity:
            #     # If there's no current actionId or the last command is different, create a new actionId
            #     actionId = str(uuid.uuid4())
            #     self.task_order_data[task_id]["actionId"] = actionId
            #     self.previous_action_id[robot_name] = actionId
            #     self.publish_instant_action(robot_name, activity, actionId)
            # else:
            #     self.previous_action_id[robot_name] = actionId

            self.node.get_logger().info(
                f"IN START ACTIVITY {activity} FOR ROBOT {robot_name} WITH ACTION ID {actionId}")
            action_state = self.fetch_action_states(robot_name, actionId)

            if action_state in ["INITIALIZING", "RUNNING", "FINISHED"]:
                print(f"Action state: {action_state}")
                if action_state == "FINISHED":
                    self.publish_task_result(robot_name, task_id)
                return True
            elif action_state == "FAILED":
                return True
            else:
                return True
        return True

    def publish_instant_action(self, robot_name: str, activity: str, actionId: str):
        action = {
            "actionId": actionId,
            "blockingType": "HARD",
            "actionParameters": {}
        }
        if activity == "delivery_pickup":
            action["actionType"] = "pick"
        elif activity == "delivery_dropoff":
            action["actionType"] = "drop"
        elif activity == "dock":
            action["actionType"] = "finePositioning"
        else:
            raise ValueError(f"Unsupported activity type: {activity}")

        instant_action = {
            "headerId": int(time.time()),
            "timestamp": datetime.utcnow().isoformat() + 'Z',
            "version": "2.0.0",
            "manufacturer": "OSRF",
            "serialNumber": robot_name,
            "actions": [action]
        }

        try:
            self.client.publish(
                f"{self.prefix}/{robot_name}/instantActions", json.dumps(instant_action))
            self.previous_action_id[robot_name] = actionId
            print(f"Published instant action for robot {robot_name}")
        except Exception as e:
            print(f"Error publishing instant action: {e}")

    def publish_task_result(self, robot_name: str, task_id):
        self.node.get_logger().info(
            f"Publishing result for robot {robot_name} with task ID {task_id} with command {self.previous_command[robot_name]}")
        if self.previous_command[robot_name] == "delivery_pickup":
            result_msg = DispenserResult()
            result_msg.time = self.node.get_clock().now().to_msg()
            result_msg.source_guid = robot_name
            result_msg.request_guid = task_id
            result_msg.status = DispenserResult.SUCCESS
            self.node.get_logger().info(
                f"Robot {robot_name} completed dispenser task: {task_id}")
            self.dispenser_result_pub.publish(result_msg)

        elif self.previous_command[robot_name] == "delivery_dropoff":
            result_msg = IngestorResult()
            result_msg.time = self.node.get_clock().now().to_msg()
            result_msg.source_guid = robot_name
            result_msg.request_guid = task_id
            result_msg.status = IngestorResult.SUCCESS
            self.node.get_logger().info(
                f"Robot {robot_name} completed ingestor task {task_id}")
            self.ingestor_result_pub.publish(result_msg)

    def stop(self, robot_name: str):
        stop_action = {
            "actionId": str(uuid.uuid4()),
            "actionType": "cancelOrder",
            "blockingType": "HARD"
        }

        instant_action = {
            "headerId": int(time.time()),
            "timestamp": datetime.utcnow().isoformat() + 'Z',
            "version": "2.0.0",
            "manufacturer": "OSRF",
            "serialNumber": robot_name,
            "actions": [stop_action]
        }
        print(f"Stopping robot {robot_name}")
        print(f"Instant action: {instant_action}")
        self.client.publish(
            f"{self.prefix}/{robot_name}/instantActions", json.dumps(instant_action))
        return True

    def position(self, robot_name: str):
        if robot_name in self.robot_state_data or self.robot_visualization_data:
            return self.current_robot_positions[robot_name]
        else:
            print(f"Robot {robot_name} not found in state data for position")
            return None

    def battery_soc(self, robot_name: str):
        if robot_name in self.robot_state_data:
            state = self.robot_state_data[robot_name]
            return 1.0
        else:
            print(
                f"Robot {robot_name} not found in state data for battery SOC")
            return None

    def map(self, robot_name: str):
        if robot_name in self.robot_state_data:
            state = self.robot_state_data[robot_name]
            return state.get("agvPosition").get("mapId")
        else:
            print(f"Robot {robot_name} not found in state data for map")
            return None

    def is_command_completed(self, robot_name: str, task_id: str):
        # print(f"Checking if command is completed for robot {robot_name}")
        # print(f"Last command: {self.previous_command[robot_name]}")

        if robot_name in self.robot_state_data:
            state = self.robot_state_data[robot_name]
            if state.get("errors"):
                for error in state.get("errors"):
                    print(f"Error: {error}")
                    # if error.get("")
                    return False, error.get("errorLevel")

            elif self.previous_command[robot_name] == "navigate":

                current_node = state.get("lastNodeId")
                if not state.get("nodeStates") and not state.get("edgeStates"):
                    if self.previous_destination_node.get(robot_name) is not None:
                        if current_node == self.previous_destination_node[robot_name]:
                            return True, None
                    else:
                        return True, None
                return False, None

            elif self.previous_command[robot_name] in ["delivery_pickup", "delivery_dropoff"]:
                while True:
                    # print(f"Checking action state for robot {robot_name}")
                    action_state = self.fetch_action_states(
                        robot_name, self.previous_action_id[robot_name])
                    if action_state:
                        print(f"Action state: {action_state}")
                        if action_state == "FINISHED":
                            print(f"Action state: {action_state}")

                            result_msg = None
                            if self.previous_command[robot_name] == "delivery_pickup":
                                result_msg = DispenserResult()
                                result_msg.time = self.node.get_clock().now().to_msg()
                                result_msg.source_guid = robot_name
                                result_msg.request_guid = task_id  # self.last_task
                                result_msg.status = DispenserResult.SUCCESS
                                self.dispenser_result_pub.publish(result_msg)
                                self.node.get_logger().info(
                                    f"Robot {robot_name} completed dispenser task")
                            elif self.previous_command[robot_name] == "delivery_dropoff":
                                result_msg = IngestorResult()
                                result_msg.time = self.node.get_clock().now().to_msg()
                                result_msg.source_guid = robot_name
                                result_msg.request_guid = task_id  # self.last_task
                                result_msg.status = IngestorResult.SUCCESS
                                self.ingestor_result_pub.publish(result_msg)
                                self.node.get_logger().info(
                                    f"Robot {robot_name} completed ingestor task")

                            # if result_msg:
                            #     if self.previous_command[robot_name] == "delivery_pickup":
                            #         self.node.get_logger().info(f"Robot {robot_name} completed dispenser task")
                            #         self.dispenser_result_pub.publish(result_msg)
                            #     elif self.previous_command[robot_name] == "delivery_dropoff":
                            #         self.node.get_logger().info(f"Robot {robot_name} completed ingestor task")
                            #         self.ingestor_result_pub.publish(result_msg)

                            return True, None
                        time.sleep(1)
                    else:
                        pass
        else:
            print(
                f"Robot {robot_name} not found in state data for action states")
            return False, None

    def get_data(self, robot_name: str):
        map = self.map(robot_name)
        position = self.current_robot_positions.get(robot_name)
        battery_soc = self.battery_soc(robot_name)
        if not (map is None or position is None or battery_soc is None):
            return RobotUpdateData(robot_name, map, position, battery_soc, self.current_robot_positions)
        return None


class RobotUpdateData:
    def __init__(self,
                 robot_name: str,
                 map: str,
                 position: list[float],
                 battery_soc: float,
                 current_robot_positions: dict,
                 requires_replan: bool | None = None):
        self.robot_name = robot_name
        self.position = current_robot_positions.get(robot_name)
        self.map = "L1"
        self.battery_soc = battery_soc
        self.requires_replan = requires_replan
