import paho.mqtt.client as mqtt
import json
import time
import uuid
from datetime import datetime

class RobotAPI:
    def __init__(self, config_yaml):
        self.prefix = config_yaml['prefix']
        self.user = config_yaml['user']
        self.password = self.timeout = 5.0
        self.debug = False
        self.state_data = {}
        self.visualization_data = {}
        self.last_destination_node = None
        self.last_order = {}
        self.task_orders = {}
        self.sequence_map = {}
        self.client = mqtt.Client()
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message
        print("Connecting to MQTT broker")
        self.client.connect("127.0.0.1", 1883, 60)
        self.client.loop_start()
        self.robot_position = []
    
    def on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            self.broker_connected = True
            self.client.subscribe(f"{self.prefix}/#")
        else:
            self.broker_connected = False
            print(f"Connection failed with result code {rc}")

    def on_message(self, client, userdata, msg):
        if msg.topic.endswith("/connection"):
            self.handle_connection_message(msg.payload.decode())
        elif msg.topic.endswith("/state"):
            self.handle_state_message(msg.payload.decode())
        elif msg.topic.endswith("/visualization"):
            self.handle_visualization_message(msg.payload.decode())

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
                self.state_data[robot_name] = message
                self.robot_position = [message.get("agvPosition").get("x"), message.get("agvPosition").get("y"), message.get("agvPosition").get("theta")]
        except json.JSONDecodeError:
            print("Error decoding JSON message")

    def handle_visualization_message(self, payload):
        try:
            message = json.loads(payload)
            robot_name = message.get("serialNumber")
            self.visualization_data[robot_name] = message
            if robot_name:
                self.robot_position = [message.get("agvPosition").get("x"), message.get("agvPosition").get("y"), message.get("agvPosition").get("theta")]

        except json.JSONDecodeError:
            print("Error decoding JSON message")
            
    def check_robot_connection(self, robot_name: str):
        return True

    def check_broker_connection(self):
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
        if task_id not in self.task_orders:
            orderId = str(uuid.uuid4())
            orderUpdateId = 0
            self.task_orders[task_id] = {"orderId": orderId, "orderUpdateId": orderUpdateId}
            self.sequence_map[task_id] = {"nodes": {}, "edges": {}}  # Initialize hashmap for the task
        else:
            self.task_orders[task_id]["orderUpdateId"] += 1
            orderId = self.task_orders[task_id]["orderId"]
            orderUpdateId = self.task_orders[task_id]["orderUpdateId"]

        print(f"Task ID: {task_id}")
        print(f"Order ID: {orderId}")
        print(f"Order Update ID: {orderUpdateId}")
        print(f"Navigate to {pose[0]}, {pose[1]} on map {map_name}")
        print(f"Nodes: {nodes}")
        print(f"Edges: {edges}")
        if edges is None:
            return

        order_nodes = []
        for node in nodes:
            node_position = {
                "x": node[1][0] - (10.034804254049215 * 1),
                "y": node[1][1] - (-9.220091158197453 * 1),
                "theta": 0,
                "mapId": "map",
                "allowedDeviationXY": 0.6,
                "allowed_deviation_theta": 3.141592653589793
            }
            sequence_id = self.get_sequence_id(task_id, "nodes", node[0])
            order_nodes.append({
                "nodeId": node[0],
                "sequenceId": sequence_id,
                "released": True,
                "nodePosition": node_position,
                "actions": []
            })

        order_edges = []
        for i, edge in enumerate(edges):
            sequence_id = self.get_sequence_id(task_id, "edges", edge)
            start_node_id = nodes[i][0] if i < len(nodes) else ""
            end_node_id = nodes[i + 1][0] if i + 1 < len(nodes) else ""
            order_edges.append({
                "edgeId": str(edge),
                "released": True,
                "sequenceId": sequence_id +1,
                "startNodeId": start_node_id,
                "endNodeId": end_node_id,
                "actions": []
            })

        order = {
            "headerId": int(time.time()),
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
            "version": "2.0.0",
            "manufacturer": "ALTINAY",
            "serialNumber": robot_name,
            "orderId": orderId,
            "orderUpdateId": orderUpdateId,
            "nodes": order_nodes,
            "edges": order_edges
        }

        self.client.publish(f"{self.prefix}/{robot_name}/order", json.dumps(order))
        print(f"Order published: {order}")
        self.last_order[robot_name] = {"orderId": orderId, "orderUpdateId": orderUpdateId}
        self.last_destination_node = nodes[-1][0] if nodes else None
        return True

    def get_sequence_id(self, task_id, entity_type, entity_id):
        """
        Get sequence ID for a given node or edge in a task.

        Args:
            task_id (str): The task ID.
            entity_type (str): Type of the entity ('nodes' or 'edges').
            entity_id (str): ID of the entity (node ID or edge ID).

        Returns:
            int: The sequence ID.
        """
        if entity_id in self.sequence_map[task_id][entity_type]:
            return self.sequence_map[task_id][entity_type][entity_id]
        else:
            existing_ids = list(self.sequence_map[task_id][entity_type].values())
            new_sequence_id = max(existing_ids) + 2 if existing_ids else 0
            self.sequence_map[task_id][entity_type][entity_id] = new_sequence_id
            return new_sequence_id

    def start_activity(
        self,
        robot_name: str,
        activity: str,
        label: str
    ):
        order = {
            "headerId": int(time.time()),
            "timestamp": datetime.utcnow().isoformat() + 'Z',
            "version": "1.3.2",
            "manufacturer": "YourManufacturer",
            "serialNumber": robot_name,
            "orderId": str(uuid.uuid4()),
            "orderUpdateId": 0,
            "nodes": [
                {
                    "nodeId": "current_position",
                    "sequenceId": 0,
                    "released": True,
                    "actions": [
                        {
                            "actionId": str(uuid.uuid4()),
                            "actionType": activity,
                            "blockingType": "HARD",
                            "actionDescription": label,
                            "actionParameters": []
                        }
                    ]
                }
            ],
            "edges": []
        }

        self.client.publish(f"{self.prefix}/{robot_name}/order", json.dumps(order))
        return True

    def stop(self, robot_name: str):
        stop_action = {
            "actionId": str(uuid.uuid4()),  # Unique action ID
            "actionType": "cancelOrder",
            "blockingType": "HARD",
            "actionParameters": [
                {
                    "key": "orderId",
                    "value": self.last_order[robot_name]["orderId"]
                },
                {
                    "key": "orderUpdateId",
                    "value": self.last_order[robot_name]["orderUpdateId"]
                }
            ]
        }

        instant_action = {
            "headerId": int(time.time()),
            "timestamp": datetime.utcnow().isoformat() + 'Z',
            "version": "2.0.0",
            "manufacturer": "ALTINAY",
            "serialNumber": robot_name,
            "actions": [stop_action]
        }
        print(f"Stopping robot {robot_name}")
        print(f"Instant action: {instant_action}")
        self.client.publish(f"{self.prefix}/{robot_name}/instantAction", json.dumps(instant_action))
        return True

    def position(self, robot_name: str):
        if robot_name in self.state_data or self.visualization_data:
            return self.robot_position
        else:
            print(f"Robot {robot_name} not found in state data for position")
            return None

    def battery_soc(self, robot_name: str):
        if robot_name in self.state_data:
            state = self.state_data[robot_name]
            return 1.0
        else:
            print(f"Robot {robot_name} not found in state data for battery SOC")
            return None

    def map(self, robot_name: str):
        if robot_name in self.state_data:
            state = self.state_data[robot_name]
            return state.get("agvPosition").get("mapId")
        else:
            print(f"Robot {robot_name} not found in state data for map")
            return None

    def is_command_completed(self, robot_name: str):
        if robot_name in self.state_data:
            state = self.state_data[robot_name]
            if not state.get("nodeStates") and not state.get("edgeStates"):
                if self.last_destination_node is not None:
                    current_node = state.get("lastNodeId")
                    if current_node == self.last_destination_node:
                        return True
                else:
                    return True
            return False
        else:
            print(f"Robot {robot_name} not found in state data for action states")
            return False


    def get_data(self, robot_name: str):
        map = self.map(robot_name)
        position = self.position(robot_name)
        battery_soc = self.battery_soc(robot_name)
        if not (map is None or position is None or battery_soc is None):
            return RobotUpdateData(robot_name, map, position, battery_soc)
        return None


class RobotUpdateData:
    def __init__(self,
                 robot_name: str,
                 map: str,
                 position: list[float],
                 battery_soc: float,
                 requires_replan: bool | None = None):
        self.robot_name = robot_name
        self.position = position
        self.map = "L1"
        self.battery_soc = battery_soc
        self.requires_replan = requires_replan
