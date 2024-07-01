import sys
import argparse
import yaml
import time
import threading
import asyncio
import nudged
import math
import networkx as nx

import rclpy
import rclpy.node
from rclpy.parameter import Parameter
from rclpy.duration import Duration
from rclpy.executors import SingleThreadedExecutor
from rclpy.qos import QoSProfile

import rmf_adapter
from rmf_adapter import Adapter
import rmf_adapter.easy_full_control as rmf_easy
from rmf_adapter import Transformation

from .RobotClientAPI import RobotAPI
from rmf_task_msgs.msg import BidResponse
from rmf_fleet_msgs.msg import FleetState

def compute_transforms(level, coords, node=None):
    """Get transforms between RMF and robot coordinates."""
    rmf_coords = coords['rmf']
    robot_coords = coords['robot']
    tf = nudged.estimate(rmf_coords, robot_coords)
    if node:
        mse = nudged.estimate_error(tf, rmf_coords, robot_coords)
        node.get_logger().info(
            f"Transformation error estimate for {level}: {mse}"
        )
        
    print(f"Rotation: {tf.get_rotation()}")
    print(f"Scale: {tf.get_scale()}")
    print(f"Translation: {tf.get_translation()}")
    return Transformation(
        tf.get_rotation(),
        tf.get_scale(),
        tf.get_translation()
    )

def parse_nav_graph(nav_graph_path):
    with open(nav_graph_path, "r") as f:
        nav_graph = yaml.safe_load(f)
    
    nodes = {}
    for i, vertex in enumerate(nav_graph['levels']['L1']['vertices']):
        name = vertex[2].get('name', f'node{i}')
        nodes[name] = {'x': vertex[0], 'y': vertex[1], 'attributes': vertex[2]}

    edges = {}
    for i, lane in enumerate(nav_graph['levels']['L1']['lanes']):
        edge_name = f'edge{i}'
        start_node = list(nodes.keys())[lane[0]]
        end_node = list(nodes.keys())[lane[1]]
        edges[edge_name] = {'start': start_node, 'end': end_node, 'attributes': lane[2]}
    
    return nodes, edges

def distance(p1, p2):
    return math.sqrt((p1[0] - p2[0])**2 + (p1[1] - p2[1])**2)

def create_graph_from_nav(nodes, edges):
    graph = nx.Graph()
    for edge_name, edge in edges.items():
        start_node = edge['start']
        end_node = edge['end']
        weight = distance((nodes[start_node]['x'], nodes[start_node]['y']), 
                          (nodes[end_node]['x'], nodes[end_node]['y']))
        graph.add_edge(start_node, end_node, weight=weight)
    return graph

def transform_node(x, y, rotation, scale, translation):
    # Apply rotation
    cos_theta = math.cos(rotation)
    sin_theta = math.sin(rotation)
    x_rotated = x * cos_theta - y * sin_theta
    y_rotated = x * sin_theta + y * cos_theta

    # Apply scaling
    x_scaled = x_rotated * scale
    y_scaled = y_rotated * scale

    # Apply translation
    x_transformed = x_scaled + translation[0]
    y_transformed = y_scaled + translation[1]

    return x_transformed, y_transformed

def apply_transformations(nodes, rotation, scale, translation):
    for name, node in nodes.items():
        x_transformed, y_transformed = transform_node(
            node['x'], node['y'], rotation, scale, translation)
        node['x'] = x_transformed
        node['y'] = y_transformed
        print(f"Transformed node {name}: x={x_transformed}, y={y_transformed}")

def find_path(graph, start, goal):
    try:
        path = nx.shortest_path(graph, source=start, target=goal, weight='weight')
        return path
    except nx.NetworkXNoPath:
        return None

def main(argv=sys.argv):
    rclpy.init(args=argv)
    rmf_adapter.init_rclcpp()
    args_without_ros = rclpy.utilities.remove_ros_args(argv)

    parser = argparse.ArgumentParser(
        prog="fleet_adapter",
        description="Configure and spin up the fleet adapter")
    parser.add_argument("-c", "--config_file", type=str, required=True,
                        help="Path to the config.yaml file")
    parser.add_argument("-n", "--nav_graph", type=str, required=True,
                        help="Path to the nav_graph for this fleet adapter")
    parser.add_argument("-s", "--server_uri", type=str, required=False, default="",
                        help="URI of the api server to transmit state and task information.")
    parser.add_argument("-sim", "--use_sim_time", action="store_true",
                        help='Use sim time, default: false')
    args = parser.parse_args(args_without_ros[1:])
    print(f"Starting fleet adapter...")
    print(f"Config file: {args.config_file}")
    config_path = args.config_file
    nav_graph_path = args.nav_graph

    fleet_config = rmf_easy.FleetConfiguration.from_config_files(
        config_path, nav_graph_path
    )
    assert fleet_config, f'Failed to parse config file [{config_path}]'

    nodes, edges = parse_nav_graph(nav_graph_path)

    graph = create_graph_from_nav(nodes, edges)

    with open(config_path, "r") as f:
        config_yaml = yaml.safe_load(f)

    fleet_name = fleet_config.fleet_name
    node = rclpy.node.Node(f'{fleet_name}_command_handle')
    adapter = Adapter.make(f'{fleet_name}_fleet_adapter')
    assert adapter, (
        'Unable to initialize fleet adapter. '
        'Please ensure RMF Schedule Node is running'
    )

    if args.use_sim_time:
        param = Parameter("use_sim_time", Parameter.Type.BOOL, True)
        node.set_parameters([param])
        adapter.node.use_sim_time()

    adapter.start()
    time.sleep(1.0)

    if args.server_uri == '':
        server_uri = None
    else:
        server_uri = args.server_uri

    fleet_config.server_uri = server_uri

    for level, coords in config_yaml['reference_coordinates'].items():
        print(f"Computing transforms for {level}")
        print(f"RMF coordinates: {coords['rmf']}")
        tf = compute_transforms(level, coords, node)
        fleet_config.add_robot_coordinates_transformation(level, tf)
        apply_transformations(nodes, tf.rotation, tf.scale, tf.translation)
        
    fleet_handle = adapter.add_easy_fleet(fleet_config)

    fleet_mgr_yaml = config_yaml['fleet_manager']
    api = RobotAPI(fleet_mgr_yaml)

    robots = {}
    for robot_name in fleet_config.known_robots:
        robot_config = fleet_config.get_known_robot_configuration(robot_name)
        robots[robot_name] = RobotAdapter(
            robot_name, robot_config, node, api, fleet_handle, nodes, edges, graph
        )

    update_period = 1.0/config_yaml['rmf_fleet'].get(
        'robot_state_update_frequency', 10.0
    )

    def update_loop():
        asyncio.set_event_loop(asyncio.new_event_loop())
        while rclpy.ok():
            now = node.get_clock().now()

            update_jobs = []
            for robot in robots.values():
                update_jobs.append(update_robot(robot))

            asyncio.get_event_loop().run_until_complete(
                asyncio.wait(update_jobs)
            )

            next_wakeup = now + Duration(nanoseconds=update_period*1e9)
            while node.get_clock().now() < next_wakeup:
                time.sleep(0.001)

    update_thread = threading.Thread(target=update_loop, args=())
    update_thread.start()

    rclpy_executor = SingleThreadedExecutor()
    rclpy_executor.add_node(node)

    rclpy_executor.spin()

    node.destroy_node()
    rclpy_executor.shutdown()
    rclpy.shutdown()


class RobotAdapter:
    def __init__(
        self,
        name: str,
        configuration,
        node,
        api: RobotAPI,
        fleet_handle,
        nodes,
        edges,
        graph
    ):
        self.name = name
        self.execution = None
        self.update_handle = None
        self.configuration = configuration
        self.node = node
        self.api = api
        self.fleet_handle = fleet_handle
        self.nodes = nodes
        self.edges = edges
        self.graph = graph
        self.task_id = None  
        self.current_node = None
        self.goal_node = None
        self.current_edge = None
        self.position = None
        self.last_task_id = None
        self.last_nodes = []
        self.last_edges = []

    def update(self, state):
        self.position = state.position  
        activity_identifier = None
        if self.execution:
            if self.api.is_command_completed(self.name):
                self.execution.finished()
                self.execution = None
            else:
                activity_identifier = self.execution.identifier

        self.update_handle.update(state, activity_identifier)

    def make_callbacks(self):
        return rmf_easy.RobotCallbacks(
            lambda destination, execution: self.navigate(
                destination, execution
            ),
            lambda activity: self.stop(activity),
            lambda category, description, execution: self.execute_action(
                category, description, execution
            )
        )

    def navigate(self, destination, execution):
        self.execution = execution
        new_goal_node = self.get_nearest_node([destination.position[0], destination.position[1]])

        if self.task_id == self.last_task_id and self.last_nodes:
            # Use the last goal node as the base node
            base_node = self.last_nodes[-1][0]  # Extract the node name
            print(f'Base node: {base_node}')
            print(f'New goal node: {new_goal_node}')
            path = find_path(self.graph, base_node, new_goal_node)

            if path:
                self.last_nodes = [[node, self.get_node_pose(node)] for node in path]
                new_edges = []
                for i in range(len(path) - 1):
                    for edge_name, edge in self.edges.items():
                        if (edge['start'] == path[i] and edge['end'] == path[i + 1]) or \
                                (edge['start'] == path[i + 1] and edge['end'] == path[i]):
                            new_edges.append(edge_name)
                            break
                self.last_edges = new_edges
        else:
            # Compute new path
            self.current_node = self.get_nearest_node(self.position)
            self.goal_node = new_goal_node
            path = find_path(self.graph, self.current_node, self.goal_node)

            if path:
                self.last_nodes = [[node, self.get_node_pose(node)] for node in path]
                self.last_edges = []
                for i in range(len(path) - 1):
                    for edge_name, edge in self.edges.items():
                        if (edge['start'] == path[i] and edge['end'] == path[i + 1]) or \
                                (edge['start'] == path[i + 1] and edge['end'] == path[i]):
                            self.last_edges.append(edge_name)
                            break
                self.current_edge = self.last_edges[0] if self.last_edges else None
            else:
                self.last_nodes = [[self.current_node, self.get_node_pose(self.current_node)], [self.goal_node, self.get_node_pose(self.goal_node)]]
                self.last_edges = []

            self.last_task_id = self.task_id
        
        self.node.get_logger().info(
            f'Commanding [{self.name}] to navigate to {destination.position} '
            f'on map [{destination.map}] with task_id [{self.task_id}]'
            f' from current node [{self.current_node}] to goal node [{self.goal_node}] '
            f'via edges [{self.last_edges}]'
        )
        self.api.navigate(
            self.name,
            destination.position,
            destination.map,
            destination.speed_limit,
            self.task_id,
            self.last_nodes, self.last_edges
        )

    def stop(self, activity):
        if self.execution is not None:
            if self.execution.identifier.is_same(activity):
                self.execution = None
                self.stop(self.name)

    def execute_action(self, category: str, description: dict, execution):
        self.execution = execution
        return

    def get_nearest_node(self, position):
        nearest_node = None
        min_distance = float('inf')
        for name, node in self.nodes.items():
            node_position = (node['x'], node['y'])
            dist = distance(position, node_position)
            if dist < min_distance:
                min_distance = dist
                nearest_node = name
        return nearest_node

    def get_node_pose(self, node):
        """
        Returns the (x, y) position of the node.
        """
        return (self.nodes[node]['x'], self.nodes[node]['y'])

def parallel(f):
    def run_in_parallel(*args, **kwargs):
        return asyncio.get_event_loop().run_in_executor(
            None, f, *args, **kwargs
        )

    return run_in_parallel

@parallel
def update_robot(robot: RobotAdapter):
    data = robot.api.get_data(robot.name)
    if data is None:
        return
    state = rmf_easy.RobotState(
        data.map,
        data.position,
        data.battery_soc
    )

    if robot.update_handle is None:
        robot.update_handle = robot.fleet_handle.add_robot(
            robot.name,
            state,
            robot.configuration,
            robot.make_callbacks()
        )
        robot.api.send_factsheet_request(robot.name)
        return

    robot.update(state)

if __name__ == '__main__':
    main(sys.argv)
