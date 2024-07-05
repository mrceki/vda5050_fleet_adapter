import sys
import argparse
import yaml
import time
import threading
import asyncio
import math
import networkx as nx

import rclpy
import rclpy.node
from rclpy.parameter import Parameter
from rclpy.duration import Duration
from rclpy.executors import SingleThreadedExecutor

import rmf_adapter
from rmf_adapter import Adapter
import rmf_adapter.easy_full_control as rmf_easy

from .RobotClientAPI import RobotAPI, RobotAPIResult
from rmf_task_msgs.msg import BidResponse
from rmf_fleet_msgs.msg import FleetState
from .adapter_utils import parse_nav_graph, compute_transforms, create_graph_from_nav, find_path, apply_transformations, transform_node, get_nearest_node, get_node_pose, compute_path_and_edges

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

    server_uri = args.server_uri or None
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

    robots = {robot_name: RobotAdapter(robot_name, fleet_config.get_known_robot_configuration(robot_name), node, api, fleet_handle, nodes, edges, graph)
              for robot_name in fleet_config.known_robots}

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

    def bid_response_callback(msg):
        robot_name = msg.proposal.expected_robot_name
        if robot_name in robots:
            robots[robot_name].task_id = msg.task_id
        print(f"Received bid response for robot [{robot_name}] with task_id [{msg.task_id}]")

    bid_response_sub = node.create_subscription(
        BidResponse,
        '/rmf_task/bid_response',
        bid_response_callback,
        10
    )

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
        self.issue_cmd_thread = None
        self.cancel_cmd_event = threading.Event()

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
        new_goal_node = get_nearest_node(self.nodes, [destination.position[0], destination.position[1]])
        self.node.get_logger().warn(f"Is that docking station? {destination.dock}")
        
        if destination.dock is not None:
            self.attempt_cmd_until_success(self.perform_docking, (destination,))
            return

        self.last_nodes, self.last_edges, self.current_node, self.goal_node, self.current_edge = compute_path_and_edges(
            self.task_id, self.last_task_id, self.last_nodes, self.graph, self.nodes, self.edges, new_goal_node, self.position
        )

        self.node.get_logger().info(
            f'Commanding [{self.name}] to navigate to {destination.position} '
            f'on map [{destination.map}] with task_id [{self.task_id}]'
            f' from current node [{self.current_node}] to goal node [{self.goal_node}] '
            f'via edges [{self.last_edges}]'
        )

        self.attempt_cmd_until_success(
            cmd=self.api.navigate,
            args=(
                self.name,
                destination.position,
                destination.map,
                destination.speed_limit,
                self.task_id,
                self.last_nodes, self.last_edges
            ),
        )

    def stop(self, activity):
        if self.execution is not None:
            if self.execution.identifier.is_same(activity):
                self.execution = None
                self.api.stop(self.name)

    def execute_action(self, category: str, description: dict, execution):
        self.execution = execution
        return
    
    def attempt_cmd_until_success(self, cmd, args):
        self.cancel_cmd_attempt()

        def loop():
            while not cmd(*args):
                self.node.get_logger().warn(
                    f'Failed to contact fleet manager for robot {self.name}'
                )
                if self.cancel_cmd_event.wait(1.0):
                    break

        self.issue_cmd_thread = threading.Thread(target=loop, args=())
        self.issue_cmd_thread.start()

    def cancel_cmd_attempt(self):
        if self.issue_cmd_thread is not None:
            self.cancel_cmd_event.set()
            if self.issue_cmd_thread.is_alive():
                self.issue_cmd_thread.join()
                self.issue_cmd_thread = None
        self.cancel_cmd_event.clear()

    def perform_docking(self, destination):
        self.node.get_logger().info(f'Performing docking for robot [{self.name}]')
        match self.api.start_activity(
            self.name,
            self.task_id,
            "dock",
            destination.dock,
        ):
            case(RobotAPIResult.SUCCESS, path):
                self.override= self.execution.override_schedule(
                    path["map_name"], path["path"]
                )
                return True
            case RobotAPIResult.RETRY:
                return False
            
            case RobotAPIResult.IMPOSSIBLE:
                new_goal_node = get_nearest_node(self.nodes, [destination.position[0], destination.position[1]])
                self.last_nodes, self.last_edges, self.current_node, self.goal_node, self.current_edge = compute_path_and_edges(
                    self.task_id, self.last_task_id, self.last_nodes, self.graph, self.nodes, self.edges, new_goal_node, self.position
                )
                self.attempt_cmd_until_success(
                    cmd=self.api.navigate,
                    args=(
                        self.name,
                        destination.position,
                        destination.map,
                        destination.speed_limit,
                        self.task_id,
                        self.last_nodes, self.last_edges
                    ),
                )
                

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
