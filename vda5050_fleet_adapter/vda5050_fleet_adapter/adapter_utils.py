import yaml
import math
import networkx as nx
import nudged
from rmf_adapter import Transformation

def parse_nav_graph(nav_graph_path):
    with open(nav_graph_path, "r") as f:
        nav_graph = yaml.safe_load(f)
    
    nodes = {}
    for i, vertex in enumerate(nav_graph['levels']['L1']['vertices']):
        name = vertex[2].get('name', f'node{i}')
        nodes[name] = {'x': vertex[0], 'y': vertex[1], 'attributes': vertex[2]}

    edges = {}
    for i, lane in enumerate(nav_graph['levels']['L1']['lanes']):
        start_node = list(nodes.keys())[lane[0]]
        end_node = list(nodes.keys())[lane[1]]
        # Create an edge for the direction from start_node to end_node
        edge_name = f'edge{i}_a'
        edges[edge_name] = {'start': start_node, 'end': end_node, 'attributes': lane[2]}
        # Create an edge for the direction from end_node to start_node
        edge_name = f'edge{i}_b'
        edges[edge_name] = {'start': end_node, 'end': start_node, 'attributes': lane[2]}
    
    return nodes, edges

def compute_transforms(level, coords, node=None):
    """Get transforms between RMF and robot coordinates."""
    rmf_coords = coords['rmf']
    robot_coords = coords['robot']
    tf = nudged.estimate(rmf_coords, robot_coords)
    if node:
        mse = nudged.estimate_error(tf, rmf_coords, robot_coords)
        node.get_logger().info(f"Transformation error estimate for {level}: {mse}")
    print(f"Rotation: {tf.get_rotation()}")
    print(f"Scale: {tf.get_scale()}")
    print(f"Translation: {tf.get_translation()}")
    return Transformation(tf.get_rotation(), tf.get_scale(), tf.get_translation())

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

def find_path(graph, start, goal):
    try:
        path = nx.shortest_path(graph, source=start, target=goal, weight='weight')
        return path
    except nx.NetworkXNoPath:
        return None
    
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

def get_nearest_node(nodes, position):
    nearest_node = None
    min_distance = float('inf')
    for name, node in nodes.items():
        node_position = (node['x'], node['y'])
        dist = distance(position, node_position)
        if dist < min_distance:
            min_distance = dist
            nearest_node = name
    return nearest_node

def get_node_pose(nodes, node):
    """Returns the (x, y) position of the node."""
    return (nodes[node]['x'], nodes[node]['y'])

def find_edge(edges, start_node, end_node):
    for edge_name, edge in edges.items():
        if edge['start'] == start_node and edge['end'] == end_node:
            return edge_name
    return None

def compute_path_and_edges(last_nodes, graph, nodes, edges, new_goal_node, position):
    """
    Compute the path and edges for the robot based on the current and goal nodes.

    Args:
        last_nodes (list): List of last nodes.
        graph (networkx.Graph): Navigation graph.
        nodes (dict): Dictionary of nodes with their attributes.
        new_goal_node (str): New goal node.
        position (tuple): Current position of the robot.

    Returns:
        tuple: Updated last nodes, last edges, current node, goal node, and current edge.
    """
    if last_nodes:
        base_node = last_nodes[-1][0]  # Extract the node name
        print(f'Base node: {base_node}')
        print(f'New goal node: {new_goal_node}')
        path = find_path(graph, base_node, new_goal_node)

        if path:
            last_nodes = [[node, get_node_pose(nodes, node)] for node in path]
            last_edges = [find_edge(edges, path[i], path[i + 1]) for i in range(len(path) - 1) if find_edge(edges, path[i], path[i + 1])]
            return last_nodes, last_edges
    else:
        current_node = get_nearest_node(nodes, position)
        goal_node = new_goal_node
        path = find_path(graph, current_node, goal_node)

        if path:
            last_nodes = [[node, get_node_pose(nodes, node)] for node in path]
            last_edges = [find_edge(edges, path[i], path[i + 1]) for i in range(len(path) - 1) if find_edge(edges, path[i], path[i + 1])]
            current_edge = last_edges[0] if last_edges else None
            return last_nodes, last_edges, current_node, goal_node, current_edge
        else:
            last_nodes = [[current_node, get_node_pose(nodes, current_node)], [goal_node, get_node_pose(nodes, goal_node)]]
            last_edges = []
            return last_nodes, last_edges, current_node, goal_node, None

    return last_nodes, last_edges
