from heapq import heappop, heappush
from typing import Tuple, List, Dict, Callable

from netsim.graphlib.graph import MultiDiGraph


DEFAULT_COST_ATTRIBUTE = "metric"


def min_cost_edges_func_fabric(attr_to_use: str) -> Callable:
    """
    Fabric producing a function to find the min-cost edges between a pair of adjacent nodes in a graph.
    Args:
        attr_to_use: name of the integer attribute that will be used to determine the cost.
    Returns:
        get_min_cost_edges_func: a callable function
    """

    def get_min_cost_edges_func(
        graph: MultiDiGraph, src_node: str, dst_node: str
    ) -> Tuple[int, List[str]]:
        """
        Returns the min-cost edges between a pair of adjacent nodes in a graph.
        Args:
            graph: MultiDiGraph object.
            src_node: node_id of the source node.
            dst_node: node_id of the destination node.
        Returns:
            min_cost: minimal cost of the edge between src_node and dst_node
            edge_list: list of all edge_ids with the min_cost
        """
        edge_list = []
        min_cost = None
        for edge_id, edge_attributes in graph.get_adj_out()[src_node][dst_node].items():
            cost = edge_attributes[attr_to_use]

            if min_cost is None or cost < min_cost:
                min_cost = cost
                edge_list = [edge_id]
            elif cost == min_cost:
                edge_list.append(edge_id)

        return min_cost, edge_list

    return get_min_cost_edges_func


def spf(
    graph: MultiDiGraph,
    src_node: str,
    min_cost_edges_func: Callable = min_cost_edges_func_fabric(DEFAULT_COST_ATTRIBUTE),
) -> Tuple[Dict, Dict]:
    """
    Implementation of the Dijkstra's Shortest Path First algorithm for finding shortest paths in the graph.
    Implemented using a min-priority queue:
    https://en.wikipedia.org/wiki/Dijkstra%27s_algorithm#Using_a_priority_queue
    Args:
        src_node: source node identifier.
    Returns:
        costs: a dict with destination nodes mapped into the cost of the shortest path to that destination
        prev: a dict with nodes mapped into their previous (preceeding) nodes and edge list
    """

    # Initialization
    min_pq = []  # min-priority queue
    costs = {src_node: 0}  # source node has has zero cost to itself
    prev = {src_node: {}}  # source node has no preceeding nodes

    heappush(
        min_pq, (0, src_node)
    )  # push source node onto the min-priority queue using cost as priority

    while min_pq:
        # pop the node with the minimal cost from the source node
        src_to_node_cost, node_id = heappop(min_pq)

        # iterate over all the neighbors of the node we're looking at
        for neighbor_id in graph.get_adj_out()[node_id]:
            min_edge_cost, edges_list = min_cost_edges_func(graph, node_id, neighbor_id)

            src_to_neigh_cost = src_to_node_cost + min_edge_cost

            if neighbor_id not in costs or src_to_neigh_cost < costs[neighbor_id]:
                # have not seen this neighbor yet or better path found
                costs[neighbor_id] = src_to_neigh_cost
                prev[neighbor_id] = {node_id: edges_list}
                heappush(min_pq, (src_to_neigh_cost, neighbor_id))

            elif costs[neighbor_id] == src_to_neigh_cost:
                # have met this neighbor, but new equal cost path found
                prev[neighbor_id][node_id] = edges_list

    return costs, prev
