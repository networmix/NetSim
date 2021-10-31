from typing import Dict, Hashable


class MultiDiGraph:
    """
    This class implements a data structure representing a Directed Multigraph.
    By definition, the edges of a directed graph are directed and there could be several parallel edges
    between a pair of nodes (so it is a multigraph).
    A graph consists of nodes and edges. Nodes are kept straight in a Python dictionary. Edges are adjacency-encoded,
    and, at the same time, each edge has its own identity in a form of a unique index.
    Nodes, edges and the graph itself can have associated attributes.
    The adjacency structure is implemented with Python nested dictionaries:
        {src_node: {dst_node: edge_id: {**edge_attr}}}
    Edges are stored as tuples indexed by unique ids:
        {edge_id: (src_node, dst_node, edge_id, {**edge_attr})}
    Attributes:
        graph: dictionary for graph attributes
        nodes: dictionary for nodes
        edges: dictionary for edges
        adj_out: dictionary for outgoing adjacencies (successors, in other words, adjacent destination nodes)
        adj_in: dictionary for incoming adjacencies (predecessors, in other words, adjacent source nodes)
    """

    def __init__(self, **attr: Dict):
        self._graph = attr  # dictionary for graph attributes
        self._nodes = {}  # dictionary for nodes
        self._edges = {}  # dictionary for edges
        self._adj_out = {}  # dictionary for outgoing adjacencies (successors)
        self._adj_in = {}  # dictionary for incoming adjacencies (predecessors)

        self._next_edge_id = 1  # the index for the next added edge

    def __contains__(self, node_id):
        """
        Enables expressions like "node" in graph
        Returns:
            True if node_id is in the graph and False otherwise.
        """
        return node_id in self._nodes

    def __iter__(self):
        """
        Making MultiDiGraph objects iterable by their nodes
        """
        return iter(self._nodes.keys())

    def __len__(self):
        """
        Return the number of nodes as the length of the graph.
        Returns:
            The number of nodes in the graph.
        """
        return len(self._nodes)

    def get_next_edge_id(self) -> int:
        next_edge_id = self._next_edge_id
        self._next_edge_id += 1
        return next_edge_id

    def add_node(self, node_to_add: Hashable, **attr: Dict) -> None:
        """
        Add a single node with optional attributes. If the node is present - do nothing.
        Args:
            node_to_add: node identifier.
                Can be any hashable Python object.
            attr: optional node attributes in a form of keyword arguments (k=v pairs).
        """
        if node_to_add not in self._nodes:
            self._nodes[node_to_add] = attr
            self._adj_out[node_to_add] = {}
            self._adj_in[node_to_add] = {}

    def add_edge(
        self, src_node: Hashable, dst_node: Hashable, edge_id: int = 0, **attr: Dict
    ) -> None:
        """
        Add a single edge between src_node and dst_node with optional attributes.
        If optional edge_id is supplied, this method checks if an edge with such id already exists.
        If it does not exist - the method creates it. Otherwise, it replaces the attributes.
        In the case where source and/or destination nodes do not exist, the method creates them.
        Args:
            src_node: source node identifier.
            dst_node: destination node identifier.
            edge_id: optional unique edge id.
            attr: optional node attributes in a form of keyword arguments (k=v pairs).
        """
        if edge_id == 0:
            edge_id = self.get_next_edge_id()

        if edge_id not in self._edges:
            self._edges[edge_id] = (src_node, dst_node, edge_id, attr)

        if src_node not in self._nodes:
            self.add_node(src_node)

        if dst_node not in self._nodes:
            self.add_node(dst_node)

        self._adj_out[src_node].setdefault(dst_node, {})[edge_id] = attr
        self._adj_in[dst_node].setdefault(src_node, {})[edge_id] = attr

    def remove_edge(
        self, src_node: Hashable, dst_node: Hashable, edge_id: int = 0
    ) -> None:
        """
        Remove an edge between src_node and dst_node.
        If edge_id is given, it will remove
        that edge or, if it doesn't exist, it will do nothing.
        If the are multiple edges between the given source and dstination nodes,
        all of them will be removed (obeying provided direction).
        Args:
            src_node: source node identifier.
            dst_node: destination node identifier.
            edge_id: optional unique edge id.
        """
        if src_node not in self._nodes or dst_node not in self._nodes:
            return

        if edge_id != 0:
            if edge_id not in self._edges:
                return

            del self._edges[edge_id]
            del self._adj_out[src_node][dst_node][edge_id]
            del self._adj_in[dst_node][src_node][edge_id]
            if not self._adj_out[src_node][dst_node]:
                del self._adj_out[src_node][dst_node]
                del self._adj_in[dst_node][src_node]

        else:
            for _edge_id in self._adj_out[src_node][dst_node]:
                del self._edges[_edge_id]
            del self._adj_out[src_node][dst_node]
            del self._adj_in[dst_node][src_node]

    def remove_node(self, node_to_remove: Hashable) -> None:
        """
        Remove a node. It also removes all the edges this node participates in.
        If the node doesn't exist, it will do nothing.
        Args:
            node_to_remove: node identifier.
        """
        if node_to_remove not in self._nodes:
            return

        for remote_node in list(self._adj_out[node_to_remove].keys()):
            self.remove_edge(node_to_remove, remote_node)
            self.remove_edge(remote_node, node_to_remove)

        del self._adj_out[node_to_remove]
        del self._adj_in[node_to_remove]
        del self._nodes[node_to_remove]

    def get_adj_out(self) -> Dict:
        """
        Get a dictionary with outgoing adjacencies (successors).
        The format is:
        {src_node: {dst_node: edge_id: {**edge_attr}}}
        Do not remove or add any adjacencies using this dictionary.
        It will break the consistency of the graph object.
        Use only respective graph methods for that purpose.
        Returns:
            A dict with all outgoing adjacencies.
        """
        return self._adj_out

    def get_adj_in(self) -> Dict:
        """
        Get a dictionary with incoming adjacencies (predecessors).
        The format is:
        {dst_node: {src_node: edge_id: {**edge_attr}}}
        Do not remove or add any adjacencies using this dictionary.
        It will break the consistency of the graph object.
        Use only respective graph methods for that purpose.
        Returns:
            A dict with all incoming adjacencies.
        """
        return self._adj_in

    def get_graph(self) -> Dict:
        """
        Get a dictionary with all graph attributes.
        Returns:
            A dict with all graph attributes.
        """
        return self._graph

    def get_nodes(self) -> Dict:
        """
        Get a dictionary with all nodes and their attributes.
        Returns:
            A dict with all node_ids maped into their attributes.
        """
        return self._nodes

    def get_edges(self) -> Dict:
        """
        Get a dictionary with all edges and their attributes.
        Edges are stored as tuples indexed by their unique ids:
            {edge_id: (src_node, dst_node, edge_id, {**edge_attr})}
        Returns:
            A dict with all edge_ids maped into their attributes.
        """
        return self._edges
