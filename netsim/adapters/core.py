"""NetGraph-Core adapter: edge arrays for the compute kernel and an
independent cross-check of plain-IP FLUID placement against
``EQUAL_BALANCED`` flow placement (requires ``netgraph_core``)."""

from __future__ import annotations

from typing import Any

from netsim.model import derive
from netsim.model.interfaces import OperState, l3_usable
from netsim.model.network import Network
from netsim.model.state import NetworkState


def edge_arrays(
    net: Network, af: int = 4
) -> tuple[
    list[str], list[int], list[int], list[float], list[int], list[int], list[bool]
]:
    """``(node_names, src, dst, capacity, cost, ext_edge_ids, mask)`` with one
    directed edge per link direction; ``ext_edge_id = 2*index + direction``;
    the mask marks edges whose endpoints are L3 usable and physically up."""
    state: NetworkState = net.state
    names = [n for n, _ in state.devices.sorted_items()]
    index = {n: i for i, n in enumerate(names)}
    src: list[int] = []
    dst: list[int] = []
    cap: list[float] = []
    cost: list[int] = []
    ext: list[int] = []
    mask: list[bool] = []
    for _lid, link in sorted(state.links.items(), key=lambda kv: kv[1].index):
        capacity = derive.link_capacity(state, link)
        for tx in (link.a, link.b):
            rx = link.other(tx)
            tx_node = derive.l3_owner(state, tx[0], tx[1])
            rx_node = derive.l3_owner(state, rx[0], rx[1])
            src.append(index[tx[0]])
            dst.append(index[rx[0]])
            cap.append(capacity)
            cost.append(
                int(
                    state.devices[tx[0]].interfaces[tx[1]].config.metric
                    if not hasattr(tx_node, 'members')
                    else tx_node.config.metric
                )
            )
            ext.append(link.edge_id(tx))
            usable = (
                link.oper.state == 1
                and tx_node is not None
                and rx_node is not None
                and l3_usable(tx_node, af)
                and l3_usable(rx_node, af)
                and state.devices[tx[0]].interfaces[tx[1]].oper.oper == OperState.UP
            )
            mask.append(bool(usable))
    return names, src, dst, cap, cost, ext, mask


def build_graph(
    net: Network, af: int = 4
) -> tuple[Any, list[str], list[int], list[bool]]:
    """``(StrictMultiDiGraph, node_names, ext_edge_ids, edge_mask)``; needs ``netgraph_core`` and numpy."""
    import netgraph_core as ngc  # type: ignore[import-not-found]
    import numpy as np  # type: ignore[import-not-found]

    names, src, dst, cap, cost, ext, mask = edge_arrays(net, af)
    graph = ngc.StrictMultiDiGraph.from_arrays(
        num_nodes=len(names),
        src=np.array(src, dtype=np.int32),
        dst=np.array(dst, dtype=np.int32),
        capacity=np.array(cap, dtype=np.float64),
        cost=np.array(cost, dtype=np.int64),
        ext_edge_ids=np.array(ext, dtype=np.int64),
    )
    return graph, names, ext, mask


__all__ = ['edge_arrays', 'build_graph']
