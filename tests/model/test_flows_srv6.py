"""Placement's packet-state contract, independent of the SR policy interpreter."""

from dataclasses import fields, replace
from fractions import Fraction

import pytest

from netsim.model import flows
from netsim.model import forwarding as fw
from netsim.model.addressing import to_int
from netsim.model.igp import oracle_igp
from netsim.model.network import Network
from netsim.model.packets import SRH, IPv6Packet, PacketTemplate
from netsim.model.srv6 import PolicyRef


def _chain():
    net = Network()
    devices = {name: net.add_device(name) for name in ('R1', 'R2', 'R3', 'R4')}
    for i, dev in enumerate(devices.values(), 1):
        dev.add_loopback('lo', ipv4=[f'10.0.0.{i}/32'], ipv6=[f'2001:db8::{i}/128'])
    links = []
    for a, b in zip(('R1', 'R2', 'R3'), ('R2', 'R3', 'R4'), strict=True):
        links.append(
            net.add_p2p(devices[a], b, devices[b], a, unnumbered=True, speed=1e9)
        )
    net.add_source(oracle_igp)
    net.converge()
    return net, [link.edge(f'R{i}') for i, link in enumerate(links, 1)]


def test_every_template_field_separates_classes():
    template = PacketTemplate(4, 1, 2)
    demand = flows.Demand('d', 'R1', 2, 4, 100e6, template=template)
    from netsim.model.packets import SRH

    for field in fields(template):
        current = getattr(template, field.name)
        if current is None:  # optional header-stack fields (srh, inner)
            value = SRH((1,), 0, 0) if field.name == 'srh' else PacketTemplate(6, 1, 2)
        else:
            value = current + 1
        changed = replace(template, **{field.name: value})
        assert replace(demand, template=changed).class_key != demand.class_key, (
            field.name
        )


def test_hash_accounts_the_transmitted_stack(monkeypatch):
    net, edges = _chain()
    net.add_demand('d', 'R1', '10.0.0.4', 100e6, mode=flows.HASH)

    def step(device, packet, *args):
        if device == 'R4':
            return fw.StepResult(fw.DELIVER, packet=packet)
        i = int(device[1:]) - 1
        if device == 'R1':
            packet = IPv6Packet(1, 2, 4, payload=packet)
        elif device == 'R2':
            packet = replace(packet, next_header=43, srh=SRH((3,), 0, 0))
        else:
            packet = packet.payload
        return fw.StepResult(
            fw.TRANSMIT, edge_id=edges[i], peer=(f'R{i + 2}', 'ingress'), packet=packet
        )

    monkeypatch.setattr(fw, 'forward_ip', step)
    monkeypatch.setattr(fw, 'receive_frame', lambda d, _i, f: step(d, f.payload))
    report = flows.derive_placement(net.state, views=lambda d: d).placement
    assert [report.offered[e] for e in edges] == pytest.approx(
        [107.4e6, 109.8e6, 103.4e6]
    )
    assert [report.carried[e] for e in edges] == pytest.approx(
        [107.4e6, 109.8e6, 103.4e6]
    )
    assert report.delivered_total == 100e6


def _encap_step(entries=0):
    """A test-only local action; real FIB, group, MTU and carrier checks follow."""

    def step(state, device, packet_state, af, dst, payload_size, **context):
        action = fw.TRANSMIT
        if device == 'R1':
            packet_state = packet_state.encap(to_int('2001:db8::4')[0], entries, (4,))
            action = 'ENCAP'
        elif device == 'R3':
            packet_state = packet_state.decap()
            action = 'DECAP'
        result, entry, edges, drops = flows._egress_edges(
            state, device, packet_state, af, dst, payload_size, **context
        )
        return (action if edges else result), entry, edges, drops

    return step


def _graph_step(graph, visited=None):
    """Ordered transitions keyed by (device, forwarding state); no policy logic."""

    def step(_state, device, packet_state, _af, _dst, payload_size, **_context):
        vertex = (device, packet_state)
        if visited is not None:
            visited.append(vertex)
        legs = graph.get(vertex, ())
        if not legs:
            return fw.DELIVER, None, [], []
        return (
            fw.TRANSMIT,
            None,
            [
                flows._Edge(
                    e,
                    peer,
                    fraction,
                    blocked,
                    successor.frame_bytes(payload_size),
                    successor,
                )
                for e, peer, successor, fraction, blocked in legs
            ],
            [],
        )

    return step


def _revisit_graph(edges, *, same_da=False):
    p = flows._PacketState(4)
    first = p.encap(100, 1, (200, 300))
    second = first._replace(active_da=100 if same_da else 200, continuation=(300,))
    inner = second.decap()
    one = Fraction(1)
    graph = {
        ('R1', p): [(edges[0], 'R2', first, one, None)],
        ('R2', first): [(edges[0] ^ 1, 'R1', second, one, None)],
        ('R1', second): [(edges[0], 'R2', inner, one, None)],
        ('R2', inner): [(edges[1], 'R3', inner, one, None)],
        ('R3', inner): [(edges[2], 'R4', inner, one, None)],
    }
    return graph


@pytest.mark.parametrize('entries,outer_wire', [(0, 107.4e6), (1, 109.8e6)])
def test_fluid_encap_transit_decap_accounting(entries, outer_wire):
    net, edges = _chain()
    net.add_demand('d', 'R1', '10.0.0.4', 100e6)
    report = flows.derive_placement(
        net.state, fluid_step=_encap_step(entries)
    ).placement
    assert [report.offered[e] for e in edges] == pytest.approx(
        [outer_wire, outer_wire, 103.4e6]
    )
    assert [report.carried[e] for e in edges] == pytest.approx(
        [outer_wire, outer_wire, 103.4e6]
    )
    assert report.delivered_total == 100e6
    assert not report.dropped_by_reason


@pytest.mark.parametrize('same_da', [False, True])
def test_repeated_device_and_physical_edge_are_separate_legs(same_da):
    net, edges = _chain()
    net.add_demand('d', 'R1', '10.0.0.4', 100e6)
    visited = []
    report = flows.derive_placement(
        net.state,
        fluid_step=_graph_step(_revisit_graph(edges, same_da=same_da), visited),
    ).placement
    assert report.delivered_total == 100e6
    assert not report.dropped_by_reason
    assert report.offered[edges[0]] == pytest.approx(109.8e6 + 103.4e6)
    assert report.carried[edges[0]] == report.offered[edges[0]]
    assert report.carried[edges[0] ^ 1] == pytest.approx(109.8e6)
    assert dict(report.demands['d'].edges)[edges[0]] == 200e6
    result = next(iter(report.classes.values()))
    assert result.visited == ('R1', 'R2', 'R3', 'R4')
    assert sorted(report.deps) == list(result.visited)
    assert len(visited) == 6
    assert [t.frame_bytes for t in result.transmissions] == [
        1098,
        1098,
        1034,
        1034,
        1034,
    ]


def test_loop_cut_uses_vertices_and_preserves_exit():
    net, edges = _chain()
    p = flows._PacketState(4)
    first, cyclic = p.encap(1, 1, (2,)), p.encap(2, 1, (1,))
    inner, one, half = cyclic.decap(), Fraction(1), Fraction(1, 2)
    graph = {
        ('R1', p): [(edges[0], 'R2', first, one, None)],
        ('R2', first): [(edges[0] ^ 1, 'R1', cyclic, one, None)],
        ('R1', cyclic): [
            (edges[0], 'R2', cyclic, half, None),
            (edges[0], 'R2', inner, half, None),
        ],
        ('R2', cyclic): [(edges[0] ^ 1, 'R1', cyclic, one, None)],
        ('R2', inner): [(edges[1], 'R3', inner, one, None)],
        ('R3', inner): [(edges[2], 'R4', inner, one, None)],
    }
    net.add_demand('d', 'R1', '10.0.0.4', 100e6)
    report = flows.derive_placement(net.state, fluid_step=_graph_step(graph)).placement
    assert report.delivered_total == 50e6
    assert dict(report.dropped_by_reason) == {fw.LOOP: 50e6}
    assert report.carried[edges[0]] == pytest.approx(109.8e6 + 51.7e6)
    assert report.offered[edges[0]] == pytest.approx(109.8e6 + 54.9e6 + 51.7e6)


@pytest.mark.parametrize(
    'device,mtu,delivered',
    [
        ('R1', 1060, 0),
        ('R2', 1083, 0),
        ('R2', 1084, 100e6),
        ('R3', 1020, 100e6),
        ('R3', 1019, 0),
    ],
)
def test_mtu_uses_that_hops_ip_stack(device, mtu, delivered):
    net, edges = _chain()
    peer = f'R{int(device[1:]) + 1}'
    net.device(device).interface(peer).configure(mtu=mtu)
    net.add_demand('d', 'R1', '10.0.0.4', 100e6)
    net.converge()
    report = flows.derive_placement(net.state, fluid_step=_encap_step(1)).placement
    assert report.delivered_total == delivered
    if not delivered:
        assert report.demands['d'].drops == ((fw.MTU_EXCEEDED, device, 100e6),)
        stopped = int(device[1:]) - 1
        assert report.offered[edges[stopped]] == 0
        if stopped:
            assert report.carried[edges[stopped - 1]] == pytest.approx(109.8e6)


def test_lossy_repeated_edge_consumes_residual_in_chain_order():
    net, edges = _chain()
    capacity = [1e9] * (max(edges) + 2)
    capacity[edges[0]] = 109.8e6 + 51.7e6
    result = flows.walk_class(
        net.state,
        4,
        4,
        1000,
        {'R1': Fraction(1)},
        capacity,
        100e6,
        step=_graph_step(_revisit_graph(edges)),
    )
    assert float(result.delivered) == pytest.approx(0.5)
    assert result.drops[0][:2] == (fw.CONGESTION, f'edge:{edges[0]}')
    assert result.delivered + result.drops[0][2] == 1
    assert capacity[edges[0]] == 0
    first, second = [t for t in result.transmissions if t.edge_id == edges[0]]
    assert first.carried == 1 and float(second.carried) == pytest.approx(0.5)


@pytest.mark.parametrize('reverse', [False, True])
def test_lossy_parallel_legs_keep_step_order(reverse):
    net, edges = _chain()
    p = flows._PacketState(4)
    thin, thick = p.encap(1), p.encap(2, 1)
    states = [thick, thin] if reverse else [thin, thick]
    graph = {('R1', p): [(edges[0], 'R2', s, Fraction(1, 2), None) for s in states]}
    caps = [1e9] * (max(edges) + 2)
    caps[edges[0]] = 80e6
    result = flows.walk_class(
        net.state, 4, 4, 1000, {'R1': Fraction(1)}, caps, 100e6, step=_graph_step(graph)
    )
    first_wire = 50e6 * states[0].frame_bytes(1000) / 1000
    second_payload = (80e6 - first_wire) / (states[1].frame_bytes(1000) / 1000)
    assert float(result.delivered) * 100e6 == pytest.approx(50e6 + second_payload)
    assert result.transmissions[0].carried == Fraction(1, 2)
    assert [t.frame_bytes for t in result.transmissions] == [
        s.frame_bytes(1000) for s in states
    ]
    assert result.delivered + sum(d[2] for d in result.drops) == 1


def test_plain_key_and_packet_state_are_stable_and_frozen():
    demand = flows.Demand('d', 'R1', 9, 4, 1)
    assert demand.class_key == (4, 9, 0, 1000)
    assert replace(demand, dscp=1).class_key != demand.class_key
    assert replace(demand, payload_size=2000).class_key != demand.class_key
    a = replace(demand, steer=PolicyRef(1, 2))
    b = replace(demand, steer=PolicyRef(1, 3))
    assert a.class_key != b.class_key != demand.class_key
    assert len(sorted([a.class_key, b.class_key, demand.class_key])) == 3
    p = flows._PacketState(4)
    assert p.decap() is p
    assert p.frame_bytes(1000) == 1034
    assert flows._PacketState(6).frame_bytes(1000) == 1054
    assert p.encap(1).frame_bytes(1000) == 1074
    assert p.encap(1, 1).frame_bytes(1000) == 1098
    with pytest.raises(AttributeError):
        p.outer = True
    with pytest.raises(ValueError, match='NESTED_ENCAP_UNSUPPORTED'):
        p.encap(1).encap(2)
    with pytest.raises(ValueError, match='non-negative'):
        p.encap(1, -1)


@pytest.mark.parametrize('mode', [flows.FLUID, flows.HASH])
def test_template_controls_actual_destination_family_and_payload(mode):
    net, edges = _chain()
    template = PacketTemplate(
        6, to_int('2001:db8::1')[0], to_int('2001:db8::4')[0], payload_size=1200
    )
    net.add_demand('d', 'R1', '10.9.9.9', 100e6, template=template, mode=mode)
    report = net.place()
    assert report.delivered_total == 100e6
    assert [report.carried[e] for e in edges] == pytest.approx(
        [100e6 * 1254 / 1200] * 3
    )


@pytest.mark.parametrize('mode', [flows.FLUID, flows.HASH])
def test_steering_never_silently_falls_through_before_sr_integration(mode):
    net, edges = _chain()
    net.add_demand('d', 'R1', '10.0.0.4', 100e6, mode=mode, steer=PolicyRef(1, 2))
    report = net.place()
    assert report.delivered_total == 0
    assert dict(report.dropped_by_reason) == {fw.SRV6_UNSUPPORTED: 100e6}
    assert report.offered[edges[0]] == 0


@pytest.mark.parametrize('model', [flows.UNCONSTRAINED, flows.LOSSY])
@pytest.mark.parametrize('rate', [100e6, 1e-10])
def test_hash_clips_per_hop_and_sums_repeated_edge(monkeypatch, model, rate):
    net, edges = _chain()
    demand = flows.Demand('d', 'R1', 4, 4, rate, mode=flows.HASH, flows=2)

    def step(device, packet, *args):
        if device == 'R1' and not isinstance(packet, IPv6Packet):
            packet = IPv6Packet(1, 2, 43, srh=SRH((3,), 0, 0), payload=packet)
            return fw.StepResult(
                fw.TRANSMIT, edge_id=edges[0], peer=('R2', 'in'), packet=packet
            )
        if device == 'R2' and isinstance(packet, IPv6Packet):
            return fw.StepResult(
                fw.TRANSMIT, edge_id=edges[0] ^ 1, peer=('R1', 'in'), packet=packet
            )
        if device == 'R1':
            return fw.StepResult(
                fw.TRANSMIT, edge_id=edges[0], peer=('R2', 'in'), packet=packet.payload
            )
        return fw.StepResult(fw.DELIVER, packet=packet)

    monkeypatch.setattr(fw, 'forward_ip', step)
    monkeypatch.setattr(fw, 'receive_frame', lambda d, _i, f: step(d, f.payload))
    residual = [1e9] * (max(edges) + 2) if model == flows.LOSSY else None
    if residual is not None:
        # Each microflow consumes this shared residual in its entire leg order.
        residual[edges[0]] = rate * (1.098 + 0.517)
    wire = ({}, {})
    offered, carried, delivered, drops = flows.walk_hash(
        net.state, demand, lambda d: d, residual=residual, wire=wire
    )
    assert sum(drops.values()) + delivered == pytest.approx(rate, rel=1e-12, abs=0)
    if model == flows.UNCONSTRAINED:
        assert offered[edges[0]] == carried[edges[0]] == 2 * rate
        assert wire[0][edges[0]] == pytest.approx(rate * 2.132, rel=1e-12, abs=0)
        assert wire[1][edges[0]] == wire[0][edges[0]]
    else:
        assert wire[1][edges[0]] == pytest.approx(rate * 1.615, rel=1e-12, abs=0)
        assert 0 < delivered < rate
        assert residual[edges[0]] == 0


def test_encapsulated_physical_failure_offers_full_stack():
    net, edges = _chain()
    net.add_demand('d', 'R1', '10.0.0.4', 100e6)
    net.links[next(iter(net.links))].fail()
    report = flows.derive_placement(net.state, fluid_step=_encap_step(1)).placement
    assert report.offered[edges[0]] == pytest.approx(109.8e6)
    assert report.carried[edges[0]] == 0
    assert dict(report.dropped_by_reason) == {fw.LINK_DOWN: 100e6}


def test_multi_source_wire_attribution_when_one_source_drops():
    net, edges = _chain()
    net.add_demand('head', 'R1', '10.0.0.4', 100e6)
    net.add_demand('tail', 'R3', '10.0.0.4', 200e6)
    net.device('R2').interface('R3').configure(mtu=1083)
    report = flows.derive_placement(net.state, fluid_step=_encap_step(1)).placement
    assert report.demands['head'].delivered == 0
    assert report.demands['tail'].delivered == 200e6
    assert report.carried[edges[0]] == pytest.approx(109.8e6)
    assert report.carried[edges[2]] == pytest.approx(206.8e6)
    assert next(iter(report.classes.values())).per_source


def test_injected_step_receives_context_and_bypasses_cache():
    net, edges = _chain()
    template = PacketTemplate(4, 1, to_int('10.0.0.4')[0], dscp=11)
    policy = PolicyRef(1, 2)
    net.add_demand(
        'd', 'R1', '10.0.0.4', 100e6, template=template, steer=policy, dscp=7
    )
    seen = []

    def step(state, device, packet_state, af, dst, size, **context):
        seen.append((device, context))
        return flows._egress_edges(state, device, packet_state, af, dst, size)

    first = flows.derive_placement(net.state, fluid_step=step)
    assert seen[0][1] == {'template': template, 'steer': policy, 'dscp': 11}
    assert first.placement.delivered_total == 100e6
    seen.clear()
    second = flows.derive_placement(first, fluid_step=step)
    assert len(seen) == 4
    assert second.placement.version == first.placement.version
    assert second is first
    # A custom view must never poison the next default cached walk.
    default = flows.derive_placement(second)
    assert default.placement.delivered_total == 0


def test_invalid_template_payload_rejected():
    with pytest.raises(ValueError, match='template needs payload_size'):
        flows.Demand(
            'd', 'R1', 4, 4, 100e6, template=PacketTemplate(4, 1, 4, payload_size=0)
        )


def test_lossy_report_repeated_edge_uses_each_frames_capacity():
    net, edges = _chain()
    for device, peer in [('R1', 'R2'), ('R2', 'R1')]:
        net.device(device).interface(peer).configure(speed=161.5e6)
    net.add_demand('d', 'R1', '10.0.0.4', 100e6)
    net.converge()
    report = flows.derive_placement(
        net.state, flows.LOSSY, fluid_step=_graph_step(_revisit_graph(edges))
    ).placement
    # 109.8M for the first visit plus 51.7M of plain traffic on the second.
    assert report.carried[edges[0]] == pytest.approx(161.5e6)
    assert report.offered[edges[0]] == pytest.approx(213.2e6)
    assert report.delivered_total == pytest.approx(50e6)
    assert report.carried[edges[1]] == pytest.approx(51.7e6)
    assert report.dropped_by_reason[fw.CONGESTION] == pytest.approx(50e6)


def test_encap_decap_cycle_is_finite_and_cut_after_first_visit():
    net, edges = _chain()
    p = flows._PacketState(4)
    outer, one = p.encap(1), Fraction(1)
    inner = outer.decap()
    graph = {
        ('R1', p): [(edges[0], 'R2', outer, one, None)],
        ('R2', outer): [(edges[0] ^ 1, 'R1', inner, one, None)],
        ('R1', inner): [(edges[0], 'R2', outer, one, None)],
    }
    net.add_demand('d', 'R1', '10.0.0.4', 100e6)
    report = flows.derive_placement(net.state, fluid_step=_graph_step(graph)).placement
    assert report.carried[edges[0]] == pytest.approx(107.4e6)
    assert report.offered[edges[0] ^ 1] == pytest.approx(103.4e6)
    assert report.carried[edges[0] ^ 1] == 0
    assert report.delivered_total == 0
    assert dict(report.dropped_by_reason) == {fw.LOOP: 100e6}
