# src/text_data_bench/engines/graph.py
"""Движки для графовых форматов: GML, GraphML, DOT"""
import polars as pl
from pathlib import Path
from .base import _standardize


def load_gml(p: Path) -> pl.DataFrame:
    """Загрузка GML графов (edge list)"""
    try:
        import networkx as nx
    except ImportError:
        raise ImportError("[GML] Install networkx: pip install networkx")

    G = nx.read_gml(p)
    edges = list(G.edges())
    if edges:
        df = pl.DataFrame({"source": [str(e[0]) for e in edges], "target": [str(e[1]) for e in edges]})
    else:
        df = pl.DataFrame({"source": [], "target": []})
    return _standardize(df, "GML")


def load_graphml(p: Path) -> pl.DataFrame:
    """Загрузка GraphML графов (edge list)"""
    try:
        import networkx as nx
    except ImportError:
        raise ImportError("[GraphML] Install networkx: pip install networkx")

    G = nx.read_graphml(p)
    edges = list(G.edges())
    if edges:
        df = pl.DataFrame({"source": [str(e[0]) for e in edges], "target": [str(e[1]) for e in edges]})
    else:
        df = pl.DataFrame({"source": [], "target": []})
    return _standardize(df, "GraphML")


def load_dot(p: Path) -> pl.DataFrame:
    """Загрузка DOT графов (edge list или node list)"""
    try:
        import networkx as nx
    except ImportError:
        raise ImportError("[DOT] Install networkx: pip install networkx")

    # Пробуем pydot (легче устанавливается)
    try:
        G = nx.drawing.nx_pydot.read_dot(p)
    except ImportError:
        # Пробуем pygraphviz
        try:
            G = nx.nx_agraph.read_dot(p)
        except ImportError:
            raise ImportError("[DOT] Install pydot (pip install pydot) or pygraphviz")

    edges = list(G.edges())
    if edges:
        df = pl.DataFrame({"source": [str(e[0]) for e in edges], "target": [str(e[1]) for e in edges]})
    else:
        # Если нет рёбер, пробуем извлечь узлы
        nodes = list(G.nodes())
        if nodes:
            df = pl.DataFrame({"node": [str(n) for n in nodes]})
        else:
            df = pl.DataFrame({"source": [], "target": []})
    return _standardize(df, "DOT")
