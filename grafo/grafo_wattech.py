import networkx as nx
import matplotlib.pyplot as plt

G = nx.DiGraph()

G.add_node("Shelly EM\n(Consumo total)")
G.add_node("Sonoff POW R3\n(Consumo individual)")
G.add_node("HMS M1c1\n(Temperatura)")
G.add_node("CT Clamp\n(Corrente)")
G.add_node("PZEM-004T\n(Fator de Potência)")
G.add_node("Fluke 1735\n(Potência)")

G.add_edges_from([
    ("Shelly EM\n(Consumo total)", "Sonoff POW R3\n(Consumo individual)"),
    ("HMS M1c1\n(Temperatura)", "CT Clamp\n(Corrente)"),
    ("HMS M1c1\n(Temperatura)", "Shelly EM\n(Consumo total)"),
    ("CT Clamp\n(Corrente)", "PZEM-004T\n(Fator de Potência)"),
    ("CT Clamp\n(Corrente)", "Fluke 1735\n(Potência)"),
    ("CT Clamp\n(Corrente)", "Sonoff POW R3\n(Consumo individual)"),
    ("PZEM-004T\n(Fator de Potência)", "Fluke 1735\n(Potência)"),
    ("PZEM-004T\n(Fator de Potência)", "CT Clamp\n(Corrente)"),
    ("Fluke 1735\n(Potência)", "HMS M1c1\n(Temperatura)"),
])

plt.figure(figsize=(12, 8))
pos = nx.spring_layout(G, seed=42)
nx.draw(G, pos, with_labels=True, node_color='lightblue', node_size=3500, font_size=10, arrows=True)
plt.title("Grafo de Comunicação entre Sensores da Rede Elétrica")
plt.show()