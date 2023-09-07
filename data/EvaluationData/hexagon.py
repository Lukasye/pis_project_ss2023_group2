import numpy as np
import matplotlib.pyplot as plt
import matplotlib.tri as tri

Flink1 = {"name": "Flink Variation 1", "pro": [0.8, 0.8, 1, 1, 0.9, 0.8]}
Flink2 = {"name": "Flink Variation 1", "pro": [1, 1, 0.8, 0.5, 0.9, 0.8]}
Flink3 = {"name": "Flink Variation 1", "pro": [0.7, 0.7, 0.7, 0.7, 0.9, 0.8]}
Spark1 = {"name": "Flink Variation 1", "pro": [0.5, 0.6, 0.5, 0.8, 0.9, 0.8]}
Spark2 = {"name": "Flink Variation 1", "pro": [0.6, 0.5, 0.6, 0.6, 0.9, 0.8]}

proportions = Spark1['pro']
title = Flink1['name']

labels = ['FL', 'DL', 'RL', 'MT', 'FT', 'DT']
N = len(proportions)
proportions = np.append(proportions, 1)
theta = np.linspace(0, 2 * np.pi, N, endpoint=False)
x = np.append(np.sin(theta), 0)
y = np.append(np.cos(theta), 0)
triangles = [[N, i, (i + 1) % N] for i in range(N)]
triang_backgr = tri.Triangulation(x, y, triangles)
triang_foregr = tri.Triangulation(x * proportions, y * proportions, triangles)
cmap = plt.cm.coolwarm
colors = np.linspace(0, 1, N + 1)
plt.tripcolor(triang_backgr, colors, cmap=cmap, shading='gouraud', alpha=0.4)
plt.tripcolor(triang_foregr, colors, cmap=cmap, shading='gouraud', alpha=0.8)
plt.triplot(triang_backgr, color='white', lw=2)
for label, color, xi, yi in zip(labels, colors, x, y):
    plt.text(xi * 1.05, yi * 1.05, label,  # color=cmap(color),
             ha='left' if xi > 0.1 else 'right' if xi < -0.1 else 'center',
             va='bottom' if yi > 0.1 else 'top' if yi < -0.1 else 'center')
plt.axis('off')
plt.gca().set_aspect('equal')
# plt.title(title)
plt.show()