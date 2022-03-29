import matplotlib.pyplot as plt
from matplotlib import animation
import pandas as pd

x = []
y = []
count = 0

#The below content runs every 1 minute
def draw_graph(i):
    data = pd.read_csv('nse_live_graph.csv')
    global count
    count += 1
    x.append(data['Time1'][count])
    y.append(data['lastPrice'][count])

    plt.cla()
    plt.plot(x, y)
    plt.scatter(x, y)
    plt.title('TCS NSE Live Graph')
    plt.xticks(rotation=90)


anim = animation.FuncAnimation(plt.gcf(), draw_graph, interval=60000)#60000 ms -> 60 secs

plt.show()