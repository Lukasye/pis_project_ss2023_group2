import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
from datetime import datetime
import altair as alt
import plotly.express as px
from datetime import datetime


alt.renderers.enable('jupyterlab')


def main():
    colums = ['create', 'location', 'speed', 'camera', 'locationpolicy', 'speedpolicy', 'camerapolicy']
    df = pd.read_csv('./timerecord.csv', names=colums, header=None)
    # df['create'] = [datetime.fromtimestamp(x) for x in df['create']]
    # for time in ['create', 'location', 'speed', 'camera']:
    #     df[time] = pd.to_datetime(df[time] * 1000000)
    # df['start'] = pd.to_datetime(df['create'])
    # df['end'] = pd.to_datetime(df['camera'])
    df['createTime'] = df['location'] - df['create']
    df['locationTime'] = df['speed'] - df['location']
    df['speedTime'] = df['camera'] - df['speed']

    df.sort_values(by=['create'])
    df['start_diff'] = df['create'] - df['create'][0]
    df = df[9:]

    species = np.arange(len(df))
    width = 0.5

    fig, ax = plt.subplots()
    bottom = np.zeros(len(df)) + list(df.to_dict()['start_diff'].values())
    print(bottom)

    legend = ['createTime', 'locationTime', 'speedTime']
    for name in legend:
        weight_count = list(df.to_dict()[name].values())
        print(weight_count)
    # for weight_count in df.to_dict().items():
        p = ax.bar(species, weight_count, width, bottom=bottom)
        bottom += weight_count

    ax.set_title("Timeline")
    ax.legend(legend)

    plt.show()


if __name__ == '__main__':
    main()