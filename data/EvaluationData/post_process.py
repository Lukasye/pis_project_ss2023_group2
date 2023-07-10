import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
from datetime import datetime
import altair as alt
import plotly.express as px
from datetime import datetime


alt.renderers.enable('jupyterlab')


def variation1(plot: bool = False):
    colums = ['create', 'location', 'speed', 'camera', 'end', 'locationpolicy', 'speedpolicy', 'camerapolicy']
    df = pd.read_csv('./timerecord.csv', names=colums, header=None)
    # df['create'] = [datetime.fromtimestamp(x) for x in df['create']]
    # for time in ['create', 'location', 'speed', 'camera']:
    #     df[time] = pd.to_datetime(df[time] * 1000000)
    # df['start'] = pd.to_datetime(df['create'])
    # df['end'] = pd.to_datetime(df['camera'])
    df = df[:100]
    df['createTime'] = df['location'] - df['create']
    df['locationTime'] = df['speed'] - df['location']
    df['speedTime'] = df['camera'] - df['speed']
    df['imageTime'] = df['end'] - df['camera']

    df.sort_values(by=['create'])
    df['start_diff'] = df['create'] - df['create'][0]
    df = df[9:]

    species = np.arange(len(df))
    width = 0.5

    fig, ax = plt.subplots()
    bottom = np.zeros(len(df)) + list(df.to_dict()['start_diff'].values())
    print(bottom)

    if plot:
        legend = ['createTime', 'locationTime', 'speedTime', 'imageTime']
        for name in legend:
            weight_count = list(df.to_dict()[name].values())
            print(weight_count)
        # for weight_count in df.to_dict().items():
            p = ax.bar(species, weight_count, width, bottom=bottom)
            bottom += weight_count

        ax.set_title("Stream Processing Timeline")
        ax.set_xlabel('Events')
        ax.set_ylabel('Duration(ns)')
        ax.legend(legend)

        plt.show()

def variation2(plot: bool = False):
    colums_gps = ['create', 'evaluation', 'location', 'speed', 'end', 'locationpolicy', 'speedpolicy', 'camerapolicy']
    colums_img = ['create', 'evaluation', 'image', 'end', 'locationpolicy', 'speedpolicy', 'camerapolicy']
    df_gps = pd.read_csv('./variation2_gps.csv', names=colums_gps, header=None)
    df_img = pd.read_csv('./variation2_img.csv', names=colums_img, header=None)


    df_gps['createTime'] = df_gps['evaluation'] - df_gps['create']
    df_img['createTime'] = df_img['evaluation'] - df_img['create']

    df_gps['evaluationTime'] = df_gps['location'] - df_gps['evaluation']
    df_img['evaluationTime'] = df_img['image'] - df_img['evaluation']

    df_gps['processTime'] = df_gps['end'] - df_gps['location']
    df_img['processTime'] = df_img['end'] - df_img['image']


    df_gps.sort_values(by=['create'])
    df_img.sort_values(by=['create'])
    df_gps['start_diff'] = df_gps['create'] - df_gps['create'][0]
    df_img['start_diff'] = df_img['create'] - df_img['create'][0]
    # df = df[9:]
    df_gps = df_gps[30:50]
    df_img = df_img[30:50]

    species = np.arange(len(df_gps))
    width = 0.5

    if plot:
        fig, axes = plt.subplots(nrows=2, ncols=1)
        bottom = np.zeros(len(df_gps)) + list(df_gps.to_dict()['start_diff'].values())
        bottom_img = np.zeros(len(df_gps)) + list(df_img.to_dict()['start_diff'].values())

        legend = ['createTime', 'evaluationTime', 'processTime']
        for name in legend:
            weight_count = list(df_gps.to_dict()[name].values())
            weight_count_img = list(df_img.to_dict()[name].values())
            axes[0].bar(species, weight_count, width, bottom=bottom)
            axes[1].bar(species, weight_count_img, width, bottom=bottom_img)
            bottom += weight_count
            bottom_img += weight_count_img
            print(bottom[0])

        # fig.set_title("Stream Processing Timeline")
        axes[1].set_xlabel('Events')
        axes[0].set_ylabel('Duration(ns)')
        axes[1].set_ylabel('Duration(ns)')
        axes[0].legend(legend)
        axes[1].legend(legend)

        plt.show()

def calculate_diff(path: str, col1: int, col2: int):
    raw = pd.read_csv(path, header=None)
    data = raw.iloc[:, col1] - raw.iloc[:, col2]
    return data

if __name__ == '__main__':
    # variation1()
    # variation2()
    data1 = calculate_diff('./raw/run100_variation3_location.csv', 2, 1)
    # data2 = calculate_diff('./raw/run100_variation2_img.csv', 2, 1)
    # data2 = calculate_diff('./raw/variation3_location.csv', 2, 1)
    # data2 = data2[5:100]
    # plt.figure()
    # plt.title('Policy start changing after 74 steps')
    # plt.plot(data1)
    # plt.ylabel('Duration(ns)')
    # plt.xlabel('Messages')
    # plt.legend(['GPS', 'Image'])
    # plt.show()
    data1.to_csv('./evaluated/run100_Variation3_dyna_load.csv')