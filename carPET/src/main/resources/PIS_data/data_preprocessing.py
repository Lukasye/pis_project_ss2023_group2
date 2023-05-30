import pickle
import pandas as pd
import time
from datetime import datetime
import numpy as np
import matplotlib.pyplot as plt
import plotly.express as px
from tqdm import tqdm
from PIL import Image
import os


# global variables
gif_save_pth = './gifs'
img_path = './img'

def convert_img_index():
    file_list = os.listdir(img_path)
    for file in file_list:
        prefix = file.split('.')[0]
        prefix = str(int(prefix))
        os.rename(os.path.join(img_path, file), os.path.join(img_path, prefix + '.png'))


def load_img(path: str, type: str = None ,resize: tuple = None):
    files = {}
    file_list = os.listdir(path)
    for file in file_list:
        if type is not None and not file.endswith(type):
            continue
        num = int(file.split('.')[0])
        img = Image.open(os.path.join(path, file))
        if resize is not None:
            img = img.resize(resize)
        files[num] = img
    return files


def extract_timestampe(path: str):
    '''
    rewrite the given timestamp to the python datetime format
    '''
    with open(os.path.join(path, 'timestamps.txt'), 'r') as f:
        timestamps = f.readlines()
    timestamps = [i.removesuffix('\n')[:-3] for i in timestamps]
    timestamps = [datetime.strptime(i, "%Y-%m-%d %H:%M:%S.%f") for i in timestamps]
    timestamps = [time.mktime(i.timetuple()) + (i.microsecond / 1000000.0) for i in timestamps]
    return timestamps


def process_gps_data(path: str):
    """
    process gps information and return a data frame
    """
    output = []
    raw_data_path = os.path.join(path, 'data')
    raw_data_dir = os.listdir(raw_data_path)
    for file in raw_data_dir:
        if file.endswith('.txt'):
            with open(os.path.join(raw_data_path, file), 'r') as f:
                # Here we only care about the specified information
                data = f.readline().split(' ')
                data[-1] = data[-1].removesuffix('\n')
                output.append(data)
    with open(os.path.join(path, 'dataformat.txt'), 'r') as f:
        indexing = f.readlines()
    indexing = [i.split(':')[0] for i in indexing]
    timestamps = extract_timestampe(path)
    df = pd.DataFrame(output, columns=indexing)
    df['timestamp'] = timestamps
    df.set_index('timestamp', inplace=True)
    return df


def plot_gps_data(df, show: bool = True, save_path: str = None):
    '''
    plot the gps figure, show or save after that
    '''
    fig = px.scatter_mapbox(df,
                            lat="lat",
                            lon="lon",
                            zoom=17,
                            height=800,
                            width=800)

    fig.update_layout(mapbox_style="open-street-map")
    fig.update_layout(margin={"r":0,"t":0,"l":0,"b":0})
    if show:
        fig.show()
    else:
        fig.write_image(save_path)

def generate_gif_pic(gps):
    if not os.path.exists(gif_save_pth):
        os.mkdir(gif_save_pth)
    for i in tqdm(range(len(gps))):
        plot_gps_data(gps[:i], show=False, save_pth=gif_save_pth + f'/{i}.png')
    
def generate_gif():
    imgs = load_img(img_path)
    gifs = load_img(gif_save_pth, resize=(300, 300))
    output = []
    for i in range(len(gifs)):
        img = imgs[i]
        gif = gifs[i]
        img.paste(gif, (0,0), mask = gif)
        # img.save(f'./test/{count}.png')
        # img.show()
        output.append(img)
    img.save('./result.gif', save_all=True, append_images=output) 


def main():
    # proceccing gps data
    if os.path.exists('./gps_info.csv'):
        # load if you have already the data
        gps_df = pd.read_csv('./gps_info.csv')
    else:
        gps_path = '../raw/oxts'
        gps_df = process_gps_data(gps_path)
        gps_df.to_csv('gps_info.csv')
    # plot_gps_data(gps_df)
    # generate_gif_pic(gps_df)
    generate_gif()
    




if __name__ == '__main__':
    # convert_img_index()
    main()