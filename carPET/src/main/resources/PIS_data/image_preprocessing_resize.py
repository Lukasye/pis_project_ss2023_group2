from PIL import Image
from tqdm import tqdm
import os
from io import BytesIO


img_path = './img/'
new_dir = './img_resize/'
byte_dir = 'img_bytes/'
size = (1024, 512)

def main():
    dir_list = os.listdir(new_dir)
    for file in tqdm(dir_list):
        if file.endswith('.jpg'):
            img = Image.open(new_dir + file)
            img = img.resize(size)
            # img.save(new_dir + file.split('.')[0] + '.jpg')
            # img_byte_arr = io.BytesIO()
            # img.save(img_byte_arr)
            # img_byte_arr = img_byte_arr.getvalue()
            # print(img_byte_arr)
            with BytesIO() as output:
                img.save(output, 'JPEG')
                data = output.getvalue()
            # tmp = list(bytes(data))
            # tmp = data.decode()
            # tmp = [int.from_bytes(i) for i in data.split('\\x')]                
            # with open(byte_dir + file.split('.')[0], "wb") as binary_file:
            #     # Write bytes to file
            #     binary_file.write(bytes(data))
            


if __name__ == '__main__':
    main()