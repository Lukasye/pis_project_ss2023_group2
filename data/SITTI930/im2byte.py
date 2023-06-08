# from PIL import Image


# def main():
#     path = './img/1.png'
#     output = './asd.bmp'
#     with open(path, 'rb') as f:
#         data = f.read()
#         b = bytearray(data)
#     print(b)
    # with Image.open(path) as img:
    #     img.save(output, 'BMP') 
import numpy as np
from cv2 import *

class Image2ByteConverter(object):

    def loadImage(self, path='./img/1.png'):
        # Load image as string from file/database
        file = open(path, "rb")
        return file

    def convertImage2Byte(self):
        file = self.loadImage()
        pixel_data = np.fromfile(file, dtype=np.uint8)
        return pixel_data

    def printData(self):
        final_data = self.convertImage2Byte()
        final_data = [i.tobytes() for i in final_data]
        return b''.join(final_data)

    def run(self):
        # self.loadImage()
        # self.convertImage2Byte()
        tmp = self.printData()
        with open('img_byte', 'wb') as f:
            f.write(tmp)

if __name__ == "__main__":
    Image2ByteConverter().run()

