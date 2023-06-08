import sys
import cv2
import numpy as np
import os
import time
import imutils
import click


def bytesToInt(b: bytes):
    result: int = 0
    shift: int = 0

    for byte in reversed(b):
        if byte > 128:
            byte = byte - 256

        result = result | ((byte & 0xFF) << shift)
        shift += 8

    return result


def intToBytes(i: int):
    return i.to_bytes(4, byteorder="big")

ENET_DIMENSIONS = (1024, 512)
RESIZED_WIDTH = 600
IMG_NORM_RATIO = 1 / 255.0

@click.command()
@click.option('--path', help='Path for the parent')
@click.option('--mode',type= int, help='mode for ImageAnonyumous')
def main(path, mode):
    threshold: int = 7

    enet_neural_network: any
    class_names: any

    # f = open("./demofile2.txt", "a")
    # f.write("Now the file has more content!")
    # f.close()

    network_path = os.path.join(path, 'enet-model.net')
    class_path = os.path.join(path, 'enet-classes.txt')
#     print(network_path)
    enet_neural_network = cv2.dnn.readNet(network_path)

    class_names = (
        open(class_path).read().strip().split("\n"))
#     print(class_names)

    while True:
        header = sys.stdin.buffer.read(4)
        fileLength = bytesToInt(header)


        body = sys.stdin.buffer.read(fileLength)
        inp = np.asarray(bytearray(body), dtype=np.uint8)

        input_img = cv2.imdecode(inp, cv2.IMREAD_COLOR)
        input_img_blob = cv2.dnn.blobFromImage(input_img, IMG_NORM_RATIO,
                                               ENET_DIMENSIONS, 0, swapRB=True, crop=False)

        enet_neural_network.setInput(input_img_blob)
        enet_neural_network_output = enet_neural_network.forward()

        (number_of_classes, hei, wi) = enet_neural_network_output.shape[1:4]

        class_map = np.argmax(enet_neural_network_output[0], axis=0)

        CLASS_PRIOS = [5, 8, 8, 4, 3, 2, 2, 6, 6, 1, 1, 0, 7, 9, 9, 9, 9, 9, 9, 7]

        # pixelate
        # Get input size
        height, width = input_img.shape[:2]
        if mode == 0:
            layer_img = np.zeros((height, width, 3), dtype="uint8")
        elif mode == 1:
            # Desired "pixelated" size
            w, h = (50, 50)

            # Resize input to "pixelated" size
            temp = cv2.resize(input_img, (w, h), interpolation=cv2.INTER_LINEAR)

            # Initialize output image
            layer_img = cv2.resize(temp, (width, height), interpolation=cv2.INTER_NEAREST)
        elif mode == 2:
            w, h = (50, 50)
            layer_img = cv2.blur(input_img, (w, h), cv2.BORDER_DEFAULT)
        else:
            raise ValueError("Illegal mode '" + str(mode) + "'.")

        class_map = cv2.resize(class_map, (input_img.shape[1], input_img.shape[0]), interpolation=cv2.INTER_NEAREST)

        class_map = class_map.reshape(width * height, 1)

        CP = np.array(CLASS_PRIOS)
        xyz = CP[class_map]

        enet_neural_network_output = np.where(xyz < threshold, layer_img.reshape(-1, 3), input_img.reshape(-1, 3))
        enet_neural_network_output = enet_neural_network_output.reshape(height, width, 3)

        combined_images = enet_neural_network_output

        encoded = cv2.imencode('.jpg', combined_images)[1]
        resultHeader = intToBytes(len(encoded))
        # f = open("../python/demofile2.txt", "a")
        # f.write(f"resultHeader: {resultHeader} \n {encoded}")
        # f.close()
        sys.stdout.buffer.write(resultHeader)
        sys.stdout.buffer.write(encoded)

#         cv2.imshow('Results', enet_neural_network_output)
#         cv2.imshow('Results', combined_images)
#         cv2.imshow("Class Legend", class_legend)
#         print(combined_images.shape)
#         cv2.waitKey(0)  # Display window until keypress
#         cv2.destroyAllWindows()  # Close OpenCV


if __name__ == "__main__":
    main()
# Display image

