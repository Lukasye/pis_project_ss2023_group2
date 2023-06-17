import math
import sys

def main():
    args = sys.argv
    if len(args) != 5:
        print("Wrong number of arguements!")
        return
    x = float(args[1])
    y = float(args[2])
    xx = float(args[3])
    yy = float(args[4])
    print(math.sqrt((x - xx) ** 2 + (y - yy) ** 2))

if __name__ == '__main__':
    main()
