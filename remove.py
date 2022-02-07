import os

for i in range(1, 3000):
    string = "file_" + str(i) + ".txt"
    os.remove(string)