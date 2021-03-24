class AppLogger:

    def __init__(self, name):
        self.file_name = name

    def write_line(self, obj):
        print("%s" % obj)
        f = open(self.file_name, "a")
        f.write("%s" % obj)
        f.write("\n")
        f.close()
