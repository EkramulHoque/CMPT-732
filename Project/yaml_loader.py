import yaml,sys
assert sys.version_info >= (3, 5)

def loader(filepath):
    "Loads a yaml file"
    with open(filepath, "r") as file_descriptor:
        data = yaml.load(file_descriptor)
    return data


def dump(filepath,data):
    "Dumps data"
    with open(filepath,"w") as file_descriptor:
        yaml.dump(data,file_descriptor)

