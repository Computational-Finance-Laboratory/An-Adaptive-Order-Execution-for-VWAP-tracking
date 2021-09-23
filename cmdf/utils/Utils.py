import os

import json
import pickle
from datetime import datetime
from IPython.display import display, HTML

import numpy as np
import pandas as pd

def copy_directory(src, dest):
    """
    Copy files or directorys from source to destination

    src (str) : path to files or directorys that will be copied
    dest (str) : path to place copied files or directorys

    """
    if "." in os.path.basename(src):
        shutil.copyfile(src, dest)
    else:
        try:
            shutil.copytree(src, dest)
        # Directories are the same
        except shutil.Error as e:
            print('Directory not copied. Error: %s' % e)
        # Any error saying that the directory doesn't exist
        except OSError as e:
            print('Directory not copied. Error: %s' % e)

def create_directory(function):
    """
    Decolator used to create directory if directory is not exist
    """
    def create_dir(*args, **kwargs):
        dirname = os.path.dirname(kwargs["filename"])
        if dirname:
            os.makedirs(dirname, exist_ok=True)
        return function(*args, **kwargs)
    return create_dir

@create_directory
def save_json(json_dict, filename="temp.json"):
    with open(filename, "w") as f:
        json.dump(json_dict, f)

def read_json(filename="temp.json"):
    with open(filename, "r") as f:
        data = json.load(f)
    return data

@create_directory
def save_text(data, filename="temp.text"):
    with open(filename, "w", encoding="utf-8") as f:
        f.write(str(data))

def read_text(filename="temp.text"):
    with open(filename, "r") as f:
        data = f.read()
    return data

@create_directory
def save_pickle(data, filename="temp.pickle"):
    with open(filename, "wb") as f:
        pickle.dump(data, f)

def read_pickle(filename="temp.pickle"):
    with open(filename, "rb") as f:
        data = pickle.load(f)
    return data

@create_directory
def save_numpy(data, filename="temp.npy"):
    with open(filename, "wb") as f:
        np.save(f, data)

def read_numpy(filename="temp.npy"):
    with open(filename, "rb") as f:
        array = np.load(f)
    return array

def read_word_list(filename="temp.txt"):
    with open(filename, "r") as f:
        word_list = f.read().splitlines()
    return word_list

#alias
def pwb(*args, **kwargs):
    print_with_bracket(*args, **kwargs)

def print_with_bracket(*args, **kwargs):
    for v in args:
        text = "==arg==" * 10
        print(text)
        if isinstance(v, pd.DataFrame):
            display(v)
        else:
            print(v)
        print("=" * len(text))
    for key in kwargs:
        text = f"=={key}==" * 3
        print(text)
        if isinstance(kwargs[key], pd.DataFrame):
            display(kwargs[key])
        else:
            print(kwargs[key])
        print(f"=" * len(text))
