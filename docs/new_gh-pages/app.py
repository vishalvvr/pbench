from jinja2 import Environment, FileSystemLoader
import os
import json
import shutil

with open('data.json') as fobj:
    site_data = json.load(fobj)

file_loader = FileSystemLoader("template")
env = Environment(loader=file_loader)

template = env.get_template('index.html').render(site_data)

public_dir = os.path.join(os.getcwd(),"public")

#if path already exists, remove it before copying with copytree()
if os.path.exists(public_dir):
    shutil.rmtree(public_dir)
shutil.copytree(os.path.join(os.getcwd(),"template"), public_dir)

with open(os.path.join(public_dir,"index.html"),"w") as fobj:
    fobj.writelines(template)
