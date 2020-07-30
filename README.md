# CORD19-k8s-preprocessing
repo for the docker to process CORD19 dataset via k8s

This repo is maintained to run in docker container from the image that has all the sciSpacy dependency baked in. 

- in current folder run docker
```
docker run --rm -it -v $(pwd):/work -v <path to CORD19folder>:/work/data coronawhy/preprocessing:scispacy bash
cd /work
python main.py [--output_path <folder for output/default=output>]
```
