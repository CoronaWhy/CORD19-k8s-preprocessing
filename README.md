# CORD19-k8s-preprocessing
repo for the docker to process CORD19 dataset via k8s

This repo is maintained to run in docker container from the image that has all the sciSpacy dependency baked in. 

- in current folder run docker
```
docker run --rm -it -v $(pwd):/work -w /work -v <path to CORD19folder>:/work/data coronawhy/preprocessing:scispacy bash
python main.py
```
- the input would be taken from /work/data folder by default (or you can `[--CORD19_path <input data folder>]`)
- the output would be put in `output` folder by default (or you can `[--output_path <folder for output/default=output>]`)

### For testing

```
docker run --rm -it -v $(pwd):/work -w /work coronawhy/preprocessing:scispacy bash
python main.py --CORD19_path data_toy
```
