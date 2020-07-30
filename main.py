# -*- coding: utf-8 -*-
import random, argparse, os, json
from preprocessutils import preprocess_metadata, chunking, str2bool
from pipeline import pipeline
from psutil import virtual_memory
import pandas as pd



def main():
    
    parser = argparse.ArgumentParser(description='Multi-CPU Preprocessing Pipeline for CORD19-v19.')
    #path
    parser.add_argument('--CORD19_path', type=str,default='./data',
                    help='Path to CORD-19 path as provided on Kaggle platform.')
    parser.add_argument('--output_path', type=str,default='output',
                    help='Path to output directory')
    #optional delta json file
    parser.add_argument('--delta', type=str,
                    help='Delta json file with file names (CORD19 SHA) already preprocessed.')
    #max. number of files 
    parser.add_argument('--max_n_files', type=int, default=None, const=10, nargs='?',
                    help='Optional maximal number of files you want to preprocess (for development purposes)')
    #supposed/desired amount of free RAM per worker 
    parser.add_argument('--RAM_per_worker', type=int, default=12, const=12, nargs='?',
                    help='Amount of RAM in GiBs that each worker should obtain. It restricts the number of engaged workers. Default value 12 GiB')
    #en_core_sci_lg
    parser.add_argument('--en_core_sci_lg', type=str2bool, default=True, nargs='?', const=True,
                        help="A full spaCy pipeline for biomedical data with a larger vocabulary and 600k word vectors.")
    #en_ner_craft_md
    parser.add_argument('--en_ner_craft_md', type=str2bool, default=True, nargs='?', const=True,
                        help="A spaCy NER model trained on the CRAFT corpus.")
    #en_ner_jnlpba_md
    parser.add_argument('--en_ner_jnlpba_md', type=str2bool, default=True, nargs='?', const=True,
                        help="A spaCy NER model trained on the JNLPBA corpus.")
    #en_ner_bc5cdr_md
    parser.add_argument('--en_ner_bc5cdr_md', type=str2bool, default=True, nargs='?', const=True,
                        help="A spaCy NER model trained on the BC5CDR corpus.")
    #en_ner_bionlp13cg_md
    parser.add_argument('--en_ner_bionlp13cg_md', type=str2bool, default=True, nargs='?', const=True,
                        help="A spaCy NER model trained on the BIONLP13CG corpus.")
    parser.add_argument('--number_of_workers', type=int, default=1, 
                        help="Total number of workers that would be used to split work")
    parser.add_argument('--this_is_worker', type=int, default=0, 
                        help="Total number of workers that would be used to split work")
        
    args = parser.parse_args()

    print(args)
    return 1
    
    assert args.CORD19_path != None, "you should specify a path to CORD19 collection"
    
    folder_output = args.output_path

    assert folder_output != None, "you should specify the path for output"

    if args.delta == None:
        print("No delta file specified. The pipeline will process the whole collection.")
        delta_sha_list = False
    else:
        with open(args.delta) as f:
            delta_file = json.load(f)
            delta_sha_list = delta_file["delta list"]
            delta_sha_list = ''.join(delta_sha_list)
    
    # # Preprocess the metadata to get folder and subfolder structre and the names of files
    # files, paths_to_files = preprocess_metadata(args.CORD19_path)

############## Insted doing v15+ paths to files directly
    directory = args.CORD19_path
    df = pd.read_csv(os.path.join(directory,"metadata.csv"), low_memory=False) 
    paths_to_files = []
    
    #drop rows that don't contain full text jsons
    df.dropna(subset=['pmc_json_files','pdf_json_files'], how='all', inplace=True)

    df_pmc_only = df['pmc_json_files'].dropna()
    for row in df_pmc_only:
        paths_to_files.append( os.path.join(directory,row.split(';')[0]) )

    df_pdf_only = df[df['pmc_json_files'].isna()]['pdf_json_files'].dropna()
    for row in df_pdf_only:
        paths_to_files.append( os.path.join(directory,row.split(';')[0]) )


    #files, paths_to_files = files[:100], paths_to_files[:100]
    if delta_sha_list:
        old_doc_nubmer = len(files)
        files_and_paths = [(file, file_path) for (file, file_path) in zip(files, paths_to_files) if file not in delta_sha_list]
        print(len(files_and_paths))
        files, paths_to_files = zip(*files_and_paths)
        new_doc_number = len(files)
        print("""
           A Delta file for CORD19 database has been applied. 
           Instead of {} files, only {} of them will be annotated. 
        """.format(old_doc_nubmer, new_doc_number))
        
    
    
    #quit()
    #paths_to_files = paths_to_files[:100]
    #print(paths_to_files)
    # file_size_list = [os.path.getsize(x) for x in paths_to_files]
    # pathsAndFileSizes = list(zip(paths_to_files,file_size_list))
    # paths_to_files_sorted, file_size_list_sorted = zip(*sorted(pathsAndFileSizes, key = lambda x: x[1]))
    # paths_to_files_sorted = list(paths_to_files_sorted)
    # random.shuffle(paths_to_files_sorted)
    
    # if args.max_n_files != None:
    #     paths_to_files_sorted = paths_to_files_sorted[:args.max_n_files]
    
    print("""
       We are going to process {} files from CORD19 database. 
    """.format(len(paths_to_files)))
       
    
    

    
    #gather SciSpacy model preferences and pack them into chunks for each process
    model_dict = {"description":"""
    A dictionary of SciSpacy model preferences specified by the user. 
    By default all models will be loaded"""}
    model_dict["en_core_sci_lg"] = args.en_core_sci_lg 
    model_dict["en_ner_craft_md"] = args.en_ner_craft_md 
    model_dict["en_ner_jnlpba_md"] = args.en_ner_jnlpba_md 
    model_dict["en_ner_bc5cdr_md"] = args.en_ner_bc5cdr_md
    model_dict["en_ner_bionlp13cg_md"] = args.en_ner_bionlp13cg_md
    
    models_selected = ["--"+k for k,v in model_dict.items() if type(v) == bool and v == True]

    #send file path chunks to a pool of workers
    results = pipeline(paths_to_files, model_dict, folder_output)
    
           
if __name__ == "__main__":
    main()