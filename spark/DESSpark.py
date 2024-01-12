import argparse
import multiprocessing
import logging
import os
import tempfile
import sys

from pyspark import SparkContext, SparkConf
from typing import List

# import libpydes
import pyDes

def setLogsVerbosity(level: int) -> None:
    targetLevel = {
        0: logging.DEBUG,
        1: logging.INFO,
        2: logging.WARN,
        3: logging.ERROR
    }.get(level, logging.WARN)
    logging.basicConfig(level=targetLevel)

def createSpark(cores: int = 1) -> SparkContext:
    if cores <= 0:
        raise ValueError("Number of cores must be a positive integer")
    elif cores > multiprocessing.cpu_count():
        logging.warn("Requested more CPU cores than are available on this system")
    
    logging.info(f"Setting up Apache Spark to use {cores} CPUs")
    conf = SparkConf()
    conf.setMaster(f"local[{cores}]")
    conf.setAppName("3DES_Spark")
    conf.set("spark.executor.cores", f"{cores}")
    return SparkContext(conf=conf)


##############################
# 3DES encryption/decryption code
##############################

def parseDESKeys(keys: List[str]) -> List[int]:
    if len(keys) != 3:
        raise ValueError("Invalid number of 3DES keys provided")
    
    values = [int(key, 16) for key in keys]

    # Check that each key is 56 bits (7 bytes) long
    if any(val < 0 or val >= 2**56 for val in values):
        raise ValueError("3DES key must be in range 0 to 2**56")
    return values

def encryptBlock(x):
    if x is None:
        return x
    
    # For C bindings:
    # input = int.from_bytes(x)
    # output = libpydes.encryptBlock(mydes, input)
    # return output.values.to_bytes(8)
    #return mydes.encrypt(x)
    for block in x:
        yield mydes.encrypt(block)

def decryptBlock(x):
    if x is None:
        return x
    
    # return mydes.decrypt(x)
    for block in x:
        yield mydes.decrypt(block)

##############################
# Program entry point
##############################
def main():
    global mydes
    parser = argparse.ArgumentParser(prog='DESSpark',
                    description='Compute 3DES algorithm using Apache Spark',
                    epilog='Created as a part of PORR 2023 projects')
    parser.add_argument('-c', '--cores', default=1, 
                      help='Number of cores to compute on', type=int)
    parser.add_argument('-v', '--verbosity', default=1, type=int,
                        help='Logs verbosity level')
    parser.add_argument('-k', '--key', required=True, type=str, nargs=3,
                        help='Three hex-encoded integers representing 3DES key')
    parser.add_argument('-m', '--mode', required=True, type=str, choices=('enc', 'dec'),
                        help='Mode of 3DES algorithm (encrypt or decrypt)')
    parser.add_argument('INPUT', type=str,
                        help='Path to input file (- for STDIN)')
    parser.add_argument('OUTPUT', type=str,
                        help='Path to output file')
    args = parser.parse_args()

    setLogsVerbosity(args.verbosity)
    keys = parseDESKeys(args.key)

    # Setup Py3DES library
    k = b''.join([key.to_bytes(8) for key in keys])
    mydes = pyDes.triple_des(k, pyDes.ECB)

    # Setup Apache Spark and load input data
    spark = createSpark(args.cores)
    logging.info("Created Apache Spark instance")

    if args.INPUT == '-':
        fp = tempfile.NamedTemporaryFile(delete=False)
        while True:
            block = sys.stdin.buffer.read()
            fp.write(block)
            if len(block) == 0:
                break
        fp.flush()
        input_data = spark.binaryRecords(fp.name, 128)
    else:
        data = []
        with open(args.INPUT, 'rb') as f:
            while True:
                block = f.read(1024)
                if len(block) == 0:
                    break
                data.append(block)
        input_data = spark.parallelize(data)
        # input_data = spark.binaryRecords(args.INPUT, 128)
    logging.info(f"Loaded {input_data.count()} blocks of data")


    # Run encryption/decryption on user data
    if args.mode == 'enc':
        res_data = input_data.mapPartitions(encryptBlock)
    else:
        res_data = input_data.mapPartitions(decryptBlock)
    logging.info("Finished 3DES processing on input data")

    # Save result to file OUTPUT
    # res_data.saveAsTextFile(args.OUTPUT)
    with open(args.OUTPUT, "wb") as f:
         for record in res_data.collect():
             f.write(record)
    logging.info(f'Saved results to "{args.OUTPUT}"')

    # Delete temp file
    if args.INPUT == '-':
        os.unlink(fp.name)
        logging.info(f'Deleted temp input file')



if __name__ == '__main__':
    main()
