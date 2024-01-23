import argparse
import multiprocessing
import os
import subprocess
import re

from time import time_ns
import matplotlib.pyplot as plt
from tqdm import tqdm


###############################
# Utils
###############################
def createTestInputFile(name: str, size: int):
    os.system(f'dd if=/dev/urandom of={name} bs=4096 count={size // 4096} 2>/dev/null')

def runDesSpark(args: list) -> float:
    proc = subprocess.run(['python3', 'DESSpark.py'] + args, capture_output=True)
    match = re.search(r'\d+\.\d+', proc.stdout.decode())
    return float(match.group(0))



###############################
# Benchmarks
###############################
def coresBenchmark():
    times = []
    coresCount = multiprocessing.cpu_count()
    createTestInputFile('test_input_file.txt', 100_000)

    print("Starting cores count benchmark...")
    for i in tqdm(range(1, coresCount+1), total=coresCount):
        subtimes = []
        for _ in range(5):
            time = runDesSpark([
                '-c', str(i),
                '-m', 'enc',
                '-k', '0', '0', '0',
                'test_input_file.txt',
                'test_output_file.txt'])
            subtimes.append(time)
        times.append(sum(subtimes) / len(subtimes))
    print("Finished benchmark")

    os.unlink('test_input_file.txt')
    os.unlink('test_output_file.txt')

    plt.plot(range(1, len(times) + 1), times)
    plt.title('3DES encryption time using Apache Spark')
    plt.xlabel('Cores count')
    plt.ylabel('Time spent')
    plt.grid(True)
    plt.show()


def inputSizeBenchmark():
    times = []
    coresCount = multiprocessing.cpu_count() // 2
    print('Starting input size benchmark...')
    for i in tqdm(range(10_000, 100_001, 10_000), total=10):
        subtimes = []
        createTestInputFile('test_input_file.txt', i)

        for i in range(5):
            time = runDesSpark([
                '-c', str(coresCount),
                '-m', 'enc',
                '-k', '0', '0', '0',
                'test_input_file.txt',
                'test_output_file.txt'])
            subtimes.append(time)
        os.unlink('test_input_file.txt')
        os.unlink('test_output_file.txt')
        times.append(sum(subtimes) / len(subtimes))
    print("Finished benchmark")

    plt.plot(range(10_000, 100_001, 10_000), times)
    plt.title('3DES encryption time using Apache Spark')
    plt.xlabel('Input size')
    plt.ylabel('Time spent')
    plt.grid(True)
    plt.show()

def testValid():
    coresCount = multiprocessing.cpu_count() // 2
    createTestInputFile('test_input_file.txt', 50_000)
    # Encrypt
    runDesSpark([
                '-c', str(coresCount),
                '-m', 'enc',
                '-k', '0', '0', '0',
                'test_input_file.txt',
                'test_output_file.txt'])
    # Decrypt
    runDesSpark([
                '-c', str(coresCount),
                '-m', 'dec',
                '-k', '0', '0', '0',
                'test_output_file.txt',
                'test_final_file.txt'])
    with open('test_input_file.txt', 'rb') as f:
        orig = f.read()
    with open('test_final_file.txt', 'rb') as f:
        dec = f.read()
    if orig == dec:
        print('Test success')
        os.unlink('test_input_file.txt')
        os.unlink('test_output_file.txt')
        os.unlink('test_final_file.txt')
    else:
        print('Test failed - files differ')


###############################
# Entry point
###############################
def main():
    parser = argparse.ArgumentParser(description='Benchmark tool for DESSpark.py prog')
    parser.add_argument('TEST', type=str, choices=('cores', 'inputSize', 'validate'))
    args = parser.parse_args()

    if args.TEST == 'cores':
        coresBenchmark()
    elif args.TEST == 'inputSize':
        inputSizeBenchmark()
    elif args.TEST == 'validate':
        testValid()
    else:
        assert False


if __name__ == '__main__':
    main()
