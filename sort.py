# -*- coding: utf-8 -*-

__author__ = 'Alexander Zhukov'
# --------import-------------
from random import randint
import ctypes
import struct
import os
from time import time
import argparse

# -------const------------
MAX_UINT_64 = 18446744073709551615L
UINT_SIZE = 8


def get_min_index(arr):
    return arr.index(min(arr))


def k_way_merge(arrays):
    data = []
    for array in arrays:
        data.append((array.next(), array))  # получаем значение и генератор

    while data:
        index = get_min_index(data)
        value, chunk_stream = data.pop(index)
        yield value
        if chunk_stream:
            try:
                data.append((next(chunk_stream), chunk_stream))
            except StopIteration:
                pass


def write_file(name, size):
    with open(name, 'wb') as f:
        i = 0
        while (i < size):
            num = ctypes.c_uint64(randint(0, MAX_UINT_64))
            f.write(num)
            i += 1


def split_file(f_handler, part, ram):
    arr = []

    mem = 0
    while (mem < ram):
        b = f_handler.read(UINT_SIZE)  # читаем по 8 байт
        if b:
            arr.append(struct.unpack('Q',b)[0])
        else:
            break
        mem += UINT_SIZE
    arr.sort()
    with open('part{0}.bin'.format(part),'wb') as f_sorted:
        for elem in arr:
            f_sorted.write(ctypes.c_uint64(elem))


def clean_up(n):
    for part in xrange(n):
        os.remove('part{0}.bin'.format(part))


def read_part(part):
    with open('part{0}.bin'.format(part),'rb') as f_sorted:
        while f_sorted:
            b = f_sorted.read(8)
            if b:
                yield struct.unpack('Q',b)[0]
            else:
                break


def test():
    f = open('result.bin', 'rb')
    prev, curr = 0, 0
    while 1:
        b = f.read(8)
        if b:
            prev = curr
            curr = struct.unpack('Q',b)[0]
        else:
            break
        if curr < prev:
            print curr, prev
        else:
            print 'ok'


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-m',
                        type=int,
                        help='memory to use for sorting (in Mb)',
                        default=100)
    parser.add_argument('filename',
                        metavar='<filename>',
                        nargs=1,
                        help='name of file to sort')
    parser.add_argument('-mode',
                        help='\'w\' : write random file with uint64 values, \'s\' : sort',
                        )
    args = parser.parse_args()
    fname = args.filename[0]

    if args.mode == 'w':
        write_file(fname, 1024*1024*10)
    elif args.mode == 's':
        ram = args.m*1024*1024/2  # /2 чтобы обеспечить доп. память для сортировки
        size = os.path.getsize(fname)  # размер файла

        if size % ram == 0:
            n = size / ram
        else:
            n = size / ram + 1

        print '--------splitting stage-------'
        start = time()

        f = open(fname, 'rb')
        for i in xrange(0,n):
            split_file(f, i, ram)
        end = time()

        print 'time passed:', end-start
        print '--------merging stage--------'

        arr = [None for i in xrange(n)]
        for i in xrange(n):
            arr[i] = read_part(i)  # получаем генератор списка для каждого файла
        gen = k_way_merge(arr)

        start = time()

        with open('result.bin', 'wb') as f_res:
            for elem in gen:
                f_res.write(ctypes.c_uint64(elem))

        end = time()
        print 'time passed:', end-start

        clean_up(n)  # удаляем промежуточные результаты
        # test()  # проверка, если нужно.
if __name__ == "__main__":
    main()
