# -*- coding:utf-8 -*-

import sys
from itertools import groupby
from operator import itemgetter


def read_mapper_output(file, separator='\t'):
    for line in file:
        yield line.rstrip().split(separator, 1)


def main():
    data = read_mapper_output(sys.stdin)
    for current_word, group in groupby(data, itemgetter(0)):
        total_count = sum(int(count) for current_word, count in group)
        print("%s%s%d" % (current_word, '\t', total_count))


if __name__ == '__main__':
    main()


# shell run
# hadoop jar /usr/local/hadoop/share/hadoop/tools/lib/hadoop-streaming-2.7.7.jar -files "map.py,reduce.py" -input /test/test/file_a -output /out/done -mapper "python map.py" -reducer "python reduce.py"

