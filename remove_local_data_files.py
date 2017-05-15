__author__ = 'anujamkelkar'

import os


def remove_files():
    for f in os.listdir('results/summary_stats/'):
        os.remove('results/summary_stats/' + f)


if __name__ == '__main__':
    remove_files()
