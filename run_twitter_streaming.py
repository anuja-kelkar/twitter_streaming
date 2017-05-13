import os
import sys

#can be called from command line as: python run_twitter_streaming.py "#children,#teachkids"
if __name__ == '__main__':
    new_keywords_lst = sys.argv[1]
    os.system("python twitter_streaming.py '%s'" % new_keywords_lst)
