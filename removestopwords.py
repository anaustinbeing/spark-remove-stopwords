'''
  This Spark Program prints out contents of file after filtering out stopwords.
'''

from pyspark import SparkContext
from nltk.corpus import stopwords

class RemoveStopWords:
    def __init__(self, name):
        self.spark = SparkContext('local[3]', name)

    def read_RDD(self, path):
        return self.spark.textFile(path)

    def filter_words(self, lines):
        stopwords_list = stopwords.words('english')
        return lines.flatMap(lambda x: x.split()).filter(lambda x: x.lower() not in stopwords_list).collect()

if __name__ == '__main__':
    rsw = RemoveStopWords('wordcount')
    lines = rsw.read_RDD('path/to/file')
    filtered_lines = rsw.filter_words(lines)
    print(' '.join(filtered_lines))
