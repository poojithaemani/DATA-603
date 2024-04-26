from mrjob.job import MRJob
import csv

class AvgWordCount(MRJob):
# define a MapReduce class named AvgWordCount, inheriting from MRJob

    def mapper(self, _, line):
        #create a csv reader and converts the line to list
        id,business_id,date,review_id,stars,text,type,user_id,cool,useful,funny=next(csv.reader([line]))
        
        # emit key-value pair where the key is 0 and the value is the length of the text split by spaces

        yield 0,len(text.split(" "))
        
    # reducer function, called for each unique key    
    def reducer(self, cell, values):
        # convert the values iterator to a list
        values = list(values)
        # calculate the sum and count of words
        total_words = sum(values)
        word_count = len(values)
        
        # calculate the average number of words
        average_words = total_words / word_count
        yield None, average_words


# entry point to run the MapReduce job if this script is executed

if __name__ == '__main__':
    AvgWordCount.run()

    
