#importing necessary modules
from mrjob.job import MRJob
import csv
from statistics import mean

# define the MRavgrating class, inheriting from MRJob

class MRavgrating(MRJob):
    
    # mapper function processes each line of input
    def mapper(self, _, line):
        # parse the CSV row from the input line
        row = next(csv.reader([line]))
        id,business_id,date,review_id,stars,text,type,user_id,cool,useful,funny=row
        
        # Check conditions to filter relevant data
        if (cool!=0 and stars != "stars"):
            
            # emit a key-value pair with key=1 and value=stars as an integer
            yield 1,int(stars)
            
    # reducer function calculates the average of received values    
    def reducer(self, key, values):
        values = list(values)
        a=sum(values)
        b=len(values)
        average_words = a / b
        yield None, average_words

# entry point to run the MRavgrating MapReduce job
if __name__ == '__main__':
    MRavgrating.run()
