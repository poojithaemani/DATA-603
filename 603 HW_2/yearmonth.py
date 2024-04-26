from mrjob.job import MRJob
import csv

# define a MapReduce job class named MRyearmonth, inheriting from MRJob
class MRyearmonth(MRJob):
    
    def mapper(self, _,line):
        # split CSV line into individual values
        
        id,business_id,date,review_id,stars,text,type,user_id,cool,useful,funny=next(csv.reader([line]))
        
        #extract the year and month from the date column and emit a key-value pair
        yield date[:7],1
        
    #define reducer function
    def reducer(self,key,values):
        #sum the values for each key
        yield key,sum(values) 
        
if __name__=='__main__':
    MRyearmonth.run()
