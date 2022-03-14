hadoop fs -mkdir /wordcount/input 
#create a txt file where we can write some phrases
hadoop fs -put input.txt /wordcount/input 
#execute the exported jar file
hadoop jar wordcount.jar /wordcount/input /wordcount/output
#we can view output 
hadoop fs -cat /wordcount/output/*
