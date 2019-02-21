import csv				
import sys				
import numbers			
import re 				
import os 				
import sqlparse

def get_column(agg_column,identifier_dict):
	with open("metadata.txt") as file: 
		begin_table=0
		attribute_num=0
		for i in file:
			if begin_table==0 and i.strip() in identifier_dict[3]:
				begin_table=1
			elif begin_table==1 and i.strip() != agg_column:
				attribute_num+=1
			elif begin_table==1 and i.strip() == agg_column:	
				break
			elif begin_table==1 and i.strip() == "<end_table>":
				break 	 
	return attribute_num			  



def avg(agg_column,table,identifier_dict):
	
	col_len=len(identifier_dict[1].split(","))

	if(col_len!= 1):
		print "provide only one column for aggregate function"
		sys.exit()

	col_index=get_column(agg_column,identifier_dict)		
	rec_count=0
	col_sum=0
	for row in table:
		rec_count+=1
		col_sum+=(int(row[col_index]))	
		
	
	print identifier_dict[1]
	print("{0:.2f}".format(round(col_sum/rec_count,2)))	

def sum(agg_column,table,identifier_dict):
	
	col_len=len(identifier_dict[1].split(","))

	if(col_len!= 1):
		print "provide only one column for aggregate function"
		sys.exit()

	col_index=get_column(agg_column,identifier_dict)	
	sum = 0

	for i in table:
		sum+=(int(i[col_index]))	
	print identifier_dict[1]
	print sum

def get_max(agg_column,table,identifier_dict):
	
	col_len=len(identifier_dict[1].split(","))

	if(col_len!= 1):
		print "provide only one column for aggregate function"
		sys.exit()

	col_index = get_column(agg_column,identifier_dict)	
	result=[]

	for row in table:
		result.append(int(row[col_index]))	
	res= max(result)
	print identifier_dict[1]
	print res		

def get_min(agg_column , table,identifier_dict):
	
	col_len=len(identifier_dict[1].split(","))

	if(col_len!= 1):
		print "provide only one column for aggregate function"
		sys.exit()

	col_index=get_column(agg_column,identifier_dict)	
	result=[]

	for row in table:
		result.append(int(row[col_index]))	
	res=min(result)
	print identifier_dict[1]
	print res




def count(agg_column,table,identifier_dict):
	
	col_len=len(identifier_dict[1].split(","))

	if(col_len!= 1):
		print "provide only one column for aggregate function"
		sys.exit()

	print identifier_dict[1]
	print len(table)

def distinct(agg_column,table,identifier_dict):

	s=agg_column.split(",")
	col_len=len(identifier_dict[1].split(","))
	if(col_len!= 1 and col_len!=2):
		print "provide only one column for aggregate function"
		sys.exit()
	if len(s)==1:
		col_index=get_column(s[0],identifier_dict)	
		result=[]
		for i in table:
			result.append(int(i[col_index]))	
		res=set(result)
		final_res = list(res)					

		for r in final_res:
			print r
	
	else:
		col_index1=get_column(s[0],identifier_dict)
		col_index2=get_column(s[1],identifier_dict)	
		result1=[]
		result2=[]
		for i in table:
			result1.append(int(i[col_index1]))
		for i in table:
			result2.append(int(i[col_index2]))	
		#print result1
		#print result2			
		final_res=[]							
		for i in result1:
			for j in result2:
				final_res.append((i,j))
		final_res=list(set(final_res))
		print identifier_dict[1]
		for r in final_res:
			print r[0],"|",r[1]

