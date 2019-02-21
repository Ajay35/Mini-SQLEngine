import csv
import sys				
import numbers			
import re 				
import os 				
import sqlparse			
from sqlparse.sql import Where
import aggregates as ag

schema_dictionary={} 		
identifier_dict=[]


				
def get_single_table(filename):						
	table_name = filename + '.csv'
	try:
		with open(table_name) as file: 
			table = csv.reader(file)
			table_rows = []
			for row in table:
				row = [col.replace(" ","") for col in row]
				row = [ int(i) for i in row]
				table_rows.append(row)
			return table_rows
	except:
		return -1;	

def get_multiple_tables():								
	table_names = identifier_dict[3].split(",")
	#print table_names
	to_be_joined_table=get_single_table(table_names[0])
	
	if(to_be_joined_table == -1):
		return -1

	else:
		for i in range(1,len(table_names)):
			temp = []
			temp_table = get_single_table(table_names[i])
			#print temp_table
			if(temp_table== -1):
				return -1
	
			else:
				# joing the tables
				for j in xrange(0,len(to_be_joined_table)):
					for k in xrange(0,len(temp_table)):
						temp.append(to_be_joined_table[j] + temp_table[k])
						pass
				to_be_joined_table = temp
	
	return to_be_joined_table											

def get_schema():
	begin_table = 0
	table_name = {}
	with open("metadata.txt") as file: 
		
		for line in file:
			if line.strip() == "<begin_table>":
				begin_table = 1
				continue

			if begin_table == 1:
				table_name=line.strip()
				schema_dictionary[table_name]=[]
				begin_table=0
				
			if begin_table==0 and line.strip() != "<end_table>":
				schema_dictionary[table_name].append(line.strip())	
		for entry in schema_dictionary:
			schema_dictionary[entry].remove(entry)		


def get_attributes(attr):
	table_name = ""
	begin_table=0
	
	for i in range(len(attr)):
		
		if ('.' not in attr[i]):
			with open(table_name) as metafile: 
				for line in metafile:	
					
					if begin_table==0 and line.strip() in identifier_dict[3]:
						begin_table=1
						table_name = line.strip()
				
					elif begin_table==1 and line.strip()== attr[i]:
						attr[i] = (table_name+"." +line.strip())
						
						
					elif begin_table==1 and line == "<end_table>":
						begin_table=0 	 
					else:
						pass	 

	return attr

def execute_where(table,condition_string):
	
	cond = re.split('AND|OR',condition_string.upper())
	connectors = []
	display_result = []
	check_connectors = condition_string.upper().split(" ");
	for i in check_connectors:
		if i=="OR" or i=="AND":
			connectors.append(i.lower())

	split_condition = execute_condition(cond)		
	condition1 = ""
	
	for i in range(0, len(connectors)):
		condition1 += str(split_condition[i]) + " "
		condition1 += str(connectors[i]) + " "

	condition1 += str(split_condition[len(split_condition) - 1])

	for x in range(len(table)):
		if eval(condition1):
			display_result.append(table[x])	
			
	return display_result

def execute_condition(condition):
	tables = identifier_dict[3].split(",")
	condition1 = []
	length = 0
	for cond in condition:
		cond = cond.replace(" ","")

		if ">" in cond:
			op=">"

		elif "<" in cond:
			op="<"
		
		elif ">=" in cond:
			op=">="

		elif "<=" in cond:
			op="<="

		elif "!=" in cond:
			op="!="

		elif "=" in cond:
			op="=="
			cond=cond.replace('=','==')
		else:
			pass

		operands=cond.split(op)
		flag=0
		count=0
		for i in schema_dictionary.keys()[::-1]:
			if i in tables:
				for j in schema_dictionary[i]:
					if '.' in operands[0]:
						temp_operand = operands[0].split(".")
						if temp_operand[1] == j and temp_operand[0].lower() == i.lower():
							operands[0] = "table[x][" + str(count) + "]"
							flag=1	


					if operands[0] == j:
						operands[0] = "table[x][" + str(count) + "]"
						flag=1
					count+=1		
		if flag==0:
				print " Invalid where clause"
				sys.exit()			
									
		
		if (not operands[1].isdigit() and '-' not in operands[1]):

			flag=0
			count=0
			for i in schema_dictionary.keys()[::-1]:
				if i in tables:
					for j in schema_dictionary[i]:

						if '.' in operands[1]:
							temp_operand = operands[1].split(".")
							if temp_operand[1] == j and temp_operand[0].lower() == i.lower():
								operands[1] = "table[x][" + str(count) + "]"
								flag=1

						if operands[1] == j:
							operands[1] = "table[x][" + str(count) + "]"
							flag=1
						count+=1
							
			if flag==0:
				print "Invalid where clause"
				sys.exit()			
					
		temp = str(operands[0] + str(op) + operands[1])
		condition1.append(temp)
		length+=1
		
	return condition1


									
def verify_columns():
	attributes = identifier_dict[1].split(",")
	tables = identifier_dict[3].split(",")

	for att in attributes:
		
		if "*" == att and len(attributes)==1:
			pass

		elif "*" == att and len(attributes)!=1:
			return "false"	

		else:
			if "." in att:
				parts = att.split(".")
				if parts[0] not in tables:
					return "false"

				if parts[1] not in schema_dictionary[parts[0]]:
					return "false"

			else:		
				flag = 0	
				flag1= 0		
				attribute_num=0
				col = []
				with open("metadata.txt") as file: 
					for line in file:
						line = line.strip()
				
						if line == "<begin_table>":
							flag = 1

						elif flag==1 and line in tables:
							flag = 0
							flag1 = 1

						elif flag1==1:
							
							if line == "<end_table>":
								flag1=0

							elif line==att:
								col.append(attribute_num)
								attribute_num+=1

							elif line!=att:		
								attribute_num+=1

							else:
								pass

						else:
							pass
			
				if(len(col) != 1):
					return "false"
	return "true"					

def display_query_result(attr_list,result_table):
	final_result=""
	for i in range(len(attr_list)):
		attr_list[i]=attr_list[i].split(",")
	for h in range(len(attr_list)):
		for h1 in attr_list[h]:
			print str(h1)+"|",
	for row in result_table:
		temp_row = []
		for cell in row:
			temp_row.append(str(cell)) 
		final_result =final_result+"\n" + "|".join(temp_row)
	print final_result

def execute(query):

	get_schema()
	
	query_tokens=sqlparse.parse(query)[0].tokens
	id_list = sqlparse.sql.IdentifierList(query_tokens).get_identifiers()				
	#print id_list
	for id in id_list:
		identifier_dict.append(str(id))

	if(sqlparse.sql.Statement(query_tokens).get_type()!='SELECT'):
		print "Only select queries supported"
	else :
		exucute_select(query)	




def exucute_select(q):

	if len(identifier_dict)<4:
		print "Invalid query"
		sys.exit()

	else:
		identifier_dict[1] = identifier_dict[1].replace(" ","")	
		#print identifier_dict[1]
		func = re.sub(ur"[\(\)]",' ', identifier_dict[1]).split()
		#print func
		
		identifier_dict[3] = identifier_dict[3].replace(" ","")	
		tables=identifier_dict[3].split(",")	
		if(len(tables)==0):
			print "Table names not proveided"
			sys.exit()

		elif(len(tables) == 1):								
			table=get_single_table(identifier_dict[3])
	
		else:	
			table=get_multiple_tables()
		
		if table == -1:
			print "Table doesn't exist"
			sys.exit()	

		if(len(identifier_dict) > 4 and "where" in identifier_dict[4].lower()):
			condition_string=identifier_dict[4][6:]
			if len(condition_string) < 3:
				print "Invalid where clause"
				sys.exit()	

			condition_string=condition_string.replace("  "," ")
			
			table = execute_where(table,condition_string)

		if len(table)==0:
			print "Empty Table"
			sys.exit()

		if table != -1:
			if len(func)==0:						
				print "No column is provided"
				sys.exit()

			elif(func[0] == '*'): 
				
				valid = verify_columns()
				if valid == "false":
					print "Invalid attributes"
					sys.exit()

				attr_list = [] 	
				tables = identifier_dict[3].split(",")
				for k in tables:
					tempStr = ""
					for i in schema_dictionary[k]:
						tempStr += (k + "." + i + ",")
					
					tempStr = tempStr.rstrip(',')	
					attr_list.append(tempStr)	
				
				display_query_result(attr_list,table)
				
			

			elif(func[0].lower() == 'max'):
				ag.get_max(func[1] , table,identifier_dict)
			

			elif(func[0].lower() == 'min'):
				ag.get_min(func[1] , table,identifier_dict)		
	


			elif(func[0].lower() == 'sum'):
				ag.sum(func[1] , table,identifier_dict)



			elif(func[0].lower() == 'avg'):
				ag.avg(func[1] , table,identifier_dict)

		
			elif(func[0].lower() == 'distinct'):
				ag.distinct(func[1],table,identifier_dict)
				

			elif(func[0].lower() == 'count'):
				ag.count(func[1] , table,identifier_dict)

			else:
				valid = verify_columns()
				if valid == "false":
					print "Invalid column name"
					sys.exit()
				
				attributes = identifier_dict[1].split(",")
				#print  attributes
				tables = re.sub(ur"[\,]",' ',identifier_dict[3]).split()					
				
				flag = 0
				flag1= 0
				
				attribute_num=0
				col = []
				attributes = get_attributes(attributes)
				tablename = ""

				with open("metadata.txt") as file: 
					for line in file:
						line = line.strip()


						if line == "<begin_table>":
							flag = 1

						elif flag==1 and line in identifier_dict[3]:
							tablename = line
							flag = 0
							flag1 = 1

						elif flag1==1:
							line1 = tablename + "." + line
							if line == "<end_table>":
								flag1=0
								tablename = ""

							elif line1 in attributes:
								col.append(attribute_num)
								attribute_num+=1

							elif line1 not in attributes:		
								attribute_num+=1

							else:
								pass

						else:
							pass

				
				
				table1 = []
				temp = 0
				for i in table:
					temp = 0
					tempList = []
					for j in i:
						if temp in col:
							tempList.append(j)
						temp+=1	
					table1.append(tempList)	 

				#print attributes	
				display_query_result(attributes,table1)	
		
		else:
			print "table doesn't exist"
			sys.exit()
	

if len(sys.argv)>2 or len(sys.argv)==1:
	print "Enter valid SQL query"
	sys.exit()
user_query = sys.argv[1]
q = user_query.split(';')
query =' '.join(q)
execute(query) 
		 