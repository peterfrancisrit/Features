
import numpy as np 
import pandas as pd 
import psycopg2 as psy 
import sys 
import csv
import math
import time
from dbfuns import value_insert, label_insert 



# Here we can change department name with feature_name however, it depends on the enriched_locations dataset. 
def spread_feature(conn,cur,feature_attribute):
	feature_name = "interest_feature" 
	# read trace id's and collect unique counts of departments with all corresponding traces within, normalise. 
	label_name = "department_index"
	data_type_values = "float_array"
	data_type_labels = "string_array"
	trace_id = pd.read_sql("select trace_id from traces_meta",conn)
	query_dep_id = "SELECT distinct " + feature_attribute + ", count(*) FROM (SELECT * FROM enriched_locations3 LIMIT 10000) a WHERE " + feature_attribute + " IS NOT NULL group by " + feature_attribute 
	dep_df = pd.read_sql(query_dep_id, conn)
	x = dep_df.iloc[:,1]
	dep_norm = x / np.linalg.norm(x,1)
	feature_length = len(dep_df)

	
	
	# For each trace, and each row in the normalised counts for total traces, match which trace is within which department 
	# If the trace is within all departments trace_score will be higher. Normalise by length. (Assume normal distribution)
	print("CALCULATING INTEREST FEATURES")
	for index_main, row_main in trace_id.iterrows():
		comm = "select case when exists (select true from feature_test where value is not null and feature_name = %s and feature_attribute = %s and trace_id =%s) then 'true' else 'false' end;"
		cur.execute(comm,(feature_name,feature_attribute,str(int(row_main['trace_id'])),))
		k = cur.fetchall()
		trace_score = []
		iterator = 0
		trace_dep_id = []
		if str(k[0]) == "("+"'"+"false"+"'"+","+")":
			sql_tscore = "SELECT distinct " + feature_attribute + ", count(*) FROM enriched_locations3 a WHERE trace_id=%s and " + feature_attribute + " IS NOT NULL group by " + feature_attribute 
			t_df = pd.read_sql(sql_tscore,conn,params=[int(row_main['trace_id'])])
			

			# Calculating probability P(X|Y)P(Y)
			y = t_df.iloc[:,1]
			trace_norm = y / np.linalg.norm(y,1)
			
			for i in t_df.iloc[:,0]:
				
				for k in dep_df.iloc[:,0]:
					
					if i == k:
						trace_score.append(round(float(dep_norm[iterator]*trace_norm[iterator]),3))
						trace_dep_id.append(str(i))
					else:
						continue
					
					iterator+= 1

			value_insert(cur,str(int(row_main['trace_id'])),feature_name,feature_attribute,feature_length,trace_score,data_type_values)
			label_insert(cur,str(int(row_main['trace_id'])),label_name,trace_dep_id,data_type_labels)
		else: 
			print("TRACE ID ALREADY EXISTS")
			continue
			
			

		



	# sql_tscore = "SELECT distinct department_name, count(*) FROM (SELECT * FROM enriched_locations3 LIMIT 10000) a WHERE trace_id=%s and department_name IS NOT NULL group by department_name" 
	# sql_dscore = "SELECT distinct department_name, count(*) FROM (SELECT * FROM enriched_locations3 LIMIT 10000) a WHERE department_name IS NOT NULL group by department_name" 

	# db_tscore = pd.read_sql_query(sql_tscore,conn)
	# db_dscore = pd.read_sql_query(sql_dscore,conn)

	# ID_idx = db_tscore.groupby(db_tscore.trace_id)

	# ober = {}
	# for key, value in db_tscore.trace_id.iteritems():
	# 	ober[value] = [db_tscore.department_name.loc[key],db_tscore.count.loc[key]]
	# 	ober[value].append(db_tscore.count.loc[key])
		
	
	# print(ober)

def main():
	try:
		conn = psy.connect(dbname='XXXXX',user='XXXX',host='geoserver.sb.dfki.de',password='XXXXX')
		conn_in = psy.connect(dbname='XXXXX',user='XXXX',host='XXXXX',password='XXXXX')
		conn_in.autocommit = True

	except:
		print("UNABLE TO CONNECT TO DATABASE")
	
	cur = conn_in.cursor()
	spread_feature(conn,cur,sys.argv[1])

if __name__ == '__main__':
	main()
