# Insertion functions for scripts into database: trace_features table: feature, feature_label
 
def value_insert(cur,trace_id,feature_name,feature_attribute,feature_length,value,data_type):
    insert_com = "INSERT INTO public.Feature_test (trace_id, feature_name, feature_attribute, feature_length, value, data_type)  VALUES (%s,%s,%s,%s,%s,%s)"
    cur.execute(insert_com,(trace_id,feature_name,feature_attribute,feature_length,value,data_type)) 

def label_insert(cur,trace_id,class_name,value,data_type):
    insert_com = "INSERT INTO public.Label_test (trace_id, label_name, value, data_type) VALUES (%s,%s,%s,%s)"
    cur.execute(insert_com,(trace_id,class_name,value,data_type))

# Add check out zone function identifier.

