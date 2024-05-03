    select_expr = [col(c).cast(attribute_datatype_dict[c]).alias(c) for c in attribute_datatype_dict.keys() if c in temp_df.columns]
        
    temp_df = temp_df.select(*select_expr)\
        .withColumn("record_source_full", F.input_file_name())

    pq_list.append(temp_df)
    
df = reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), pq_list)
