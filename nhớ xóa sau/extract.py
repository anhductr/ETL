# from io import StringIO

# output = StringIO()
# dim_constructors_df.to_csv(output, sep='\t', index=False, header=False)
# output.seek(0)
# cursor.copy_expert("COPY dim_constructors(constructorRef, name, nationality) FROM STDIN WITH CSV DELIMITER '\t'", output)
# conn.commit()
import kagglehub

dataset_path = kagglehub.dataset_download("rohanrao/formula-1-world-championship-1950-2020")
print(f"Dataset path: {dataset_path}")
