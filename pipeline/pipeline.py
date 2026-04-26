import sys
import pandas as pd

print("arguments:", sys.argv)

month = int(sys.argv[1])
print(f"month: {month}")

df = pd.DataFrame({"day": [1, 2, 3], "num_passengers": [4, 5, 6]})
df["month"] = month
print(df)