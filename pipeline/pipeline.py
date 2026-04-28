from html import parser
import sys
import pandas as pd

print("arguments:", sys.argv)

month = sys.argv[1]

args = parser.parse_args()

print(args)

print(f"month: {month}")

df = pd.DataFrame({"day": [1, 2, 3], "num_passengers": [4, 5, 6]})
df["month"] = month
print(df)