import boto3
import io

s3 = boto3.resource('s3')
cities = s3.Object('intellica-ridvan-test','global_sea_temp/Cities.csv')

with io.BytesIO() as f:
    cities.download_fileobj(f)

    f.seek(0)
    print("Content of cities.csv")
    print(f.read().decode("utf-8"))