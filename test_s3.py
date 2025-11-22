import boto3

# Configuration MinIO
s3 = boto3.client(
    "s3",
    endpoint_url="http://localhost:9000",  # ou http://192.168.x.x:9000 depuis un autre PC
    aws_access_key_id="ducklake_s3_writer",  # ou minio
    aws_secret_access_key="writer_secret",  # ou password
)

# Vérifier les buckets existants
print("Buckets disponibles:")
for bucket in s3.list_buckets()["Buckets"]:
    print(" -", bucket["Name"])

# Lister les objets dans le bucket 'coucou'
print("\nObjets dans 'coucou':")
for obj in s3.list_objects_v2(Bucket="coucou").get("Contents", []):
    print(" -", obj["Key"])
