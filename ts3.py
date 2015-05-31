import boto
import boto.s3.connection

access_key = 'GC3M918UQ42W60JHQ6AU'
secret_key = '+PCdZYSAhunBuxkRPKJ03MjeojHQFibuHbTLeivQ'

conn = boto.connect_s3(
    aws_access_key_id=access_key,
    aws_secret_access_key=secret_key,
    host='koder-centos-ceph0',
    is_secure=False,
    calling_format=boto.s3.connection.OrdinaryCallingFormat(),
)

bucket = conn.create_bucket('my-new-bucket')

for bucket in conn.get_all_buckets():
        print bucket.name, bucket.creation_date
