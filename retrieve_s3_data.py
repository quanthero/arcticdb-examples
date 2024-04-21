import arcticdb as adb


# oplink minio s3
# ac = adb.Arctic('s3://ip:port:bucket?access=R5RhikEM91s34QdZWi2y&secret=kqkPcDn4GZNDmjh4I2UxXNSulL5s6FfTUzrn34i5')

ac = adb.Arctic('s3://ip:port:bucket?access=mSbX5lsJr9dESERhScAL&secret=4QIT7zwGGk6WX88jwSqnWl0fsWzGlDsbdqiHX1Jj')
print(ac.list_libraries())

library = ac['crypto_tick_data']
print(library.list_symbols())
from_storage_df = library.read('ETH-USD').data
print(from_storage_df)
