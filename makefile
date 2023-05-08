build:
   cd C:\Users\sujee\pydev\overflowwithpyspark\
    mkdir ./target
    cp src/init.py ./target
    cd .src/
    zip -r ../target/src.zip .
    cd ../target
    spark-submit --master local --py-files src.zip init.py