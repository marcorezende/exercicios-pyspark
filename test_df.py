from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .getOrCreate()


def checar(sample, df):
    try:
        empty = sample.subtract(df).isEmpty()
        if empty:
            print("Você acertou!")
        else:
            print("Você errou!")
    except Exception as e:
        print("Você errou!")


def testar(numero, df):
    path = f'/content/exercicios_pyspark/arquivos/exercicios/exercicio{numero}'
    sample = spark.read.parquet(path)
    checar(sample, df)
