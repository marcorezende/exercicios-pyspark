from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .getOrCreate()


class VerificaoExercicio:

    @staticmethod
    def checar(sample, df):
        try:
            empty = sample.subtract(df).isEmpty()
            if empty:
                print("Você acertou!")
            else:
                print("Você errou!")
        except Exception as e:
            print("Você errou!")

    def exercicio(self, numero, df):
        path = f'./arquivos/exercicios/exercicio{numero}'
        sample = spark.read.parquet(path)
        self.checar(sample, df)


