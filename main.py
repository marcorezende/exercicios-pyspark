import os

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Exemplo de Configuração do Delta Lake") \
    .getOrCreate()

#INTRODUCAO
#ARQUITETURA
#CONCEITOS
#TRANSFORMACOES
#ACTIONS
#FUNCOES SPARK SQL
#UDFS
#CONCEITOS NA PRÁTICA
#AQE E DPP
#ETL COMPLETO COM SPARK
#PROBLEMAS REAIS
#STRUCTED STREAMING
#SPARK ML


##1
#HELLO WORLD SPARK
#SCHEMAS
#PRINTSCHEMA
#COMANDO SHOW

##2
#LENDO ARQUIVOS

##2
#COMANDO SELECT

##3
#COMANDO DROP

##4
#COMANDO RENOMEAR COLUNA

##5
#CRIAR NOVA COLUNA


##6
#FILTRAR COLUNA (ALIAS WHERE)

##7
#ORDERNAR COLUNA

##8
#UNIR POR POSICAO

##9
#UNIR POR NOME

##10
#SUBTRAIR

##11
#DISTINCT, DROP DUPLICATE

##12
#AGG

##13
#GROUP BY

##14
#JOIN


#ACTIONS
#SHOW
#COUNT
#COLLECT
#TAKE
#TAIL
#isEmpty

#FUNCOES
#CAST
#SPLIT
#CONCAT
#CONCAT_WS
#EXPLODE
#CASE WHEN
#HIGH ORDER
#WINDOW FUNCTIONS

#UDFs
#python ufds
#pandas udfs

#CONCEITOS NA PRATICA
#SPARK UI
#lazy evaluation
#joins broadcast, explain
#cache vs persist
#numero de particoes
#coalesce e repartition
#leitura de pastas, jsons, txt, multiline
#transformacao para pandas, apache arrow


#Exercicio 1 - Ler WorldCupPlayers.csv CSV COM HEADER
# df = spark.read.option('header', True).csv('./arquivos/WorldCupPlayers.csv')
# df.write.format('parquet').save('./arquivos/exercicios/exercicio1')

#Exercicio 2 - Selecionar apenas a colunar 'Player Name' do arquivo WorldCupPlayers.csv
# df = spark.read.option('header', True).csv('./arquivos/WorldCupPlayers.csv')
# df = df.select('Player Name')
# df.write.format('parquet').save('./arquivos/exercicios/exercicio2')

#Exercicio 3 - Selecionar a coluna 'Player Name' e 'Team Initials' do arquivo WorldCupPlayers.csv
# df = spark.read.option('header', True).csv('./arquivos/WorldCupPlayers.csv')
# df = df.select('Player Name', 'Team Initials')
# df.write.format('parquet').save('./arquivos/exercicios/exercicio3')

#Exercicio 4 - Renomear coluna 'Team Initials' para 'Inicial Time' do arquivo WorldCupPlayers.csv
# df = spark.read.option('header', True).csv('./arquivos/WorldCupPlayers.csv')
# df = df.withColumnRenamed('Team Initials', 'Inicial Time')
# df.write.format('parquet').save('./arquivos/exercicios/exercicio4')

#Exercicio 5 - Dropar Coluna 'Shirt Number' do arquivo WorldCupPlayers.csv
# df = spark.read.option('header', True).csv('./arquivos/WorldCupPlayers.csv')
# df = df.drop('Shirt Number')
# df.write.format('parquet').save('./arquivos/exercicios/exercicio5')

#Exercicio 6 - Filtrar a coluna 'Team Initials' para vir somente 'BRA' (Brasil) do arquivo WorldCupPlayers.csv
# df = spark.read.option('header', True).csv('./arquivos/WorldCupPlayers.csv')
# df = df.filter(col('Team Initials') == 'BRA')
# df.write.format('parquet').save('./arquivos/exercicios/exercicio6')

#Exercicio 7 - Criar nova Coluna 'Gols' com o valor fixo de 10 no arquivo WorldCupPlayers.csv
# df = spark.read.option('header', True).csv('./arquivos/WorldCupPlayers.csv')
# df = df.withColumn('Gols', lit(10))
# df.write.format('parquet').save('./arquivos/exercicios/exercicio7')