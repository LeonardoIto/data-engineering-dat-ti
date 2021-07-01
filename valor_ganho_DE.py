# -*- coding: utf-8 -*-

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, DoubleType, IntegerType, BooleanType, StringType


def init_spark():
  return SparkSession.builder.appName("GanhoTotal").getOrCreate()


def massa_teste_transacoes():
  schema = StructType([ 
    StructField("transaction_id", IntegerType(), True),
    StructField("client_id", IntegerType(), True), 
    StructField("total_amount", DoubleType(), True),
    StructField("discount_percentage", DoubleType(), True)])
  df_arquivo = spark.read.csv("massa_teste_transacoes.csv", header=False, nullValue='null', schema=schema, sep='|')
  return df_arquivo


def massa_teste_contratos():
  schema = StructType([ 
    StructField("contract_id", IntegerType(), True),
    StructField("client_id", IntegerType(), True), 
    StructField("client_name", StringType(), True),
    StructField("percentage", DoubleType(), True),
    StructField("is_active", BooleanType(), True)])
  df_arquivo = spark.read.csv("massa_teste_contratos.csv", header=False, nullValue='null', schema=schema, sep='|')
  return df_arquivo


def main():
  spark = init_spark()
  # Olá! Tomei a liberdade de colocar a tabela em arquivos .csv separados por pipe para facilitar a alteração e inclusão de dados.

  print('Leitura de dados')
  df_transacoes = massa_teste_transacoes()
  df_contratos = massa_teste_contratos()

  print('Limpeza de dados')
  df_transacoes = df_transacoes.na.fill(0)
    
  print('Obtendo apenas contratos ativos')
  df_contratos_ativos = df_contratos.filter(df_contratos.is_active == True)

  print('Obtendo valor líquido de cada transação')
  df_transacoes_valor_liquido = df_transacoes.withColumn(
      'valor_liquido', df_transacoes['total_amount'] - 
      (df_transacoes['total_amount'] * df_transacoes['discount_percentage'] / 100.0)
  )
    
  print('Juntando dados de transações e contratos')
  df_final = df_contratos_ativos.join(df_transacoes_valor_liquido, 'client_id')
    
  print('Obtendo valor ganho com cada transação de acordo com o percentual no contrato ativo')
  df_final = df_final.withColumn('valor_ganho_pelo_contrato', df_final['valor_liquido'] * df_final['percentage'] / 100.0 )
    
  print('Obtendo somatória do valor ganho com todas as transações de acordo com o percentual no contrato ativo')
  resultado = df_final.groupBy().agg(F.sum(df_final.valor_ganho_pelo_contrato)).collect()[0][0]

  print('Valor Ganho: ' + str(resultado)) # PRINT utilizado para mostrar o resultado.
  print('Valor Ganho (Formatado): ' + format(resultado, '.3f')) # PRINT utilizado para mostrar o resultado conforme indicado no pdf do teste. :D


if __name__ == '__main__':
  main()