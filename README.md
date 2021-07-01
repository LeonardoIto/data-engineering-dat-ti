# valor_ganho_de
Valor Ganho - Data Engineering Test

Feito utilizando Jupyter Notebook conectando no Spark (Segui os passos desse artigo da Naomi Fridman: <https://naomi-fridman.medium.com/install-pyspark-to-run-on-jupyter-notebook-on-windows-4ec2009de21f>)
Também foi gerado um .py do código copiando do Jupyter Notebook.

Tomei a liberdade de colocar a tabela em arquivos .csv separados por pipe para facilitar a alteração e inclusão de dados.
Também está com print a cada passo indicando o que o código faz, para auxiliar na leitura :)

--
"""Além do código acima, considere que uma escala de ~200 milhões de transações por dia e que o cálculo
deverá apresentar um resultado do valor total do mês. Descreva em até 500 palavras que tecnologias e arquitetura
você usaria para escalar a solução acima"""

Para escalar a solução, inicialmente indico trabalhar com leitura via banco de dados (Padrão: hive), e não com arquivos.
Desse modo é possível particionar por data e obter o valor ganho por mês com base no particionamento.
Também já está utilizando spark, visando uma alta performance em uma escala de mais de cem milhões de transações diárias.
Por se tratar de um projeto feito para teste, foram incluídos print's mostrando o passo a passo. Em um projeto produtivo, isso não seria feito.
Também por se tratar de um projeto feito para teste, foi utilizada apenas uma classe. Em um projeto produtivo, a arquitetura adotada seria outra, separando melhor as responsabilidades de cada classe.
:)
