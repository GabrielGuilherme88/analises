import json
import argparse
import subprocess
import boto3
import time
import pandas as pd
from pyathena import connect
import pandas.io.sql as sqlio
import sys
from ydata_profiling import ProfileReport
import numpy as np
from botocore import UNSIGNED
from botocore.config import Config
import boto3.session
from botocore import exceptions

class CustomException(Exception):
    pass

json_manifest_dbt = "target/manifest.json"
athena_bucket = "todos-athena-us-east-1"
athena_tmp_folder = f"s3://{athena_bucket}/"

def execute_athena_sql(query):
    client = boto3.client('athena', region_name='us-east-1')
    queryStart = client.start_query_execution(
    QueryString = query,
    ResultConfiguration = { 'OutputLocation': athena_tmp_folder})
    queryExecution = client.get_query_execution(QueryExecutionId=queryStart['QueryExecutionId'])
    while queryExecution['QueryExecution']['Status']['State'] in ('RUNNING', 'QUEUED'):
        time.sleep(5)
        queryExecution = client.get_query_execution(QueryExecutionId=queryStart['QueryExecutionId'])
        
def execute_athena_query(query):
    cursor = connect(s3_staging_dir=athena_tmp_folder,
                    region_name="us-east-1").cursor()
    cursor.execute(query)
    colls=','.join(str(f"{e[0]}") for e in cursor.description)
    results=pd.DataFrame(list(cursor), columns=colls.split(","))
    return results

#Query dentro da PDGT
fl_especialidades = 'select * from pdgt_sandbox_gabrielguilherme.fl_especialidades'
fl_fornecedores ='select * from pdgt_sandbox_gabrielguilherme.fl_fornecedores'
fl_profissionais = 'select fp.id_profissional, fp.nm_profissional, fp.id_conselho, fp.nro_conselho, fp.cpf, fp.genero, fp.id_unidade, fp.unidade, fp.email1 , fp.email2, fp.telefone1, fp.telefone2, fp.celular1, fp.celular2, fp.dt_atualizacao, fp.dt_criacao, fp.status_cadastro from pdgt_sandbox_gabrielguilherme.fl_profissionais fp'
fl_regionais = 'select * from pdgt_sandbox_gabrielguilherme.fl_regionais'
fl_unidades = 'select * from pdgt_sandbox_gabrielguilherme.fl_unidades'
fl_agendamentos = 'select a.id_agendamento, a.id_regional, a.regional, a.id_unidade, a.unidade,  a.id_paciente, a.nm_paciente, a.cpf, a.celular, a.email, a.sexo, a.estado,a.cidade ,a.bairro ,a.logradouro ,a.numero ,a.id_status ,a.nm_status ,a.id_profissional ,a.nm_profissional ,a.id_especialidade ,a.nm_especialidade ,a.id_procedimento ,a.nm_procedimento ,a.id_tipoprocedimento ,a.id_grupoprocedimento ,a.id_tabela ,a.nm_tabela ,a.valor  from pdgt_sandbox_gabrielguilherme.fl_agendamentos a limit 10'
fl_canais = 'select * from pdgt_sandbox_gabrielguilherme.fl_canais'
fl_indicadores = 'select * from pdgt_sandbox_gabrielguilherme.fl_indicadores limit 100000000'
fl_pacientes = '''select * from todos_data_lake_trusted_feegow.pacientes p
where sysdate between date('2023-01-01') and date ('2023-12-31')
and p.sys_active = 1'''

df = execute_athena_query(fl_pacientes)


#import sweetviz as sv

#report = sv.analyze(df)

# Salvar o relatório em HTML
#report.show_html("data_profile_report.html")

profile = seu_dataframe.profile_report()

profile.to_file("data_profile_report.html")