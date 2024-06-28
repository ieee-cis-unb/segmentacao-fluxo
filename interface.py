# Crie uma interface no streamlit para receber um arquivo csv
import streamlit as st
import pandas as pd
import time
import asyncio

def interface_upload():
    """
    Cria uma interface para upload de arquivo csv.
    """
    st.title('Pipeline de Processamento e Enriquecimento de Dados')
    st.write('Faça o upload de um arquivo csv com os dados dos clientes.')
    uploaded_file = st.file_uploader("Escolha um arquivo csv", type="csv")

    if uploaded_file is not None:
        data = pd.read_csv(uploaded_file)
        st.write('Upload Realizado com sucesso!')
        return data

async def pipeline_dados(data):
    """
    Simula um pipeline de processamento de dados.
    """
    time.sleep(30)
    return data

def interface_download(updated_file):
    """
    Cria um botão para download do arquivo processado.
    """
    st.write('Clique no botão abaixo para baixar o arquivo processado.')
    with open(updated_file, 'r') as file:
        st.download_button(
            label="Baixar CSV processado",
            data=''.join(file.readlines()),
            file_name="updated_data.csv",
            mime="text/csv"
        )

async def main():
    """
    Função principal que executa a interface e executa o pipeline de dados.
    """
    data = interface_upload()
    btn_process = st.button('Processar')

    if btn_process:
        st.write('Processando...')
        # Inicia a tarefa assíncrona para pipeline_dados
        
        # Tempo estimado para cada 3 linhas 1 minuto
        tempo_inicial = tempo_estimado = (data.shape[0] // 3) * 60
        st.write(f'Tempo estimado para processamento: {(tempo_inicial//3600)} horas e {(tempo_inicial%3600//60)} minutos')
        time_placeholder = st.empty()
        task = asyncio.create_task(pipeline_dados(data))
        while tempo_estimado > 0:
            await asyncio.sleep(1)
            tempo_estimado -= 1
            h, m, s = tempo_estimado // 3600, (tempo_estimado % 3600) // 60, tempo_estimado % 60
            # time_placeholder.write(f'Tempo restante: {h} horas, {m} minutos e {s} segundos')

            # Verifica se a tarefa foi concluída
            if task.done():
                updated_data = task.result()
                break

        # Aguarda a conclusão da tarefa se ainda não estiver concluída
        updated_data = await task

        st.write('Processamento concluído!')
        updated_data.to_csv('data_updated.csv', index=False)

        st.write(' ')

        interface_download(updated_file='data_updated.csv')

if __name__ == '__main__':
    st.set_page_config(layout='wide', page_title='Segmentação de Clientes', page_icon=':bar_chart:', initial_sidebar_state='auto')
    asyncio.run(main())