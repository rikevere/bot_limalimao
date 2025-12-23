# coopervere/scripts/run_notifier.py

import os
import time
from datetime import datetime, date, time as dt_time
import sys, pathlib
sys.path.append(str(pathlib.Path(__file__).resolve().parents[1]))

from dotenv import load_dotenv

# Serviços existentes
#from services.notifier_service import processar_pedidos_pendentes
#from services.danfe_service import processar_notas_pendentes
from services.aniversario_service import processar_aniversariantes

# Novo serviço semanal
#from services.pagar_service import processar_contas_pagar

from services.festividades_service import processar_festividades

from services.state_manager import (
    load_ultima_execucao_semana,
    save_ultima_execucao_semana,
)


load_dotenv()


def should_run_weekly(hoje: date, agora: datetime, ultima_execucao: date) -> bool:
    """
    Regras:
    - Executa SOMENTE no dia da semana configurado (0=segunda).
    - Executa SOMENTE se horário atual >= horário configurado no .env.
    - Executa SOMENTE se não rodou ainda no mesmo dia.
    """

    # Lê configs do .env
    dia_config = int(os.getenv("PAY_REPORT_DAY_OF_WEEK", "0"))  # padrão segunda
    hora_config = int(os.getenv("PAY_REPORT_HOUR", "8"))        # padrão 08h
    min_config  = int(os.getenv("PAY_REPORT_MINUTE", "0"))      # padrão 00m

    # Verifica o dia da semana
    if hoje.weekday() != dia_config:
        return False

    # Evita executar mais de uma vez no dia
    if ultima_execucao == hoje:
        return False

    # Verifica horário mínimo
    if agora.time() < dt_time(hora_config, min_config):
        return False

    return True

def should_run_festividade(agora: datetime) -> str | None:
    """Retorna 'natal' ou 'ano_novo' se estiver no dia e horário válidos."""
    if agora.month != 12:
        return None

    if agora.hour < 22:
        return None

    if agora.day == 24:
        return "natal"
    if agora.day == 31:
        return "ano_novo"

    return None


def main():

    interval_min = int(os.getenv("NOTIFY_INTERVAL_MINUTES", "30"))
    interval_sec = max(60, interval_min * 60)

    print(f"[Notifier] Iniciado. Intervalo: {interval_min} min")

    ultima_execucao_semana = load_ultima_execucao_semana()   # armazenamos apenas o dia

    while True:
        agora = datetime.now()
        hoje = agora.date()

        # --------------------------------------------------------
        # 1) PROCESSA PEDIDOS PENDENTES (EXECUÇÃO NORMAL)
        # --------------------------------------------------------
        #try:
        #    res = processar_pedidos_pendentes()
        #    print(f"[Notifier {agora}] Pedidos pendentes => {res}")
        #except Exception as e:
        #    print(f"[Notifier {agora}][ERRO Pedidos] {e}")

        # --------------------------------------------------------
        # 2) PROCESSA NOTAS EMITIDAS (EXECUÇÃO NORMAL)
        # --------------------------------------------------------
        #try:
        #    res_nf = processar_notas_pendentes()
        #    print(f"[Notifier {agora}] NF pendentes => {res_nf}")
        #except Exception as e:
        #    print(f"[Notifier {agora}][ERRO Notas] {e}")

        # --------------------------------------------------------
        # 2.1) PROCESSA ANIVERSARIANTES DO DIA (EXECUÇÃO NORMAL)
        # --------------------------------------------------------
        try:
            res_bday = processar_aniversariantes()
            print(f"[Notifier {agora}] Aniversariantes => {res_bday}")
        except Exception as e:
            print(f"[Notifier {agora}][ERRO Aniversariantes] {e}")

        # --------------------------------------------------------
        # 2.2) PROCESSA FESTIVIDADES (NATAL / ANO NOVO)
        # --------------------------------------------------------
        try:
            #data_texto = "2025-12-24 22:20:47"
            #agora1 = datetime.strptime(data_texto, "%Y-%m-%d %H:%M:%S")
            #tipo_festividade = should_run_festividade(agora1)
            tipo_festividade = should_run_festividade(agora)
            if tipo_festividade:
                #hoje1 = date.fromisoformat('2025-12-24')
                res_fest = processar_festividades(tipo=tipo_festividade, data_referencia=hoje)
                print(f"[Notifier {agora}] Festividades ({tipo_festividade}) => {res_fest}")
        except Exception as e:
            print(f"[Notifier {agora}][ERRO Festividades] {e}")

        # --------------------------------------------------------
        # 3) PROCESSO SEMANAL — CONTAS A PAGAR
        # --------------------------------------------------------
        #try:
        #    if should_run_weekly(hoje, agora, ultima_execucao_semana):
        #        print(f"[Notifier {agora}] Executando relatório semanal de contas a pagar...")

        #        res2 = processar_contas_pagar()
                # ... após enviar o relatório com sucesso:
        #        hoje = date.today()
        #        save_ultima_execucao_semana(hoje)
        #        ultima_execucao_semana = hoje

        #        print(f"[Notifier {agora}] Relatório semanal enviado => {res2}")


        #except Exception as e:
        #    print(f"[Notifier {agora}][ERRO Relatório semanal] {e}")

        # --------------------------------------------------------
        # 3) AGUARDA PRÓXIMO CICLO
        # --------------------------------------------------------
        time.sleep(interval_sec)


if __name__ == "__main__":
    main()
