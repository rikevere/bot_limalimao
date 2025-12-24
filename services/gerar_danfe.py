# pip install brazilfiscalreport

from typing import Optional
from io import BytesIO
from brazilfiscalreport.danfe import (
    Danfe,
    DanfeConfig,
    DecimalConfig,
    FontType,
    InvoiceDisplay,
    Margins,
    ReceiptPosition,
    TaxConfiguration,
)


def gerar_danfe(xml) -> None:
    """
    Gera um arquivo PDF de DANFE a partir do conteúdo XML em texto.

    :param xml:   Conteúdo XML da NF-e em formato de string (não precisa ser arquivo).
    :param logo:  Caminho para o arquivo de logo (PNG/JPG). Se None, gera sem logo.
    :param output: Caminho completo do arquivo PDF de saída (ex.: 'saida/danfe.pdf').
    """

    # Cria o objeto de configuração do DANFE com alguns padrões “sensatos”.
    config = DanfeConfig(
        # 📄 MARGENS DO PDF (em milímetros)
        # Ajuste se precisar de mais/menos espaço em impressoras específicas.
        margins=Margins(
            top=10,    # margem superior
            right=10,  # margem direita
            bottom=10, # margem inferior
            left=10,   # margem esquerda
        ),

        # 🧾 POSIÇÃO DO CANHOTO (RECIBO)
        # TOP    -> canhoto no topo da página
        # BOTTOM -> canhoto na parte de baixo
        # (LEFT é usado internamente em layout paisagem)
        receipt_pos=ReceiptPosition.TOP,

        # 🔢 CASAS DECIMAIS
        # price_precision    -> casas decimais para preços
        # quantity_precision -> casas decimais para quantidades
        # 2 casas costuma ser o mais comum em nota de venda.
        decimal_config=DecimalConfig(
            price_precision=2,
            quantity_precision=2,
        ),

        # ⚖️ EXIBIÇÃO DE TRIBUTOS (a biblioteca ainda não implementa tudo)
        # STANDARD_ICMS_IPI -> padrão ICMS + IPI
        # ICMS_ST_ONLY      -> foco em ICMS ST
        # WITHOUT_IPI       -> oculta IPI
        tax_configuration=TaxConfiguration.STANDARD_ICMS_IPI,

        # 📑 DETALHES DA FATURA (DUPLICATAS)
        # DUPLICATES_ONLY -> mostra apenas duplicatas
        # FULL_DETAILS    -> mostra todos os detalhes de fatura
        invoice_display=InvoiceDisplay.FULL_DETAILS,

        # ✍️ TIPO DE FONTE
        # COURIER -> monoespaçada
        # TIMES   -> serifada (mais “oficial”)
        font_type=FontType.TIMES,
    )

    # 🖼 LOGO (opcional)
    # Se o caminho do logo foi informado, atribui na configuração.
    # A lib aceita também bytes/BytesIO, mas aqui usamos apenas o caminho.
    config.logo = 'C:/BotCop/coopervere/services/LogoLima.png'  

    # 💰 EXIBIR PIS/COFINS NOS TOTAIS
    # True  -> mostra PIS e COFINS
    # False -> não mostra
    config.display_pis_cofins = True

    # 🧾 DESCRIÇÃO DE PRODUTOS – OPCIONAIS
    # Mostra a “filial” / ramificação do produto na descrição (se existir).
    config.display_branch = True
    # Prefixo antes da informação de filial (apenas estético).
    config.branch_info_prefix = "=> "
    # Mostra informações adicionais do item (infAdProd etc.).
    config.display_additional_info = True
    # Mostra dados ANVISA em produtos que tiverem essa info.
    config.display_anvisa = True
    # Mostra dados ANP em itens de combustíveis.
    config.display_anp = True

    # 💧 MARCA D’ÁGUA PARA CANCELADAS / SEM PROTOCOLO
    # False -> usa “SEM VALOR FISCAL” quando não tiver protNFe.
    # True  -> em NF cancelada, exibe watermark “CANCELADA”,
    #         e também trata casos sem protNFe.
    config.watermark_cancelled = False

    # Cria o objeto DANFE a partir do XML em string + configuração definida acima
     # Instancia o gerador de DANFE
    danfe = Danfe(xml=xml, config=config)

    # Buffer para armazenar o PDF em memória
    buffer = BytesIO()

    # A biblioteca só aceita nome de arquivo, mas BytesIO tem atributo .name
    buffer.name = "danfe.pdf"

    # Gera o PDF dentro do buffer
    danfe.output(buffer)

    # Retorna os bytes do PDF
    return buffer.getvalue()


