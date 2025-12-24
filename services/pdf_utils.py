# services/pdf_utils.py
from io import BytesIO
from datetime import datetime, date
import base64
import os

# pip install reportlab
from reportlab.lib.pagesizes import A4
from reportlab.pdfgen import canvas
from reportlab.lib.units import mm
from reportlab.lib.colors import black
from reportlab.lib.utils import simpleSplit


# ---------------------------
# Helpers de formatação
# ---------------------------
def fmt_moeda(v) -> str:
    """Formata número como moeda pt-BR (1.234,56)."""
    try:
        s = f"{float(v):,.2f}"
        return s.replace(",", "X").replace(".", ",").replace("X", ".")
    except Exception:
        return str(v)


def fmt_qtd(v) -> str:
    """Formata quantidade com duas casas no padrão pt-BR."""
    try:
        s = f"{float(v):,.2f}"
        return s.replace(",", "X").replace(".", ",").replace("X", ".")
    except Exception:
        return str(v)
    



def fmt_data(dt) -> str:
    """
    Formata para DD/MM/YYYY.
    Aceita: datetime, date, ou string ISO (ex.: 2025-05-27, 2025-05-27 14:30:00, 2025-05-27T14:30:00, com/sem milissegundos).
    """
    if dt is None:
        return "-"

    # Objetos nativos
    if isinstance(dt, (datetime, date)):
        try:
            return dt.strftime("%d/%m/%Y")
        except Exception:
            return str(dt)

    # Strings
    if isinstance(dt, str):
        s = dt.strip()
        # Normaliza T -> espaço
        s = s.replace("T", " ")
        # Remove fração de segundo, se houver (ex.: ".0000", ".123456")
        if "." in s:
            s = s.split(".", 1)[0]

        # Tenta vários formatos comuns
        patterns = [
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%d",              # só data
            "%Y/%m/%d %H:%M:%S",
            "%Y/%m/%d",
        ]
        for p in patterns:
            try:
                dtp = datetime.strptime(s, p)
                return dtp.strftime("%d/%m/%Y")
            except ValueError:
                pass

        # Último recurso: tenta fromisoformat
        try:
            dtp = datetime.fromisoformat(dt.replace("T", " "))
            return dtp.strftime("%d/%m/%Y")
        except Exception:
            return dt  # mantém como veio, se for formato inesperado

    return str(dt)


# ---------------------------
# Geração do PDF
# ---------------------------
def build_pedido_pdf(dados: dict) -> tuple[str, str]:
    """
    Gera um PDF (em memória) com cabeçalho do pedido e tabela de itens.
    Retorna (file_name, base64_pdf).
    Adequações:
      1) Quantidade com 2 casas no padrão pt-BR
      2) Mais espaço para Vlr Un. e Vlr; menos para Descrição
      3) Remoção de Status, Situação e Entr/Saída do cabeçalho
      4) "Valor total do pedido" em negrito
      5) Logo à direita no cabeçalho (30x20 mm recomendados)
    """
    if not dados:
        raise ValueError("Sem dados para gerar PDF")

    header = dados["header"]
    items = dados.get("items", [])

    buf = BytesIO()
    c = canvas.Canvas(buf, pagesize=A4)
    width, height = A4

    # Margens e coordenadas iniciais
    M = 15 * mm
    x0 = M
    y = height - M

    def draw_line(y_pos: float):
        c.setStrokeColor(black)
        c.line(M, y_pos, width - M, y_pos)

    # ---------------------------
    # Cabeçalho com título + logo
    # ---------------------------
    c.setFont("Helvetica-Bold", 14)
    c.drawString(x0, y, "CooperVerê - Novo Pedido Faturado")

    # Logo (opcional) no canto superior direito
    # Recomendação: 30x20 mm (~300x200 px), PNG com fundo transparente.
    LOGO_W, LOGO_H = 30 * mm, 20 * mm
    logo_path = os.path.join(os.path.dirname(__file__), "LogoLima.png")
    if os.path.exists(logo_path):
        c.drawImage(
            logo_path,
            width - M - LOGO_W,
            y - (LOGO_H - 4 * mm),  # sobe um pouco para alinhar com o título
            width=LOGO_W,
            height=LOGO_H,
            preserveAspectRatio=True,
            mask="auto",
        )

    y -= 10 * mm

    # ---------------------------
    # Bloco de informações (sem Status/Situação/EntrSaída)
    # ---------------------------
    c.setFont("Helvetica", 10)
    info_linhas = [
        f"Número: {header.get('NUMERO','-')}   Estab: {header.get('ESTAB','-')}",
        f"Emissão: {fmt_data(header.get('DTEMISSAO'))}   "
        f"Validade: {fmt_data(header.get('DTVALIDADE'))}   "
        f"Previsão: {fmt_data(header.get('DTPREVISAO'))}",
        f"Cliente: {header.get('NOME','-')}",
        f"Endereço: {header.get('ENDERECO_COMP','-')}",
    ]

    for linha in info_linhas:
        parts = simpleSplit(linha, "Helvetica", 10, width - 2 * M)
        for p in parts:
            c.drawString(x0, y, p)
            y -= 6 * mm
        y -= 2 * mm

    # Total em negrito
    c.setFont("Helvetica-Bold", 11)
    c.drawString(
        x0,
        y,
        f"Valor total do pedido: R$ {fmt_moeda(header.get('VALOR_TOTAL_PEDIDO'))}",
    )
    y -= 8 * mm
    c.setFont("Helvetica", 10)

    # Linha separadora
    draw_line(y)
    y -= 6 * mm

    # ---------------------------
    # Definição das colunas
    # ---------------------------
    # Larguras (mm): Seq, Descrição, Qtde, Unid., Vlr Un., Vlr
    # Ajustadas para dar mais espaço aos valores e reduzir a descrição.
    widths_mm = [15, 70, 20, 20, 25, 25]
    widths = [w * mm for w in widths_mm]

    # Bordas direitas (para alinhamento à direita)
    col_right = []
    acc = x0
    for w in widths:
        acc += w
        col_right.append(acc)

    # Posições iniciais de cada coluna (esquerda)
    col_left = [x0]
    acc = x0
    for i, w in enumerate(widths[:-1]):
        acc += w
        col_left.append(acc)

    # ---------------------------
    # Cabeçalho da tabela
    # ---------------------------
    c.setFont("Helvetica-Bold", 10)
    c.drawString(col_left[0], y, "Seq")
    c.drawString(col_left[1], y, "Descrição (Marca)")
    c.drawRightString(col_right[2], y, "Qtde")
    c.drawRightString(col_right[3], y, "Unid.")
    c.drawRightString(col_right[4], y, "Vlr Un.")
    c.drawRightString(col_right[5], y, "Vlr Total")
    y -= 6 * mm
    draw_line(y)
    y -= 4 * mm
    c.setFont("Helvetica", 10)

    def maybe_new_page():
        nonlocal y
        if y < 25 * mm:
            c.showPage()
            # Título de continuação
            y = height - M
            c.setFont("Helvetica-Bold", 12)
            c.drawString(x0, y, "Pedido (itens) — continuação")
            y -= 10 * mm
            c.setFont("Helvetica", 10)

            # Reimprimir cabeçalho da tabela na nova página
            c.setFont("Helvetica-Bold", 10)
            c.drawString(col_left[0], y, "Seq")
            c.drawString(col_left[1], y, "Descrição (Marca)")
            c.drawRightString(col_right[2], y, "Qtde")
            c.drawRightString(col_right[3], y, "Unid.")
            c.drawRightString(col_right[4], y, "Vlr Un.")
            c.drawRightString(col_right[5], y, "Vlr Total")
            y -= 6 * mm
            draw_line(y)
            y -= 4 * mm
            c.setFont("Helvetica", 10)

    # ---------------------------
    # Linhas da tabela
    # ---------------------------
    for it in items:
        maybe_new_page()

        # Descrição (quebra automática)
        desc = it.get("ITEMDESCRICAO", "-")
        marca = it.get("MARCA")
        if marca:
            desc = f"{desc} ({marca})"

        # Largura efetiva da coluna de descrição
        desc_width = widths[1]
        desc_lines = simpleSplit(desc, "Helvetica", 10, desc_width)

        # Valores formatados
        seq = str(it.get("SEQPEDITE", ""))
        qt = fmt_qtd(it.get("QUANTIDADE", ""))
        un = str(it.get("UNIDADE", "") or "")
        vlu = fmt_moeda(it.get("VALORUNITARIO"))
        vl = fmt_moeda(it.get("VALOR"))

        # Primeira linha
        c.drawString(col_left[0], y, seq)
        c.drawString(col_left[1], y, desc_lines[0])
        c.drawRightString(col_right[2], y, qt)
        c.drawRightString(col_right[3], y, un)
        c.drawRightString(col_right[4], y, vlu)
        c.drawRightString(col_right[5], y, vl)
        y -= 6 * mm

        # Linhas extras da descrição
        for extra in desc_lines[1:]:
            maybe_new_page()
            c.drawString(col_left[1], y, extra)
            y -= 6 * mm

        y -= 2 * mm

    # Finaliza
    c.showPage()
    c.save()

    pdf_bytes = buf.getvalue()
    buf.close()

    b64 = base64.b64encode(pdf_bytes).decode("utf-8")
    file_name = f"Pedido_{header.get('NUMERO','s-n')}.pdf"
    return file_name, b64
