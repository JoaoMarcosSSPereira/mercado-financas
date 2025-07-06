#!/usr/bin/env python3
# atualiza_sheets.py
"""
Pipeline de coleta de dados financeiros e upload para Google Sheets.
Rodar 100% na nuvem (GitHub Actions), autenticado via Service Account.
"""

import time
import pandas as pd
import yfinance as yf
import gspread
from gspread_dataframe import set_with_dataframe
from google.oauth2.service_account import Credentials

# ===============================
# 1. Autenticação Google Sheets
# ===============================

SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive'
]

# credentials.json será criado pelo GitHub Actions a partir do secret
creds = Credentials.from_service_account_file('credentials.json', scopes=SCOPES)
gc = gspread.authorize(creds)

# ===============================
# 2. Parâmetros de Configuração
# ===============================

# Você pode adicionar/remover tickers nessa lista
TICKERS    = ['BOVA11.SA', 'BTC-USD']
PERIODO    = '1d'   # ex: '1d', '5d', '1mo', '1y'
INTERVALO  = '1h'   # ex: '1h', '1d', '5m'
SHEET_ID   = '1D8wlSpiPqu-7F0bvMzLo6EPWH6SfxOzO2E7KlvmX9Pw'
ABA        = 'Sheet1'
MAX_TRIES  = 3     # tentativas para fatiamento intermitente

# Campos extras do info() do yfinance
EXTRA_COLS = [
    'sector', 'industry', 'longName', 'country', 'currency',
    'marketCap', 'dividendYield', 'symbol', 'shortName'
]

# ===============================
# 3. Coleta e Tratamento de Dados
# ===============================

all_df = []

for ticker in TICKERS:
    tries = 0
    while tries < MAX_TRIES:
        try:
            # download do OHLCV
            df = yf.download(ticker,
                             period=PERIODO,
                             interval=INTERVALO,
                             progress=False)
            # se não veio dado, aborta o loop
            if df.empty:
                print(f"⚠️ Nenhum dado retornado para {ticker}")
                break

            # flatten multiindex (caso haja)
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = [col[-1] for col in df.columns]

            df.reset_index(inplace=True)
            df.rename(columns={
                'Datetime': 'date_time',
                'Date':     'date_time',
                'Open':     'open',
                'High':     'high',
                'Low':      'low',
                'Close':    'close',
                'Volume':   'volume'
            }, inplace=True)

            # normalize date_time para string 'YYYY-MM-DD HH:MM'
            df['date_time'] = pd.to_datetime(df['date_time']).dt.strftime('%Y-%m-%d %H:%M')

            # adiciona colunas fixas
            df['Ticker'] = ticker.replace('.SA', '')

            # busca info fundamentalista
            try:
                info = yf.Ticker(ticker).info
            except Exception as e:
                print(f"⚠️ Erro ao obter info({ticker}): {e}")
                info = {}

            for col in EXTRA_COLS:
                df[col] = info.get(col, None)

            # converte marketCap para bilhões (float)
            if 'marketCap' in df.columns:
                df['marketCap'] = df['marketCap'].apply(
                    lambda x: round(x/1e9, 2) if pd.notnull(x) else None
                )

            # seleciona colunas na ordem desejada
            base_cols = ['Ticker', 'date_time', 'open', 'high', 'low', 'close', 'volume']
            cols = [c for c in base_cols + EXTRA_COLS if c in df.columns]
            df = df[cols]

            all_df.append(df)
            break  # sucesso, sai do while
        except Exception as e:
            tries += 1
            print(f"⚠️ Erro ao baixar {ticker} (tentativa {tries}/{MAX_TRIES}): {e}")
            time.sleep(2)
    else:
        print(f"❌ Falha ao baixar {ticker} após {MAX_TRIES} tentativas")

# concatena tudo
if not all_df:
    print("❌ Nenhum dado foi coletado para nenhum ticker. Abortando.")
    exit(1)

df_final = pd.concat(all_df, ignore_index=True)

# ===============================
# 4. Upload para Google Sheets
# ===============================

try:
    sh = gc.open_by_key(SHEET_ID)
    ws = sh.worksheet(ABA)
    ws.clear()
    set_with_dataframe(ws, df_final)
    print(f"✅ {len(df_final)} linhas atualizadas em '{ABA}' (Sheet ID: {SHEET_ID})")
except Exception as e:
    print(f"❌ Erro ao atualizar Google Sheets: {e}")
    exit(1)
