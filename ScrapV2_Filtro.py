from flask import Flask
import threading
import time
import requests
from bs4 import BeautifulSoup
import pandas as pd
import re
import os

def obtener_emergencias(distritos_filtrados=None):
    """
    Scrapea emergencias y solo retorna las que correspondan a los distritos definidos en 'distritos_filtrados'.
    Si distritos_filtrados es None o [], retorna todas.
    """
    url = "https://sgonorte.bomberosperu.gob.pe/24horas"
    response = requests.get(url)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, "html.parser")

    tabla = soup.find("table", class_="table")
    headers = [
        "#", "Nro Parte", "Fecha y hora", "Dirección limpia", "Latitud", "Longitud", "Distrito",
        "Tipo", "Estado", "Máquinas", "Ver Mapa"
    ]
    rows = []
    for tr in tabla.find("tbody").find_all("tr"):
        cols = tr.find_all(["th", "td"])
        fila = []
        for i, td in enumerate(cols):
            if i == 3:  # Dirección/Distrito
                if td.find("canvas"):
                    direccion = "No disponible"
                    lat = lon = distrito = ""
                else:
                    texto = td.text.strip()
                    # Regex para extraer coordenadas entre paréntesis al final
                    match = re.search(r'\(([-\d\.]+),\s*([-\d\.]+)\)', texto)
                    if match:
                        lat, lon = match.group(1), match.group(2)
                        # Quita coordenadas y espacios
                        direccion_sin_coord = re.sub(r'\s*\([-\d\.,\s]+\)\s*', '', texto).strip()
                    else:
                        lat = lon = ""
                        direccion_sin_coord = texto

                    # Extraer distrito: después de "Nro." y "-" o después del último "-"
                    distrito = ""
                    match_distrito = re.search(r'Nro\.\s*-+\s*(.+)', direccion_sin_coord)
                    if match_distrito:
                        distrito = match_distrito.group(1).strip()
                        direccion_limpia = re.sub(r'Nro\.\s*-+\s*.+$', '', direccion_sin_coord).strip()
                    else:
                        partes = direccion_sin_coord.rsplit('-', 1)
                        if len(partes) == 2:
                            direccion_limpia = partes[0].strip()
                            distrito = partes[1].strip()
                        else:
                            direccion_limpia = direccion_sin_coord.strip()
                            distrito = ""

                    fila.append(direccion_limpia)
                    fila.append(lat)
                    fila.append(lon)
                    fila.append(distrito)
                    continue  # Ya agregamos campos extra
            elif i == 4:
                span = td.find("span")
                fila.append(span.text.strip() if span else td.text.strip())
            else:
                span = td.find("span")
                if span:
                    fila.append(span.text.strip())
                else:
                    fila.append(td.text.strip())
        rows.append(fila)

    df = pd.DataFrame(rows, columns=headers)

    # ===== FILTRO DE DISTRITOS =====
    if distritos_filtrados:
        # Normaliza mayúsculas/minúsculas y espacios
        distritos_filtrados_norm = [d.strip().upper() for d in distritos_filtrados]
        df = df[df["Distrito"].str.strip().str.upper().isin(distritos_filtrados_norm)]

    return df

# Configura tu bot y chat de Telegram
BOT_TOKEN = os.environ.get("BOT_TOKEN")
CHAT_ID = os.environ.get("CHAT_ID")

def enviar_alerta_telegram(row):
    mensaje = (
        f"🚨 <b>Emergencia detectada</b>\n"
        f"<b>{row['Tipo']}</b> en <b>{row['Dirección limpia']}</b> ({row['Distrito']})\n"
        f"Fecha: <b>{row['Fecha y hora']}</b>\n"
        f"<a href='https://maps.google.com/?q={row['Latitud']},{row['Longitud']}'>Ver en Google Maps</a>"
    )
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": CHAT_ID,
        "text": mensaje,
        "parse_mode": "HTML"
    }
    response = requests.post(url, data=payload)
    if response.status_code == 200:
        print(f"✅ Alerta enviada a Telegram: {row['Tipo']} en {row['Distrito']} ({row['Dirección limpia']})")
    else:
        print(f"❌ Error al enviar alerta: {response.text}")

def cargar_ultimas_partes(path="notificados.txt"):
    try:
        with open(path, "r") as f:
            return set(line.strip() for line in f)
    except FileNotFoundError:
        return set()

def guardar_ultimas_partes(ultimas_partes, path="notificados.txt"):
    with open(path, "w") as f:
        for parte in ultimas_partes:
            f.write(parte + "\n")

def observar_emergencias_bg(frecuencia=300, distritos_filtrados=None):
    ultimas_partes = cargar_ultimas_partes()  # Carga historial al iniciar
    while True:
        try:
            df = obtener_emergencias(distritos_filtrados=distritos_filtrados)
            nuevos = []
            for idx, row in df.iterrows():
                nro_parte = row["Nro Parte"]
                if nro_parte not in ultimas_partes:
                    nuevos.append(row)
                    ultimas_partes.add(nro_parte)
            if nuevos:
                for fila in nuevos:
                    enviar_alerta_telegram(fila)
                guardar_ultimas_partes(ultimas_partes)  # Guarda historial actualizado
            time.sleep(frecuencia)
        except Exception as e:
            print("Error en observador:", e)
            time.sleep(60)

# 4. Tu aplicación Flask
app = Flask(__name__)

@app.route('/')
def home():
    return 'App web + bot observador funcionando'

if __name__ == "__main__":
    mis_distritos = ["JESUS MARIA", "BREÑA", "PUEBLO LIBRE", "LIMA"]  # aquí defines tus distritos de interés
    t = threading.Thread(target=observar_emergencias_bg, args=(300, mis_distritos), daemon=True)
    t.start()
    port = int(os.environ.get("PORT", 5000))  # Render asigna el puerto en la variable de entorno PORT
    app.run(host="0.0.0.0", port=port, debug=True, use_reloader=False)




