# Polymarket Highest Temperature tracker v2

Este script está pensado para la idea que describiste:

- revisar **solo** mercados cuyo título empiece con `Highest temperature in`
- correr alineado cada **5 minutos** en los minutos **1, 6, 11, 16, ...**
- guardar precios **YES midpoint / best bid / best ask**
- detectar **mercados nuevos**
- cuando aparece un evento nuevo, elegir una **apuesta teórica inicial** según el forecast:
  - primero intenta leer el forecast de la propia página de Polymarket
  - si no lo puede extraer, usa **Open-Meteo** como fallback
- buscar el outcome que coincide con esa máxima pronosticada y medir:
  - precio de entrada (ask)
  - bid actual
  - mejor bid visto
  - si habría habido ganancia bruta al vender en el interín

## Instalación

```bash
pip install requests
```

## Ejemplos

Una sola corrida:

```bash
python polymarket_highest_temp_tracker_v2.py run-once --db polymarket_highest_temp.db
```

Loop alineado a `1,6,11,16,...`:

```bash
python polymarket_highest_temp_tracker_v2.py watch-aligned --interval-minutes 5 --start-minute 1 --db polymarket_highest_temp.db
```

Ver picks teóricos guardados:

```bash
python polymarket_highest_temp_tracker_v2.py report-picks --db polymarket_highest_temp.db
```

Exportar todos los snapshots:

```bash
python polymarket_highest_temp_tracker_v2.py export-csv --db polymarket_highest_temp.db --out snapshots.csv
```

Exportar picks teóricos:

```bash
python polymarket_highest_temp_tracker_v2.py export-picks-csv --db polymarket_highest_temp.db --out picks.csv
```

## Cron

Si prefieres cron en vez de loop interno, esta expresión corre exactamente en:

`1,6,11,16,...,56`

```cron
1-59/5 * * * * /usr/bin/python3 /ruta/polymarket_highest_temp_tracker_v2.py run-once --db /ruta/polymarket_highest_temp.db >> /ruta/polymarket_highest_temp.log 2>&1
```

## Qué guarda

### events
Un evento por mercado principal de Polymarket.

### markets
Cada outcome binario del evento (`33°C`, `32°C`, `31°C or below`, etc.).

### snapshots
Una fila por mercado y por timestamp capturado.

### forecast_picks
La apuesta teórica inicial por evento nuevo:
- método del forecast (`polymarket_page_summary` u `open_meteo`)
- valor de máxima usado
- outcome elegido
- ask de entrada
- bid actual
- mejor bid visto
- PnL bruto teórico

## Importante

- El precio de “entrada” es el **primer ask observado por tu tracker**, no necesariamente el primer trade histórico real.
- El PnL calculado es **bruto**. No descuenta fees, slippage ni el hecho de que a veces el tamaño disponible al bid/ask puede ser chico.
- El forecast extraído de la página de Polymarket es **informativo**. La resolución de estos mercados suele apoyarse en la fuente histórica indicada en Rules.
