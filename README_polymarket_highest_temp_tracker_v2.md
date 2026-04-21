# Polymarket Highest Temperature tracker v2

Este script está pensado para la idea que describiste:

- revisar **solo** mercados cuyo título empiece con `Highest temperature in`
- detectar **mercados nuevos**
- guardar solo la **fecha de evento más nueva** (si aparece 24 de abril, borra 23 y anteriores)
- guardar precios **YES y NO explícitos** (`midpoint / best bid / best ask`)
- correr cada **5 minutos** y guardar cómo se mueve desde que apareció
- cortar seguimiento automáticamente cuando pasa el día del evento (por defecto +1 día)
- cuando aparece un evento nuevo, elegir una **apuesta teórica inicial** según el forecast:
  - primero intenta leer el forecast de la propia página de Polymarket
  - si no lo puede extraer, usa **Open-Meteo** como fallback
- marcar la posición teórica del forecast:
  - `YES` en el outcome que coincide con la máxima pronosticada
  - `NO` en todos los demás outcomes del mismo evento
  - siempre medir salida con el **bid** de ese lado (`YES bid` o `NO bid`)

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

Ver precios de salida por evento (sin evolución), con `SI/NO` por outcome y forecast:

```bash
python polymarket_highest_temp_tracker_v2.py report-launch --db polymarket_highest_temp.db --limit-events 3
```

Exportar todos los snapshots:

```bash
python polymarket_highest_temp_tracker_v2.py export-csv --db polymarket_highest_temp.db --out snapshots.csv
```

Exportar picks teóricos:

```bash
python polymarket_highest_temp_tracker_v2.py export-picks-csv --db polymarket_highest_temp.db --out picks.csv
```

Exportar posiciones teóricas por market (`YES` al target del forecast, `NO` al resto):

```bash
python polymarket_highest_temp_tracker_v2.py export-forecast-positions-csv --db polymarket_highest_temp.db --out forecast_positions.csv
```

## Cron

Si prefieres cron en vez de loop interno, para correr entre 04:00 y 10:55 cada 5 minutos:

```cron
*/5 4-10 * * * /usr/bin/python3 /ruta/polymarket_highest_temp_tracker_v2.py run-once --db /ruta/polymarket_highest_temp.db >> /ruta/polymarket_highest_temp.log 2>&1
```

Si quieres mantener tracking mas tiempo despues del dia del evento:

```bash
python polymarket_highest_temp_tracker_v2.py --stop-tracking-days-after-event 2 run-once --db polymarket_highest_temp.db
```

## Qué guarda

### events
Un evento por mercado principal de Polymarket.

### markets
Cada outcome binario del evento (`33°C`, `32°C`, `31°C or below`, etc.).

### snapshots
Una fila por mercado y por timestamp capturado, con `YES` y `NO` explícitos.

### forecast_picks
La apuesta teórica inicial por evento nuevo:
- método del forecast (`polymarket_page_summary` u `open_meteo`)
- valor de máxima usado
- outcome elegido
- ask de entrada
- bid actual
- mejor bid visto
- PnL bruto teórico

### forecast_positions
Una fila por market para la estrategia teórica del forecast:
- `YES` en el market objetivo
- `NO` en todos los demás
- entrada al `ask` de ese lado
- salida al mejor `bid` visto de ese lado

## Importante

- El precio de “entrada” es el **primer ask observado por tu tracker**, no necesariamente el primer trade histórico real.
- El PnL calculado es **bruto**. No descuenta fees, slippage ni el hecho de que a veces el tamaño disponible al bid/ask puede ser chico.
- El forecast extraído de la página de Polymarket es **informativo**. La resolución de estos mercados suele apoyarse en la fuente histórica indicada en Rules.
