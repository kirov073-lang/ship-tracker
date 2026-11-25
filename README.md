# Мой флот

Личный трекер судов: поиск по названию, IMO и MMSI, свой список судов с заметками и карта с позициями по AIS.

- `site/` — сайт (GitHub Pages).
- `scripts/collector.py` — сборщик: каждые 30 минут слушает бесплатный поток [aisstream.io](https://aisstream.io) и пополняет базу судов.
- `.github/workflows/update.yml` — запуск сборщика и публикация сайта.

Данные не хранятся в git: каждая публикация берёт прошлую базу с живого сайта и дополняет её.

## Настройка

1. Settings → Pages → Source: **GitHub Actions**.
2. Settings → Secrets and variables → Actions → New repository secret: имя `AISSTREAM_KEY`, значение — ключ с aisstream.io.
3. Actions → «Обновить данные AIS и сайт» → Run workflow.

Ограничение бесплатного AIS: только береговые станции. В открытом океане судно показывается по последней известной позиции.

Проверка сборщика без интернета: `python test/mock_stream.py 15 &` и затем `AIS_URL=ws://127.0.0.1:8765 AISSTREAM_KEY=test-key LISTEN_SECONDS=4 python scripts/collector.py`.
