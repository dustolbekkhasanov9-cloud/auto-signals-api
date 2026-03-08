# Деплой AutoSignal API на Render

## Файлы в репозитории
В корне проекта должны быть:
- `api.py`
- `signal_engine.py`
- `requirements.txt`
- `render.yaml` *(необязательно, но удобно)*

## Шаг 1. Подготовь `requirements.txt`
Используй такой состав:

```txt
fastapi
uvicorn[standard]
pandas
requests
yfinance
openpyxl
beautifulsoup4
matplotlib
numpy
```

## Шаг 2. Загрузи проект в GitHub
Пример команд:

```bash
git init
git add .
git commit -m "Initial deploy"
git branch -M main
git remote add origin https://github.com/USERNAME/REPO.git
git push -u origin main
```

## Шаг 3. Создай сервис на Render
1. Открой Render.
2. Нажми **New +**.
3. Выбери **Web Service**.
4. Подключи GitHub-репозиторий.
5. Выбери нужный репозиторий.

## Шаг 4. Настройки Render
Если настраиваешь вручную:

- **Environment**: `Python`
- **Build Command**:
  ```bash
  pip install -r requirements.txt
  ```
- **Start Command**:
  ```bash
  uvicorn api:app --host 0.0.0.0 --port $PORT
  ```

## Шаг 5. Дождись деплоя
После деплоя Render даст адрес вида:

```text
https://autosignal-api-xxxx.onrender.com
```

## Шаг 6. Проверь API
Открой в браузере:

```text
https://YOUR-RENDER-URL.onrender.com/
https://YOUR-RENDER-URL.onrender.com/signals
https://YOUR-RENDER-URL.onrender.com/signal?symbol=EURUSD=X
```

Если всё настроено правильно, `/` вернет `{"status": "ok"}`.

## Шаг 7. Подключи iPhone-приложение
В `ContentView.swift` замени:

```swift
let apiBaseURL = "http://172.20.10.4:8000"
```

на:

```swift
let apiBaseURL = "https://YOUR-RENDER-URL.onrender.com"
```

## Важно
- После перехода на `https://` блок `NSAppTransportSecurity / NSAllowsArbitraryLoads` обычно больше не нужен.
- Бесплатный план может «засыпать». Первый запрос после простоя может открываться дольше.
- Если Render выдаст ошибку по зависимостям, проверь, что в репозитории лежит именно обновленный `requirements.txt`.
