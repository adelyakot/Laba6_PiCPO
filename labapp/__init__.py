from flask import Flask

# Регистрируем приложение Flask
app = Flask(__name__)

# Подключаем маршруты (адреса страниц)
from labapp import routes
