from flask import Flask
from config import FLASK_HOST, FLASK_PORT, FLASK_DEBUG, validate_config
from routes import register_routes

validate_config()
app = Flask(__name__)
register_routes(app)

if __name__ == "__main__":
    app.run(host=FLASK_HOST, port=FLASK_PORT, debug=FLASK_DEBUG)
