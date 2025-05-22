from flask import Flask, send_from_directory
from flask_cors import CORS

from flasgger import Swagger
from routes import register_routes
import os

from core.SingletonLogger import SingletonLogger

logger = SingletonLogger("routes").get_logger()

# Absolute path to the frontend build folder
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
FRONTEND_BUILD_DIR = os.path.abspath(os.path.join(BASE_DIR, "../../frontend/build"))

logger.info(f"Base directory: {BASE_DIR}")
logger.info(f"Frontend build directory: {FRONTEND_BUILD_DIR}")


def create_app():
    app = Flask(
        __name__,
        static_folder=FRONTEND_BUILD_DIR,
        static_url_path="/",  # Serve static assets from root
    )

    Swagger(app)
    register_routes(app)

    @app.route("/", defaults={"path": ""})
    @app.route("/<path:path>")
    def serve_react(path):
        file_path = os.path.join(app.static_folder, path)

        if path != "" and os.path.exists(file_path):
            print(f"Serving file from: {file_path}")
            return send_from_directory(app.static_folder, path)

        else:
            print(f"Serving file from: {app.static_folder}")
            return send_from_directory(app.static_folder, "index.html")

    @app.route("/auth", defaults={"path": ""})
    @app.route("/auth/<path:path>")
    def serve_auth(path):
        return send_from_directory(app.static_folder, "index.html")

    # List your allowed frontends here
    trusted_origins = [
        "https://azri.us",  # production site
        "https://app.azrius.com",  # alternate frontend domain
        "http://localhost:8830",  # dev server
        "http://localhost:8830/auth",
    ]

    CORS(app, origins=trusted_origins)

    return app


if __name__ == "__main__":
    app = create_app()
    app.run(debug=True)
