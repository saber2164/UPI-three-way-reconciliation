FROM python:3.11-slim
RUN pip install --no-cache-dir streamlit==1.37.0 psycopg2-binary==2.9.9 pandas==2.2.2 plotly==5.23.0
COPY app.py /app/app.py
CMD ["streamlit", "run", "/app/app.py", "--server.port=8501", "--server.address=0.0.0.0"]
