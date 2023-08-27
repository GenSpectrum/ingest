FROM condaforge/mambaforge
WORKDIR /app

COPY environment.yml .
RUN mamba env create -f environment.yml


ENTRYPOINT ["./entrypoint.sh"]
