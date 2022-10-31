FROM jupyter/scipy-notebook:python-3.9.5

# Provide password-less sudo to NB_USER
USER root
RUN \
    sed -i /etc/sudoers -re 's/^%sudo.*/%sudo ALL=(ALL:ALL) NOPASSWD: ALL/g' && \
    sed -i /etc/sudoers -re 's/^root.*/root ALL=(ALL:ALL) NOPASSWD: ALL/g' && \
    sed -i /etc/sudoers -re 's/^#includedir.*/## **Removed the include directive** ##"/g' && \
    echo "${NB_USER} ALL=(ALL) NOPASSWD: ALL" >> /etc/sudoers && \  
    chmod g+w /etc/passwd

# Installing extra packages to Debian.
# gcc and g++ are necessary for skorch_forecasting and
# libpq-dev is necessary for psycopg2.
USER $NB_UID
RUN sudo apt-get update && \
    sudo apt-get -y install gcc && \
    sudo apt-get -y install g++ && \
    sudo apt-get -y install libpq-dev
ENV PATH="/home/${NB_USER}/.local/bin:${PATH}"

# Adding Python deps
# For instructions for pip install in jupyter child Docker images see:
# https://jupyter-docker-stacks.readthedocs.io/en/latest/using/recipes.html
# COPY --chown=${NB_UID}:${NB_GID} requirements.txt /tmp/
COPY --chown=${NB_UID}:${NB_GID} /skorch_forecasting /opt/skorch_forecasting
RUN pip install --user --default-timeout=900 /opt/skorch_forecasting/. && \
    pip install --user celery>=5.2 && \
    pip install --user boto3==1.24.59 && \
    pip install --user cloudpickle==2.1.0 && \
    pip install --user ipython==8.4.0 && \
    pip install --user psycopg2 && \
    pip install --user mlflow==1.25.1 && \
    pip install --user minio>=7.1.5 && \
    pip install --user pyarrow>=7.0.0 && \
    pip install --user fastparquet>=0.8.0 && \
    pip install --user s3fs>=2022.2.0 && \
    pip install --user fastapi>=0.68.0 && \
    pip install --user "uvicorn[standard]" && \
    pip install --user awswrangler>=2.16.1 && \
    fix-permissions "${CONDA_DIR}" && \
    fix-permissions "/home/${NB_USER}"    
