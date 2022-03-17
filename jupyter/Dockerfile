FROM jupyter/scipy-notebook

# Provide password-less sudo to NB_USER
USER root
RUN \
    sed -i /etc/sudoers -re 's/^%sudo.*/%sudo ALL=(ALL:ALL) NOPASSWD: ALL/g' && \
    sed -i /etc/sudoers -re 's/^root.*/root ALL=(ALL:ALL) NOPASSWD: ALL/g' && \
    sed -i /etc/sudoers -re 's/^#includedir.*/## **Removed the include directive** ##"/g' && \
    echo "${NB_USER} ALL=(ALL) NOPASSWD: ALL" >> /etc/sudoers && \  
    chmod g+w /etc/passwd

# Installing extra packages to Debian.
# gcc and g++ are necessary for mooncake and
# libpq-dev is necessary for psycopg2.
USER $NB_UID
RUN sudo apt-get update && \
	sudo apt-get -y install gcc && \
	sudo apt-get -y install g++ && \
    sudo apt-get -y install libpq-dev

# Installing minio client
# RUN \
#     wget https://dl.min.io/client/mc/release/linux-amd64/mc && \
#     chmod +x mcc

# Adding Python deps
# For instructions for pip install in jupyter child Docker images see:
# https://jupyter-docker-stacks.readthedocs.io/en/latest/using/recipes.html
# COPY --chown=${NB_UID}:${NB_GID} requirements.txt /tmp/
COPY --chown=${NB_UID}:${NB_GID} /mooncake /opt/mooncake
RUN pip install --user /opt/mooncake/. && \
    pip install --user psycopg2 && \
    pip install --user mlflow && \
    pip install --user boto3 && \
    pip install --user minio && \
    pip install --user pyarrow && \
    pip install --user fastparquet && \
    pip install --user s3fs && \
    pip install --user fastapi && \
    pip install --user "uvicorn[standard]" && \
    fix-permissions "${CONDA_DIR}" && \
    fix-permissions "/home/${NB_USER}"    