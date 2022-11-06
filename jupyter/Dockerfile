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

# Adding Python deps
COPY --chown=${NB_UID}:${NB_GID} . /home/${NB_USER}/
RUN pip install --no-cache-dir --requirement requirements.txt && \
    fix-permissions "${CONDA_DIR}" && \
    fix-permissions "/home/${NB_USER}"