#!/bin/bash
# Démarre le bon processus Spark selon SPARK_ROLE (défini dans docker-compose.yml)
#   master  -> Spark master (+ JupyterLab si JUPYTER_ENABLE=true)
#   worker  -> Spark worker relié à SPARK_MASTER_URL
#   history -> Spark History Server (lit /tmp/spark-events)

case "$SPARK_ROLE" in

  master)
    if [ "$JUPYTER_ENABLE" = "true" ]; then
      # master en arrière-plan, Jupyter au premier plan (garde le conteneur en vie)
      /opt/spark/sbin/start-master.sh
      exec jupyter lab \
        --ip=0.0.0.0 \
        --port="${JUPYTER_PORT:-8888}" \
        --no-browser \
        --allow-root \
        --ServerApp.token='' \
        --ServerApp.password='' \
        --ServerApp.root_dir=/opt/spark-apps
    else
      exec /opt/spark/bin/spark-class org.apache.spark.deploy.master.Master \
        --host "$SPARK_MASTER_HOST" \
        --port "$SPARK_MASTER_PORT" \
        --webui-port "$SPARK_MASTER_WEBUI_PORT"
    fi
    ;;

  worker)
    # attend que le master réponde avant de s'y connecter
    until nc -z spark-master 7077; do
      echo "Attente du master..."
      sleep 2
    done
    exec /opt/spark/bin/spark-class org.apache.spark.deploy.worker.Worker \
      --cores "$SPARK_WORKER_CORES" \
      --memory "$SPARK_WORKER_MEMORY" \
      --webui-port "$SPARK_WORKER_WEBUI_PORT" \
      "$SPARK_MASTER_URL"
    ;;

  history)
    exec /opt/spark/bin/spark-class org.apache.spark.deploy.history.HistoryServer
    ;;

  *)
    exec "$@"
    ;;
esac
