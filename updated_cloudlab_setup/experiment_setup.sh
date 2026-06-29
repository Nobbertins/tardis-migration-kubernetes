#!/bin/bash
kubectl label node node-0 topology.kubernetes.io/node-name=alpha --overwrite
kubectl label node node-1 topology.kubernetes.io/node-name=beta --overwrite
kubectl label node node-2 topology.kubernetes.io/node-name=gamma --overwrite
python -m pip install aiohttp numpy
helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
helm repo add stable https://charts.helm.sh/stable
helm repo update
kubectl create namespace monitoring
helm install prometheus prometheus-community/kube-prometheus-stack --namespace monitoring
kubectl apply -f rbac.yaml
kubectl apply -f daemonset.yaml
kubectl create namespace orch
kubectl apply -f k8s/orchestrator-deployment.yaml --namespace orch