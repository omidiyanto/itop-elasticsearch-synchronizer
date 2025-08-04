#!/bin/bash
docker build -t registry.satnusa.com/developer/itop-elasticsearch-synchronizer:latest .
docker push registry.satnusa.com/developer/itop-elasticsearch-synchronizer:latest