#!/bin/bash

path_virtuoso_t=$VIRTUOSO7_t;
path_virtuoso_db=$VIRTUOSO7_db;
system_password="161905";

echo "Cargando servidor"
cd $path_virtuoso_db && (echo -e $system_password | sudo -S $path_virtuoso_t -f);
